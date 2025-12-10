/*
 * Copyright 2025 ThatScalaGuy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.nats.client

import cats.effect.{Async, Deferred, Ref, Resource, Temporal}
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import com.comcast.ip4s.{SocketAddress}
import fs2.{Chunk, Stream}
import fs2.io.net.Network
import fs2.io.net.tls.TLSContext
import fs2.nats.errors.NatsError
import fs2.nats.protocol.{Connect, Info, NatsFrame, ParserConfig}
import fs2.nats.publish.SerializationUtils
import fs2.nats.transport.{NatsSocket, TlsTransport, Transport, TransportConfig}
import io.circe.syntax.*
import java.time.Instant

/** State of the NATS connection.
  *
  * @param serverInfo
  *   The latest server INFO
  * @param connectedAt
  *   When the connection was established
  * @param reconnectAttempt
  *   Current reconnection attempt (0 if connected)
  */
final case class ConnectionState(
    serverInfo: Info,
    connectedAt: Instant,
    reconnectAttempt: Int
)

/** Manages the NATS connection lifecycle including reconnection.
  *
  * Responsibilities:
  *   - Establish initial connection (wait for INFO, send CONNECT)
  *   - Handle reconnection with exponential backoff and jitter
  *   - Reapply active subscriptions after reconnect
  *   - Update connection state atomically
  *   - Emit connection events
  *
  * @tparam F
  *   The effect type
  */
trait ConnectionManager[F[_]]:

  /** Get the current transport.
    */
  def transport: F[Transport[F]]

  /** Get the current connection state.
    */
  def state: F[ConnectionState]

  /** Send bytes through the current transport.
    */
  def send(bytes: Chunk[Byte]): F[Unit]

  /** Stream of incoming NATS frames from the server.
    */
  def frames: Stream[F, NatsFrame]

  /** Stream of connection events.
    */
  def events: Stream[F, ClientEvent]

  /** Trigger a reconnection attempt.
    *
    * @param reason
    *   The reason for reconnection
    */
  def reconnect(reason: String): F[Unit]

  /** Close the connection manager and all resources.
    */
  def close: F[Unit]

  /** Register a callback for resubscription after reconnect.
    *
    * @param resubscribe
    *   Function to call after reconnection
    */
  def onReconnect(resubscribe: Info => F[Unit]): F[Unit]

  /** Update the max_payload from a new INFO message.
    */
  def updateServerInfo(info: Info): F[Unit]

object ConnectionManager:

  /** Create a connection manager and establish the initial connection.
    *
    * @param config
    *   Client configuration
    * @param transportConfig
    *   Transport configuration
    * @param parserConfig
    *   Parser configuration
    * @param tlsContext
    *   Optional TLS context for secure connections
    * @tparam F
    *   The effect type
    * @return
    *   Resource managing the ConnectionManager lifecycle
    */
  def connect[F[_]: Async: Network](
      config: ClientConfig,
      transportConfig: TransportConfig = TransportConfig.default,
      parserConfig: ParserConfig = ParserConfig.default,
      tlsContext: Option[TLSContext[F]] = None
  ): Resource[F, ConnectionManager[F]] =
    for
      eventQueue <- Resource.eval(Queue.unbounded[F, ClientEvent])
      stateRef <- Resource.eval(Ref.of[F, Option[ConnectionState]](None))
      transportRef <- Resource.eval(Ref.of[F, Option[Transport[F]]](None))
      closedRef <- Resource.eval(Ref.of[F, Boolean](false))
      resubscribeRef <- Resource.eval(Ref.of[F, Option[Info => F[Unit]]](None))
      supervisor <- Supervisor[F]

      manager = new ConnectionManagerImpl[F](
        config,
        transportConfig,
        parserConfig,
        tlsContext,
        eventQueue,
        stateRef,
        transportRef,
        closedRef,
        resubscribeRef,
        supervisor
      )

      _ <- Resource.make(manager.initialize)(_ => manager.close)
    yield manager

  private class ConnectionManagerImpl[F[_]: Async: Network](
      config: ClientConfig,
      transportConfig: TransportConfig,
      parserConfig: ParserConfig,
      tlsContext: Option[TLSContext[F]],
      eventQueue: Queue[F, ClientEvent],
      stateRef: Ref[F, Option[ConnectionState]],
      transportRef: Ref[F, Option[Transport[F]]],
      closedRef: Ref[F, Boolean],
      resubscribeRef: Ref[F, Option[Info => F[Unit]]],
      supervisor: Supervisor[F]
  ) extends ConnectionManager[F]:

    private val backoffPolicy = Backoff.fromConfig(config.backoff)

    def initialize: F[Unit] =
      establishConnection().flatMap { case (transport, info) =>
        val now = Instant.now()
        val connState = ConnectionState(info, now, 0)
        stateRef.set(Some(connState)) *>
          transportRef.set(Some(transport)) *>
          eventQueue.offer(ClientEvent.Connected(info))
      }

    override def transport: F[Transport[F]] =
      transportRef.get.flatMap {
        case Some(t) => Async[F].pure(t)
        case None    => Async[F].raiseError(NatsError.ClientClosed)
      }

    override def state: F[ConnectionState] =
      stateRef.get.flatMap {
        case Some(s) => Async[F].pure(s)
        case None    => Async[F].raiseError(NatsError.ClientClosed)
      }

    override def send(bytes: Chunk[Byte]): F[Unit] =
      transport.flatMap(_.send(bytes))

    override def frames: Stream[F, NatsFrame] =
      Stream.eval(transport).flatMap(_.frames)

    override def events: Stream[F, ClientEvent] =
      Stream.fromQueueUnterminated(eventQueue)

    override def reconnect(reason: String): F[Unit] =
      closedRef.get.flatMap { closed =>
        if closed then Async[F].unit
        else
          eventQueue.offer(
            ClientEvent.Disconnected(reason, willReconnect = true)
          ) *>
            reconnectLoop(1)
      }

    override def close: F[Unit] =
      closedRef.set(true) *>
        transportRef.get.flatMap {
          case Some(t) => t.close
          case None    => Async[F].unit
        } *>
        transportRef.set(None) *>
        stateRef.set(None)

    override def onReconnect(resubscribe: Info => F[Unit]): F[Unit] =
      resubscribeRef.set(Some(resubscribe))

    override def updateServerInfo(info: Info): F[Unit] =
      stateRef.update {
        case Some(s) => Some(s.copy(serverInfo = info))
        case None    => None
      } *> eventQueue.offer(ClientEvent.ServerInfoUpdated(info))

    private def reconnectLoop(attempt: Int): F[Unit] =
      closedRef.get.flatMap { closed =>
        if closed then Async[F].unit
        else
          backoffPolicy.delay(attempt) match
            case None =>
              val error = NatsError.MaxReconnectsExceeded(attempt - 1, None)
              eventQueue.offer(
                ClientEvent.MaxReconnectsExceeded(attempt - 1, error.getMessage)
              ) *> Async[F].raiseError(error)

            case Some(delay) =>
              eventQueue.offer(
                ClientEvent.Reconnecting(attempt, delay.toMillis)
              ) *>
                Async[F].sleep(delay) *>
                establishConnection().attempt.flatMap {
                  case Right((newTransport, info)) =>
                    val now = Instant.now()
                    val connState = ConnectionState(info, now, 0)

                    transportRef.set(Some(newTransport)) *>
                      stateRef.set(Some(connState)) *>
                      resubscribeRef.get.flatMap {
                        case Some(resub) => resub(info)
                        case None        => Async[F].unit
                      } *>
                      eventQueue.offer(ClientEvent.Reconnected(info, attempt))

                  case Left(_) =>
                    reconnectLoop(attempt + 1)
                }
      }

    private def establishConnection(): F[(Transport[F], Info)] =
      val address = SocketAddress(config.host, config.port)

      val transportResource: Resource[F, Transport[F]] =
        if config.useTls then
          tlsContext match
            case Some(ctx) =>
              for
                socket <- Network[F].client(address)
                params = config.tlsParams.getOrElse(
                  fs2.io.net.tls.TLSParameters.Default
                )
                transport <- TlsTransport.wrap(
                  ctx,
                  socket,
                  params,
                  transportConfig,
                  parserConfig
                )
              yield transport
            case None =>
              Resource.eval(
                Async[F].raiseError[Transport[F]](
                  NatsError.TlsError("TLS requested but no TLSContext provided")
                )
              )
        else NatsSocket.resource(address, transportConfig, parserConfig)

      transportResource.allocated.flatMap { case (transport, release) =>
        waitForInfoAndConnect(transport, release)
      }

    private def waitForInfoAndConnect(
        transport: Transport[F],
        releaseTransport: F[Unit]
    ): F[(Transport[F], Info)] =
      val infoDeferred = Deferred.unsafe[F, Either[Throwable, Info]]

      val infoReader = transport.frames
        .collectFirst { case NatsFrame.InfoFrame(info) => info }
        .compile
        .last
        .flatMap {
          case Some(info) => infoDeferred.complete(Right(info))
          case None       =>
            infoDeferred.complete(
              Left(NatsError.ConnectionFailed("No INFO received from server"))
            )
        }
        .handleErrorWith(err => infoDeferred.complete(Left(err)).as(true))

      val timeout = Temporal[F].sleep(transportConfig.connectTimeout) *>
        infoDeferred.complete(
          Left(
            NatsError.Timeout(
              "Waiting for server INFO",
              transportConfig.connectTimeout.toMillis
            )
          )
        )

      Async[F]
        .race(
          supervisor.supervise(infoReader) *> infoDeferred.get,
          timeout
        )
        .flatMap {
          case Left(Left(err)) =>
            releaseTransport *> Async[F].raiseError(err)

          case Left(Right(info)) =>
            sendConnect(transport, info).as((transport, info))

          case Right(_) =>
            releaseTransport *>
              Async[F].raiseError(
                NatsError.Timeout(
                  "Waiting for server INFO",
                  transportConfig.connectTimeout.toMillis
                )
              )
        }

    private def sendConnect(transport: Transport[F], info: Info): F[Unit] =
      val connectMsg = buildConnectMessage(info)
      val connectJson = connectMsg.asJson.noSpaces
      val connectBytes = SerializationUtils.buildConnect(connectJson)

      transport.send(connectBytes) *>
        transport.send(SerializationUtils.buildPing)

    private def buildConnectMessage(info: Info): Connect =
      val base = Connect(
        verbose = config.verbose,
        pedantic = config.pedantic,
        tlsRequired = config.useTls,
        name = config.name,
        echo = config.echo,
        headersSupported = info.headersSupported
      )

      config.credentials match
        case Some(NatsCredentials.UserPassword(user, pass)) =>
          base.copy(user = Some(user), pass = Some(pass))
        case Some(NatsCredentials.Token(token)) =>
          base.copy(authToken = Some(token))
        case Some(NatsCredentials.NKey(nkey, jwt)) =>
          base.copy(nkey = Some(nkey), jwt = jwt)
        case None =>
          base
