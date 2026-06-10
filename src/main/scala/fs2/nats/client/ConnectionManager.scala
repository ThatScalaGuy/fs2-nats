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
import cats.effect.syntax.all.*
import cats.syntax.all.*
import scala.concurrent.duration.*
import fs2.{Chunk, Stream}
import fs2.io.net.{Network, Socket}
import fs2.io.net.tls.TLSContext
import fs2.nats.auth.NKey
import fs2.nats.errors.NatsError
import fs2.nats.protocol.{
  Connect,
  Frame,
  Info,
  MsgBuilder,
  NatsFrame,
  ParserConfig,
  ProtocolParser
}
import fs2.nats.publish.SerializationUtils
import fs2.nats.transport.{
  NatsSocket,
  Outgoing,
  TlsTransport,
  Transport,
  TransportConfig
}
import com.github.plokhotnyuk.jsoniter_scala.core.*
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

/** The send-side phase of the connection. Held in a single `Ref` so that the
  * decision "send now vs buffer" and the reconnect transition "flush buffer and
  * go online" are atomic with respect to each other.
  *
  *   - [[ConnPhase.Online]]: connected; writes go straight to the transport.
  *   - [[ConnPhase.Offline]]: disconnected/reconnecting; writes accumulate in a
  *     bounded buffer to be replayed on reconnect.
  *   - [[ConnPhase.Closed]]: the client is closed; writes fail.
  */
private[client] sealed trait ConnPhase[F[_]]
private[client] object ConnPhase:
  final case class Online[F[_]](transport: Transport[F]) extends ConnPhase[F]
  final case class Offline[F[_]](pending: Vector[Chunk[Byte]], bytes: Long)
      extends ConnPhase[F]
  final case class Closed[F[_]]() extends ConnPhase[F]

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

  /** Send an [[Outgoing]] write descriptor through the current transport. On
    * the Online fast path the descriptor reaches the writer without allocating
    * a combined frame array; while Offline it is materialized into a `Chunk`
    * and held in the reconnect buffer like any other pending write.
    */
  def sendOutgoing(out: Outgoing): F[Unit]

  /** Stream of incoming protocol elements from the server (control `NatsFrame`s
    * and already-built `NatsMessage` data frames; see [[Frame]]).
    */
  def frames: Stream[F, Frame]

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
      msgBuilder: MsgBuilder[Frame],
      transportConfig: TransportConfig = TransportConfig.default,
      parserConfig: ParserConfig = ParserConfig.default,
      tlsContext: Option[TLSContext[F]] = None
  ): Resource[F, ConnectionManager[F]] =
    for
      eventQueue <- Resource.eval(Queue.unbounded[F, ClientEvent])
      stateRef <- Resource.eval(Ref.of[F, Option[ConnectionState]](None))
      connRef <- Resource.eval(
        Ref.of[F, ConnPhase[F]](ConnPhase.Offline(Vector.empty, 0L))
      )
      closedRef <- Resource.eval(Ref.of[F, Boolean](false))
      resubscribeRef <- Resource.eval(Ref.of[F, Option[Info => F[Unit]]](None))
      poolRef <- Resource.eval(
        Async[F].delay(new scala.util.Random()).flatMap { rng =>
          Ref.of[F, ServerPool](
            ServerPool.fromSeeds(
              config.seedServers,
              randomize = !config.noRandomize,
              rng
            )
          )
        }
      )
      connectedRef <- Resource.eval(Ref.of[F, Option[ServerAddress]](None))
      supervisor <- Supervisor[F]

      manager = new ConnectionManagerImpl[F](
        config,
        msgBuilder,
        transportConfig,
        parserConfig,
        tlsContext,
        eventQueue,
        stateRef,
        connRef,
        closedRef,
        resubscribeRef,
        poolRef,
        connectedRef,
        supervisor
      )

      _ <- Resource.make(manager.initialize)(_ => manager.close)
    yield manager

  private class ConnectionManagerImpl[F[_]: Async: Network](
      config: ClientConfig,
      msgBuilder: MsgBuilder[Frame],
      transportConfig: TransportConfig,
      parserConfig: ParserConfig,
      tlsContext: Option[TLSContext[F]],
      eventQueue: Queue[F, ClientEvent],
      stateRef: Ref[F, Option[ConnectionState]],
      connRef: Ref[F, ConnPhase[F]],
      closedRef: Ref[F, Boolean],
      resubscribeRef: Ref[F, Option[Info => F[Unit]]],
      poolRef: Ref[F, ServerPool],
      connectedRef: Ref[F, Option[ServerAddress]],
      supervisor: Supervisor[F]
  ) extends ConnectionManager[F]:

    private val backoffPolicy = Backoff.fromConfig(config.backoff)

    def initialize: F[Unit] =
      poolRef.get.map(_.candidates.size).flatMap(bootstrap(_, None))

    /** Sweep the seed pool once at startup, trying each server until one
      * accepts. If no seed is reachable, surface the last connection error (so
      * a config error such as a missing TLSContext is reported as-is).
      */
    private def bootstrap(
        remaining: Int,
        lastError: Option[Throwable]
    ): F[Unit] =
      if remaining <= 0 then
        Async[F].raiseError(
          lastError.getOrElse(
            NatsError.ConnectionFailed("could not connect to any seed server")
          )
        )
      else
        poolRef.modify(_.next).flatMap { case (server, _) =>
          establishConnection(server).attempt.flatMap {
            case Right((transport, info)) =>
              goOnline(transport, info, server)
            case Left(err) =>
              bootstrap(remaining - 1, Some(err))
          }
        }

    /** Bring the initial connection online: swap to Online, record state +
      * discovery, emit `Connected`, then flush any buffered writes (empty at
      * startup). The install is masked; the (pollable) flush stays
      * interruptible.
      */
    private def goOnline(
        transport: Transport[F],
        info: Info,
        server: ServerAddress
    ): F[Unit] =
      Async[F].uncancelable { poll =>
        swapOnline(transport).flatMap { pending =>
          recordConnection(info, server) *>
            eventQueue.offer(ClientEvent.Connected(info)) *>
            poll(pending.traverse_(transport.send))
        }
      }

    /** Atomically swap the phase to Online and return any writes buffered while
      * disconnected (empty on the initial connect).
      */
    private def swapOnline(transport: Transport[F]): F[Vector[Chunk[Byte]]] =
      connRef.modify {
        case ConnPhase.Offline(pending, _) =>
          (ConnPhase.Online(transport), pending)
        case _ =>
          (ConnPhase.Online(transport), Vector.empty[Chunk[Byte]])
      }

    /** Record connection state and fold the server's `connect_urls` into the
      * pool (discovery). Keyed off the address we actually dialed, not
      * `info.host` (which the server often reports as `0.0.0.0`). Discovered
      * peers inherit the dialed server's TLS scheme (gossip carries none).
      */
    private def recordConnection(info: Info, server: ServerAddress): F[Unit] =
      stateRef.set(Some(ConnectionState(info, Instant.now(), 0))) *>
        connectedRef.set(Some(server)) *>
        poolRef.update(
          _.merge(
            parseConnectUrls(info).map(_.copy(useTls = server.useTls)),
            server
          )
        )

    private def parseConnectUrls(info: Info): List[ServerAddress] =
      info.connectUrls.getOrElse(Nil).flatMap(ServerAddress.fromHostPort)

    override def transport: F[Transport[F]] =
      connRef.get.flatMap {
        case ConnPhase.Online(t)     => Async[F].pure(t)
        case ConnPhase.Offline(_, _) =>
          Async[F].raiseError(NatsError.ConnectionFailed("not connected"))
        case ConnPhase.Closed() => Async[F].raiseError(NatsError.ClientClosed)
      }

    override def state: F[ConnectionState] =
      stateRef.get.flatMap {
        case Some(s) => Async[F].pure(s)
        case None    => Async[F].raiseError(NatsError.ClientClosed)
      }

    override def send(bytes: Chunk[Byte]): F[Unit] =
      sendOutgoing(Outgoing.Raw(bytes))

    override def sendOutgoing(out: Outgoing): F[Unit] =
      // Fast path: `connRef.get` is a single volatile read (AtomicReference.get)
      // with no allocation. When Online — the overwhelmingly common case — hand
      // the descriptor straight to the transport, skipping the `modify` CAS and
      // its per-publish Either+Some+tuple allocation. `connRef` stays the single
      // source of truth; a transition racing this read is the same best-effort
      // window the old `modify`-then-`send` had. Offline/Closed fall through to
      // the authoritative `modify` path, which also re-checks Online in case a
      // reconnect raced us.
      connRef.get.flatMap {
        case ConnPhase.Online(t) => t.sendOutgoing(out)
        case _                   => sendBuffered(out)
      }

    /** Slow path for the non-Online phases: atomically buffer (Offline,
      * materializing the descriptor into a self-contained `Chunk` so the
      * reconnect buffer holds a plain value), reject (Closed), or — if a
      * reconnect transitioned us back Online between the `connRef.get` above
      * and here — send directly. `connRef.modify` is the authoritative arbiter
      * so concurrent sends never lose a buffered write.
      */
    private def sendBuffered(out: Outgoing): F[Unit] =
      connRef
        .modify[Either[NatsError, Option[Transport[F]]]] {
          case online @ ConnPhase.Online(t) =>
            (online, Right(Some(t)))
          case ConnPhase.Closed() =>
            (ConnPhase.Closed(), Left(NatsError.ClientClosed))
          case off @ ConnPhase.Offline(buf, sz) =>
            val bytes = out.materialize
            val ns = sz + bytes.size
            if config.reconnectBufferSize <= 0 || ns > config.reconnectBufferSize
            then
              (
                off,
                Left(
                  NatsError.ReconnectBufferExceeded(config.reconnectBufferSize)
                )
              )
            else (ConnPhase.Offline(buf :+ bytes, ns), Right(None))
        }
        .flatMap {
          case Right(Some(t)) => t.sendOutgoing(out)
          case Right(None)    => Async[F].unit
          case Left(err)      => Async[F].raiseError(err)
        }

    override def frames: Stream[F, Frame] =
      Stream.eval(transport).flatMap(_.frames)

    override def events: Stream[F, ClientEvent] =
      Stream.fromQueueUnterminated(eventQueue)

    override def reconnect(reason: String): F[Unit] =
      closedRef.get.flatMap { closed =>
        if closed then Async[F].unit
        else
          // Enter the buffering phase so writes issued during the outage are
          // queued (up to reconnectBufferSize) instead of hitting a dead socket.
          connRef.update {
            case ConnPhase.Online(_) => ConnPhase.Offline(Vector.empty, 0L)
            case other               => other
          } *>
            eventQueue.offer(
              ClientEvent.Disconnected(reason, willReconnect = true)
            ) *>
            reconnectLoop(1)
      }

    override def close: F[Unit] =
      closedRef.set(true) *>
        connRef.getAndSet(ConnPhase.Closed()).flatMap {
          case ConnPhase.Online(t) => t.close
          case _                   => Async[F].unit
        } *>
        stateRef.set(None)

    override def onReconnect(resubscribe: Info => F[Unit]): F[Unit] =
      resubscribeRef.set(Some(resubscribe))

    override def updateServerInfo(info: Info): F[Unit] =
      stateRef.update {
        case Some(s) => Some(s.copy(serverInfo = info))
        case None    => None
      } *>
        connectedRef.get.flatMap { connected =>
          val addr =
            connected.getOrElse(
              ServerAddress(config.host, config.port, config.useTls)
            )
          poolRef.update(
            _.merge(
              parseConnectUrls(info).map(_.copy(useTls = addr.useTls)),
              addr
            )
          )
        } *>
        eventQueue.offer(ClientEvent.ServerInfoUpdated(info))

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
              // Rotate to the next server in the pool each attempt, so an N-node
              // cluster fails over instead of hammering the original node. Sleep
              // only at sweep boundaries: a full pass over the pool is tried
              // back-to-back; the backoff delay applies between sweeps.
              poolRef.modify(_.next).flatMap { case (server, startsNewSweep) =>
                val sleepFor: FiniteDuration =
                  if attempt > 1 && startsNewSweep then delay else Duration.Zero
                eventQueue.offer(
                  ClientEvent.Reconnecting(attempt, sleepFor.toMillis)
                ) *>
                  Async[F].sleep(sleepFor) *>
                  establishConnection(server).attempt.flatMap {
                    case Right((newTransport, info)) =>
                      goOnlineReconnect(newTransport, info, server, attempt)
                    case Left(_) =>
                      reconnectLoop(attempt + 1)
                  }
              }
      }

    /** Bring a freshly re-established connection online. The install (swap +
      * record + resubscribe + `Reconnected` event) is masked so a cancel can't
      * leave a "connected but silent" client; the reconnect-buffer flush —
      * which may be large and blocks on the bounded write queue — stays
      * pollable. SUBs are replayed before the buffered PUBs flush.
      */
    private def goOnlineReconnect(
        transport: Transport[F],
        info: Info,
        server: ServerAddress,
        attempt: Int
    ): F[Unit] =
      Async[F].uncancelable { poll =>
        swapOnline(transport).flatMap { pending =>
          recordConnection(info, server) *>
            resubscribeRef.get.flatMap(_.traverse_(resub => resub(info))) *>
            eventQueue.offer(ClientEvent.Reconnected(info, attempt)) *>
            poll(pending.traverse_(transport.send))
        }
      }

    private def establishConnection(
        server: ServerAddress
    ): F[(Transport[F], Info)] =
      val address = server.socketAddress

      // `allocated` decouples acquire from a Resource scope, so a cancel after
      // the socket/TLS FD is acquired but before a finalizer is armed would leak
      // it. Mask the allocate->arm handoff with uncancelable; keep the handshake
      // (incl. the INFO wait) pollable; release on cancel or error, never on
      // success (the caller owns the live transport).
      if server.useTls then
        tlsContext match
          case None =>
            Async[F].raiseError(
              NatsError.TlsError("TLS requested but no TLSContext provided")
            )
          case Some(ctx) =>
            val params =
              config.tlsParams.getOrElse(fs2.io.net.tls.TLSParameters.Default)
            // NATS sends INFO in plaintext and only then upgrades to TLS, so
            // read the INFO off the bare socket first and wrap it afterwards
            // (the standard flow; `handshake_first` servers are not supported).
            val resource =
              for
                socket <- Network[F].connect(address)
                info <- Resource.eval(readPlaintextInfo(socket))
                transport <- TlsTransport.wrap(
                  ctx,
                  socket,
                  msgBuilder,
                  params,
                  transportConfig,
                  parserConfig
                )
              yield (transport, info)

            Async[F].uncancelable { poll =>
              poll(resource.allocated).flatMap {
                case ((transport, info), release) =>
                  poll(
                    sendConnect(transport, info, server.useTls)
                      .as((transport, info))
                  )
                    .onCancel(release)
                    .onError { case _ => release }
              }
            }
      else
        Async[F].uncancelable { poll =>
          poll(
            NatsSocket
              .resource(address, msgBuilder, transportConfig, parserConfig)
              .allocated
          ).flatMap { case (transport, release) =>
            poll(waitForInfoAndConnect(transport, server.useTls))
              .onCancel(release)
              .onError { case _ => release }
          }
        }

    /** Read the server's INFO line from the still-plaintext socket. NATS always
      * sends INFO in the clear first; on TLS connections the client reads it
      * here and only then upgrades the socket to TLS.
      */
    private def readPlaintextInfo(socket: Socket[F]): F[Info] =
      val readInfo = socket.reads
        .through(ProtocolParser.parseStream(parserConfig))
        .collectFirst { case NatsFrame.InfoFrame(info) => info }
        .compile
        .last

      Async[F]
        .race(readInfo, Temporal[F].sleep(transportConfig.connectTimeout))
        .flatMap {
          case Left(Some(info)) => Async[F].pure(info)
          case Left(None)       =>
            Async[F].raiseError(
              NatsError.ConnectionFailed("No INFO received from server")
            )
          case Right(_) =>
            Async[F].raiseError(
              NatsError.Timeout(
                "Waiting for server INFO",
                transportConfig.connectTimeout.toMillis
              )
            )
        }

    // Release of the transport on error/timeout/cancel is owned by the caller
    // (establishConnection's uncancelable onCancel/onError); this only raises.
    private def waitForInfoAndConnect(
        transport: Transport[F],
        useTls: Boolean
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
            Async[F].raiseError(err)

          case Left(Right(info)) =>
            sendConnect(transport, info, useTls).as((transport, info))

          case Right(_) =>
            Async[F].raiseError(
              NatsError.Timeout(
                "Waiting for server INFO",
                transportConfig.connectTimeout.toMillis
              )
            )
        }

    private def sendConnect(
        transport: Transport[F],
        info: Info,
        useTls: Boolean
    ): F[Unit] =
      Async[F].fromEither(buildConnectMessage(info, useTls)).flatMap {
        connectMsg =>
          val connectJson = writeToString(connectMsg)
          val connectBytes = SerializationUtils.buildConnect(connectJson)

          transport.send(connectBytes) *>
            transport.send(SerializationUtils.buildPing)
      }

    private def buildConnectMessage(
        info: Info,
        useTls: Boolean
    ): Either[Throwable, Connect] =
      val base = Connect(
        verbose = config.verbose,
        pedantic = config.pedantic,
        tlsRequired = useTls,
        name = config.name,
        echo = config.echo,
        headersSupported = info.headersSupported
      )

      config.credentials match
        case Some(NatsCredentials.UserPassword(user, pass)) =>
          Right(base.copy(user = Some(user), pass = Some(pass)))
        case Some(NatsCredentials.Token(token)) =>
          Right(base.copy(authToken = Some(token)))
        case Some(NatsCredentials.NKey(seed, jwt)) =>
          info.nonce match
            case None =>
              Left(
                NatsError.AuthorizationError(
                  "server did not send a nonce; NKey authentication requires a nonce"
                )
              )
            case Some(nonce) =>
              NKey.signNonce(seed, nonce).flatMap { sig =>
                jwt match
                  // JWT path (operator/decentralized auth): send jwt + sig.
                  case Some(j) =>
                    Right(base.copy(jwt = Some(j), sig = Some(sig)))
                  // Pure-NKey path: send the derived public nkey + sig.
                  case None =>
                    NKey.publicKeyFromSeed(seed).map { pub =>
                      base.copy(nkey = Some(pub), sig = Some(sig))
                    }
              }
        case None =>
          Right(base)
