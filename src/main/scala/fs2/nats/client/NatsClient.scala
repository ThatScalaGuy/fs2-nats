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

import cats.effect.{Async, Ref, Resource}
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.io.net.Network
import fs2.io.net.tls.TLSContext
import fs2.nats.errors.NatsError
import fs2.nats.protocol.{Headers, Info, NatsFrame, ParserConfig}
import fs2.nats.publish.{Publisher, SerializationUtils}
import fs2.nats.subscriptions.{NatsMessage, SidAllocator, SubscriptionManager}
import fs2.nats.transport.TransportConfig

/** NATS client for publishing and subscribing to messages.
  *
  * Provides a high-level API for interacting with a NATS server including:
  *   - Publishing messages (with and without headers)
  *   - Subscribing to subjects (with queue groups)
  *   - Connection event notifications
  *   - Automatic reconnection with subscription recovery
  *
  * @tparam F
  *   The effect type
  */
trait NatsClient[F[_]]:

  /** Publish a message to a subject.
    *
    * @param subject
    *   The subject to publish to
    * @param payload
    *   The message payload
    * @param headers
    *   Optional message headers (default empty)
    * @param replyTo
    *   Optional reply-to subject for request/reply pattern
    * @return
    *   Effect that completes when message is enqueued for sending
    */
  def publish(
      subject: String,
      payload: Chunk[Byte],
      headers: Headers = Headers.empty,
      replyTo: Option[String] = None
  ): F[Unit]

  /** Subscribe to a subject.
    *
    * @param subject
    *   The subject to subscribe to (may contain wildcards)
    * @param queueGroup
    *   Optional queue group for load-balanced delivery
    * @return
    *   Resource providing a stream of messages and automatic unsubscribe on
    *   release
    */
  def subscribe(
      subject: String,
      queueGroup: Option[String] = None
  ): Resource[F, Stream[F, NatsMessage]]

  /** Stream of client events (connections, disconnections, errors).
    *
    * @return
    *   A stream of ClientEvent values
    */
  def events: Stream[F, ClientEvent]

  /** Close the client and release all resources. All subscriptions will be
    * closed and no further operations allowed.
    *
    * @return
    *   Effect that completes when the client is fully closed
    */
  def close: F[Unit]

  /** Get the current server INFO.
    *
    * @return
    *   The latest server INFO received
    */
  def serverInfo: F[Info]

  /** Check if the client is connected.
    *
    * @return
    *   True if currently connected to the server
    */
  def isConnected: F[Boolean]

object NatsClient:

  /** Connect to a NATS server and create a client.
    *
    * The resource will:
    *   - Establish a TCP/TLS connection
    *   - Wait for server INFO and send CONNECT
    *   - Set up subscription management
    *   - Handle PING/PONG keepalive
    *   - Manage reconnection on failure
    *
    * @param config
    *   Client configuration
    * @param transportConfig
    *   Optional transport configuration
    * @param parserConfig
    *   Optional parser configuration
    * @param tlsContext
    *   Optional TLS context for secure connections
    * @tparam F
    *   The effect type with Async and Network capabilities
    * @return
    *   Resource managing the NatsClient lifecycle
    */
  def connect[F[_]: Async: Network](
      config: ClientConfig,
      transportConfig: TransportConfig = TransportConfig.default,
      parserConfig: ParserConfig = ParserConfig.default,
      tlsContext: Option[TLSContext[F]] = None
  ): Resource[F, NatsClient[F]] =
    for
      connManager <- ConnectionManager.connect(
        config,
        transportConfig,
        parserConfig,
        tlsContext
      )

      sidAllocator <- Resource.eval(SidAllocator[F])

      subManager <- Resource.eval(
        SubscriptionManager[F](
          config.queueCapacity,
          config.slowConsumerPolicy,
          sidAllocator,
          sendUnsub(connManager)
        )
      )

      publisher <- Resource.eval(
        connManager.state.flatMap { s =>
          Publisher[F](
            new TransportAdapter(connManager),
            s.serverInfo.maxPayload
          )
        }
      )

      eventQueue <- Resource.eval(Queue.unbounded[F, ClientEvent])
      closedRef <- Resource.eval(Ref.of[F, Boolean](false))
      supervisor <- Supervisor[F]

      client = new NatsClientImpl[F](
        connManager,
        subManager,
        sidAllocator,
        publisher,
        eventQueue,
        closedRef,
        supervisor
      )

      _ <- Resource.make(client.initialize)(_ => client.close)
    yield client

  private def sendUnsub[F[_]](connManager: ConnectionManager[F])(
      sid: Long,
      maxMsgs: Option[Int]
  ): F[Unit] =
    connManager.send(SerializationUtils.buildUnsub(sid, maxMsgs))

  private class TransportAdapter[F[_]: Async](
      connManager: ConnectionManager[F]
  ) extends fs2.nats.transport.Transport[F]:
    override def frames: Stream[F, NatsFrame] = connManager.frames
    override def send(bytes: Chunk[Byte]): F[Unit] = connManager.send(bytes)
    override def close: F[Unit] = connManager.close
    override def isConnected: F[Boolean] =
      connManager.transport.flatMap(_.isConnected)

  private class NatsClientImpl[F[_]: Async](
      connManager: ConnectionManager[F],
      subManager: SubscriptionManager[F],
      sidAllocator: SidAllocator[F],
      publisher: Publisher[F],
      eventQueue: Queue[F, ClientEvent],
      closedRef: Ref[F, Boolean],
      supervisor: Supervisor[F]
  ) extends NatsClient[F]:

    def initialize: F[Unit] =
      connManager.onReconnect(handleReconnect) *>
        supervisor.supervise(frameProcessor).void

    private def handleReconnect(info: Info): F[Unit] =
      publisher.updateMaxPayload(info.maxPayload) *>
        resubscribeAll

    private def resubscribeAll: F[Unit] =
      subManager.activeSubscriptions.flatMap { subs =>
        subs.traverse_ { case (sid, subject, queueGroup) =>
          connManager.send(
            SerializationUtils.buildSub(subject, queueGroup, sid)
          )
        }
      }

    private def frameProcessor: F[Unit] =
      connManager.frames
        .evalMap(handleFrame)
        .compile
        .drain
        .handleErrorWith { err =>
          closedRef.get.flatMap { closed =>
            if closed then Async[F].unit
            else
              eventQueue.offer(
                ClientEvent.ProtocolError(err.getMessage, fatal = false)
              ) *> connManager.reconnect(err.getMessage)
          }
        }

    private def handleFrame(frame: NatsFrame): F[Unit] =
      frame match
        case NatsFrame.PingFrame =>
          connManager.send(SerializationUtils.buildPong)

        case NatsFrame.PongFrame =>
          Async[F].unit

        case NatsFrame.OkFrame =>
          Async[F].unit

        case NatsFrame.ErrFrame(msg) =>
          eventQueue.offer(
            ClientEvent.ProtocolError(msg, fatal = isAuthError(msg))
          )

        case NatsFrame.InfoFrame(info) =>
          connManager.updateServerInfo(info) *>
            publisher.updateMaxPayload(info.maxPayload) *>
            (if info.ldmMode then eventQueue.offer(ClientEvent.LameDuckMode)
             else Async[F].unit)

        case msg @ (NatsFrame.MsgFrame(_, _, _, _) |
            NatsFrame.HMsgFrame(_, _, _, _, _, _)) =>
          subManager.routeMessage(msg).flatMap {
            case Some(slowConsumer) => eventQueue.offer(slowConsumer)
            case None               => Async[F].unit
          }

        case NatsFrame.ParseErrorFrame(msg, _) =>
          eventQueue.offer(ClientEvent.ProtocolError(msg, fatal = false))

    private def isAuthError(msg: String): Boolean =
      val lower = msg.toLowerCase
      lower.contains("authorization") ||
      lower.contains("authentication") ||
      lower.contains("permission")

    override def publish(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        replyTo: Option[String]
    ): F[Unit] =
      checkClosed *>
        (if headers.isEmpty then publisher.publish(subject, payload, replyTo)
         else publisher.publishWithHeaders(subject, payload, headers, replyTo))

    override def subscribe(
        subject: String,
        queueGroup: Option[String]
    ): Resource[F, Stream[F, NatsMessage]] =
      Resource
        .make(
          for
            _ <- checkClosed
            sid <- sidAllocator.next
            result <- subManager.register(sid, subject, queueGroup)
            (stream, handle) = result
            _ <- connManager.send(
              SerializationUtils.buildSub(subject, queueGroup, sid)
            )
          yield (stream, handle)
        ) { case (_, handle) =>
          handle.unsubscribe
        }
        .map(_._1)

    override def events: Stream[F, ClientEvent] =
      Stream.fromQueueUnterminated(eventQueue).merge(connManager.events)

    override def close: F[Unit] =
      closedRef.get.flatMap { alreadyClosed =>
        if alreadyClosed then Async[F].unit
        else
          closedRef.set(true) *>
            subManager.closeAll *>
            connManager.close
      }

    override def serverInfo: F[Info] =
      connManager.state.map(_.serverInfo)

    override def isConnected: F[Boolean] =
      closedRef.get.flatMap { closed =>
        if closed then Async[F].pure(false)
        else connManager.transport.flatMap(_.isConnected)
      }

    private def checkClosed: F[Unit] =
      closedRef.get.flatMap { closed =>
        if closed then Async[F].raiseError(NatsError.ClientClosed)
        else Async[F].unit
      }
