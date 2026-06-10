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

package fs2.nats.transport

import cats.effect.{Async, Ref, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.io.net.Socket
import fs2.io.net.tls.{TLSContext, TLSParameters, TLSSocket}
import fs2.nats.protocol.{Frame, MsgBuilder, ParserConfig, ProtocolParser}

/** TLS-wrapped NATS transport.
  *
  * Provides the same Transport interface over a TLS-encrypted connection. Used
  * when connecting to NATS servers that require TLS.
  */
object TlsTransport:

  /** Wrap an existing socket with TLS and return a Transport.
    *
    * @param tlsContext
    *   The TLS context for creating secure connections
    * @param underlying
    *   The underlying TCP socket
    * @param params
    *   TLS parameters (hostname verification, protocols, etc.)
    * @param config
    *   Transport configuration
    * @param parserConfig
    *   Parser configuration
    * @tparam F
    *   The effect type
    * @return
    *   A Resource that manages the TLS Transport lifecycle
    */
  def wrap[F[_]: Async](
      tlsContext: TLSContext[F],
      underlying: Socket[F],
      msgBuilder: MsgBuilder[Frame],
      params: TLSParameters = TLSParameters.Default,
      config: TransportConfig = TransportConfig.default,
      parserConfig: ParserConfig = ParserConfig.default
  ): Resource[F, Transport[F]] =
    for
      tlsSocket <- tlsContext
        .clientBuilder(underlying)
        .withParameters(params)
        .build
      transport <- fromTlsSocket(tlsSocket, msgBuilder, config, parserConfig)
    yield transport

  /** Create a Transport from an existing TLS socket.
    *
    * @param socket
    *   The TLS socket
    * @param config
    *   Transport configuration
    * @param parserConfig
    *   Parser configuration
    * @tparam F
    *   The effect type
    * @return
    *   A Resource that manages the Transport lifecycle
    */
  def fromTlsSocket[F[_]: Async](
      socket: TLSSocket[F],
      msgBuilder: MsgBuilder[Frame],
      config: TransportConfig = TransportConfig.default,
      parserConfig: ParserConfig = ParserConfig.default
  ): Resource[F, Transport[F]] =
    for
      writeQueue <- Resource.eval(
        Queue.bounded[F, Outgoing](config.writeQueueCapacity)
      )
      connectedRef <- Resource.eval(Ref.of[F, Boolean](true))
      _ <- Transport.startWriter(socket, writeQueue, connectedRef, config)
    yield new TlsSocketTransport[F](
      socket,
      writeQueue,
      connectedRef,
      msgBuilder,
      parserConfig
    )

  private class TlsSocketTransport[F[_]: Async](
      socket: TLSSocket[F],
      writeQueue: Queue[F, Outgoing],
      connectedRef: Ref[F, Boolean],
      msgBuilder: MsgBuilder[Frame],
      parserConfig: ParserConfig
  ) extends Transport[F]:

    override def frames: Stream[F, Frame] =
      socket.reads
        .through(ProtocolParser.parseStreamWith(msgBuilder, parserConfig))

    override def send(bytes: Chunk[Byte]): F[Unit] =
      sendOutgoing(Outgoing.Raw(bytes))

    override def sendOutgoing(out: Outgoing): F[Unit] =
      connectedRef.get.flatMap { connected =>
        if connected then writeQueue.offer(out)
        else Async[F].raiseError(fs2.nats.errors.NatsError.ClientClosed)
      }

    override def close: F[Unit] =
      connectedRef.set(false) *>
        writeQueue.offer(Outgoing.Poison) *>
        socket.endOfOutput

    override def isConnected: F[Boolean] =
      connectedRef.get
