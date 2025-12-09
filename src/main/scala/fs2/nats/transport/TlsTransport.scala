/*
 * Copyright 2024 fs2-nats contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.transport

import cats.effect.{Async, Ref, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.io.net.Socket
import fs2.io.net.tls.{TLSContext, TLSParameters, TLSSocket}
import fs2.nats.protocol.{NatsFrame, ParserConfig, ProtocolParser}

/**
 * TLS-wrapped NATS transport.
 *
 * Provides the same Transport interface over a TLS-encrypted connection.
 * Used when connecting to NATS servers that require TLS.
 */
object TlsTransport:

  /**
   * Wrap an existing socket with TLS and return a Transport.
   *
   * @param tlsContext The TLS context for creating secure connections
   * @param underlying The underlying TCP socket
   * @param params TLS parameters (hostname verification, protocols, etc.)
   * @param config Transport configuration
   * @param parserConfig Parser configuration
   * @tparam F The effect type
   * @return A Resource that manages the TLS Transport lifecycle
   */
  def wrap[F[_]: Async](
      tlsContext: TLSContext[F],
      underlying: Socket[F],
      params: TLSParameters = TLSParameters.Default,
      config: TransportConfig = TransportConfig.default,
      parserConfig: ParserConfig = ParserConfig.default
  ): Resource[F, Transport[F]] =
    for
      tlsSocket <- tlsContext.clientBuilder(underlying).withParameters(params).build
      transport <- fromTlsSocket(tlsSocket, config, parserConfig)
    yield transport

  /**
   * Create a Transport from an existing TLS socket.
   *
   * @param socket The TLS socket
   * @param config Transport configuration
   * @param parserConfig Parser configuration
   * @tparam F The effect type
   * @return A Resource that manages the Transport lifecycle
   */
  def fromTlsSocket[F[_]: Async](
      socket: TLSSocket[F],
      config: TransportConfig = TransportConfig.default,
      parserConfig: ParserConfig = ParserConfig.default
  ): Resource[F, Transport[F]] =
    for
      writeQueue <- Resource.eval(Queue.bounded[F, Option[Chunk[Byte]]](config.writeQueueCapacity))
      connectedRef <- Resource.eval(Ref.of[F, Boolean](true))
      _ <- startWriter(socket, writeQueue, connectedRef)
    yield new TlsSocketTransport[F](socket, writeQueue, connectedRef, config, parserConfig)

  private def startWriter[F[_]: Async](
      socket: TLSSocket[F],
      writeQueue: Queue[F, Option[Chunk[Byte]]],
      connectedRef: Ref[F, Boolean]
  ): Resource[F, Unit] =
    val writerFiber = Stream
      .fromQueueNoneTerminated(writeQueue)
      .foreach(chunk => socket.write(chunk))
      .compile
      .drain
      .handleErrorWith { err =>
        connectedRef.set(false) *>
          Async[F].raiseError(err)
      }

    Resource.make(Async[F].start(writerFiber))(_ =>
      writeQueue.offer(None) *> Async[F].unit
    ).void

  private class TlsSocketTransport[F[_]: Async](
      socket: TLSSocket[F],
      writeQueue: Queue[F, Option[Chunk[Byte]]],
      connectedRef: Ref[F, Boolean],
      config: TransportConfig,
      parserConfig: ParserConfig
  ) extends Transport[F]:

    override def frames: Stream[F, NatsFrame] =
      socket
        .reads
        .through(ProtocolParser.parseStream(parserConfig))

    override def send(bytes: Chunk[Byte]): F[Unit] =
      connectedRef.get.flatMap { connected =>
        if connected then writeQueue.offer(Some(bytes))
        else Async[F].raiseError(fs2.nats.errors.NatsError.ClientClosed)
      }

    override def close: F[Unit] =
      connectedRef.set(false) *>
        writeQueue.offer(None) *>
        socket.endOfOutput

    override def isConnected: F[Boolean] =
      connectedRef.get
