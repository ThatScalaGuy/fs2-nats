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
import com.comcast.ip4s.{Host, SocketAddress}
import fs2.{Chunk, Stream}
import fs2.io.net.{Network, Socket}
import fs2.nats.protocol.{NatsFrame, ParserConfig, ProtocolParser}

/**
 * NATS socket transport implementation.
 *
 * Creates a transport over a TCP socket connection to a NATS server.
 * Frames are parsed using the incremental protocol parser, and writes
 * are serialized through a bounded queue processed by a single writer fiber.
 */
object NatsSocket:

  /**
   * Create a Transport resource connected to the specified address.
   *
   * The resource will:
   * - Establish a TCP connection to the NATS server
   * - Start a background writer fiber for serialized writes
   * - Parse incoming bytes into NATS frames
   *
   * @param address The socket address to connect to
   * @param config Transport configuration
   * @param parserConfig Parser configuration
   * @tparam F The effect type with Async and Network capabilities
   * @return A Resource that manages the Transport lifecycle
   */
  def resource[F[_]: Async: Network](
      address: SocketAddress[Host],
      config: TransportConfig = TransportConfig.default,
      parserConfig: ParserConfig = ParserConfig.default
  ): Resource[F, Transport[F]] =
    for
      socket <- Network[F].client(address)
      transport <- fromSocket(socket, config, parserConfig)
    yield transport

  /**
   * Create a Transport from an existing socket.
   *
   * @param socket The underlying socket
   * @param config Transport configuration
   * @param parserConfig Parser configuration
   * @tparam F The effect type
   * @return A Resource that manages the Transport lifecycle
   */
  def fromSocket[F[_]: Async](
      socket: Socket[F],
      config: TransportConfig = TransportConfig.default,
      parserConfig: ParserConfig = ParserConfig.default
  ): Resource[F, Transport[F]] =
    for
      writeQueue <- Resource.eval(Queue.bounded[F, Option[Chunk[Byte]]](config.writeQueueCapacity))
      connectedRef <- Resource.eval(Ref.of[F, Boolean](true))
      _ <- startWriter(socket, writeQueue, connectedRef)
    yield new SocketTransport[F](socket, writeQueue, connectedRef, config, parserConfig)

  private def startWriter[F[_]: Async](
      socket: Socket[F],
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

  private class SocketTransport[F[_]: Async](
      socket: Socket[F],
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
