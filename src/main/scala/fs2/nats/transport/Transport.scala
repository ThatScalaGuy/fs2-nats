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
import fs2.nats.protocol.NatsFrame

/** Transport abstraction for NATS socket communication.
  *
  * Provides a high-level interface for reading parsed NATS frames and writing
  * raw bytes to the underlying socket. Writes are serialized and ordered
  * through an internal queue.
  *
  * @tparam F
  *   The effect type
  */
trait Transport[F[_]]:

  /** Stream of parsed NATS frames from the server. This stream never drops
    * frames silently - errors are surfaced as ParseErrorFrame or as stream
    * failures depending on parser config.
    *
    * @return
    *   A continuous stream of NatsFrame values
    */
  def frames: Stream[F, NatsFrame]

  /** Send raw bytes to the server. Writes are enqueued and processed in order
    * by a single writer fiber, ensuring atomic and ordered transmission.
    *
    * @param bytes
    *   The bytes to send
    * @return
    *   Effect that completes when bytes are enqueued
    */
  def send(bytes: Chunk[Byte]): F[Unit]

  /** Close the transport and release all resources. After closing, all
    * operations will fail.
    *
    * @return
    *   Effect that completes when transport is closed
    */
  def close: F[Unit]

  /** Check if the transport is currently connected.
    *
    * @return
    *   True if connected and operational
    */
  def isConnected: F[Boolean]

object Transport:

  /** Sentinel enqueued to terminate a transport's writer drain loop. Compared
    * by reference identity (`ne`), so real frames need not be boxed in an
    * `Option` just to carry an out-of-band end-of-stream signal — a queued
    * frame is always a distinct `Chunk` instance, so `eq` never collides.
    */
  private[transport] val WriterPoison: Chunk[Byte] = Chunk.singleton(0.toByte)

  /** Initial capacity of a writer's reusable coalescing buffer. Sized so a
    * typical drain never has to grow it; a single oversized message grows it
    * once and the larger buffer is retained for the connection's lifetime.
    */
  private val InitialWriteBufferCapacity: Int = 64 * 1024

  /** A growable byte buffer, reused across the writer's drain cycles and owned
    * by the single writer fiber. [[fill]] copies a drained batch of frames into
    * the buffer once and returns a Chunk viewing the filled prefix, so the
    * whole batch is flushed in one `socket.write` (one syscall) without
    * allocating a fresh array per drain. A single-element drain (the common
    * low-load case) bypasses this entirely and is written as-is.
    *
    * Safety: the returned Chunk aliases the buffer, but the writer processes
    * one batch at a time — `socket.write` fully consumes the Chunk before the
    * next [[fill]] overwrites the buffer — so the buffer never escapes the
    * fiber and is never mutated while a write is in flight.
    */
  private[transport] final class WriteBuffer:
    private var buf: Array[Byte] = new Array[Byte](InitialWriteBufferCapacity)

    def fill(batch: Chunk[Chunk[Byte]]): Chunk[Byte] =
      var total = 0
      var i = 0
      while i < batch.size do
        total += batch(i).size
        i += 1
      if total > buf.length then buf = new Array[Byte](total)
      var off = 0
      i = 0
      while i < batch.size do
        val c = batch(i)
        c.copyToArray(buf, off)
        off += c.size
        i += 1
      Chunk.array(buf, 0, total)

  /** Start the single writer fiber for a transport.
    *
    * Drains the write queue in batches (`fromQueueUnterminated` blocks for the
    * first element then sweeps up everything immediately available), coalesces
    * each batch into the reusable [[WriteBuffer]], and flushes it in one
    * `socket.write`. Each write is bounded by `writeTimeout` so a stalled
    * socket (dead peer, full send buffer) fails the writer and triggers
    * reconnect instead of hanging; the timeout is per drain, i.e. amortized
    * over the whole coalesced batch, not per message. On any error the
    * transport is marked disconnected and the error re-raised. The returned
    * Resource supervises the fiber and, on release, pushes the [[WriterPoison]]
    * sentinel to terminate it.
    */
  private[transport] def startWriter[F[_]: Async](
      socket: Socket[F],
      writeQueue: Queue[F, Chunk[Byte]],
      connectedRef: Ref[F, Boolean],
      config: TransportConfig
  ): Resource[F, Unit] =
    val writerFiber =
      Async[F]
        .delay(new WriteBuffer)
        .flatMap { wb =>
          Stream
            .fromQueueUnterminated(writeQueue)
            .takeWhile(_ ne WriterPoison)
            .chunks
            .foreach { batch =>
              Async[F]
                .delay(if batch.size == 1 then batch(0) else wb.fill(batch))
                .flatMap { out =>
                  Async[F].timeoutTo(
                    socket.write(out),
                    config.writeTimeout,
                    Async[F].raiseError(
                      fs2.nats.errors.NatsError
                        .ConnectionFailed("Write timed out")
                    )
                  )
                }
            }
            .compile
            .drain
        }
        .handleErrorWith { err =>
          connectedRef.set(false) *>
            Async[F].raiseError(err)
        }

    Resource
      .make(Async[F].start(writerFiber))(_ =>
        writeQueue.offer(WriterPoison) *> Async[F].unit
      )
      .void
