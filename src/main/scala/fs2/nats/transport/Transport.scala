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
import fs2.nats.protocol.Frame

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

  /** Stream of parsed protocol elements from the server. Control frames are
    * `NatsFrame`s; MSG/HMSG data frames are built by the injected message
    * builder (the client builds `NatsMessage` directly). This stream never
    * drops frames silently - errors are surfaced as ParseErrorFrame or as
    * stream failures depending on parser config.
    *
    * @return
    *   A continuous stream of [[Frame]] values
    */
  def frames: Stream[F, Frame]

  /** Send raw bytes to the server. Writes are enqueued and processed in order
    * by a single writer fiber, ensuring atomic and ordered transmission.
    *
    * @param bytes
    *   The bytes to send
    * @return
    *   Effect that completes when bytes are enqueued
    */
  def send(bytes: Chunk[Byte]): F[Unit]

  /** Enqueue an outbound write described by an [[Outgoing]] descriptor. The
    * single writer fiber assembles it (header + payload, no per-publish
    * combined array) into its reused buffer at write time. `send` is the
    * special case of a fully-built [[Outgoing.Raw]] frame.
    *
    * @param out
    *   The write descriptor
    * @return
    *   Effect that completes when the descriptor is enqueued
    */
  def sendOutgoing(out: Outgoing): F[Unit]

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

  /** Bytes requested per socket read. `Socket.reads` hardcodes 8 KiB, which
    * caps the inbound path at ~8K read effects per 64 MiB and dominates large
    * payload receive time (e.g. Object Store 128 KiB chunks span 16 reads
    * each). 64 KiB matches jnats's reader buffer; fs2 reuses one per-socket
    * buffer grown to the requested size, so this costs no per-read allocation.
    */
  private val ReadChunkSize: Int = 64 * 1024

  /** The inbound byte stream of `socket`, read in [[ReadChunkSize]] requests.
    * Identical to `socket.reads` apart from the per-read size.
    */
  private[transport] def reads[F[_]](socket: Socket[F]): Stream[F, Byte] =
    Stream.repeatEval(socket.read(ReadChunkSize)).unNoneTerminate.unchunks

  /** Initial capacity of a writer's reusable coalescing buffer. Sized so a
    * typical drain never has to grow it; a single oversized message grows it
    * once and the larger buffer is retained for the connection's lifetime.
    */
  private val InitialWriteBufferCapacity: Int = 64 * 1024

  /** A growable byte buffer, reused across the writer's drain cycles and owned
    * by the single writer fiber. [[fill]] assembles a drained batch of
    * [[Outgoing]] descriptors into the buffer once and returns a Chunk viewing
    * the filled prefix, so the whole batch is flushed in one `socket.write`
    * (one syscall) without allocating a fresh array per drain. A `Pub`/`HPub`
    * descriptor's payload is copied here exactly once (its only copy). A
    * single-element drain that is already a fully-built [[Outgoing.Raw]] frame
    * (the common low-load case) bypasses this entirely and is written as-is.
    *
    * Safety: the returned Chunk aliases the buffer, but the writer processes
    * one batch at a time — `socket.write` fully consumes the Chunk before the
    * next [[fill]] overwrites the buffer — so the buffer never escapes the
    * fiber and is never mutated while a write is in flight.
    */
  private[transport] final class WriteBuffer:
    private var buf: Array[Byte] = new Array[Byte](InitialWriteBufferCapacity)

    def fill(batch: Chunk[Outgoing]): Chunk[Byte] =
      var total = 0
      var i = 0
      while i < batch.size do
        total += batch(i).size
        i += 1
      if total > buf.length then buf = new Array[Byte](total)
      var off = 0
      i = 0
      while i < batch.size do
        off = batch(i).copyInto(buf, off)
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
      writeQueue: Queue[F, Outgoing],
      connectedRef: Ref[F, Boolean],
      config: TransportConfig
  ): Resource[F, Unit] =
    val writerFiber =
      Async[F]
        .delay(new WriteBuffer)
        .flatMap { wb =>
          Stream
            .fromQueueUnterminated(writeQueue)
            .takeWhile(_ ne Outgoing.Poison)
            .chunks
            .foreach { batch =>
              Async[F]
                .delay {
                  // A lone, already-complete Raw frame is written as-is; a lone
                  // Pub/HPub still needs its header + payload assembled, so it
                  // goes through the reused buffer like any multi-frame batch.
                  if batch.size == 1 then
                    batch(0) match
                      case Outgoing.Raw(c) => c
                      case _               => wb.fill(batch)
                  else wb.fill(batch)
                }
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
        writeQueue.offer(Outgoing.Poison) *> Async[F].unit
      )
      .void
