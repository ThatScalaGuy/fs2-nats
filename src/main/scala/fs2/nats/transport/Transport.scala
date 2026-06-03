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

import fs2.{Chunk, Stream}
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

  /** Concatenate a batch of queued byte chunks into a single array-backed Chunk
    * so the whole batch can be flushed to the socket in one write (one syscall).
    *
    * The writer drains all immediately-available frames as one batch; coalescing
    * them here is what turns "one write per message" into "one write per drain
    * cycle". A single-element batch (the common low-load case) is returned as-is,
    * avoiding any copy.
    *
    * @param batch
    *   The frames dequeued together, each a complete protocol message
    * @return
    *   A single contiguous Chunk containing every frame in order
    */
  private[transport] def coalesce(batch: Chunk[Chunk[Byte]]): Chunk[Byte] =
    if batch.size == 1 then batch(0)
    else
      var total = 0
      var i = 0
      while i < batch.size do
        total += batch(i).size
        i += 1
      val arr = new Array[Byte](total)
      var off = 0
      i = 0
      while i < batch.size do
        val c = batch(i)
        c.copyToArray(arr, off)
        off += c.size
        i += 1
      Chunk.array(arr)
