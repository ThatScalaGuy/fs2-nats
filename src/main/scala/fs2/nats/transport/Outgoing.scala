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

import fs2.Chunk

/** A queued outbound write, drained and assembled by the single writer fiber.
  *
  * `Pub`/`HPub` carry a small prebuilt protocol header (the control line up to
  * and including its CRLF) plus a *reference* to the user's immutable payload
  * `Chunk` (and, for `HPub`, the already-serialized headers). This mirrors the
  * Java client's prebuilt-header + payload-reference model: the per-publish
  * combined `PUB ...\r\n<payload>\r\n` array and the payload copy into it are
  * gone — the writer copies the payload exactly once, into its reused buffer,
  * at write time. `Raw` wraps an already-complete frame (control frames and
  * writes replayed from the offline reconnect buffer).
  *
  * FP-safety: every referenced byte source is immutable (`header` is a freshly
  * built array that never escapes; `payload`/`headers` are the caller's
  * immutable `Chunk`s), so a descriptor may cross the publish-fiber →
  * writer-fiber boundary by value without a defensive copy.
  */
private[nats] sealed trait Outgoing:

  /** Total on-wire byte length. */
  def size: Int

  /** Copy the on-wire bytes into `dest` starting at `off`; returns the offset
    * just past the last byte written (`off + size`).
    */
  def copyInto(dest: Array[Byte], off: Int): Int

  /** Materialize into a standalone immutable `Chunk` — used only for the rare
    * offline-reconnect buffering path, where a write must be retained as a
    * value until the connection is re-established.
    */
  def materialize: Chunk[Byte] =
    val arr = new Array[Byte](size)
    copyInto(arr, 0)
    Chunk.array(arr)

private[nats] object Outgoing:

  private val Cr: Byte = '\r'.toByte
  private val Lf: Byte = '\n'.toByte

  /** An already-complete frame (control frames, offline-replayed writes). */
  final case class Raw(chunk: Chunk[Byte]) extends Outgoing:
    def size: Int = chunk.size
    def copyInto(dest: Array[Byte], off: Int): Int =
      chunk.copyToArray(dest, off)
      off + chunk.size

  /** A `PUB`: `header` is `PUB <subject> [reply ]<#bytes>\r\n`; the payload and
    * its trailing CRLF are appended by the writer.
    */
  final case class Pub(header: Array[Byte], payload: Chunk[Byte])
      extends Outgoing:
    def size: Int = header.length + payload.size + 2
    def copyInto(dest: Array[Byte], off: Int): Int =
      System.arraycopy(header, 0, dest, off, header.length)
      var o = off + header.length
      payload.copyToArray(dest, o)
      o += payload.size
      dest(o) = Cr
      dest(o + 1) = Lf
      o + 2

  /** An `HPUB`: `header` is `HPUB <subject> [reply ]<#hdr> <#total>\r\n`;
    * `headers` and `payload` follow, then a trailing CRLF.
    */
  final case class HPub(
      header: Array[Byte],
      headers: Chunk[Byte],
      payload: Chunk[Byte]
  ) extends Outgoing:
    def size: Int = header.length + headers.size + payload.size + 2
    def copyInto(dest: Array[Byte], off: Int): Int =
      System.arraycopy(header, 0, dest, off, header.length)
      var o = off + header.length
      headers.copyToArray(dest, o)
      o += headers.size
      payload.copyToArray(dest, o)
      o += payload.size
      dest(o) = Cr
      dest(o + 1) = Lf
      o + 2

  /** Sentinel enqueued to terminate the writer drain loop. Compared by
    * reference identity (`ne`), so real writes need not be wrapped in an
    * `Option` to carry an out-of-band end-of-stream signal.
    */
  case object Poison extends Outgoing:
    def size: Int = 0
    def copyInto(dest: Array[Byte], off: Int): Int = off
