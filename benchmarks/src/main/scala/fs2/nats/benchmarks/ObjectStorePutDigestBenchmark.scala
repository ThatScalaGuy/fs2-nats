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

package fs2.nats.benchmarks

import fs2.Chunk
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.security.MessageDigest
import java.util.concurrent.TimeUnit

/** Isolates the per-chunk cost of the Object Store put digest fold.
  *
  * `put` hashes every chunk and then hands the same chunk to the transport,
  * which copies it into the write buffer. Feeding the digest from
  * `Chunk.toArray` therefore copied every object byte twice before it reached
  * the socket. `chunkN` emits two shapes in practice - an offset slice of the
  * caller's array, and a composite `Chunk.Queue` when source chunks straddle
  * `maxChunkSize` - and only the leaf walk avoids the copy on both. Run with
  * `-prof gc` and compare `gc.alloc.rate.norm` (bytes/op): the digest-side
  * garbage drops from roughly one byte per object byte to the ByteBuffer
  * wrappers alone.
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*ObjectStorePutDigestBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class ObjectStorePutDigestBenchmark:

  /** The two chunk shapes `chunkN` actually produces: a sub-slice of the
    * caller's array (`putBytes` of a large object), and a queue of leaves
    * spliced from several source chunks (`putFile`, any streamed source).
    */
  @Param(Array("queue", "slice"))
  var shape: String = "queue"

  private val chunkSize = 128 * 1024

  private var chunk: Chunk[Byte] = scala.compiletime.uninitialized
  private var md: MessageDigest = scala.compiletime.uninitialized
  // Stands in for the writer's grow-only buffer: every variant pays this copy,
  // so the measured delta is the digest-side copy alone.
  private var buf: Array[Byte] = scala.compiletime.uninitialized

  @Setup
  def setup(): Unit =
    md = MessageDigest.getInstance("SHA-256")
    buf = new Array[Byte](chunkSize)
    val source =
      Chunk.array(Array.tabulate(chunkSize * 2)(i => (i % 256).toByte))
    chunk = shape match
      case "slice" => source.drop(chunkSize / 2).take(chunkSize)
      case _       =>
        // Two 64 KiB source reads spliced into one 128 KiB published chunk.
        val half = chunkSize / 2
        Chunk.Queue(source.take(half), source.drop(half).take(half))

  /** Old path: one full copy of the chunk purely to feed the digest. */
  @Benchmark
  def digestViaToArray(bh: Blackhole): Unit =
    md.update(chunk.toArray)
    chunk.copyToArray(buf, 0)
    bh.consume(md.digest())

  /** Compacting first: a no-op only for a chunk that already spans its whole
    * backing array, so both shapes above still copy.
    */
  @Benchmark
  def digestViaCompact(bh: Blackhole): Unit =
    val cc = chunk.compact
    md.update(cc.toByteBuffer)
    cc.copyToArray(buf, 0)
    bh.consume(md.digest())

  /** New path: wrap each array-backed leaf in place, copy nothing. */
  @Benchmark
  def digestViaLeaves(bh: Blackhole): Unit =
    digest(chunk)
    chunk.copyToArray(buf, 0)
    bh.consume(md.digest())

  private def digest(c: Chunk[Byte]): Unit =
    c match
      case q: Chunk.Queue[Byte] => q.chunks.foreach(digest)
      case _                    => if c.nonEmpty then md.update(c.toByteBuffer)
