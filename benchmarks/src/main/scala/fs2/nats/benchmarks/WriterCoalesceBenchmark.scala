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
import java.util.concurrent.TimeUnit

/** Isolates the writer's per-drain coalescing allocation.
  *
  * The old writer built a fresh `Array[Byte](total)` for every multi-frame
  * drain (`Transport.coalesce`); the new writer copies the drain into a single
  * grow-only buffer reused across drains (`Transport.WriteBuffer.fill`). The two
  * methods below are those exact code paths. Run with `-prof gc` and compare
  * `gc.alloc.rate.norm` (bytes/op): the delta is the garbage removed per drain,
  * which is ~the batch's total byte size and grows with load (bigger drains).
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*WriterCoalesceBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class WriterCoalesceBenchmark:

  // Number of frames coalesced in one drain (the writer's batch size under load).
  @Param(Array("16", "256"))
  var batchSize: Int = 256

  private var batch: Chunk[Chunk[Byte]] = scala.compiletime.uninitialized
  private var buf: Array[Byte] = new Array[Byte](64 * 1024)

  @Setup
  def setup(): Unit =
    // A representative small PUB frame (16-byte payload).
    val frame = Chunk.array(("PUB events.x 16\r\n" + "x" * 16 + "\r\n").getBytes)
    batch = Chunk.from(Vector.fill(batchSize)(frame))

  /** Old `Transport.coalesce`: a fresh array sized to the whole batch per drain. */
  @Benchmark
  def coalesceFreshArray(bh: Blackhole): Unit =
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
    bh.consume(Chunk.array(arr))

  /** New `WriteBuffer.fill`: copy into the reused buffer, grow only on overflow. */
  @Benchmark
  def reusedBuffer(bh: Blackhole): Unit =
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
    bh.consume(Chunk.array(buf, 0, total))
