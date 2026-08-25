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

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.{Chunk, Stream}
import fs2.nats.protocol.{Frame, Headers}
import fs2.nats.subscriptions.NatsMessage
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit

/** Isolates the receive-path *dispatch* shape in `NatsClient.frameProcessor`:
  * what fs2 charges to hand each parsed frame to `handleFrame`.
  *
  * `ProtocolParserBenchmark` ends in `.compile.count`, which is `foldChunks`
  * and never touches an element, so it measures the parser's own per-step cost
  * and none of the dispatch cost. This does the opposite — pre-built frames, no
  * parsing, one `IO` per frame:
  *
  *   - `perFrameEvalMap` is today's shape, whose mapped `Unit` output
  *     `.compile.drain` throws away.
  *   - `perFrameForeach` drops that discarded output but keeps per-element
  *     dispatch.
  *   - `chunked` is what ships: `.chunks.foreach` with an indexed `flatMap`
  *     chain over the chunk.
  *
  * `chunkSize` is how many frames the parser emits per socket read. `1` is an
  * idle connection or a byte-drip transport, and is the case that shows
  * chunk-aware dispatch is a small *regression* on its own — which is why the
  * two halves of the change ship together. `173` is a 64 KiB read of 16-byte
  * MSG deliveries.
  *
  * Run with `-prof gc` and read `gc.alloc.rate.norm` (bytes/op): as in
  * `SubscriptionLookupBenchmark`, `unsafeRunSync` dominates wall time and
  * carries a fixed fiber+latch floor, so compare deltas between the shapes, not
  * absolute numbers.
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*FrameDispatchBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class FrameDispatchBenchmark:

  @Param(Array("1", "173"))
  var chunkSize: Int = 173

  private val frameCount = 2000

  private var chunks: List[Chunk[Frame]] = Nil

  @Setup
  def setup(): Unit =
    val payload = Chunk.array(Array.fill[Byte](16)('x'.toByte))
    val frames = (0 until frameCount).toList.map { i =>
      NatsMessage(
        s"events.orders.${i % 100}",
        None,
        Headers.empty,
        payload,
        i.toLong
      ): Frame
    }
    chunks = frames.grouped(chunkSize).map(Chunk.from).toList

  private def source: Stream[IO, Frame] =
    Stream.emits(chunks).unchunks

  @Benchmark
  def perFrameEvalMap(bh: Blackhole): Unit =
    source.evalMap(f => IO(bh.consume(f))).compile.drain.unsafeRunSync()

  @Benchmark
  def perFrameForeach(bh: Blackhole): Unit =
    source.foreach(f => IO(bh.consume(f))).compile.drain.unsafeRunSync()

  @Benchmark
  def chunked(bh: Blackhole): Unit =
    source.chunks
      .foreach { c =>
        val size = c.size
        def loop(i: Int): IO[Unit] =
          if i >= size then IO.unit
          else IO(bh.consume(c(i))) >> loop(i + 1)
        loop(0)
      }
      .compile
      .drain
      .unsafeRunSync()
