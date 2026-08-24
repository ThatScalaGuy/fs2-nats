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
import fs2.nats.protocol.Headers
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

/** Micro-benchmark for NATS/1.0 header-block parsing in isolation.
  *
  * `ProtocolParserBenchmark shape=hmsg` measures this end to end, mixed in with
  * the control-line scan and the payload copy; this one isolates the block
  * parse so the allocation profile of `Headers.parseWithStatus` can be read
  * directly.
  *
  * `block` brackets what actually arrives on the wire: `js` is a JetStream
  * delivery (version line plus three headers), `status` is a control message
  * (idle heartbeat, 404/408/409, 503 — a status line and no entries at all,
  * which is most of the header traffic on a pull consumer) and `plain` is a
  * bare header block.
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run .*HeadersBenchmark.*"
  *   sbt "benchmarks/Jmh/run -prof gc .*HeadersBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class HeadersBenchmark:

  @Param(Array("js", "status", "plain"))
  var block: String = "js"

  private var text: String = ""
  private var chunk: Chunk[Byte] = Chunk.empty

  @Setup
  def setup(): Unit =
    text = block match
      case "js" =>
        "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Sequence: 12345\r\n" +
          "Nats-Time-Stamp: 2025-01-01T00:00:00Z\r\n\r\n"
      case "status" => "NATS/1.0 100 Idle Heartbeat\r\n\r\n"
      case _        => "NATS/1.0\r\n\r\n"
    chunk = Chunk.array(text.getBytes(StandardCharsets.UTF_8))

  @Benchmark
  def parseWithStatusChunk(bh: Blackhole): Unit =
    bh.consume(Headers.parseWithStatus(chunk))

  @Benchmark
  def parseWithStatusString(bh: Blackhole): Unit =
    bh.consume(Headers.parseWithStatus(text))
