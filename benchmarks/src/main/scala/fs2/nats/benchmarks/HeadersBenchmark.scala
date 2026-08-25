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

/** Micro-benchmark for NATS/1.0 header blocks in isolation — both directions.
  *
  * `ProtocolParserBenchmark shape=hmsg` measures the parse end to end, mixed in
  * with the control-line scan and the payload copy; this one isolates the block
  * parse so the allocation profile of `Headers.parseWithStatus` can be read
  * directly. `toBytes`/`byteLength` serialize the same block the parse side
  * reads, so the two halves are directly comparable within one JVM run —
  * `toBytes` has exactly one production call site (`Publisher.publishWithHeaders`)
  * and nothing in the repo measured it before.
  *
  * `block` brackets what actually arrives on the wire: `js` is a JetStream
  * delivery (version line plus three headers), `status` is a control message
  * (idle heartbeat, 404/408/409, 503 — a status line and no entries at all,
  * which is most of the header traffic on a pull consumer) and `plain` is a
  * bare header block. `status` and `plain` therefore have no entries, so on
  * those two the serialize benchmarks measure the empty fast exit — a control,
  * not a shape. `utf8` carries multi-byte values: the other three fixtures are
  * pure ASCII, which is exactly the case a hand-rolled encoder gets right by
  * accident.
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

  @Param(Array("js", "status", "plain", "utf8", "bigAscii"))
  var block: String = "js"

  private var text: String = ""
  private var chunk: Chunk[Byte] = Chunk.empty
  private var headers: Headers = Headers.empty

  @Setup
  def setup(): Unit =
    text = block match
      case "js" =>
        "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Sequence: 12345\r\n" +
          "Nats-Time-Stamp: 2025-01-01T00:00:00Z\r\n\r\n"
      case "status" => "NATS/1.0 100 Idle Heartbeat\r\n\r\n"
      case "utf8"   =>
        "NATS/1.0\r\nNats-Subject: orders.münchen\r\nX-Note: テスト\r\n\r\n"
      // A long ASCII value — a bearer token or a base64 signature. `toBytes`
      // writes ASCII a char at a time, where the StringBuilder route it
      // replaces bulk-copied; this is the shape where that could lose.
      case "bigAscii" =>
        "NATS/1.0\r\nAuthorization: Bearer " + ("QWxhZGRpbjpvcGVuc2VzYW1l" * 80) +
          "\r\nX-Trace: " + ("0123456789abcdef" * 8) + "\r\n\r\n"
      case _ => "NATS/1.0\r\n\r\n"
    chunk = Chunk.array(text.getBytes(StandardCharsets.UTF_8))
    headers = Headers.parseWithStatus(text).toOption.get._3

  @Benchmark
  def parseWithStatusChunk(bh: Blackhole): Unit =
    bh.consume(Headers.parseWithStatus(chunk))

  @Benchmark
  def parseWithStatusString(bh: Blackhole): Unit =
    bh.consume(Headers.parseWithStatus(text))

  @Benchmark
  def toBytes(bh: Blackhole): Unit =
    bh.consume(headers.toBytes)

  @Benchmark
  def byteLength(bh: Blackhole): Unit =
    bh.consume(headers.byteLength)

  /** The pre-#54 `toBytes`, reproduced locally so both shapes are timed in one
    * JVM run. The shipped version writes ASCII a char at a time where this one
    * bulk-copies twice, so this is the honest check that the char loop does not
    * lose on a long ASCII value even though it allocates far less.
    */
  @Benchmark
  def toBytesStringBuilder(bh: Blackhole): Unit =
    val entries = headers.entries
    val out =
      if entries.isEmpty then Chunk.empty
      else
        val sb = new StringBuilder
        sb.append("NATS/1.0")
        sb.append("\r\n")
        entries.foreach { case (k, v) =>
          sb.append(k)
          sb.append(": ")
          sb.append(v)
          sb.append("\r\n")
        }
        sb.append("\r\n")
        Chunk.array(sb.toString.getBytes(StandardCharsets.UTF_8))
    bh.consume(out)
