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
import fs2.nats.protocol.ProtocolParser
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

/** Micro-benchmark for the inbound parser (Tier 1 in-place findCrlf + Tier 2.1
  * byte-level tokenizer).
  *
  * `frameCount` frames are serialized once at setup and fed through the parser
  * in ~8 KB chunks, mirroring how `socket.reads` delivers data. This exercises
  * the per-frame control-line scan and tokenization on the receive hot path.
  *
  * The `shape` parameter brackets what the control-line parser has to do per
  * frame, since both tokenization and dispatch cost scale with the token count:
  * `msg` is a plain delivery (4 tokens), `msgReply` adds the reply subject (5
  * tokens, the request/reply and JetStream delivery shape) and `hmsg` is the
  * worst case (6 tokens, three numbers to parse, plus a header block).
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run .*ProtocolParserBenchmark.*"
  *   sbt "benchmarks/Jmh/run -prof gc .*ProtocolParserBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class ProtocolParserBenchmark:

  @Param(Array("16", "256"))
  var payloadSize: Int = 16

  @Param(Array("msg", "msgReply", "hmsg"))
  var shape: String = "msg"

  private val frameCount = 2000

  // A JetStream-style header block: version line plus three headers. ASCII
  // only, so its byte length is its String length.
  private val headerBlock =
    "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Sequence: 12345\r\nNats-Time-Stamp: 2025-01-01T00:00:00Z\r\n\r\n"

  // ~8 KB read chunks, like fs2-io socket.reads.
  private var chunks: List[Chunk[Byte]] = Nil

  @Setup
  def setup(): Unit =
    val payload = "x" * payloadSize
    val sb = new java.lang.StringBuilder
    var i = 0
    while i < frameCount do
      shape match
        case "msg" =>
          sb.append("MSG events.orders.")
            .append(i % 100)
            .append(' ')
            .append(i)
            .append(' ')
            .append(payloadSize)
            .append("\r\n")
        case "msgReply" =>
          sb.append("MSG events.orders.")
            .append(i % 100)
            .append(' ')
            .append(i)
            .append(" _INBOX.")
            .append(i)
            .append(' ')
            .append(payloadSize)
            .append("\r\n")
        case _ =>
          // HMSG carries both the header length and the total (header + body)
          // length, and the header block is sent ahead of the payload.
          sb.append("HMSG events.orders.")
            .append(i % 100)
            .append(' ')
            .append(i)
            .append(" _INBOX.")
            .append(i)
            .append(' ')
            .append(headerBlock.length)
            .append(' ')
            .append(headerBlock.length + payloadSize)
            .append("\r\n")
            .append(headerBlock)
      sb.append(payload).append("\r\n")
      i += 1
    val bytes = sb.toString.getBytes(StandardCharsets.UTF_8)
    chunks = bytes.grouped(8192).map(Chunk.array(_)).toList

  @Benchmark
  def parse(bh: Blackhole): Unit =
    val count = Stream
      .emits(chunks)
      .unchunks
      .through(ProtocolParser.parseStream[IO])
      .compile
      .count
      .unsafeRunSync()
    bh.consume(count)
