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

// Declared in the library's package so the benchmark can reach the
// package-private `JsAckParser` (the per-message consume hot path).
package fs2.nats.jetstream

import fs2.nats.jetstream.protocol.PubAck
import io.circe.parser.decode
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit

/** Micro-benchmark for the per-message JetStream hot paths: parsing the
  * `$JS.ACK` reply subject into metadata (every consumed message) and decoding
  * a `PubAck` JSON (every publish).
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*JetStreamBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class JetStreamBenchmark:

  // V1 (9 tokens) and V2 (12 tokens, current server) ack subjects.
  private val ackV1 =
    "$JS.ACK.ORDERS.workers.1.10.5.1700000000000000000.3"
  private val ackV2 =
    "$JS.ACK.hub.ACCHASH.ORDERS.workers.1.10.5.1700000000000000000.3.rnd"

  private val pubAckJson =
    """{"stream":"ORDERS","seq":12345,"domain":"hub"}"""

  @Benchmark
  def parseAckV1(): Either[String, JsMetadata] =
    JsAckParser.parse(ackV1)

  @Benchmark
  def parseAckV2(): Either[String, JsMetadata] =
    JsAckParser.parse(ackV2)

  @Benchmark
  def decodePubAck(): Either[io.circe.Error, PubAck] =
    decode[PubAck](pubAckJson)
