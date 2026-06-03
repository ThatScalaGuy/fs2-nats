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
import fs2.nats.publish.SerializationUtils
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit

/** Micro-benchmark for frame serialization (Tier 1 — single-array build).
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*SerializationBenchmark.*"
  * }}}
  * The `-prof gc` profiler reports bytes allocated per op, which is the metric
  * the single-array rewrite targets.
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class SerializationBenchmark:

  private val subject = "events.orders.created"
  private val replyTo = Some("_INBOX.9f3c1a2b4d5e6f70")
  private val payload16 = Chunk.array(Array.fill[Byte](16)('x'.toByte))
  private val payload256 = Chunk.array(Array.fill[Byte](256)('x'.toByte))
  private val headerBytes =
    Headers("X-Request-Id" -> "9f3c1a2b", "X-Trace" -> "00-1234abcd").toBytes

  @Benchmark
  def buildPub_16(): Chunk[Byte] =
    SerializationUtils.buildPub(subject, None, payload16)

  @Benchmark
  def buildPub_256(): Chunk[Byte] =
    SerializationUtils.buildPub(subject, None, payload256)

  @Benchmark
  def buildPub_replyTo_16(): Chunk[Byte] =
    SerializationUtils.buildPub(subject, replyTo, payload16)

  @Benchmark
  def buildHPub_16(): Chunk[Byte] =
    SerializationUtils.buildHPub(subject, None, headerBytes, payload16)
