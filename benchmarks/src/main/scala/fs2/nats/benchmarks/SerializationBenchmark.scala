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
import fs2.nats.transport.Outgoing
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

  // --- P1.4: the descriptor path replaces each buildPub/buildHPub above. It
  // allocates only the small control-line header + the descriptor object and
  // references the existing payload Chunk, so its bytes/op is ~constant in the
  // payload size (compare pubDescriptor_16 vs pubDescriptor_256 against
  // buildPub_16 vs buildPub_256). The payload copy that buildPub did is moved
  // to the writer's reused buffer and so is no longer allocated here. ---

  @Benchmark
  def pubDescriptor_16(): Outgoing =
    Outgoing.Pub(
      SerializationUtils.buildPubHeader(subject, None, payload16.size),
      payload16
    )

  @Benchmark
  def pubDescriptor_256(): Outgoing =
    Outgoing.Pub(
      SerializationUtils.buildPubHeader(subject, None, payload256.size),
      payload256
    )

  @Benchmark
  def pubDescriptor_replyTo_16(): Outgoing =
    Outgoing.Pub(
      SerializationUtils.buildPubHeader(subject, replyTo, payload16.size),
      payload16
    )

  @Benchmark
  def hpubDescriptor_16(): Outgoing =
    Outgoing.HPub(
      SerializationUtils.buildHPubHeader(
        subject,
        None,
        headerBytes.size,
        headerBytes.size + payload16.size
      ),
      headerBytes,
      payload16
    )
