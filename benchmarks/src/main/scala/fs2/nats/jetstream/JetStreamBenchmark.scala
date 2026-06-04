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

import com.github.plokhotnyuk.jsoniter_scala.core.*
import fs2.nats.jetstream.protocol.*
import fs2.nats.protocol.Connect
import org.openjdk.jmh.annotations.*

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Micro-benchmark for the JetStream JSON hot paths and the `$JS.ACK` subject
  * parser. Covers encode and decode of the per-message and management types so
  * the circe→jsoniter migration can be measured before/after with `-prof gc`.
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

  // ---- $JS.ACK subject parsing (not JSON; kept for continuity) ----

  // V1 (9 tokens) and V2 (12 tokens, current server) ack subjects.
  private val ackV1 =
    "$JS.ACK.ORDERS.workers.1.10.5.1700000000000000000.3"
  private val ackV2 =
    "$JS.ACK.hub.ACCHASH.ORDERS.workers.1.10.5.1700000000000000000.3.rnd"

  @Benchmark
  def parseAckV1(): Either[String, JsMetadata] =
    JsAckParser.parse(ackV1)

  @Benchmark
  def parseAckV2(): Either[String, JsMetadata] =
    JsAckParser.parse(ackV2)

  // ---- Decode (readFromArray) ----

  private val pubAckJson =
    """{"stream":"ORDERS","seq":12345,"domain":"hub"}""".getBytes(UTF_8)

  private val infoJson =
    """{"server_id":"NABC","server_name":"n1","version":"2.10.0","proto":1,"go":"go1.21","host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,"client_id":5,"jetstream":true,"connect_urls":["10.0.0.1:4222","10.0.0.2:4222"]}""".getBytes(
      UTF_8
    )

  private val streamInfoJson =
    """{"config":{"name":"ORDERS","subjects":["orders.*"],"retention":"limits","storage":"file","max_consumers":-1,"max_msgs":-1,"max_bytes":-1,"max_msg_size":-1,"max_msgs_per_subject":-1,"num_replicas":1,"discard":"old","compression":"none","allow_direct":false},"state":{"messages":1000,"bytes":204800,"first_seq":1,"last_seq":1000,"consumer_count":3},"created":"2024-01-15T10:30:00Z"}""".getBytes(
      UTF_8
    )

  private val consumerInfoJson =
    """{"stream_name":"ORDERS","name":"workers","created":"2024-01-15T10:30:00Z","config":{"durable_name":"workers","ack_policy":"explicit","deliver_policy":"all","replay_policy":"instant","ack_wait":30000000000,"max_deliver":5},"delivered":{"consumer_seq":100,"stream_seq":500},"ack_floor":{"consumer_seq":99,"stream_seq":499},"num_pending":50,"num_ack_pending":1,"num_redelivered":0,"num_waiting":0}""".getBytes(
      UTF_8
    )

  private val storedMessageJson =
    val data = Base64.getEncoder.encodeToString("hello world".getBytes(UTF_8))
    val hdrs = Base64.getEncoder
      .encodeToString("NATS/1.0\r\nX-Order: 42\r\n\r\n".getBytes(UTF_8))
    s"""{"subject":"ORDERS.new","seq":42,"hdrs":"$hdrs","data":"$data","time":"2024-01-15T10:30:00.123456789Z"}""".getBytes(
      UTF_8
    )

  @Benchmark
  def decodePubAck(): PubAck =
    readFromArray[PubAck](pubAckJson)

  @Benchmark
  def decodeInfo(): fs2.nats.protocol.Info =
    readFromArray[fs2.nats.protocol.Info](infoJson)

  @Benchmark
  def decodeStreamInfo(): StreamInfo =
    readFromArray[StreamInfo](streamInfoJson)

  @Benchmark
  def decodeConsumerInfo(): ConsumerInfo =
    readFromArray[ConsumerInfo](consumerInfoJson)

  @Benchmark
  def decodeStoredMessage(): StoredMessage =
    readFromArray[StoredMessage](storedMessageJson)

  // ---- Encode (writeToArray) ----

  private val pullRequest =
    PullRequest(
      batch = 100,
      expires = 30.seconds,
      maxBytes = Some(1024 * 1024),
      idleHeartbeat = Some(5.seconds)
    )

  private val streamConfig =
    StreamConfig(
      name = "ORDERS",
      subjects = List("orders.*", "orders.new"),
      retention = RetentionPolicy.Limits,
      storage = StorageType.File,
      maxAge = Some(24.hours),
      duplicateWindow = Some(2.minutes),
      description = Some("orders stream")
    )

  private val consumerConfig =
    ConsumerConfig(
      durable = Some("workers"),
      ackPolicy = AckPolicy.Explicit,
      ackWait = Some(30.seconds),
      maxDeliver = 5,
      backoff = List(1.second, 5.seconds),
      filterSubject = Some("orders.new"),
      maxAckPending = 100
    )

  private val connect = Connect(name = Some("bench"))

  @Benchmark
  def encodePullRequest(): Array[Byte] =
    writeToArray(pullRequest)

  @Benchmark
  def encodeStreamConfig(): Array[Byte] =
    writeToArray(streamConfig)

  @Benchmark
  def encodeConsumerConfig(): Array[Byte] =
    writeToArray(consumerConfig)

  @Benchmark
  def encodeConnect(): Array[Byte] =
    writeToArray(connect)
