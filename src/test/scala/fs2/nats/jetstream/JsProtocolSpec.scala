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

package fs2.nats.jetstream.protocol

import com.github.plokhotnyuk.jsoniter_scala.core.*
import munit.FunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import scala.concurrent.duration.*

class JsProtocolSpec extends FunSuite:

  test("enums encode to their wire strings") {
    assertEquals(
      writeToString[RetentionPolicy](RetentionPolicy.WorkQueue),
      "\"workqueue\""
    )
    assertEquals(writeToString[StorageType](StorageType.Memory), "\"memory\"")
    assertEquals(
      writeToString[DeliverPolicy](DeliverPolicy.ByStartSequence),
      "\"by_start_sequence\""
    )
    assertEquals(
      writeToString[DeliverPolicy](DeliverPolicy.LastPerSubject),
      "\"last_per_subject\""
    )
    assertEquals(writeToString[AckPolicy](AckPolicy.Explicit), "\"explicit\"")
    assertEquals(writeToString[StoreCompression](StoreCompression.S2), "\"s2\"")
  }

  test("enum decode rejects unknown values") {
    intercept[JsonReaderException](readFromString[RetentionPolicy]("\"bogus\""))
  }

  test("StreamConfig round-trips through JSON") {
    val cfg = StreamConfig(
      name = "ORDERS",
      subjects = List("orders.*", "orders.new"),
      retention = RetentionPolicy.WorkQueue,
      storage = StorageType.Memory,
      maxConsumers = 5,
      maxMsgs = 1000L,
      maxBytes = 1048576L,
      maxAge = Some(1.hour),
      maxMsgSize = 4096,
      maxMsgsPerSubject = 10L,
      replicas = 3,
      discard = DiscardPolicy.New,
      duplicateWindow = Some(2.minutes),
      description = Some("orders stream"),
      compression = StoreCompression.S2,
      allowDirect = true
    )
    assertEquals(readFromString[StreamConfig](writeToString(cfg)), cfg)
  }

  test("durations encode as int64 nanoseconds") {
    val json = writeToString(StreamConfig("S", maxAge = Some(1.second)))
    assert(
      json.contains("\"max_age\":1000000000"),
      clue = json
    )
  }

  test("optional fields are omitted when None") {
    val json = writeToString(StreamConfig("S"))
    assert(!json.contains("max_age"), clue = json)
    assert(!json.contains("duplicate_window"), clue = json)
    assert(!json.contains("description"), clue = json)
    // required fields still present
    assert(json.contains("\"name\":\"S\""), clue = json)
    assert(json.contains("\"retention\":\"limits\""), clue = json)
  }

  test("StreamConfig omits the KV flags by default (byte-stability)") {
    val json = writeToString(StreamConfig("S"))
    assert(!json.contains("allow_rollup_hdrs"), clue = json)
    assert(!json.contains("deny_delete"), clue = json)
    assert(!json.contains("discard_new_per_subject"), clue = json)
  }

  test("StreamConfig emits the KV flags when set and round-trips") {
    val cfg = StreamConfig(
      name = "KV_config",
      subjects = List("$KV.config.>"),
      allowRollupHdrs = true,
      denyDelete = true,
      discardNewPerSubject = true
    )
    val json = writeToString(cfg)
    assert(json.contains("\"allow_rollup_hdrs\":true"), clue = json)
    assert(json.contains("\"deny_delete\":true"), clue = json)
    assert(json.contains("\"discard_new_per_subject\":true"), clue = json)
    assertEquals(readFromString[StreamConfig](json), cfg)
  }

  test("StreamConfig omits the advanced fields by default (byte-stability)") {
    val json = writeToString(StreamConfig("S"))
    assert(!json.contains("mirror"), clue = json)
    assert(!json.contains("\"sources\""), clue = json)
    assert(!json.contains("republish"), clue = json)
    assert(!json.contains("subject_transform"), clue = json)
    assert(!json.contains("placement"), clue = json)
    assert(!json.contains("\"sealed\""), clue = json)
  }

  test("StreamConfig emits the advanced fields when set and round-trips") {
    val cfg = StreamConfig(
      name = "AGG",
      subjects = List("agg.>"),
      mirror = Some(
        StreamSource(
          "SRC",
          optStartSeq = Some(10L),
          filterSubject = Some("a.>")
        )
      ),
      sources = List(
        StreamSource("S1"),
        StreamSource(
          "S2",
          subjectTransforms = List(SubjectTransform("x.>", "y.>"))
        )
      ),
      republish = Some(Republish("agg.>", "out.>", headersOnly = true)),
      subjectTransform = Some(SubjectTransform("in.>", "agg.>")),
      placement =
        Some(Placement(cluster = Some("c1"), tags = List("ssd", "fast"))),
      sealedStream = true
    )
    val json = writeToString(cfg)
    assert(json.contains("\"mirror\""), clue = json)
    assert(json.contains("\"sources\""), clue = json)
    assert(json.contains("\"subject_transforms\""), clue = json)
    assert(json.contains("\"republish\""), clue = json)
    assert(json.contains("\"subject_transform\""), clue = json)
    assert(json.contains("\"placement\""), clue = json)
    assert(json.contains("\"sealed\":true"), clue = json)
    assertEquals(readFromString[StreamConfig](json), cfg)
  }

  test("PubAck decodes including duplicate flag") {
    assertEquals(
      readFromString[PubAck](
        """{"stream":"ORDERS","seq":7,"duplicate":true}"""
      ),
      PubAck("ORDERS", 7L, duplicate = true, domain = None)
    )
    assertEquals(
      readFromString[PubAck]("""{"stream":"ORDERS","seq":1}"""),
      PubAck("ORDERS", 1L, duplicate = false, domain = None)
    )
  }

  test("ApiError decodes from the error envelope") {
    val env = readFromString[ApiErrorEnvelope](
      """{"type":"io.nats.jetstream.api.v1.pub_ack_response","error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"}}"""
    )
    assertEquals(
      env.error,
      Some(ApiError(400, 10071, "wrong last sequence: 5"))
    )
  }

  test("StoredMessage decodes base64 data and header block") {
    val data = Base64.getEncoder.encodeToString("hello".getBytes(UTF_8))
    val hdrs = Base64.getEncoder
      .encodeToString("NATS/1.0\r\nX-Order: 42\r\n\r\n".getBytes(UTF_8))
    val json =
      s"""{"subject":"ORDERS.new","seq":3,"hdrs":"$hdrs","data":"$data","time":"2024-01-15T10:30:00.123456789Z"}"""
    val sm = readFromString[StoredMessage](json)
    assertEquals(sm.subject, "ORDERS.new")
    assertEquals(sm.seq, 3L)
    assertEquals(new String(sm.data.toArray, UTF_8), "hello")
    assertEquals(sm.headers.get("X-Order"), Some("42"))
  }

  test("StreamInfo decodes nested config + state + created") {
    val json =
      """{"config":{"name":"ORDERS","subjects":["orders.*"],"retention":"limits","storage":"file"},"state":{"messages":10,"bytes":2048,"first_seq":1,"last_seq":10,"consumer_count":2},"created":"2024-01-15T10:30:00Z"}"""
    val info = readFromString[StreamInfo](json)
    assertEquals(info.config.name, "ORDERS")
    assertEquals(info.config.subjects, List("orders.*"))
    assertEquals(info.state.messages, 10L)
    assertEquals(info.state.lastSeq, 10L)
    assertEquals(info.state.consumerCount, 2)
  }

  test("ConsumerConfig round-trips through JSON") {
    val cfg = ConsumerConfig(
      durable = Some("workers"),
      description = Some("worker consumer"),
      deliverPolicy = DeliverPolicy.New,
      ackPolicy = AckPolicy.Explicit,
      ackWait = Some(30.seconds),
      maxDeliver = 5,
      backoff = List(1.second, 5.seconds),
      filterSubject = Some("orders.new"),
      replayPolicy = ReplayPolicy.Instant,
      maxAckPending = 100,
      inactiveThreshold = Some(5.minutes)
    )
    assertEquals(readFromString[ConsumerConfig](writeToString(cfg)), cfg)
  }

  test("ConsumerConfig encodes durable_name and omits -1 sentinels") {
    val json = writeToString(ConsumerConfig(durable = Some("d")))
    assert(json.contains("\"durable_name\":\"d\""), clue = json)
    assert(json.contains("\"ack_policy\":\"explicit\""), clue = json)
    assert(!json.contains("max_deliver"), clue = json)
    assert(!json.contains("max_ack_pending"), clue = json)
    assert(!json.contains("max_waiting"), clue = json)
  }

  test("ConsumerConfig filter_subjects suppresses filter_subject") {
    val json = writeToString(
      ConsumerConfig(
        filterSubject = Some("a"),
        filterSubjects = List("a", "b")
      )
    )
    assert(!json.contains("\"filter_subject\":"), clue = json)
    assert(json.contains("\"filter_subjects\":[\"a\",\"b\"]"), clue = json)
  }

  test("PullRequest encodes batch, expires (ns), and optional fields") {
    val json = writeToString(
      PullRequest(
        batch = 100,
        expires = 30.seconds,
        maxBytes = Some(1024),
        noWait = false,
        idleHeartbeat = Some(5.seconds)
      )
    )
    assert(json.contains("\"batch\":100"), clue = json)
    assert(json.contains("\"expires\":30000000000"), clue = json)
    assert(json.contains("\"max_bytes\":1024"), clue = json)
    assert(json.contains("\"idle_heartbeat\":5000000000"), clue = json)
    assert(!json.contains("no_wait"), clue = json)
  }

  test("PullRequest encodes no_wait when true") {
    val json = writeToString(PullRequest(1, 1.second, noWait = true))
    assert(json.contains("\"no_wait\":true"), clue = json)
  }

  test("ConsumerInfo decodes sequences and counts") {
    val json =
      """{"stream_name":"ORDERS","name":"workers","created":"2024-01-15T10:30:00Z","config":{"durable_name":"workers","ack_policy":"explicit","deliver_policy":"all","replay_policy":"instant"},"delivered":{"consumer_seq":3,"stream_seq":10},"ack_floor":{"consumer_seq":2,"stream_seq":9},"num_pending":7,"num_ack_pending":1,"num_redelivered":0,"num_waiting":0}"""
    val info = readFromString[ConsumerInfo](json)
    assertEquals(info.stream, "ORDERS")
    assertEquals(info.name, "workers")
    assertEquals(info.delivered, SequencePair(3L, 10L))
    assertEquals(info.ackFloor, SequencePair(2L, 9L))
    assertEquals(info.numPending, 7L)
  }
