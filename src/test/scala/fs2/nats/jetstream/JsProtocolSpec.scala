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

import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.*
import munit.FunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import scala.concurrent.duration.*

class JsProtocolSpec extends FunSuite:

  test("enums encode to their wire strings") {
    assertEquals(
      (RetentionPolicy.WorkQueue: RetentionPolicy).asJson,
      Json.fromString("workqueue")
    )
    assertEquals(
      (StorageType.Memory: StorageType).asJson,
      Json.fromString("memory")
    )
    assertEquals(
      (DeliverPolicy.ByStartSequence: DeliverPolicy).asJson,
      Json.fromString("by_start_sequence")
    )
    assertEquals(
      (DeliverPolicy.LastPerSubject: DeliverPolicy).asJson,
      Json.fromString("last_per_subject")
    )
    assertEquals(
      (AckPolicy.Explicit: AckPolicy).asJson,
      Json.fromString("explicit")
    )
    assertEquals(
      (StoreCompression.S2: StoreCompression).asJson,
      Json.fromString("s2")
    )
  }

  test("enum decode rejects unknown values") {
    assert(decode[RetentionPolicy]("\"bogus\"").isLeft)
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
    val decoded = decode[StreamConfig](cfg.asJson.noSpaces)
    assertEquals(decoded, Right(cfg))
  }

  test("durations encode as int64 nanoseconds") {
    val cfg = StreamConfig("S", maxAge = Some(1.second))
    val json = cfg.asJson
    assertEquals(json.hcursor.downField("max_age").as[Long], Right(1000000000L))
  }

  test("optional fields are omitted when None") {
    val json = StreamConfig("S").asJson
    val obj = json.asObject.get
    assertEquals(obj("max_age"), None)
    assertEquals(obj("duplicate_window"), None)
    assertEquals(obj("description"), None)
    // required fields still present
    assertEquals(obj("name"), Some(Json.fromString("S")))
    assertEquals(obj("retention"), Some(Json.fromString("limits")))
  }

  test("PubAck decodes including duplicate flag") {
    assertEquals(
      decode[PubAck]("""{"stream":"ORDERS","seq":7,"duplicate":true}"""),
      Right(PubAck("ORDERS", 7L, duplicate = true, domain = None))
    )
    assertEquals(
      decode[PubAck]("""{"stream":"ORDERS","seq":1}"""),
      Right(PubAck("ORDERS", 1L, duplicate = false, domain = None))
    )
  }

  test("ApiError decodes from the error envelope") {
    val json = io.circe.parser
      .parse(
        """{"type":"io.nats.jetstream.api.v1.pub_ack_response","error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"}}"""
      )
      .toOption
      .get
    assertEquals(
      json.hcursor.downField("error").as[ApiError],
      Right(ApiError(400, 10071, "wrong last sequence: 5"))
    )
  }

  test("StoredMessage decodes base64 data and header block") {
    val data = Base64.getEncoder.encodeToString("hello".getBytes(UTF_8))
    val hdrs = Base64.getEncoder
      .encodeToString("NATS/1.0\r\nX-Order: 42\r\n\r\n".getBytes(UTF_8))
    val json =
      s"""{"subject":"ORDERS.new","seq":3,"hdrs":"$hdrs","data":"$data","time":"2024-01-15T10:30:00.123456789Z"}"""
    val sm = decode[StoredMessage](json).toOption.get
    assertEquals(sm.subject, "ORDERS.new")
    assertEquals(sm.seq, 3L)
    assertEquals(new String(sm.data.toArray, UTF_8), "hello")
    assertEquals(sm.headers.get("X-Order"), Some("42"))
  }

  test("StreamInfo decodes nested config + state + created") {
    val json =
      """{"config":{"name":"ORDERS","subjects":["orders.*"],"retention":"limits","storage":"file"},"state":{"messages":10,"bytes":2048,"first_seq":1,"last_seq":10,"consumer_count":2},"created":"2024-01-15T10:30:00Z"}"""
    val info = decode[StreamInfo](json).toOption.get
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
    assertEquals(decode[ConsumerConfig](cfg.asJson.noSpaces), Right(cfg))
  }

  test("ConsumerConfig encodes durable_name and omits -1 sentinels") {
    val obj = ConsumerConfig(durable = Some("d")).asJson.asObject.get
    assertEquals(obj("durable_name"), Some(Json.fromString("d")))
    assertEquals(obj("ack_policy"), Some(Json.fromString("explicit")))
    assertEquals(obj("max_deliver"), None)
    assertEquals(obj("max_ack_pending"), None)
    assertEquals(obj("max_waiting"), None)
  }

  test("ConsumerConfig filter_subjects suppresses filter_subject") {
    val obj = ConsumerConfig(
      filterSubject = Some("a"),
      filterSubjects = List("a", "b")
    ).asJson.asObject.get
    assertEquals(obj("filter_subject"), None)
    assertEquals(
      obj("filter_subjects"),
      Some(Json.arr(Json.fromString("a"), Json.fromString("b")))
    )
  }

  test("PullRequest encodes batch, expires (ns), and optional fields") {
    val obj = PullRequest(
      batch = 100,
      expires = 30.seconds,
      maxBytes = Some(1024),
      noWait = false,
      idleHeartbeat = Some(5.seconds)
    ).asJson.asObject.get
    assertEquals(obj("batch"), Some(Json.fromInt(100)))
    assertEquals(obj("expires"), Some(Json.fromLong(30000000000L)))
    assertEquals(obj("max_bytes"), Some(Json.fromInt(1024)))
    assertEquals(obj("idle_heartbeat"), Some(Json.fromLong(5000000000L)))
    assertEquals(obj("no_wait"), None)
  }

  test("PullRequest encodes no_wait when true") {
    val obj = PullRequest(1, 1.second, noWait = true).asJson.asObject.get
    assertEquals(obj("no_wait"), Some(Json.fromBoolean(true)))
  }

  test("ConsumerInfo decodes sequences and counts") {
    val json =
      """{"stream_name":"ORDERS","name":"workers","created":"2024-01-15T10:30:00Z","config":{"durable_name":"workers","ack_policy":"explicit","deliver_policy":"all","replay_policy":"instant"},"delivered":{"consumer_seq":3,"stream_seq":10},"ack_floor":{"consumer_seq":2,"stream_seq":9},"num_pending":7,"num_ack_pending":1,"num_redelivered":0,"num_waiting":0}"""
    val info = decode[ConsumerInfo](json).toOption.get
    assertEquals(info.stream, "ORDERS")
    assertEquals(info.name, "workers")
    assertEquals(info.delivered, SequencePair(3L, 10L))
    assertEquals(info.ackFloor, SequencePair(2L, 9L))
    assertEquals(info.numPending, 7L)
  }
