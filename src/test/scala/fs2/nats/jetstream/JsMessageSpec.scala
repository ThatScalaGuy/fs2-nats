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

package fs2.nats.jetstream

import fs2.Chunk
import munit.FunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import scala.concurrent.duration.*

class JsMessageSpec extends FunSuite:

  private val tsNanos = 1700000000000000000L

  test("parses a V1 (9-token) ACK subject") {
    val reply = s"$$JS.ACK.ORDERS.workers.1.10.5.$tsNanos.3"
    val meta = JsAckParser.parse(reply).toOption.get
    assertEquals(meta.stream, "ORDERS")
    assertEquals(meta.consumer, "workers")
    assertEquals(meta.numDelivered, 1L)
    assertEquals(meta.streamSeq, 10L)
    assertEquals(meta.consumerSeq, 5L)
    assertEquals(meta.numPending, 3L)
    assertEquals(meta.domain, None)
    assertEquals(meta.timestamp, Instant.ofEpochSecond(0L, tsNanos))
    assertEquals(meta.isRedelivery, false)
  }

  test("V1 and V2 (underscore domain) yield identical metadata") {
    val v1 = s"$$JS.ACK.ORDERS.workers.1.10.5.$tsNanos.3"
    val v2 = s"$$JS.ACK._.ACCHASH.ORDERS.workers.1.10.5.$tsNanos.3.rnd"
    assertEquals(JsAckParser.parse(v1), JsAckParser.parse(v2))
  }

  test("V2 with a domain captures domain and redelivery") {
    val v2 = s"$$JS.ACK.hub.ACCHASH.ORDERS.workers.2.10.5.$tsNanos.3.rnd"
    val meta = JsAckParser.parse(v2).toOption.get
    assertEquals(meta.domain, Some("hub"))
    assertEquals(meta.numDelivered, 2L)
    assert(meta.isRedelivery)
  }

  test("malformed ACK subjects fail") {
    // 10 tokens -> malformed
    assert(
      JsAckParser.parse(s"$$JS.ACK.ORDERS.workers.1.10.5.$tsNanos.3.10").isLeft
    )
    // wrong prefix
    assert(JsAckParser.parse("$JS.NOTACK.ORDERS.workers.1.10.5.5.3").isLeft)
    // too few
    assert(JsAckParser.parse("$JS.ACK.too.few").isLeft)
  }

  test("ack payload bytes are exact") {
    def str(c: Chunk[Byte]): String = new String(c.toArray, UTF_8)
    assertEquals(str(AckBytes.Ack), "+ACK")
    assertEquals(str(AckBytes.Nak), "-NAK")
    assertEquals(str(AckBytes.InProgress), "+WPI")
    assertEquals(str(AckBytes.Term), "+TERM")
    assertEquals(
      str(AckBytes.nakWithDelay(5.seconds)),
      """-NAK {"delay": 5000000000}"""
    )
    assertEquals(str(AckBytes.termWith("bad payload")), "+TERM bad payload")
  }

  test("PullStatus classifies control messages") {
    assertEquals(PullStatus.classify(None, None), PullSignal.Data)
    assertEquals(
      PullStatus.classify(Some(100), Some("Idle Heartbeat")),
      PullSignal.Heartbeat
    )
    assertEquals(
      PullStatus.classify(Some(404), Some("No Messages")),
      PullSignal.Complete
    )
    assertEquals(
      PullStatus.classify(Some(408), Some("Request Timeout")),
      PullSignal.Complete
    )
    assertEquals(
      PullStatus.classify(Some(409), Some("Batch Completed")),
      PullSignal.Complete
    )
    // Cluster events are transient for a durable consumer => complete + re-pull.
    assertEquals(
      PullStatus.classify(Some(409), Some("Server Shutdown")),
      PullSignal.Complete
    )
    assertEquals(
      PullStatus.classify(Some(409), Some("Leadership Change")),
      PullSignal.Complete
    )
  }

  test("PullStatus fails on terminal 409 / 400") {
    PullStatus.classify(Some(409), Some("Consumer Deleted")) match
      case PullSignal.Fail(409, _) => ()
      case other                   => fail(s"expected Fail(409), got $other")
    PullStatus.classify(Some(409), Some("Consumer is push based")) match
      case PullSignal.Fail(409, _) => ()
      case other                   => fail(s"expected Fail(409), got $other")
    PullStatus.classify(Some(400), Some("Bad Request")) match
      case PullSignal.Fail(400, _) => ()
      case other                   => fail(s"expected Fail(400), got $other")
  }
