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

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import fs2.nats.client.{BackoffConfig, ClientConfig, NatsClient}
import fs2.nats.jetstream.protocol.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Integration tests for pull consumption and ack semantics. Requires a
  * JetStream-enabled NATS server (docker-compose up -d).
  */
class PullIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast.copy(maxRetries = Some(3))
  )

  private def uniqueName: String = s"S${System.nanoTime()}"

  private def withPullConsumer[A](
      name: String
  )(f: (JetStream[IO], JsConsumer[IO]) => IO[A]): IO[A] =
    NatsClient.connect[IO](clientConfig).use { client =>
      client.jetStream().use { js =>
        (js.addStream(StreamConfig(name = name, subjects = List(s"$name.>"))) >>
          js.createConsumer(
            name,
            ConsumerConfig(
              durable = Some("workers"),
              ackPolicy = AckPolicy.Explicit,
              ackWait = Some(5.seconds)
            )
          ).flatMap(c => f(js, c)))
          .guarantee(js.deleteStream(name).attempt.void)
      }
    }

  private def publishN(js: JetStream[IO], subject: String, n: Int): IO[Unit] =
    (1 to n).toList.traverse_(i =>
      js.publish(subject, Chunk.array(s"m$i".getBytes))
    )

  test("fetch returns published messages in order") {
    val name = uniqueName
    withPullConsumer(name) { (js, c) =>
      for
        _ <- publishN(js, s"$name.a", 5)
        msgs <- c.fetch(5, 2.seconds)
        _ <- msgs.traverse_(_.ack)
      yield
        assertEquals(msgs.size, 5)
        assertEquals(
          msgs.map(_.payloadAsString).toList,
          List("m1", "m2", "m3", "m4", "m5")
        )
        assertEquals(msgs.toList.head.metadata.streamSeq, 1L)
    }
  }

  test("ack advances the ack floor") {
    val name = uniqueName
    withPullConsumer(name) { (js, c) =>
      for
        _ <- publishN(js, s"$name.a", 3)
        msgs <- c.fetch(3, 2.seconds)
        _ <- msgs.traverse_(_.ackSync)
        info <- c.info
      yield
        assertEquals(msgs.size, 3)
        assertEquals(info.ackFloor.streamSeq, 3L)
        assertEquals(info.numAckPending, 0L)
    }
  }

  test("nak triggers redelivery with an incremented delivery count") {
    val name = uniqueName
    withPullConsumer(name) { (js, c) =>
      for
        _ <- publishN(js, s"$name.a", 1)
        first <- c.fetch(1, 2.seconds)
        _ <- first.traverse_(_.nak)
        second <- c.fetch(1, 2.seconds)
        _ <- second.traverse_(_.ack)
      yield
        assertEquals(first.toList.head.metadata.numDelivered, 1L)
        assertEquals(second.toList.head.metadata.numDelivered, 2L)
    }
  }

  test("term prevents redelivery") {
    val name = uniqueName
    withPullConsumer(name) { (js, c) =>
      for
        _ <- publishN(js, s"$name.a", 1)
        first <- c.fetch(1, 2.seconds)
        _ <- first.traverse_(_.term)
        again <- c.fetchNoWait(5)
      yield
        assertEquals(first.size, 1)
        assertEquals(again.size, 0)
    }
  }

  test("fetchNoWait returns empty when nothing is buffered") {
    val name = uniqueName
    withPullConsumer(name) { (_, c) =>
      c.fetchNoWait(10).map(msgs => assertEquals(msgs.size, 0))
    }
  }

  test("consume drains a continuous stream") {
    val name = uniqueName
    withPullConsumer(name) { (js, c) =>
      for
        _ <- publishN(js, s"$name.a", 10)
        received <- c
          .consume(
            ConsumeOptions(
              maxMessages = 4,
              expires = 2.seconds,
              idleHeartbeat = 500.millis
            )
          )
          .use { stream =>
            stream
              .evalTap(_.ack)
              .take(10)
              .map(_.payloadAsString)
              .compile
              .toList
          }
      yield
        assertEquals(received.size, 10)
        assertEquals(received.toSet, (1 to 10).map(i => s"m$i").toSet)
    }
  }
