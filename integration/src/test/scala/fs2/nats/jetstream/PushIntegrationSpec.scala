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

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import fs2.nats.client.{BackoffConfig, ClientConfig, NatsClient}
import fs2.nats.jetstream.protocol.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Integration tests for push consumption, queue groups, and flow control.
  * Requires a JetStream-enabled NATS server (docker-compose up -d).
  */
class PushIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast.copy(maxRetries = Some(3))
  )

  private def uniqueName: String = s"S${System.nanoTime()}"

  private def withJs[A](name: String)(f: JetStream[IO] => IO[A]): IO[A] =
    NatsClient.connect[IO](clientConfig).use { client =>
      client.jetStream().use { js =>
        (js.addStream(StreamConfig(name = name, subjects = List(s"$name.>"))) >>
          f(js)).guarantee(js.deleteStream(name).attempt.void)
      }
    }

  private def publishN(js: JetStream[IO], subject: String, n: Int): IO[Unit] =
    (1 to n).toList.traverse_(i =>
      js.publish(subject, Chunk.array(s"m$i".getBytes))
    )

  test("push durable consumer receives and acks") {
    val name = uniqueName
    withJs(name) { js =>
      val cfg = ConsumerConfig(
        durable = Some("pushd"),
        deliverSubject = Some(s"deliver.$name"),
        ackPolicy = AckPolicy.Explicit
      )
      js.subscribePush(name, cfg).use { stream =>
        for
          _ <- IO.sleep(250.millis)
          _ <- publishN(js, s"$name.a", 3)
          msgs <- stream
            .evalTap(_.ack)
            .take(3)
            .compile
            .toList
            .timeout(8.seconds)
        yield
          assertEquals(msgs.size, 3)
          assertEquals(msgs.map(_.payloadAsString), List("m1", "m2", "m3"))
      }
    }
  }

  test("push queue group load-balances across two subscriptions") {
    val name = uniqueName
    withJs(name) { js =>
      val cfg = ConsumerConfig(
        durable = Some("qg"),
        deliverSubject = Some(s"qg.$name"),
        deliverGroup = Some("workers"),
        ackPolicy = AckPolicy.Explicit
      )
      js.subscribePush(name, cfg).use { s1 =>
        js.subscribePush(name, cfg).use { s2 =>
          for
            c1 <- Ref.of[IO, Int](0)
            c2 <- Ref.of[IO, Int](0)
            _ <- IO.sleep(300.millis)
            _ <- publishN(js, s"$name.a", 10)
            drain1 = s1
              .evalTap(_.ack)
              .evalTap(_ => c1.update(_ + 1))
              .interruptAfter(4.seconds)
              .compile
              .drain
            drain2 = s2
              .evalTap(_.ack)
              .evalTap(_ => c2.update(_ + 1))
              .interruptAfter(4.seconds)
              .compile
              .drain
            _ <- (drain1, drain2).parTupled
            n1 <- c1.get
            n2 <- c2.get
          yield
            assertEquals(n1 + n2, 10)
            assert(n1 > 0 && n2 > 0, s"expected balancing, got $n1 / $n2")
        }
      }
    }
  }

  test("flow-control push consumer does not stall") {
    val name = uniqueName
    withJs(name) { js =>
      val cfg = ConsumerConfig(
        durable = Some("fc"),
        deliverSubject = Some(s"fc.$name"),
        ackPolicy = AckPolicy.Explicit,
        flowControl = true,
        idleHeartbeat = Some(1.second)
      )
      js.subscribePush(name, cfg).use { stream =>
        for
          _ <- IO.sleep(250.millis)
          _ <- publishN(js, s"$name.a", 20)
          msgs <- stream
            .evalTap(_.ack)
            .take(20)
            .compile
            .toList
            .timeout(15.seconds)
        yield assertEquals(msgs.size, 20)
      }
    }
  }

  test("ephemeral push consumer is deleted on release") {
    val name = uniqueName
    withJs(name) { js =>
      val cfg = ConsumerConfig(
        deliverSubject = Some(s"eph.$name"),
        ackPolicy = AckPolicy.Explicit
      )
      for
        during <- js.subscribePush(name, cfg).use { _ =>
          IO.sleep(250.millis) *> js.consumerNames(name).compile.toList
        }
        after <- js.consumerNames(name).compile.toList
      yield
        assertEquals(during.size, 1)
        assertEquals(after.size, 0)
    }
  }
