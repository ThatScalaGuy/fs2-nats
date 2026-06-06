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

/** Integration tests for the gap-resetting ordered push consumer. Requires a
  * JetStream-enabled NATS server (docker-compose up -d).
  */
class OrderedConsumerIntegrationSpec extends CatsEffectSuite:

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

  private def publishRange(
      js: JetStream[IO],
      subject: String,
      range: Range
  ): IO[Unit] =
    range.toList.traverse_(i =>
      js.publish(subject, Chunk.array(s"m$i".getBytes))
    )

  test("ordered consumer delivers every message in order") {
    val name = uniqueName
    withJs(name) { js =>
      val subject = s"$name.obj"
      for
        _ <- publishRange(js, subject, 1 to 20)
        got <- js
          .subscribeOrdered(name, Some(subject))
          .use(
            _.map(_.payloadAsString)
              .take(20)
              .compile
              .toList
              .timeout(15.seconds)
          )
      yield assertEquals(got, (1 to 20).map(i => s"m$i").toList)
    }
  }

  test(
    "ordered consumer recovers in-order when its consumer is lost mid-stream"
  ) {
    val name = uniqueName
    withJs(name) { js =>
      val subject = s"$name.obj"
      val opts = OrderedConsumerOptions(idleHeartbeat = 1.second)
      for
        _ <- publishRange(js, subject, 1 to 5)
        got <- js.subscribeOrdered(name, Some(subject), opts).use { s =>
          for
            fiber <- s.map(_.payloadAsString).take(10).compile.toList.start
            // Let the first batch deliver, then delete the live consumer
            // out-of-band so the remaining messages can only arrive via a
            // transparent recreate.
            _ <- IO.sleep(1.second)
            _ <- js
              .consumerNames(name)
              .evalMap(js.deleteConsumer(name, _))
              .compile
              .drain
            _ <- publishRange(js, subject, 6 to 10)
            res <- fiber.joinWithNever.timeout(30.seconds)
          yield res
        }
      yield assertEquals(got, (1 to 10).map(i => s"m$i").toList)
    }
  }
