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

package fs2.nats.client

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import fs2.nats.errors.NatsError
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Integration tests for the core request/reply primitive.
  *
  * These tests require a running NATS server. Start one with: docker-compose up
  * -d
  */
class RequestReplyIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast.copy(maxRetries = Some(3))
  )

  test("request gets a reply from a responder") {
    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val subject = s"test.rr.${System.currentTimeMillis()}"

        // A responder echoes each request payload back to its reply inbox.
        client.subscribe(subject).use { requests =>
          val responder = requests
            .evalMap(m =>
              m.replyTo.traverse_(r => client.publish(r, m.payload))
            )
            .compile
            .drain

          for
            fiber <- responder.start
            _ <- IO.sleep(150.millis)
            reply <- client.request(subject, Chunk.array("ping".getBytes))
            _ <- fiber.cancel
          yield assertEquals(reply.payloadAsString, "ping")
        }
      }
      .timeout(15.seconds)
  }

  test("request correlates many concurrent requests to distinct replies") {
    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val subject = s"test.rr.multi.${System.currentTimeMillis()}"

        // Responder appends "-pong" so we can verify reply matches request.
        client.subscribe(subject).use { requests =>
          val responder = requests
            .evalMap { m =>
              m.replyTo.traverse_ { r =>
                client.publish(
                  r,
                  Chunk.array((m.payloadAsString + "-pong").getBytes)
                )
              }
            }
            .compile
            .drain

          for
            fiber <- responder.start
            _ <- IO.sleep(150.millis)
            replies <- (1 to 20).toList.parTraverse { i =>
              client
                .request(subject, Chunk.array(s"req$i".getBytes))
                .map(_.payloadAsString)
            }
            _ <- fiber.cancel
          yield assertEquals(
            replies.toSet,
            (1 to 20).map(i => s"req$i-pong").toSet
          )
        }
      }
      .timeout(15.seconds)
  }

  test("request to a subject with no responders fails fast with NoResponders") {
    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val subject = s"test.rr.dead.${System.currentTimeMillis()}"
        client
          .request(subject, Chunk.array("x".getBytes), timeout = 5.seconds)
          .attempt
          .map {
            case Left(err: NatsError.NoResponders) =>
              assert(err.subject == subject)
            case Left(other) =>
              fail(s"Expected NoResponders, got $other")
            case Right(msg) =>
              fail(s"Expected NoResponders, got a reply: $msg")
          }
      }
      .timeout(15.seconds)
  }

  test("request times out when responder never replies") {
    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val subject = s"test.rr.silent.${System.currentTimeMillis()}"

        // Subscribe (so there IS a responder => no 503) but never reply.
        client.subscribe(subject).use { _ =>
          for
            _ <- IO.sleep(150.millis)
            result <- client
              .request(subject, Chunk.array("x".getBytes), timeout = 1.second)
              .attempt
          yield result match
            case Left(_: NatsError.Timeout) => ()
            case other => fail(s"Expected Timeout, got $other")
        }
      }
      .timeout(15.seconds)
  }
