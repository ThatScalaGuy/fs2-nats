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
import scala.sys.process.*
import scala.util.Try

/** Reconnect-resume tests. These restart the docker NATS broker mid-consume, so
  * they require docker and the docker-compose `nats` service to be running.
  * JetStream file storage survives a container restart, so streams/consumers
  * persist across the reconnect.
  */
class ReconnectIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 90.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  // Generous retries so the client keeps reconnecting while the broker restarts.
  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast.copy(maxRetries = Some(200))
  )

  private def uniqueName: String = s"S${System.nanoTime()}"

  private def dockerAvailable: Boolean =
    Try(Seq("docker", "info").!(ProcessLogger(_ => ())) == 0).getOrElse(false)

  private def natsContainerId: Option[String] =
    Try(
      Seq("docker", "ps", "-q", "--filter", "ancestor=nats:latest").!!.trim
    ).toOption.filter(_.nonEmpty).map(_.linesIterator.next())

  private def restartBroker: IO[Unit] =
    IO.blocking {
      natsContainerId.foreach(id =>
        Seq("docker", "restart", "-t", "0", id).!(ProcessLogger(_ => ()))
      )
    }

  private def waitHealthy: IO[Unit] =
    val check = IO.blocking {
      Try(
        Seq("curl", "-sf", "http://localhost:8222/healthz")
          .!(ProcessLogger(_ => ())) == 0
      ).getOrElse(false)
    }
    fs2.Stream
      .repeatEval(check)
      .metered(200.millis)
      .find(identity)
      .compile
      .drain
      .timeout(30.seconds)

  /** Poll an effect until it returns true or the timeout elapses. */
  private def waitUntil(cond: IO[Boolean]): IO[Unit] =
    fs2.Stream
      .repeatEval(cond)
      .metered(200.millis)
      .find(identity)
      .compile
      .drain
      .timeout(30.seconds)

  test("pull consume resumes after a broker restart".flaky) {
    assume(dockerAvailable, "docker is not available")
    val name = uniqueName
    NatsClient.connect[IO](clientConfig).use { client =>
      client.jetStream().use { js =>
        (for
          _ <- js.addStream(
            StreamConfig(name = name, subjects = List(s"$name.>"))
          )
          c <- js.createConsumer(
            name,
            ConsumerConfig(
              durable = Some("workers"),
              ackPolicy = AckPolicy.Explicit,
              ackWait = Some(2.seconds)
            )
          )
          // Persist 20 messages up front; the durable consumer must deliver
          // all of them, resuming across the restart.
          _ <- (1 to 20).toList.traverse_(i =>
            js.publish(s"$name.a", Chunk.array(s"m$i".getBytes))
          )
          seen <- Ref.of[IO, Set[Long]](Set.empty)
          consumer <- c
            .consume(
              ConsumeOptions(
                maxMessages = 5,
                expires = 2.seconds,
                idleHeartbeat = 500.millis
              )
            )
            .use { stream =>
              for
                fiber <- stream
                  .evalTap(m => seen.update(_ + m.metadata.streamSeq) *> m.ack)
                  .compile
                  .drain
                  .start
                // Let some arrive, then bounce the broker mid-consume.
                _ <- waitUntil(seen.get.map(_.nonEmpty))
                _ <- restartBroker
                _ <- waitHealthy
                // After reconnect + keepalive re-pull, the rest are delivered.
                _ <- waitUntil(seen.get.map(_.size == 20))
                _ <- fiber.cancel
                total <- seen.get
              yield total
            }
        yield assertEquals(consumer.size, 20))
          .guarantee(js.deleteStream(name).attempt.void)
      }
    }
  }

  test("push consume resumes after a broker restart".flaky) {
    assume(dockerAvailable, "docker is not available")
    val name = uniqueName
    NatsClient.connect[IO](clientConfig).use { client =>
      client.jetStream().use { js =>
        val cfg = ConsumerConfig(
          durable = Some("pushd"),
          deliverSubject = Some(s"deliver.$name"),
          ackPolicy = AckPolicy.Explicit,
          ackWait = Some(2.seconds),
          idleHeartbeat = Some(500.millis)
        )
        (for
          _ <- js.addStream(
            StreamConfig(name = name, subjects = List(s"$name.>"))
          )
          seen <- Ref.of[IO, Set[Long]](Set.empty)
          result <- js.subscribePush(name, cfg).use { stream =>
            for
              fiber <- stream
                .evalTap(m => seen.update(_ + m.metadata.streamSeq) *> m.ack)
                .compile
                .drain
                .start
              _ <- IO.sleep(300.millis)
              _ <- (1 to 20).toList.traverse_(i =>
                js.publish(s"$name.a", Chunk.array(s"m$i".getBytes))
              )
              _ <- waitUntil(seen.get.map(_.nonEmpty))
              _ <- restartBroker
              _ <- waitHealthy
              _ <- waitUntil(seen.get.map(_.size == 20))
              _ <- fiber.cancel
              total <- seen.get
            yield total
          }
        yield assertEquals(result.size, 20))
          .guarantee(js.deleteStream(name).attempt.void)
      }
    }
  }
