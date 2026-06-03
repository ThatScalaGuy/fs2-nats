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
import fs2.nats.client.{BackoffConfig, ClientConfig, NatsClient}
import fs2.nats.jetstream.protocol.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Integration tests for JetStream consumer management. Requires a
  * JetStream-enabled NATS server (docker-compose up -d).
  */
class ConsumerIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast.copy(maxRetries = Some(3))
  )

  private def withStream[A](
      name: String
  )(f: JetStream[IO] => IO[A]): IO[A] =
    NatsClient.connect[IO](clientConfig).use { client =>
      client.jetStream().use { js =>
        js.addStream(StreamConfig(name = name, subjects = List(s"$name.>"))) >>
          f(js).guarantee(js.deleteStream(name).attempt.void)
      }
    }

  private def uniqueName: String = s"S${System.nanoTime()}"

  test("create a durable consumer and read its info") {
    val name = uniqueName
    withStream(name) { js =>
      val cfg = ConsumerConfig(
        durable = Some("workers"),
        ackPolicy = AckPolicy.Explicit,
        filterSubject = Some(s"$name.new"),
        maxDeliver = 5
      )
      for
        created <- js.addConsumer(name, cfg)
        info <- js.consumerInfo(name, "workers")
      yield
        assertEquals(created.name, "workers")
        assertEquals(info.config.durable, Some("workers"))
        assertEquals(info.config.filterSubject, Some(s"$name.new"))
        assertEquals(info.config.maxDeliver, 5)
        assertEquals(info.config.ackPolicy, AckPolicy.Explicit)
    }
  }

  test("create an ephemeral consumer (server-assigned name)") {
    val name = uniqueName
    withStream(name) { js =>
      val cfg = ConsumerConfig(ackPolicy = AckPolicy.Explicit)
      for
        created <- js.addConsumer(name, cfg)
        info <- js.consumerInfo(name, created.name)
      yield
        assert(created.name.nonEmpty)
        assertEquals(info.name, created.name)
        assertEquals(info.config.durable, None)
    }
  }

  test("consumerNames and listConsumers include a created consumer") {
    val name = uniqueName
    withStream(name) { js =>
      for
        _ <- js.addConsumer(name, ConsumerConfig(durable = Some("c1")))
        _ <- js.addConsumer(name, ConsumerConfig(durable = Some("c2")))
        names <- js.consumerNames(name).compile.toList
        infos <- js.listConsumers(name).compile.toList
      yield
        assert(names.contains("c1") && names.contains("c2"))
        assertEquals(infos.map(_.name).toSet, Set("c1", "c2"))
    }
  }

  test("delete a consumer") {
    val name = uniqueName
    withStream(name) { js =>
      for
        _ <- js.addConsumer(name, ConsumerConfig(durable = Some("temp")))
        _ <- js.deleteConsumer(name, "temp")
        afterErr <- js.consumerInfo(name, "temp").attempt
      yield assert(afterErr.isLeft, "consumerInfo after delete should fail")
    }
  }

  test("createConsumer factory returns a working handle") {
    val name = uniqueName
    withStream(name) { js =>
      for
        handle <- js.createConsumer(name, ConsumerConfig(durable = Some("h")))
        info <- handle.info
      yield
        assertEquals(handle.stream, name)
        assertEquals(handle.name, "h")
        assertEquals(info.config.durable, Some("h"))
    }
  }

  test("consumer factory attaches to an existing consumer") {
    val name = uniqueName
    withStream(name) { js =>
      for
        _ <- js.addConsumer(name, ConsumerConfig(durable = Some("existing")))
        handle <- js.consumer(name, "existing")
        info <- handle.info
      yield
        assertEquals(handle.name, "existing")
        assertEquals(info.name, "existing")
    }
  }
