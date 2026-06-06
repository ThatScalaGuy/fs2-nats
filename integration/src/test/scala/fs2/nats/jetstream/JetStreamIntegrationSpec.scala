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
import fs2.nats.errors.NatsError
import fs2.nats.jetstream.protocol.*
import fs2.nats.protocol.Headers
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Integration tests for JetStream publish + stream management. Requires a
  * JetStream-enabled NATS server (docker-compose up -d).
  */
class JetStreamIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast.copy(maxRetries = Some(3))
  )

  private def withJs[A](f: JetStream[IO] => IO[A]): IO[A] =
    NatsClient.connect[IO](clientConfig).use { client =>
      client.jetStream().use(js => f(js))
    }

  private def uniqueName: String = s"S${System.nanoTime()}"

  private def cleanup(js: JetStream[IO], name: String): IO[Unit] =
    js.deleteStream(name).attempt.void

  test("create, info, and delete a stream") {
    withJs { js =>
      val name = uniqueName
      val cfg = StreamConfig(name = name, subjects = List(s"$name.>"))
      (for
        created <- js.addStream(cfg)
        info <- js.streamInfo(name)
        _ <- js.deleteStream(name)
        afterErr <- js.streamInfo(name).attempt
      yield
        assertEquals(created.config.name, name)
        assertEquals(info.config.subjects, List(s"$name.>"))
        assert(afterErr.isLeft, "streamInfo after delete should fail")
      ).guarantee(cleanup(js, name))
    }
  }

  test("publish returns PubAck with increasing sequence") {
    withJs { js =>
      val name = uniqueName
      val cfg = StreamConfig(name = name, subjects = List(s"$name.>"))
      (for
        _ <- js.addStream(cfg)
        a1 <- js.publish(s"$name.a", Chunk.array("one".getBytes))
        a2 <- js.publish(s"$name.b", Chunk.array("two".getBytes))
      yield
        assertEquals(a1.stream, name)
        assertEquals(a1.seq, 1L)
        assertEquals(a2.seq, 2L)
        assertEquals(a1.duplicate, false)
      ).guarantee(cleanup(js, name))
    }
  }

  test("publish with Nats-Msg-Id de-duplicates") {
    withJs { js =>
      val name = uniqueName
      val cfg = StreamConfig(name = name, subjects = List(s"$name.>"))
      val payload = Chunk.array("dedup".getBytes)
      val opts = PublishOptions(msgId = Some("msg-1"))
      (for
        _ <- js.addStream(cfg)
        a1 <- js.publish(s"$name.a", payload, opts = opts)
        a2 <- js.publish(s"$name.a", payload, opts = opts)
      yield
        assertEquals(a1.duplicate, false)
        assertEquals(a2.duplicate, true)
        assertEquals(a2.seq, a1.seq)
      ).guarantee(cleanup(js, name))
    }
  }

  test("Nats-Expected-Last-Sequence mismatch raises JetStreamApiError") {
    withJs { js =>
      val name = uniqueName
      val cfg = StreamConfig(name = name, subjects = List(s"$name.>"))
      (for
        _ <- js.addStream(cfg)
        _ <- js.publish(s"$name.a", Chunk.array("one".getBytes))
        result <- js
          .publish(
            s"$name.a",
            Chunk.array("two".getBytes),
            opts = PublishOptions(expectedLastSeq = Some(99L))
          )
          .attempt
      yield result match
        case Left(_: NatsError.JetStreamApiError) => ()
        case other => fail(s"Expected JetStreamApiError, got $other")
      ).guarantee(cleanup(js, name))
    }
  }

  test("purge removes all messages") {
    withJs { js =>
      val name = uniqueName
      val cfg = StreamConfig(name = name, subjects = List(s"$name.>"))
      (for
        _ <- js.addStream(cfg)
        _ <- (1 to 3).toList.traverse_(i =>
          js.publish(s"$name.a", Chunk.array(s"m$i".getBytes))
        )
        before <- js.streamInfo(name)
        purged <- js.purgeStream(name)
        after <- js.streamInfo(name)
      yield
        assertEquals(before.state.messages, 3L)
        assert(purged.success)
        assertEquals(after.state.messages, 0L)
      ).guarantee(cleanup(js, name))
    }
  }

  test("get and delete a stored message") {
    withJs { js =>
      val name = uniqueName
      val cfg = StreamConfig(name = name, subjects = List(s"$name.>"))
      (for
        _ <- js.addStream(cfg)
        _ <- js.publish(
          s"$name.a",
          Chunk.array("payload".getBytes),
          Headers("X-T" -> "v")
        )
        msg <- js.getMessage(name, MessageGet.BySeq(1L))
        _ <- js.deleteMessage(name, 1L)
        afterErr <- js.getMessage(name, MessageGet.BySeq(1L)).attempt
      yield
        assertEquals(msg.subject, s"$name.a")
        assertEquals(new String(msg.data.toArray), "payload")
        assertEquals(msg.headers.get("X-T"), Some("v"))
        assert(afterErr.isLeft, "getMessage after delete should fail")
      ).guarantee(cleanup(js, name))
    }
  }

  test("streamNames and listStreams include a created stream") {
    withJs { js =>
      val name = uniqueName
      val cfg = StreamConfig(name = name, subjects = List(s"$name.>"))
      (for
        _ <- js.addStream(cfg)
        names <- js.streamNames.compile.toList
        infos <- js.listStreams.compile.toList
      yield
        assert(names.contains(name), s"streamNames should contain $name")
        assert(
          infos.exists(_.config.name == name),
          s"listStreams should contain $name"
        )
      ).guarantee(cleanup(js, name))
    }
  }

  test("accountInfo reports JetStream usage") {
    withJs { js =>
      js.accountInfo.map { info =>
        assert(info.streams >= 0)
        assert(info.limits.maxMemory >= -1)
      }
    }
  }

  test("getMessage with LastBySubject returns the latest") {
    withJs { js =>
      val name = uniqueName
      val cfg = StreamConfig(name = name, subjects = List(s"$name.>"))
      (for
        _ <- js.addStream(cfg)
        _ <- js.publish(s"$name.a", Chunk.array("first".getBytes))
        _ <- js.publish(s"$name.a", Chunk.array("second".getBytes))
        msg <- js.getMessage(name, MessageGet.LastBySubject(s"$name.a"))
      yield assertEquals(new String(msg.data.toArray), "second"))
        .guarantee(cleanup(js, name))
    }
  }
