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

package fs2.nats.kv

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.{Chunk, Stream}
import fs2.nats.client.{BackoffConfig, ClientConfig, NatsClient}
import fs2.nats.errors.NatsError
import fs2.nats.jetstream.JetStream
import munit.CatsEffectSuite

import scala.concurrent.duration.*

/** Integration tests for the KV layer. Requires a JetStream-enabled NATS server
  * (docker-compose up -d).
  */
class KvIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast.copy(maxRetries = Some(3))
  )

  private def uniqueName: String = s"B${System.nanoTime()}"

  private def bytes(s: String): Chunk[Byte] = Chunk.array(s.getBytes)
  private def str(c: Chunk[Byte]): String = new String(c.toArray)

  private def withJsKv[A](
      cfg: KvConfig
  )(f: (JetStream[IO], KeyValue[IO]) => IO[A]): IO[A] =
    NatsClient.connect[IO](clientConfig).use { client =>
      client.jetStream().use { js =>
        js.createKeyValue(cfg).flatMap { kv =>
          f(js, kv).guarantee(js.deleteKeyValue(cfg.bucket).attempt.void)
        }
      }
    }

  private def withKv[A](cfg: KvConfig)(f: KeyValue[IO] => IO[A]): IO[A] =
    withJsKv(cfg)((_, kv) => f(kv))

  test("put then get round-trips value, revision, and operation") {
    withKv(KvConfig(uniqueName)) { kv =>
      for
        r <- kv.put("a", bytes("hello"))
        e <- kv.get("a")
        missing <- kv.get("nope")
      yield
        assertEquals(r, 1L)
        assertEquals(e.map(_.revision), Some(1L))
        assertEquals(e.map(x => str(x.value)), Some("hello"))
        assertEquals(e.map(_.operation), Some(KvOperation.Put))
        assertEquals(missing, None)
    }
  }

  test("put twice increments revision and get returns the latest") {
    withKv(KvConfig(uniqueName)) { kv =>
      for
        r1 <- kv.put("a", bytes("1"))
        r2 <- kv.put("a", bytes("2"))
        e <- kv.get("a")
      yield
        assertEquals(r1, 1L)
        assertEquals(r2, 2L)
        assertEquals(e.map(x => str(x.value)), Some("2"))
        assertEquals(e.map(_.revision), Some(2L))
    }
  }

  test("create succeeds once then fails on an existing key") {
    withKv(KvConfig(uniqueName)) { kv =>
      for
        r1 <- kv.create("a", bytes("1"))
        r2 <- kv.create("a", bytes("2")).attempt
      yield
        assertEquals(r1, 1L)
        assert(
          r2.left.exists(_.isInstanceOf[NatsError.KeyValueWrongLastSequence]),
          clue = r2
        )
    }
  }

  test("update respects optimistic concurrency") {
    withKv(KvConfig(uniqueName)) { kv =>
      for
        r1 <- kv.put("a", bytes("1"))
        good <- kv.update("a", bytes("2"), r1)
        bad <- kv.update("a", bytes("3"), r1).attempt
      yield
        assertEquals(good, r1 + 1)
        assert(
          bad.left.exists(_.isInstanceOf[NatsError.KeyValueWrongLastSequence]),
          clue = bad
        )
    }
  }

  test("delete writes a tombstone visible in history; get returns None") {
    withKv(KvConfig(uniqueName, history = 10)) { kv =>
      for
        _ <- kv.put("a", bytes("1"))
        _ <- kv.delete("a")
        g <- kv.get("a")
        h <- kv.history("a")
      yield
        assertEquals(g, None)
        assertEquals(
          h.map(_.operation),
          List(KvOperation.Put, KvOperation.Delete)
        )
    }
  }

  test("purge collapses history; get returns None") {
    withKv(KvConfig(uniqueName, history = 10)) { kv =>
      for
        _ <- kv.put("a", bytes("1"))
        _ <- kv.put("a", bytes("2"))
        _ <- kv.purge("a")
        g <- kv.get("a")
        h <- kv.history("a")
      yield
        assertEquals(g, None)
        assertEquals(h.map(_.operation), List(KvOperation.Purge))
    }
  }

  test("history honors the configured depth") {
    withKv(KvConfig(uniqueName, history = 2)) { kv =>
      for
        _ <- kv.put("a", bytes("1"))
        _ <- kv.put("a", bytes("2"))
        _ <- kv.put("a", bytes("3"))
        h <- kv.history("a")
      yield
        assertEquals(h.map(e => str(e.value)), List("2", "3"))
        assertEquals(h.map(_.revision), List(2L, 3L))
    }
  }

  test("get by revision returns that specific revision") {
    withKv(KvConfig(uniqueName, history = 10)) { kv =>
      for
        r1 <- kv.put("a", bytes("1"))
        r2 <- kv.put("a", bytes("2"))
        e1 <- kv.get("a", r1)
        e2 <- kv.get("a", r2)
      yield
        assertEquals(e1.map(e => str(e.value)), Some("1"))
        assertEquals(e2.map(e => str(e.value)), Some("2"))
    }
  }

  test("keys lists live keys and excludes deleted") {
    withKv(KvConfig(uniqueName)) { kv =>
      for
        _ <- kv.put("a", bytes("1"))
        _ <- kv.put("b", bytes("2"))
        _ <- kv.put("c", bytes("3"))
        _ <- kv.delete("b")
        ks <- kv.keys.compile.toList
      yield assertEquals(ks.toSet, Set("a", "c"))
    }
  }

  test("get works with allowDirect = false (STREAM.MSG.GET fallback)") {
    withKv(KvConfig(uniqueName, allowDirect = false)) { kv =>
      for
        _ <- kv.put("a", bytes("hello"))
        e <- kv.get("a")
        missing <- kv.get("missing")
      yield
        assertEquals(e.map(x => str(x.value)), Some("hello"))
        assertEquals(missing, None)
    }
  }

  test("keys on an empty bucket returns empty promptly (no hang)") {
    withKv(KvConfig(uniqueName)) { kv =>
      kv.keys.compile.toList.map(ks => assertEquals(ks, Nil))
    }
  }

  test("watch on an empty bucket emits EndOfData promptly (no hang)") {
    withKv(KvConfig(uniqueName)) { kv =>
      kv.watch(">")
        .use(_.take(1).compile.toList)
        .map(evs => assertEquals(evs, List(KvWatchEvent.EndOfData)))
    }
  }

  test("watch emits the snapshot, EndOfData, then live updates") {
    withKv(KvConfig(uniqueName)) { kv =>
      for
        _ <- kv.put("a", bytes("1"))
        events <- kv
          .watch(">")
          .use { stream =>
            stream
              .concurrently(
                Stream.eval(IO.sleep(300.millis) *> kv.put("b", bytes("2")))
              )
              .take(3)
              .compile
              .toList
          }
      yield
        val keys = events.collect { case KvWatchEvent.Entry(e) => e.key }
        assertEquals(keys, List("a", "b"))
        assert(events.contains(KvWatchEvent.EndOfData), clue = events)
        assertEquals(events.indexOf(KvWatchEvent.EndOfData), 1)
    }
  }

  test("status and keyValueNames reflect the bucket") {
    val name = uniqueName
    withJsKv(KvConfig(name, history = 5)) { (js, kv) =>
      for
        _ <- kv.put("a", bytes("1"))
        st <- kv.status
        names <- js.keyValueNames.compile.toList
      yield
        assertEquals(st.bucket, name)
        assertEquals(st.history, 5L)
        assertEquals(st.values, 1L)
        assert(names.contains(name), clue = names)
    }
  }
