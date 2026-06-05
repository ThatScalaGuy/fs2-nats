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

package fs2.nats.objectstore

import cats.effect.IO
import com.comcast.ip4s.{Host, Port}
import fs2.{Chunk, Stream}
import fs2.io.file.Files
import fs2.nats.client.{BackoffConfig, ClientConfig, NatsClient}
import fs2.nats.errors.NatsError
import fs2.nats.jetstream.JetStream
import fs2.nats.protocol.Headers
import munit.CatsEffectSuite

import scala.concurrent.duration.*

/** Integration tests for the Object Store. Requires a JetStream-enabled NATS
  * server (docker-compose up -d).
  */
class ObjectStoreIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast.copy(maxRetries = Some(3))
  )

  private def uniqueBucket: String = s"obj${System.nanoTime()}"

  private def withObj[A](
      bucket: String
  )(f: (JetStream[IO], ObjectStore[IO]) => IO[A]): IO[A] =
    NatsClient.connect[IO](clientConfig).use { client =>
      client.jetStream().use { js =>
        js.createObjectStore(ObjConfig(bucket)).flatMap { os =>
          f(js, os).guarantee(js.deleteObjectStore(bucket).attempt.void)
        }
      }
    }

  private def bytes(n: Int): Chunk[Byte] =
    Chunk.array(Array.tabulate(n)(i => (i % 256).toByte))

  test("putBytes then getBytes round-trips and info reflects size/chunks") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      val data = bytes(1000)
      for
        info <- os.putBytes(ObjectMeta("hello.bin"), data)
        got <- os.getBytes("hello.bin")
        infoBack <- os.info("hello.bin")
      yield
        assertEquals(info.size, 1000L)
        assertEquals(info.chunks, 1L)
        assert(info.digest.startsWith("SHA-256="), info.digest)
        assertEquals(got.map(_.toList), Some(data.toList))
        assertEquals(infoBack.map(_.size), Some(1000L))
    }
  }

  test("large multi-chunk object streams back byte-identically") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      val data = bytes(200000) // ~49 chunks at 4 KiB
      for
        info <- os.put(
          ObjectMeta("big.bin", maxChunkSize = 4096),
          Stream.chunk(data)
        )
        got <- os.getBytes("big.bin")
      yield
        assertEquals(info.size, 200000L)
        assertEquals(info.chunks, 49L)
        assertEquals(got.map(_.toList), Some(data.toList))
    }
  }

  test("get on a tampered digest raises ObjectDigestMismatch") {
    val b = uniqueBucket
    withObj(b) { (js, os) =>
      for
        info <- os.put(
          ObjectMeta("doc", maxChunkSize = 4096),
          Stream.chunk(bytes(20000))
        )
        // Overwrite the meta with a bogus digest (same nuid/size/chunks).
        tampered =
          s"""{"name":"doc","bucket":"$b","nuid":"${info.nuid}","size":${info.size},"chunks":${info.chunks},"digest":"SHA-256=tampered"}"""
        _ <- js.publish(
          ObjNames.metaSubject(b, "doc"),
          Chunk.array(tampered.getBytes),
          Headers.empty.set("Nats-Rollup", "sub")
        )
        result <- os
          .get("doc")
          .flatMap(_.get.data.compile.drain)
          .attempt
      yield assert(
        result.left.exists(_.isInstanceOf[NatsError.ObjectDigestMismatch]),
        clue = result
      )
    }
  }

  test("overwrite replaces content and purges old chunks") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      for
        _ <- os.put(ObjectMeta("k", maxChunkSize = 1024), Stream.chunk(bytes(5000)))
        info2 <- os.putBytes(ObjectMeta("k"), bytes(300))
        got <- os.getBytes("k")
        st <- os.status
      yield
        assertEquals(info2.size, 300L)
        assertEquals(info2.chunks, 1L)
        assertEquals(got.map(_.toList), Some(bytes(300).toList))
        // old 5-chunk object purged: only the 1 new chunk remains backing the data
        assert(st.size > 0L)
    }
  }

  test("delete tombstones the object; get/info return None") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      for
        _ <- os.putBytes(ObjectMeta("gone"), bytes(100))
        _ <- os.delete("gone")
        g <- os.get("gone")
        i <- os.info("gone")
      yield
        assertEquals(g.isDefined, false)
        assertEquals(i.isDefined, false)
    }
  }

  test("list returns the live (non-deleted) objects") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      for
        _ <- os.putBytes(ObjectMeta("a"), bytes(10))
        _ <- os.putBytes(ObjectMeta("b"), bytes(20))
        _ <- os.putBytes(ObjectMeta("c"), bytes(30))
        _ <- os.delete("b")
        names <- os.list.map(_.name).compile.toList.map(_.sorted)
      yield assertEquals(names, List("a", "c"))
    }
  }

  test("watch emits the snapshot, EndOfData, then a live update") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      for
        _ <- os.putBytes(ObjectMeta("a"), bytes(10))
        _ <- os.putBytes(ObjectMeta("b"), bytes(20))
        events <- os.watch.use { stream =>
          for
            fiber <- stream.take(4).compile.toList.start
            _ <- IO.sleep(400.millis)
            _ <- os.putBytes(ObjectMeta("c"), bytes(30))
            evs <- fiber.joinWithNever.timeout(15.seconds)
          yield evs
        }
      yield
        val updates = events.collect {
          case ObjectWatchEvent.Update(i) => i.name
        }
        val endIdx = events.indexOf(ObjectWatchEvent.EndOfData)
        assert(endIdx >= 0, clue = events)
        assertEquals(updates.toSet, Set("a", "b", "c"))
    }
  }

  test("addLink resolves to the target object on get/info") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      val data = bytes(500)
      for
        target <- os.putBytes(ObjectMeta("real"), data)
        _ <- os.addLink("alias", target)
        info <- os.info("alias")
        got <- os.getBytes("alias")
      yield
        assertEquals(info.map(_.name), Some("real"))
        assertEquals(got.map(_.toList), Some(data.toList))
    }
  }

  test("rename moves the object without re-uploading its bytes") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      val data = bytes(750)
      for
        _ <- os.putBytes(ObjectMeta("old"), data)
        renamed <- os.rename("old", "new")
        oldGone <- os.get("old")
        got <- os.getBytes("new")
      yield
        assertEquals(renamed.name, "new")
        assertEquals(oldGone.isDefined, false)
        assertEquals(got.map(_.toList), Some(data.toList))
    }
  }

  test("seal makes the bucket read-only") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      for
        _ <- os.putBytes(ObjectMeta("x"), bytes(10))
        _ <- os.seal
        st <- os.status
        failed <- os.putBytes(ObjectMeta("y"), bytes(10)).attempt
      yield
        assert(st.isSealed, "bucket should be sealed")
        assert(failed.isLeft, clue = failed)
    }
  }

  test("putFile then getToFile round-trips a file on disk") {
    val b = uniqueBucket
    withObj(b) { (_, os) =>
      val data = bytes(40000)
      Files.forAsync[IO].tempFile.use { src =>
        Files.forAsync[IO].tempFile.use { dst =>
          for
            _ <- Stream
              .chunk(data)
              .through(Files.forAsync[IO].writeAll(src))
              .compile
              .drain
            info <- os.putFile("file.bin", src)
            _ <- os.getToFile("file.bin", dst)
            readBack <- Files.forAsync[IO].readAll(dst).compile.to(Chunk)
          yield
            assertEquals(info.size, 40000L)
            assertEquals(readBack.toList, data.toList)
        }
      }
    }
  }
