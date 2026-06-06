/*
 * Copyright 2025 ThatScalaGuy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.examples

import cats.effect.{ExitCode, IO, IOApp}
import com.comcast.ip4s.{Host, Port}
import fs2.{Chunk, Stream}
import fs2.nats.client.{ClientConfig, NatsClient}
import fs2.nats.objectstore.*

/** Object Store example: streaming put/get of binary objects, info, links,
  * listing, watching, and sealing a bucket.
  *
  * Prerequisites:
  *   - Start a JetStream-enabled NATS server: docker run -p 4222:4222
  *     nats:latest -js
  *
  * Run with: sbt "runMain fs2.nats.examples.ObjectStoreExample"
  */
object ObjectStoreExample extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val config = ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(4222).get
    )

    NatsClient.connect[IO](config).use { client =>
      client.jetStream().use { js =>
        for
          // 1. Create a bucket (S2-compressed, direct reads, on by default).
          os <- js.createObjectStore(ObjConfig(bucket = "assets"))
          _ <- IO.println("Created Object Store bucket 'assets'")

          // 2. Stream a (here, in-memory) byte source in as 64 KiB chunks. For a
          //    real large object you'd pass a Stream[IO, Byte] from a file or
          //    socket — nothing is buffered whole.
          data = Stream
            .range(0, 1_000_000)
            .map(i => (i % 256).toByte)
            .covary[IO]
          info <- os.put(
            ObjectMeta(
              "logo.bin",
              description = Some("a million bytes"),
              maxChunkSize = 64 * 1024
            ),
            data
          )
          _ <- IO.println(
            s"put logo.bin -> ${info.size} bytes in ${info.chunks} chunks, ${info.digest}"
          )

          // 3. Stream it back out and count the bytes (digest verified at end).
          count <- os
            .get("logo.bin")
            .flatMap(_.get.data.compile.count)
          _ <- IO.println(s"get logo.bin -> streamed $count bytes back")

          // 4. Small objects: putBytes / getBytes convenience.
          _ <- os.putBytes(ObjectMeta("readme.txt"), Chunk.array("hello".getBytes))
          txt <- os.getBytes("readme.txt")
          _ <- IO.println(s"readme.txt -> ${txt.map(c => new String(c.toArray))}")

          // 5. A link that transparently resolves to another object.
          _ <- os.addLink("logo-latest", info)
          linked <- os.info("logo-latest")
          _ <- IO.println(s"logo-latest resolves to -> ${linked.map(_.name)}")

          // 6. List the live objects.
          names <- os.list.map(_.name).compile.toList
          _ <- IO.println(s"objects: ${names.sorted}")

          // 7. Watch: snapshot then EndOfData (then it would stay open for live
          //    updates).
          _ <- os.watch.use { stream =>
            stream
              .evalTap {
                case ObjectWatchEvent.Update(i) =>
                  IO.println(s"  watch: ${i.name} (${i.size} bytes)")
                case ObjectWatchEvent.EndOfData =>
                  IO.println("  watch: <caught up>")
              }
              .takeWhile(_ != ObjectWatchEvent.EndOfData, takeFailure = true)
              .compile
              .drain
          }

          // 8. Seal the bucket (now read-only), then clean up.
          _ <- os.seal
          _ <- IO.println("sealed 'assets'")
          _ <- js.deleteObjectStore("assets")
          _ <- IO.println("Done")
        yield ExitCode.Success
      }
    }
