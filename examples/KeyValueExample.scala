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
import fs2.Chunk
import fs2.nats.client.{ClientConfig, NatsClient}
import fs2.nats.kv.*

/** Key-Value example: bucket management, put/get with revisions, optimistic
  * concurrency, delete, listing keys, and watching for changes.
  *
  * Prerequisites:
  *   - Start a JetStream-enabled NATS server: docker run -p 4222:4222
  *     nats:latest -js
  *
  * Run with: sbt "runMain fs2.nats.examples.KeyValueExample"
  */
object KeyValueExample extends IOApp:

  private def bytes(s: String): Chunk[Byte] = Chunk.array(s.getBytes)

  override def run(args: List[String]): IO[ExitCode] =
    val config = ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(4222).get
    )

    NatsClient.connect[IO](config).use { client =>
      client.jetStream().use { js =>
        for
          // 1. Create a bucket keeping the last 5 revisions of each key.
          kv <- js.createKeyValue(KvConfig(bucket = "config", history = 5))
          _ <- IO.println("Created KV bucket 'config'")

          // 2. Put a value; the returned revision is the entry's sequence.
          rev <- kv.put("db.url", bytes("postgres://localhost:5432"))
          _ <- IO.println(s"put db.url -> revision $rev")

          // 3. Read it back (Direct Get fast path).
          entry <- kv.get("db.url")
          _ <- IO.println(
            s"get db.url -> ${entry.map(e => new String(e.value.toArray))}"
          )

          // 4. Optimistic-concurrency update: only succeeds if the revision
          //    still matches.
          rev2 <- kv.update("db.url", bytes("postgres://prod:5432"), rev)
          _ <- IO.println(s"update db.url -> revision $rev2")

          // 5. More keys, then list the live keys.
          _ <- kv.put("feature.flags", bytes("a,b,c"))
          _ <- kv.put("legacy", bytes("x"))
          _ <- kv.delete("legacy")
          keys <- kv.keys.compile.toList
          _ <- IO.println(s"live keys: ${keys.sorted}")

          // 6. Watch the bucket: the current snapshot is delivered, followed by
          //    a single EndOfData marker; the stream then stays open for live
          //    updates. Here we just print the snapshot and stop at EndOfData.
          _ <- kv
            .watch(">")
            .use { stream =>
              stream
                .evalTap {
                  case KvWatchEvent.Entry(e) =>
                    IO.println(
                      s"  watch: ${e.key} = ${new String(e.value.toArray)}"
                    )
                  case KvWatchEvent.EndOfData =>
                    IO.println("  watch: <caught up>")
                }
                .takeWhile(_ != KvWatchEvent.EndOfData, takeFailure = true)
                .compile
                .drain
            }

          _ <- js.deleteKeyValue("config")
          _ <- IO.println("Done")
        yield ExitCode.Success
      }
    }
