# Object Store

An Object Store bucket is an opinionated JetStream stream (`OBJ_<bucket>`,
subjects `$O.<bucket>.C.>` for chunks and `$O.<bucket>.M.>` for per-object
meta). It stores arbitrarily large binary objects by chunking them across the
stream, with a rolled-up meta message recording each object's size, chunk count,
and SHA-256 digest. Both `put` and `get` are fully streaming — neither
materializes a whole object in memory.

The snippets below share these imports and helpers:

```scala mdoc:silent
import cats.effect.IO
import fs2.Chunk
import fs2.io.file.{Files, Path}
import fs2.nats.client.NatsClient
import fs2.nats.objectstore.*

// A stand-in for wherever you send the bytes (a file, a socket, ...).
val sink: fs2.Pipe[IO, Byte, Unit] = _.map(_ => ())
```

## Streaming put and get

`put` takes an `fs2.Stream[F, Byte]`; `get` returns an `ObjectResult` whose
`.data` is an `fs2.Stream[F, Byte]`. The SHA-256 digest is verified once all
chunks are read:

```scala mdoc:silent
def objectStoreBasics(client: NatsClient[IO]): IO[Unit] =
  client.jetStream().use { js =>
    for
      os <- js.createObjectStore(ObjConfig(bucket = "assets"))

      // Stream bytes in (here from a file); nothing is buffered whole.
      info <- os.put(
                ObjectMeta("logo.png", maxChunkSize = 128 * 1024),
                Files[IO].readAll(Path("logo.png"))
              )

      // Stream bytes out; the digest is verified at end of stream.
      _ <- os.get("logo.png").flatMap {
             case Some(r) => r.data.through(sink).compile.drain
             case None    => IO.unit
           }

      // Convenience for small objects and files
      _   <- os.putBytes(ObjectMeta("readme.txt"), Chunk.array("hi".getBytes))
      txt <- os.getBytes("readme.txt")              // Option[Chunk[Byte]]
      _   <- os.putFile("backup.tar", Path("backup.tar"))
      _   <- os.getToFile("backup.tar", Path("restored.tar"))

      _   <- IO.println(s"stored ${info.size} bytes; readme present=${txt.isDefined}")
    yield ()
  }
```

## Links, rename, list, watch, and seal

Objects support links (`addLink` / `addBucketLink`, transparently resolved on
`get`/`info`), metadata updates and `rename` (no re-upload), `delete`, `list`,
`watch` (snapshot + `EndOfData` + live updates), and `seal` (make the bucket
read-only):

```scala mdoc:silent
def objectStoreAdmin(os: ObjectStore[IO]): IO[Unit] =
  for
    _    <- os.rename("old.txt", "new.txt")       // no re-upload
    list <- os.list.compile.toList                // live (non-deleted) objects
    _    <- os.watch.use {
              _.evalMap {
                case ObjectWatchEvent.Update(i)  => IO.println(s"updated ${i.name}")
                case ObjectWatchEvent.EndOfData  => IO.println("caught up")
              }.compile.drain
            }
    _    <- os.seal                                // make the bucket read-only
    _    <- IO.println(s"${list.size} live objects")
  yield ()
```

Reads of object meta use the JetStream **Direct Get** fast path when the bucket
allows it; chunk reads use the gap-resetting ordered consumer, so a `get`
recovers in order across a reconnect. Bucket management lives on the JetStream
context: `createObjectStore`, `objectStore`, `deleteObjectStore`,
`objectStoreStatus`, `objectStoreNames`.

See also [Authentication & TLS](auth.md) to secure the connection.
