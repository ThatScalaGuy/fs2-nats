# Object Store

An Object Store bucket is an opinionated JetStream stream (`OBJ_<bucket>`,
subjects `$O.<bucket>.C.>` for chunks and `$O.<bucket>.M.>` for per-object
meta). It stores arbitrarily large binary objects by chunking them across the
stream, with a rolled-up meta message recording each object's size, chunk count,
and SHA-256 digest. Both `put` and `get` are fully streaming — neither
materializes a whole object in memory. A runnable version of this page is
[`ObjectStoreExample.scala`](https://github.com/ThatScalaGuy/fs2-nats/blob/main/examples/ObjectStoreExample.scala)
on GitHub.

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
                ObjectMeta("logo.png"),
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

Chunks default to 128 KiB (`ObjConfig.DefaultChunkSize`); override per object
via `ObjectMeta.maxChunkSize`. `ObjectMeta` also carries a `description` and a
free-form `metadata` map, both stored with the object. Putting under an
existing name replaces the object — the previous chunks are purged once the
new meta is durable; there is no versioning. Note the digest is only verified
when the data stream is read to its end — a consumer that stops early skips
the check. On a full read with a mismatch, the stream fails with
`NatsError.ObjectDigestMismatch`.

Every write and read returns or resolves to an `ObjectInfo`: `size`, `chunks`,
the `digest` (`SHA-256=<url-base64>`), `deleted` flag, `modified` timestamp,
your description/metadata, and the `link` field for link entries. `info(name)`
fetches it without downloading anything — `None` if the object is absent or
deleted:

```scala mdoc:silent
def exists(os: ObjectStore[IO], name: String): IO[Boolean] =
  os.info(name).map(_.isDefined)
```

`updateMeta(name, meta)` rewrites description/metadata in place (and renames,
if `meta.name` differs) without re-uploading data.

## Bucket configuration

`ObjConfig` covers the bucket options: `description`, a `ttl` for object
expiry, `maxBytes`, `storage` (`File`/`Memory`), `replicas`, `compression`
(S2 **by default** — trades CPU for storage and needs NATS Server 2.10+), and
`allowDirect` for the meta fast-read path:

```scala mdoc:silent
import scala.concurrent.duration.*
import fs2.nats.jetstream.protocol.StoreCompression

val cacheConfig = ObjConfig(
  bucket = "render-cache",
  ttl = Some(7.days),
  maxBytes = 10L * 1024 * 1024 * 1024,
  compression = StoreCompression.None
)
```

Bucket names must match `[A-Za-z0-9_-]+`. Object names are nearly free-form
(slashes, dots and spaces are fine — the meta subject token is URL-safe
base64) but limited to 190 UTF-8 bytes.

## Links, rename, list, watch, and seal

`addLink(linkName, target)` stores a pointer to another object — the target is
the resolved `ObjectInfo`, not a name — and `get`/`info` follow it
transparently. Linking to a deleted object raises `NatsError.ObjectNotFound`;
linking to a link raises `NatsError.ObjectIsLink`. `addBucketLink(linkName,
target)` takes a bucket handle and records a pointer to a whole bucket; it is
*not* resolved — reading it
returns the link entry itself, whose `ObjectLink` has `name = None`.

The admin surface: `rename` (no re-upload), `delete` (tombstones the meta and
purges the object's chunks, reclaiming storage; `ObjectNotFound` if already
gone), `list`, `watch` (snapshot + `EndOfData` + live updates), and `seal`
(make the bucket read-only):

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

In a `watch`, deletions arrive as `Update` events with `info.deleted = true`
(the initial snapshot includes tombstones — unlike `list`, which filters
them), and an empty bucket emits `EndOfData` immediately.

Reads of object meta use the JetStream **Direct Get** fast path when the bucket
allows it; chunk reads use the gap-resetting ordered consumer, so a `get`
recovers in order across a reconnect. Bucket management lives on the JetStream
context: `createObjectStore`, `objectStore` (bind to an existing bucket),
`deleteObjectStore`, `objectStoreStatus`, and `objectStoreNames` (a `Stream`
of bucket names). The handle itself exposes `os.bucket` and `os.status` — an
`ObjStatus` with description, TTL, storage type, replicas, total size, and
`isSealed`.

Operations raise typed errors from `NatsError`: `ObjectNotFound`,
`ObjectAlreadyExists` (e.g. renaming onto an existing name), `ObjectIsLink`,
and `ObjectDigestMismatch`.

See also [Authentication & TLS](auth.md) to secure the connection.
