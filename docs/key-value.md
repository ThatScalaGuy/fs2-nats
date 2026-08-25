# Key-Value Store

A Key-Value bucket is an opinionated JetStream stream (`KV_<bucket>`, subjects
`$KV.<bucket>.>`). KV handles are obtained from the JetStream context. Reads use
JetStream **Direct Get** when the bucket allows it (`allowDirect`, the default),
so a `get` returns the raw message payload with no JSON/base64 decoding on the
hot path; writes ride the JetStream publish/coalescing window. A runnable
version of this page is
[`KeyValueExample.scala`](https://github.com/ThatScalaGuy/fs2-nats/blob/main/examples/KeyValueExample.scala)
on GitHub.

The snippets below share these imports and helpers:

```scala mdoc:silent
import cats.effect.IO
import fs2.Chunk
import fs2.nats.client.NatsClient
import fs2.nats.kv.*

def onChange(key: String, value: Chunk[Byte], op: KvOperation): IO[Unit] =
  IO.println(s"$key = ${value.size} bytes ($op)")
```

## Put, get, and optimistic concurrency

`put` returns the new revision (the entry's stream sequence). `update` only
writes if the revision still matches, raising `NatsError.KeyValueWrongLastSequence`
otherwise:

```scala mdoc:silent
def kvBasics(client: NatsClient[IO]): IO[Unit] =
  client.jetStream().use { js =>
    for
      // Create a bucket keeping the last 5 revisions of each key
      kv   <- js.createKeyValue(KvConfig(bucket = "config", history = 5))

      // Put returns the new revision
      rev  <- kv.put("db.url", Chunk.array("postgres://localhost".getBytes))
      cur  <- kv.get("db.url")                 // Option[KvEntry] (Direct Get)

      // Optimistic concurrency: only writes if the revision still matches
      rev2 <- kv.update("db.url", Chunk.array("postgres://prod".getBytes), rev)

      // delete writes a tombstone; purge collapses a key's history
      _    <- kv.delete("legacy")
      keys <- kv.keys.compile.toList           // live keys (excludes deletes)

      _    <- IO.println(s"current=$cur updated-rev=$rev2 live-keys=${keys.size}")
    yield ()
  }
```

`create` (fails if the key exists) and `update` raise
`NatsError.KeyValueWrongLastSequence` when their optimistic-concurrency
precondition fails.

A `KvEntry` carries more than the value: its `revision`, `created` timestamp,
`delta` (distance from the latest revision — `0` means current) and
`operation` (`Put`, or the `Delete`/`Purge` tombstones).

For pipelined writes, `putAsync` returns nested effects: the outer one
completes once the publish is on the wire, the inner one yields the revision.

## History and point-in-time reads

With `history > 1` the bucket retains up to that many revisions per key
(server limit: 64). `history(key)` lists them oldest-first, and `get` with a
revision reads one directly. `delete` writes a tombstone on top; `purge`
instead collapses the key's whole history into a single `Purge` marker:

```scala mdoc:silent
def kvHistory(kv: KeyValue[IO]): IO[Unit] =
  for
    revs <- kv.history("db.url")
    _    <- IO.println(revs.map(e => s"${e.revision}: ${e.operation}").mkString(", "))
    entry <- kv.get("db.url", revision = revs.head.revision)
    _     <- IO.println(s"first revision: ${entry.map(_.value.size)} bytes")
    _     <- kv.purge("legacy")
  yield ()
```

## Watch

Watch delivers the current entries, then a single `KvWatchEvent.EndOfData`
marker, then live changes:

```scala mdoc:silent
def kvWatch(kv: KeyValue[IO]): IO[Unit] =
  kv.watch(">").use { stream =>
    stream.evalMap {
      case KvWatchEvent.Entry(e)  => onChange(e.key, e.value, e.operation)
      case KvWatchEvent.EndOfData => IO.println("caught up")
    }.compile.drain
  }
```

The first argument is a key pattern and accepts the NATS wildcards `*` and
`>` — `watch("config.>")` follows a subtree, `watchAll()` is shorthand for the
whole bucket. `WatchOptions` refines the delivery: `includeHistory` replays
every retained revision, `updatesOnly` skips the initial snapshot (and takes
precedence over `includeHistory` if both are set), `ignoreDeletes` drops the
`Delete`/`Purge` tombstones,
and `metaOnly` delivers entries without their values:

```scala mdoc:silent
def kvUpdates(kv: KeyValue[IO]): IO[Unit] =
  kv.watch("config.>", WatchOptions(updatesOnly = true, ignoreDeletes = true))
    .use(_.compile.drain)
```

`keys`/`history`/`watch` stream from a gap-resetting **ordered consumer**, so a
reconnect mid-watch recovers in order rather than missing updates.

## Bucket configuration and management

`KvConfig` exposes the bucket options beyond `history`: a per-entry `ttl`,
`storage` (`File`/`Memory`), size limits (`maxBytes`, `maxValueSize` — `-1`
means unlimited), `replicas`, S2 `compression`, and `allowDirect` (disable it
to route reads through `STREAM.MSG.GET` instead of Direct Get):

```scala mdoc:silent
import scala.concurrent.duration.*
import fs2.nats.jetstream.protocol.{StorageType, StoreCompression}

val sessionsConfig = KvConfig(
  bucket = "sessions",
  history = 1,
  ttl = Some(30.minutes),
  storage = StorageType.Memory,
  maxValueSize = 64 * 1024,
  compression = StoreCompression.S2
)
```

Bucket management lives on the JetStream context: `createKeyValue`, `keyValue`
(bind to an existing bucket), `deleteKeyValue` (destroys the bucket and its
data), `keyValueStatus`, and `keyValueNames` (a `Stream` of bucket names). The
handle itself reports its bucket via `kv.bucket` and its live state via
`kv.status` — a `KvStatus` with the value count, retained history depth, TTL,
total bytes, storage type and replica count.

Bucket names must match `[A-Za-z0-9_-]+`; keys may contain alphanumerics and
`-/_=.` but must not start or end with a dot. Invalid names fail with
`NatsError.InvalidSubject`.

Next: the [Object Store](object-store.md) for large binary objects.
