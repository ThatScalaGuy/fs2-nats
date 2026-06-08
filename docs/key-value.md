# Key-Value Store

A Key-Value bucket is an opinionated JetStream stream (`KV_<bucket>`, subjects
`$KV.<bucket>.>`). KV handles are obtained from the JetStream context. Reads use
JetStream **Direct Get** when the bucket allows it (`allowDirect`, the default),
so a `get` returns the raw message payload with no JSON/base64 decoding on the
hot path; writes ride the JetStream publish/coalescing window.

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

`keys`/`history`/`watch` stream from a gap-resetting **ordered consumer**, so a
reconnect mid-watch recovers in order rather than missing updates.

## Bucket management

Bucket management lives on the JetStream context: `createKeyValue`, `keyValue`
(bind to an existing bucket), `deleteKeyValue`, `keyValueStatus`, and
`keyValueNames`.

Next: the [Object Store](object-store.md) for large binary objects.
