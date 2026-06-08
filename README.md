<div align="center">

# fs2-nats 🐈‍⬛📡

**A functional, streaming [NATS](https://nats.io/) client for Scala 3 — built on [Cats Effect 3](https://typelevel.org/cats-effect/) and [FS2](https://fs2.io/).**

_Every subscription is a `Stream`. Every connection is a `Resource`. Nothing happens until you ask it to._

[![Maven Central](https://img.shields.io/maven-central/v/de.thatscalaguy/fs2-nats_3?style=flat-square&logo=apachemaven&logoColor=white&label=Maven%20Central&color=blue)](https://central.sonatype.com/artifact/de.thatscalaguy/fs2-nats_3)
[![Cats Friendly](https://typelevel.org/cats/img/cats-badge-tiny.png)](https://typelevel.org/cats/#cats-friendly-libraries)
[![CI](https://img.shields.io/github/actions/workflow/status/ThatScalaGuy/fs2-nats/ci.yml?branch=main&style=flat-square&logo=github&label=CI)](https://github.com/ThatScalaGuy/fs2-nats/actions/workflows/ci.yml)
[![javadoc](https://javadoc.io/badge2/de.thatscalaguy/fs2-nats_3/scaladoc.svg?style=flat-square&label=API%20docs)](https://javadoc.io/doc/de.thatscalaguy/fs2-nats_3)
<br/>
[![Scala 3.3 LTS](https://img.shields.io/badge/Scala-3.3%20LTS-DC322F?style=flat-square&logo=scala&logoColor=white)](https://www.scala-lang.org/)
[![JDK 11+](https://img.shields.io/badge/JDK-11%2B-007396?style=flat-square&logo=openjdk&logoColor=white)](https://adoptium.net/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue?style=flat-square)](https://www.apache.org/licenses/LICENSE-2.0)

</div>

---

`fs2-nats` lets you talk to NATS the functional way. Subscriptions are native `fs2.Stream`s,
the client is a `Resource` that cleans up after itself, reconnection and backpressure are
built in, and the whole surface — core pub/sub, request/reply, JetStream, Key-Value, and
Object Store — stays inside Cats Effect from the first byte to the last. No callbacks, no
hidden threads, no surprises. 🎉

## ✨ Highlights

- 🧊 **Pure functional** — built entirely on Cats Effect 3 and FS2, referentially transparent end to end
- 🌊 **Streaming first** — every subscription is an `fs2.Stream[F, NatsMessage]`
- 🔁 **Request/Reply** — shared-inbox request primitive with no-responders fast-fail
- 💾 **JetStream** — streams, consumers, persistent publish (PubAck), pull & push consume with full ack semantics
- 🗂️ **Key-Value** — buckets over JetStream with a Direct Get fast read path, optimistic concurrency, history, and watch
- 📦 **Object Store** — large binary objects chunked over JetStream with streaming put/get, SHA-256 verification, links, and watch
- 🏷️ **Headers** — full NATS 2.2+ headers support (HPUB/HMSG)
- 🚦 **Backpressure** — configurable slow-consumer policies (block / drop / error)
- 🔌 **Reconnection** — exponential backoff with full jitter, automatic subscription replay, and cluster failover
- 🔐 **Authentication** — token, user/password, NKey (Ed25519), and decentralized JWT (`.creds`)
- 🔒 **TLS & mutual TLS** — standard INFO-then-upgrade TLS with a caller-provided `TLSContext`
- 🎯 **Type-safe** — leverages Scala 3 features for safety

## 🧩 Compatibility

|                 | Version                             |
| --------------- | ----------------------------------- |
| **Scala**       | 3.3.7 (Scala 3.3 **LTS**)           |
| **JDK**         | 11, 17, 21, 25 — **minimum JDK 11** |
| **Cats Effect** | 3.7.x                               |
| **FS2**         | 3.13.x                              |
| **NATS Server** | **2.9+** recommended                |

> **NATS server versions:** core messaging, headers, and JetStream work against **NATS Server 2.2+**.
> The Key-Value and Object Store features use the JetStream **Direct Get** API, which requires
> **NATS Server 2.9+**. The test suite runs against the latest `nats` Docker image.

## 📦 Installation

> **Latest release: `0.2.0`** — published to Maven Central for Scala 3.

**sbt**

```scala
libraryDependencies += "de.thatscalaguy" %% "fs2-nats" % "0.2.0"
```

**Mill**

```scala
ivy"de.thatscalaguy::fs2-nats:0.2.0"
```

**scala-cli**

```scala
//> using dep de.thatscalaguy::fs2-nats:0.2.0
```

## 🚀 Quick Start

### Prerequisites

Start a NATS server:

```bash
docker run -p 4222:4222 nats:latest
```

### Basic Usage

```scala
import cats.effect.{IO, IOApp, ExitCode}
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import fs2.nats.client.{ClientConfig, NatsClient}

object Main extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val config = ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(4222).get
    )

    NatsClient.connect[IO](config).use { client =>
      for
        // Subscribe to a subject
        _ <- client.subscribe("hello.world").use { messages =>
          for
            // Publish a message
            _ <- client.publish(
              "hello.world",
              Chunk.array("Hello, NATS!".getBytes)
            )

            // Receive the message
            msg <- messages.take(1).compile.lastOrError
            _ <- IO.println(s"Received: ${msg.payloadAsString}")
          yield ()
        }
      yield ExitCode.Success
    }
```

### Publishing with Headers

```scala
import fs2.nats.protocol.Headers

val headers = Headers(
  "X-Request-Id" -> "abc123",
  "X-Timestamp" -> System.currentTimeMillis().toString
)

client.publish(
  "events.created",
  Chunk.array("""{"id": 1}""".getBytes),
  headers
)
```

### Wildcard Subscriptions

```scala
// Subscribe to all events under events.*
client.subscribe("events.*").use { messages =>
  messages.evalMap { msg =>
    IO.println(s"${msg.subject}: ${msg.payloadAsString}")
  }.compile.drain
}

// Subscribe to all events under events.>
client.subscribe("events.>").use { messages =>
  // Handles events.a, events.a.b, events.a.b.c, etc.
  messages.compile.drain
}
```

### Queue Groups (Load Balancing)

```scala
// Multiple subscribers in same queue group share messages
client.subscribe("work.queue", queueGroup = Some("workers")).use { messages =>
  messages.evalMap { msg =>
    processWork(msg)
  }.compile.drain
}
```

### Connection Events

```scala
import fs2.nats.client.ClientEvent

client.events.evalMap {
  case ClientEvent.Connected(info) =>
    IO.println(s"Connected to ${info.serverId}")
  case ClientEvent.Disconnected(reason, willReconnect) =>
    IO.println(s"Disconnected: $reason, reconnecting: $willReconnect")
  case ClientEvent.Reconnected(info, attempt) =>
    IO.println(s"Reconnected after $attempt attempts")
  case ClientEvent.SlowConsumer(sid, subject, dropped) =>
    IO.println(s"Slow consumer on $subject, dropped $dropped messages")
  case other =>
    IO.println(s"Event: $other")
}.compile.drain
```

### Request/Reply

```scala
// Send a request and await a single reply (shared response inbox).
val reply = client.request("service.echo", Chunk.array("ping".getBytes))
// Fails fast with NatsError.NoResponders if nobody is listening (503),
// or NatsError.Timeout if no reply arrives within the timeout.
```

## 💾 JetStream

JetStream is obtained as a `Resource` over a connected client (requires a
JetStream-enabled server, e.g. `nats-server -js`). It owns the publish window
and supervised fibers, and releases them with the `Resource`.

```scala
import fs2.nats.jetstream.*
import fs2.nats.jetstream.protocol.*

client.jetStream().use { js =>
  for
    // Stream management
    _   <- js.addStream(StreamConfig(name = "ORDERS", subjects = List("orders.>")))

    // Persistent publish with PubAck (+ dedup via Nats-Msg-Id)
    ack <- js.publish(
             "orders.new",
             Chunk.array("order #1".getBytes),
             opts = PublishOptions(msgId = Some("order-1"))
           )
    _   <- IO.println(s"stored seq=${ack.seq} duplicate=${ack.duplicate}")

    // Pull consumer: create + fetch + ack
    c   <- js.createConsumer(
             "ORDERS",
             ConsumerConfig(durable = Some("workers"), filterSubject = Some("orders.new"))
           )
    msgs <- c.fetch(batch = 10, maxWait = 2.seconds)
    _    <- msgs.traverse_(m => process(m.payload) *> m.ack)
  yield ()
}
```

Continuous pull consumption (background pull loop owned by the `Resource`):

```scala
c.consume().use { stream =>
  stream.evalMap(m => process(m.payload) *> m.ack).compile.drain
}
```

Push consumption (durable or ephemeral, optional queue group). Idle heartbeats
are filtered and flow-control requests answered automatically; ephemeral
consumers are deleted on release:

```scala
js.subscribePush(
    "ORDERS",
    ConsumerConfig(durable = Some("push-workers"), deliverGroup = Some("workers"))
  )
  .use(_.evalMap(m => process(m.payload) *> m.ack).compile.drain)
```

**Ack semantics:** `ack` (fire-and-forget), `ackSync` (double-ack, awaits server
confirmation), `nak` / `nakWithDelay`, `inProgress` (resets the ack-wait timer),
and `term` / `termWith`. Finalizing acks take effect once; `inProgress` is
repeatable.

**Reconnect:** push and pull subscriptions ride the client's automatic
subscription replay on reconnect; the pull `consume` loop additionally re-issues
its request on a cadence so it resumes after a dropped connection.

## 🗂️ Key-Value Store

A Key-Value bucket is an opinionated JetStream stream (`KV_<bucket>`, subjects
`$KV.<bucket>.>`). KV handles are obtained from the JetStream context. Reads use
JetStream **Direct Get** when the bucket allows it (`allow_direct`, the default),
so a `get` returns the raw message payload with no JSON/base64 decoding on the
hot path; writes ride the JetStream publish/coalescing window.

```scala
import fs2.nats.kv.*

client.jetStream().use { js =>
  for
    // Create a bucket keeping the last 5 revisions of each key
    kv   <- js.createKeyValue(KvConfig(bucket = "config", history = 5))

    // Put returns the new revision (the entry's stream sequence)
    rev  <- kv.put("db.url", Chunk.array("postgres://localhost".getBytes))
    cur  <- kv.get("db.url")                 // Option[KvEntry] (Direct Get)

    // Optimistic concurrency: only writes if the revision still matches
    rev2 <- kv.update("db.url", Chunk.array("postgres://prod".getBytes), rev)

    // create fails if the key exists; delete writes a tombstone, purge
    // collapses a key's history
    _    <- kv.delete("legacy")
    keys <- kv.keys.compile.toList           // live keys (excludes deletes)
  yield ()
}
```

Watch the snapshot and live updates. The stream delivers the current entries,
then a single `KvWatchEvent.EndOfData` marker, then live changes:

```scala
kv.watch(">").use { stream =>
  stream.evalMap {
    case KvWatchEvent.Entry(e)  => onChange(e.key, e.value, e.operation)
    case KvWatchEvent.EndOfData => IO.println("caught up")
  }.compile.drain
}
```

Bucket management lives on the JetStream context: `createKeyValue`, `keyValue`
(bind to an existing bucket), `deleteKeyValue`, `keyValueStatus`, and
`keyValueNames`. `create`/`update` raise `NatsError.KeyValueWrongLastSequence`
when their optimistic-concurrency precondition fails.

`keys`/`history`/`watch` stream from a gap-resetting **ordered consumer**, so a
reconnect mid-watch recovers in order rather than missing updates.

## 📦 Object Store

An Object Store bucket is an opinionated JetStream stream (`OBJ_<bucket>`,
subjects `$O.<bucket>.C.>` for chunks and `$O.<bucket>.M.>` for per-object
meta). It stores arbitrarily large binary objects by chunking them across the
stream, with a rolled-up meta message recording each object's size, chunk count,
and SHA-256 digest. Both `put` and `get` are fully streaming — neither
materializes a whole object in memory.

```scala
import fs2.nats.objectstore.*

client.jetStream().use { js =>
  for
    os <- js.createObjectStore(ObjConfig(bucket = "assets"))

    // Stream bytes in (here from a file); chunks are pipelined through the
    // publish window and coalesced into socket writes. Nothing is buffered whole.
    info <- os.put(
      ObjectMeta("logo.png", maxChunkSize = 128 * 1024),
      fs2.io.file.Files[IO].readAll(fs2.io.file.Path("logo.png"))
    )

    // Stream bytes out; the SHA-256 digest is verified once all chunks are read.
    _ <- os.get("logo.png").flatMap {
      case Some(r) => r.data.through(sink).compile.drain
      case None    => IO.unit
    }

    // Convenience for small objects and files
    _   <- os.putBytes(ObjectMeta("readme.txt"), Chunk.array("hi".getBytes))
    txt <- os.getBytes("readme.txt")              // Option[Chunk[Byte]]
    _   <- os.putFile("backup.tar", fs2.io.file.Path("backup.tar"))
    _   <- os.getToFile("backup.tar", fs2.io.file.Path("restored.tar"))
  yield ()
}
```

Objects support links (`addLink`/`addBucketLink`, transparently resolved on
`get`/`info`), metadata updates and `rename` (no re-upload), `delete`, `list`,
`watch` (snapshot + `EndOfData` + live updates), and `seal` (make the bucket
read-only). Reads of object meta use the JetStream **Direct Get** fast path when
the bucket allows it; chunk reads use the gap-resetting ordered consumer, so a
`get` recovers in order across a reconnect. Bucket management lives on the
JetStream context: `createObjectStore`, `objectStore`, `deleteObjectStore`,
`objectStoreStatus`, `objectStoreNames`.

## 🔐 Authentication & TLS

`fs2-nats` supports every client-side NATS authentication mechanism. Choose one
by setting `ClientConfig.credentials`.

### Token

```scala
ClientConfig(host = host, port = port, credentials = Some(NatsCredentials.Token("s3cr3t")))
```

### Username / password

```scala
ClientConfig(host = host, port = port, credentials = Some(NatsCredentials.UserPassword("user", "pass")))
```

### NKey (Ed25519)

Provide the NKey seed (an `S...` string); the client signs the server's nonce
and derives the public key from it:

```scala
ClientConfig(host = host, port = port, credentials = Some(NatsCredentials.NKey("SUAB...seed...")))
```

### Decentralized JWT (`.creds` files)

Operator-mode deployments (NGS / Synadia Cloud / self-hosted with `nsc`) issue a
`.creds` file bundling a user JWT and an NKey seed. Load it directly:

```scala
import fs2.io.file.Path
import fs2.nats.client.{ClientConfig, NatsClient, NatsCredentials}

NatsCredentials.fromCredsFile[IO](Path("user.creds")).flatMap { creds =>
  NatsClient
    .connect[IO](ClientConfig(host = host, port = port, credentials = Some(creds)))
    .use { client => /* ... */ }
}
```

`NatsCredentials.fromCreds(content)` parses an already-loaded string.

### TLS

Set `useTls = true` and supply a `TLSContext` — one is required; the client
never falls back to plaintext or a default context:

```scala
import fs2.io.net.Network

val tls = Network[IO].tlsContext.system // or .fromSSLContext(...)

NatsClient
  .connect[IO](ClientConfig(host = host, port = port, useTls = true), tlsContext = Some(tls))
  .use { client => /* ... */ }
```

The client follows the standard NATS handshake: it reads the plaintext `INFO`,
then upgrades the connection to TLS. Servers configured with
`handshake_first: true` (TLS before `INFO`) are not supported.

### Mutual TLS

For mutual TLS, build the `TLSContext` from an `SSLContext` whose `KeyManager`
presents your client certificate (and whose `TrustManager` trusts the server's
CA), then pass it exactly as above:

```scala
val tls = Network[IO].tlsContext.fromSSLContext(mySslContext)
```

## ⚙️ Configuration

### ClientConfig

```scala
import scala.concurrent.duration._
import fs2.nats.client._

val config = ClientConfig(
  host = Host.fromString("nats.example.com").get,
  port = Port.fromInt(4222).get,
  useTls = false,
  tlsParams = None,
  name = Some("my-app"),
  credentials = Some(NatsCredentials.UserPassword("user", "pass")),
  backoff = BackoffConfig(
    baseDelay = 100.millis,
    maxDelay = 30.seconds,
    factor = 2.0,
    maxRetries = None  // unlimited
  ),
  queueCapacity = 10000,
  slowConsumerPolicy = SlowConsumerPolicy.Block,
  verbose = false,
  pedantic = false,
  echo = true
)
```

### Slow Consumer Policies

When a subscription queue fills up:

- `SlowConsumerPolicy.Block` - Backpressure (default)
- `SlowConsumerPolicy.DropNew` - Drop incoming messages
- `SlowConsumerPolicy.DropOldest` - Drop oldest queued messages
- `SlowConsumerPolicy.ErrorAndDrop` - Emit event and drop

### Backoff Strategies

```scala
import fs2.nats.client.Backoff

// Exponential backoff with jitter (recommended)
val policy = Backoff.exponentialWithJitter(
  base = 100.millis,
  max = 30.seconds,
  factor = 2.0,
  maxRetries = Some(10)
)

// Fixed delay
val fixed = Backoff.fixed(5.seconds, maxRetries = Some(5))

// No delay (for testing)
val immediate = Backoff.immediate(maxRetries = 3)
```

## 🏗️ Architecture

```
fs2.nats
├── client/
│   ├── NatsClient        # Main public API
│   ├── ClientConfig      # Configuration
│   ├── ConnectionManager # Connection lifecycle & reconnection
│   └── Backoff           # Retry policies
├── protocol/
│   ├── ProtocolParser    # Incremental NATS protocol parser
│   ├── NatsModel         # Protocol data types (Info, Connect, etc.)
│   ├── Headers           # NATS/1.0 headers support
│   └── NatsFrame         # Parsed frame ADT
├── transport/
│   ├── Transport         # Transport abstraction
│   ├── NatsSocket        # TCP transport
│   └── TlsTransport      # TLS transport wrapper
├── subscriptions/
│   ├── SubscriptionManager # Message routing & slow consumer handling
│   ├── SidAllocator      # Subscription ID allocation
│   └── NatsMessage       # User-facing message type
├── publish/
│   ├── Publisher         # Publish with max_payload validation
│   └── SerializationUtils # Protocol serialization
└── errors/
    └── NatsError         # Error ADT
```

## 🧪 Testing

Run unit tests:

```bash
sbt test
```

Run integration tests (requires NATS server):

```bash
docker-compose up -d
sbt integration/test
docker-compose down
```

`docker-compose.yml` defines a broker per auth mode — `nats-nkey` (4223),
`nats-token` (4224), `nats-userpass` (4225), `nats-creds` (4226, operator mode),
`nats-tls` (4227) and `nats-mtls` (4228) — alongside the default `nats` (4222).
The decentralized-JWT fixtures (`integration/src/test/resources/testuser.creds`,
`nats-creds.conf`) are generated with `nsc`, and the TLS certificates are
regenerated with `integration/src/test/resources/tls/gen-certs.sh`.

### Pre-push hook (optional)

A `.githooks/pre-push` hook runs the same gate CI enforces (`githubWorkflowCheck`,
`scalafmtCheckAll`/`headerCheckAll`, `test`, `mimaReportBinaryIssues`, `doc`).
Enable it once per clone:

```bash
git config core.hooksPath .githooks
```

Bypass a single push with `SKIP_PREPUSH=1 git push`.

## 📚 Examples

See the `examples/` directory for complete examples:

- `Basic.scala` - Simple publish/subscribe
- `RequestReplyExample` - Request/reply pattern
- `QueueGroupExample` - Load-balanced workers
- `JetStreamExample.scala` - Streams, persistent publish, and pull consumption (requires `-js`)
- `KeyValueExample.scala` - Key-Value buckets: put/get, optimistic concurrency, keys, and watch (requires `-js`)
- `ObjectStoreExample.scala` - Object Store: streaming put/get, links, list, watch, and seal (requires `-js`)

Run examples:

```bash
sbt "runMain fs2.nats.examples.Basic"
```

## 💼 Commercial Support

fs2-nats is built and maintained by **[ThatScalaGuy](https://www.thatscalaguy.de)**

Need extended or help on your project?
**Get in touch at [thatscalaguy.de](https://www.thatscalaguy.de).**

## 🤝 Contributing

Contributions are welcome! Issues and pull requests are happily accepted over on
[GitHub](https://github.com/ThatScalaGuy/fs2-nats). The pre-push hook above runs
the same checks as CI, so enabling it is the quickest way to keep the build green.

## 📄 License

Licensed under the [Apache License 2.0](LICENSE).
