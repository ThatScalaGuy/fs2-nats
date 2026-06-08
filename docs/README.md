# fs2-nats

**A functional, streaming [NATS](https://nats.io/) client for Scala 3 — built on
[Cats Effect 3](https://typelevel.org/cats-effect/) and [FS2](https://fs2.io/).**

*Every subscription is a `Stream`. Every connection is a `Resource`. Nothing
happens until you ask it to.*

`fs2-nats` lets you talk to NATS the functional way. Subscriptions are native
`fs2.Stream`s, the client is a `Resource` that cleans up after itself,
reconnection and backpressure are built in, and the whole surface — core
pub/sub, request/reply, JetStream, Key-Value, and Object Store — stays inside
Cats Effect from the first byte to the last. No callbacks, no hidden threads, no
surprises.

## Highlights

- **Pure functional** — built entirely on Cats Effect 3 and FS2, referentially transparent end to end
- **Streaming first** — every subscription is an `fs2.Stream[F, NatsMessage]`
- **Request/Reply** — shared-inbox request primitive with no-responders fast-fail
- **JetStream** — streams, consumers, persistent publish (`PubAck`), pull & push consume with full ack semantics
- **Key-Value** — buckets over JetStream with a Direct Get fast read path, optimistic concurrency, history, and watch
- **Object Store** — large binary objects chunked over JetStream with streaming put/get, SHA-256 verification, links, and watch
- **Headers** — full NATS 2.2+ headers support (HPUB/HMSG)
- **Backpressure** — configurable slow-consumer policies (block / drop / error)
- **Reconnection** — exponential backoff with full jitter, automatic subscription replay, and cluster failover
- **Authentication** — token, user/password, NKey (Ed25519), and decentralized JWT (`.creds`)
- **TLS & mutual TLS** — standard INFO-then-upgrade TLS with a caller-provided `TLSContext`

## Compatibility

|                 | Version                       |
| --------------- | ----------------------------- |
| **Scala**       | 3.3.7 (Scala 3.3 **LTS**)     |
| **JDK**         | 11, 17, 21, 25 — minimum 11   |
| **Cats Effect** | 3.7.x                         |
| **FS2**         | 3.13.x                        |
| **NATS Server** | **2.9+** recommended          |

Core messaging, headers, and JetStream work against NATS Server **2.2+**. The
Key-Value and Object Store features use the JetStream **Direct Get** API, which
requires NATS Server **2.9+**.

## Installation

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

## Hello, NATS

Start a server with `docker run -p 4222:4222 nats:latest`, then:

```scala mdoc:silent
import cats.effect.IO
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import fs2.nats.client.{ClientConfig, NatsClient}

val config = ClientConfig(
  host = Host.fromString("localhost").get,
  port = Port.fromInt(4222).get
)

val program: IO[Unit] =
  NatsClient.connect[IO](config).use { client =>
    client.subscribe("hello.world").use { messages =>
      for
        _   <- client.publish("hello.world", Chunk.array("Hello, NATS!".getBytes))
        msg <- messages.take(1).compile.lastOrError
        _   <- IO.println(s"Received: ${msg.payloadAsString}")
      yield ()
    }
  }
```

## Where to next

- **[Getting Started](getting-started.md)** — connect, publish, subscribe, wildcards, queue groups, request/reply.
- **[JetStream](jetstream.md)** — streams, persistent publish, and pull/push consumers.
- **[Key-Value Store](key-value.md)** — buckets, optimistic concurrency, history, and watch.
- **[Object Store](object-store.md)** — streaming large binary objects with SHA-256 verification.
- **[Authentication & TLS](auth.md)** — token, user/password, NKey, `.creds`, and (mutual) TLS.

The code examples on every page are compiled against the published API as part
of the build, so they stay in step with the library.
