# Getting Started

This page covers the core pub/sub surface: connecting, publishing, subscribing,
wildcards, queue groups, connection events, and request/reply. Runnable
versions of everything here (plus a request/reply responder and a queue-group
demo) are in
[`Basic.scala`](https://github.com/ThatScalaGuy/fs2-nats/blob/main/examples/Basic.scala)
on GitHub.

## Prerequisites

Start a NATS server:

```bash
docker run -p 4222:4222 nats:latest
```

The snippets below share these imports and helpers:

```scala mdoc:silent
import cats.effect.IO
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import fs2.nats.client.{ClientConfig, ClientEvent, NatsClient}
import fs2.nats.protocol.Headers
import fs2.nats.subscriptions.NatsMessage

val host = Host.fromString("localhost").get
val port = Port.fromInt(4222).get

def processWork(msg: NatsMessage): IO[Unit] = IO.println(s"processing ${msg.subject}")
```

## Basic usage

`NatsClient.connect` hands you the client as a `Resource`; a subscription is in
turn a `Resource` over an `fs2.Stream[F, NatsMessage]`. Nothing connects or
subscribes until the `Resource` is used.

```scala mdoc:silent
val config = ClientConfig(host = host, port = port)

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

Instead of building `Host`/`Port` by hand, `ClientConfig.localhost()` covers
local development and `ClientConfig.fromUrl` parses `nats://host:4222` and
`tls://host:4222` URLs (credentials are configured separately — see
[Authentication & TLS](auth.md)); `fromUrls` takes a list of cluster seed
servers:

```scala mdoc:silent
val local  = ClientConfig.localhost()
val parsed = ClientConfig.fromUrl("nats://demo.nats.io:4222")
```

## Publishing with headers

NATS 2.2+ headers are first class. Build a `Headers` value and pass it to
`publish`:

```scala mdoc:silent
def publishWithHeaders(client: NatsClient[IO]): IO[Unit] =
  val headers = Headers(
    "X-Request-Id" -> "abc123",
    "X-Timestamp"  -> System.currentTimeMillis().toString
  )
  client.publish("events.created", Chunk.array("""{"id": 1}""".getBytes), headers)
```

On the receiving side, `msg.headers` reads case-insensitively: `get` returns
the first value, `getAll` every value of a repeated key. `Headers` is
immutable — `add` appends a value, `set` replaces all values of a key, and
`remove` drops it:

```scala mdoc:silent
def requestId(msg: NatsMessage): Option[String] =
  msg.headers.get("x-request-id")
```

## Wildcard subscriptions

`*` matches a single token; `>` matches one or more trailing tokens:

```scala mdoc:silent
// Subscribe to all events under events.*
def singleToken(client: NatsClient[IO]): IO[Unit] =
  client.subscribe("events.*").use { messages =>
    messages.evalMap(msg => IO.println(s"${msg.subject}: ${msg.payloadAsString}")).compile.drain
  }

// Subscribe to events.a, events.a.b, events.a.b.c, ...
def multiToken(client: NatsClient[IO]): IO[Unit] =
  client.subscribe("events.>").use(_.compile.drain)
```

## Queue groups (load balancing)

Subscribers sharing a queue group split the messages between them:

```scala mdoc:silent
def worker(client: NatsClient[IO]): IO[Unit] =
  client.subscribe("work.queue", queueGroup = Some("workers")).use { messages =>
    messages.evalMap(processWork).compile.drain
  }
```

## Connection events

`client.events` is a `Stream[F, ClientEvent]` reporting connection lifecycle,
slow consumers, and protocol errors:

```scala mdoc:silent
def watchEvents(client: NatsClient[IO]): IO[Unit] =
  client.events.evalMap {
    case ClientEvent.Connected(info) =>
      IO.println(s"Connected to ${info.serverId}")
    case ClientEvent.Disconnected(reason, willReconnect) =>
      IO.println(s"Disconnected: $reason, reconnecting: $willReconnect")
    case ClientEvent.Reconnected(info, attempt) =>
      IO.println(s"Reconnected to ${info.serverId} after $attempt attempts")
    case ClientEvent.SlowConsumer(sid, subject, dropped) =>
      IO.println(s"Slow consumer on $subject, dropped $dropped messages")
    case other =>
      IO.println(s"Event: $other")
  }.compile.drain
```

Further variants: `Reconnecting(attempt, delayMs)` before each attempt,
`ProtocolError(message, fatal)`, `ServerInfoUpdated(info)` on a fresh `INFO`,
`LameDuckMode` when the server announces it is draining, and
`MaxReconnectsExceeded(attempts, lastError)` when the client gives up.

## Inspecting the connection

`serverInfo` exposes the server's `INFO` (id, version, `maxPayload`, whether
JetStream and headers are available, ...), and `isConnected` reports the live
connection state:

```scala mdoc:silent
def payloadLimit(client: NatsClient[IO]): IO[Long] =
  client.serverInfo.map(_.maxPayload)
```

## Request/Reply

`request` publishes to a shared response inbox and awaits a single reply. It
fails fast with `NatsError.NoResponders` if nobody is listening (503), or
`NatsError.Timeout` if no reply arrives within the timeout (5 seconds unless
overridden); request headers ride along as a parameter:

```scala mdoc:silent
import scala.concurrent.duration.*

def echo(client: NatsClient[IO]): IO[NatsMessage] =
  client.request("service.echo", Chunk.array("ping".getBytes))

def echoTuned(client: NatsClient[IO]): IO[NatsMessage] =
  client.request(
    "service.echo",
    Chunk.array("ping".getBytes),
    headers = Headers("X-Request-Id" -> "abc123"),
    timeout = 10.seconds
  )
```

The responder side is a plain subscription: a request is a message whose
`replyTo` is set (`msg.isRequest`), and answering means publishing to that
subject:

```scala mdoc:silent
def echoResponder(client: NatsClient[IO]): IO[Unit] =
  client.subscribe("service.echo").use { requests =>
    requests.evalMap { req =>
      req.replyTo match
        case Some(reply) => client.publish(reply, req.payload)
        case None        => IO.unit
    }.compile.drain
  }
```

(`publish` also takes a `replyTo` parameter for wiring the pattern manually.)
For request/reply with typed payloads, typed errors and discovery, see
[Micro Services](micro.md).

Next up: [JetStream](jetstream.md) for persistence and consumers.
