# Getting Started

This page covers the core pub/sub surface: connecting, publishing, subscribing,
wildcards, queue groups, connection events, and request/reply.

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

## Request/Reply

`request` publishes to a shared response inbox and awaits a single reply. It
fails fast with `NatsError.NoResponders` if nobody is listening (503), or
`NatsError.Timeout` if no reply arrives within the timeout:

```scala mdoc:silent
def echo(client: NatsClient[IO]): IO[NatsMessage] =
  client.request("service.echo", Chunk.array("ping".getBytes))
```

Next up: [JetStream](jetstream.md) for persistence and consumers.
