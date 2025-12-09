# fs2-nats

A functional, streaming NATS client for Scala 3, built on [FS2](https://fs2.io/) and [Cats Effect 3](https://typelevel.org/cats-effect/).

## Features

- **Pure functional** - Built entirely on Cats Effect 3 and FS2
- **Streaming first** - Native FS2 streams for message handling
- **Headers support** - Full NATS 2.2+ headers support (HPUB/HMSG)
- **Backpressure** - Configurable slow consumer policies
- **Reconnection** - Exponential backoff with full jitter
- **TLS support** - Secure connections with configurable TLS
- **Type-safe** - Leverages Scala 3 features for safety

## Installation

Add to your `build.sbt`:

```scala
libraryDependencies += "io.github.thatscalaguy" %% "fs2-nats" % "0.1.0"
```

## Quick Start

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

## Configuration

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

## Architecture

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

## Testing

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

## Examples

See the `examples/` directory for complete examples:

- `Basic.scala` - Simple publish/subscribe
- `RequestReplyExample` - Request/reply pattern
- `QueueGroupExample` - Load-balanced workers

Run examples:

```bash
sbt "runMain fs2.nats.examples.Basic"
```

## License

Apache License 2.0
