# JetStream

JetStream is obtained as a `Resource` over a connected client (requires a
JetStream-enabled server, e.g. `nats-server -js`). It owns the publish window
and supervised fibers, and releases them with the `Resource`.

The snippets below share these imports and helpers:

```scala mdoc:silent
import cats.effect.IO
import cats.syntax.all.*
import scala.concurrent.duration.*
import fs2.Chunk
import fs2.nats.client.NatsClient
import fs2.nats.jetstream.*
import fs2.nats.jetstream.protocol.*

def process(payload: Chunk[Byte]): IO[Unit] = IO.println(s"got ${payload.size} bytes")
```

## Streams and persistent publish

Create a stream, then publish to it with a `PubAck`. The optional `Nats-Msg-Id`
(set via `PublishOptions.msgId`) enables server-side de-duplication:

```scala mdoc:silent
def jetStreamBasics(client: NatsClient[IO]): IO[Unit] =
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
      c    <- js.createConsumer(
                "ORDERS",
                ConsumerConfig(durable = Some("workers"), filterSubject = Some("orders.new"))
              )
      msgs <- c.fetch(batch = 10, maxWait = 2.seconds)
      _    <- msgs.traverse_(m => process(m.payload) *> m.ack)
    yield ()
  }
```

## Continuous pull consumption

`consume` owns a background pull loop as part of the `Resource`; it re-issues its
request on a cadence so it resumes after a dropped connection:

```scala mdoc:silent
def consumeLoop(c: JsConsumer[IO]): IO[Unit] =
  c.consume().use { stream =>
    stream.evalMap(m => process(m.payload) *> m.ack).compile.drain
  }
```

## Push consumption

Durable or ephemeral, with an optional queue group. Idle heartbeats are filtered
and flow-control requests answered automatically; ephemeral consumers are deleted
on release:

```scala mdoc:silent
def pushConsume(client: NatsClient[IO]): IO[Unit] =
  client.jetStream().use { js =>
    js.subscribePush(
        "ORDERS",
        ConsumerConfig(durable = Some("push-workers"), deliverGroup = Some("workers"))
      )
      .use(_.evalMap(m => process(m.payload) *> m.ack).compile.drain)
  }
```

## Ack semantics

Each delivered `JsMessage` carries the acks:

- `ack` — fire-and-forget acknowledgement.
- `ackSync` — double-ack; awaits server confirmation.
- `nak` / `nakWithDelay(delay)` — negative acknowledgement, optionally delayed.
- `inProgress` — resets the ack-wait timer; repeatable.
- `term` / `termWith(reason)` — stop redelivery for this message.

Finalizing acks take effect once; `inProgress` is repeatable.

**Reconnect:** push and pull subscriptions ride the client's automatic
subscription replay on reconnect; the pull `consume` loop additionally re-issues
its request on a cadence so it resumes after a dropped connection.

Built on JetStream: [Key-Value](key-value.md) and [Object Store](object-store.md).
