# JetStream

JetStream is obtained as a `Resource` over a connected client (requires a
JetStream-enabled server, e.g. `nats-server -js`). It scopes the bounded
`publishAsync` in-flight window; acquisition fails with
`NatsError.JetStreamNotEnabled` if the server has no JetStream. A runnable version of this page is
[`JetStreamExample.scala`](https://github.com/ThatScalaGuy/fs2-nats/blob/main/examples/JetStreamExample.scala)
on GitHub.

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

`client.jetStream()` takes an optional `JetStreamConfig`: the API prefix and
`domain` for leaf-node setups (a domain routes requests via
`$JS.<domain>.API.` and skips the JetStream-enabled check), the default
request `timeout` (5 seconds), and the `publishAsync` window size (256):

```scala mdoc:silent
val leafConfig = JetStreamConfig(domain = Some("hub"), timeout = 10.seconds)
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

`StreamConfig` goes far beyond name and subjects: retention
(`Limits`/`Interest`/`WorkQueue`), storage (`File`/`Memory`), limits
(`maxMsgs`, `maxBytes`, `maxAge`, `maxMsgsPerSubject`, ... — `-1` means
unlimited), the discard policy, replication, S2 compression, and the
`duplicateWindow` inside which `Nats-Msg-Id` de-duplication applies. Mirrors,
sources, republish, subject transforms and placement are available as well:

```scala mdoc:silent
val archiveConfig = StreamConfig(
  name = "ORDERS-ARCHIVE",
  subjects = List("orders.>"),
  retention = RetentionPolicy.Limits,
  storage = StorageType.File,
  maxAge = Some(30.days),
  maxMsgsPerSubject = 1000,
  discard = DiscardPolicy.Old,
  replicas = 1,
  compression = StoreCompression.S2,
  duplicateWindow = Some(2.minutes)
)
```

`PublishOptions` also carries the optimistic-concurrency preconditions — the
publish is rejected unless the stream, its last sequence, the last sequence on
this subject, or the last `Nats-Msg-Id` match — plus a per-publish `timeout`:

```scala mdoc:silent
def casPublish(js: JetStream[IO], lastSeq: Long): IO[PubAck] =
  js.publish(
    "orders.new",
    Chunk.array("order #2".getBytes),
    opts = PublishOptions(
      expectedStream = Some("ORDERS"),
      expectedLastSubjectSeq = Some(lastSeq)
    )
  )
```

For high throughput, `publishAsync` pipelines instead of awaiting each ack:
the outer effect completes once a window slot is taken and the request is on
the wire, the inner one yields the `PubAck`. The window
(`JetStreamConfig.publishAsyncMaxPending`, default 256) bounds unacknowledged
publishes:

```scala mdoc:silent
def firehose(js: JetStream[IO], payloads: List[Chunk[Byte]]): IO[List[PubAck]] =
  payloads
    .traverse(p => js.publishAsync("orders.new", p))  // all on the wire
    .flatMap(_.sequence)                              // then await the acks
```

## Stream management

The context covers the full stream admin surface — update, inspect, delete,
purge (all messages, by subject filter, up to a sequence, or keeping the last
`n`), and paginated listing:

```scala mdoc:silent
def streamAdmin(js: JetStream[IO]): IO[Unit] =
  for
    _     <- js.updateStream(archiveConfig)
    info  <- js.streamInfo("ORDERS")
    _     <- IO.println(s"${info.state.messages} messages, ${info.state.bytes} bytes")
    purged <- js.purgeStream("ORDERS", PurgeOptions(filter = Some("orders.cancelled")))
    _     <- IO.println(s"purged ${purged.purged}")
    names <- js.streamNames.compile.toList
    _     <- IO.println(s"streams: ${names.mkString(", ")}")
    _     <- js.deleteStream("ORDERS-ARCHIVE")
  yield ()
```

Individual stored messages are accessible without a consumer — by sequence or
last-on-subject — and can be deleted (erased by default):

```scala mdoc:silent
def inspectMessage(js: JetStream[IO]): IO[Unit] =
  for
    msg <- js.getMessage("ORDERS", MessageGet.LastBySubject("orders.new"))
    _   <- IO.println(s"seq=${msg.seq}: ${msg.data.size} bytes")
    _   <- js.deleteMessage("ORDERS", msg.seq)
  yield ()
```

`js.accountInfo` reports account-level usage and limits (`maxMemory`,
`maxStorage`, `maxStreams`, `maxConsumers`; `-1` = unlimited).

## Continuous pull consumption

`consume` runs a pull loop alongside the returned stream — the `Resource` owns
the reply-inbox subscription, and no pulls are issued until the stream is
actually consumed. The loop re-issues its request on a cadence so it resumes
after a dropped connection. `ConsumeOptions` tunes the batching: messages (or
bytes) per pull request, how long a request stays open (`expires`, 30 seconds),
and the idle heartbeat:

```scala mdoc:silent
def consumeLoop(c: JsConsumer[IO]): IO[Unit] =
  c.consume(ConsumeOptions(maxMessages = 100, expires = 10.seconds)).use { stream =>
    stream.evalMap(m => process(m.payload) *> m.ack).compile.drain
  }
```

Besides the blocking `fetch(batch, maxWait)`, `fetchNoWait(batch)` returns
only what is already buffered server-side — an empty `Chunk` when there is
nothing — which makes polling without a deadline cheap. A handle to an
existing, pre-provisioned consumer (its existence is verified) comes from
`js.consumer(stream, name)`; the handle also exposes `stream`, `name` and
`info` (delivered/ack-floor sequences, pending counts, redeliveries).

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

## Ordered consumption

`subscribeOrdered` is the third consumption mode: gap-free, in-order delivery
without acks. The client tracks the consumer sequence and transparently
recreates its ephemeral flow-controlled consumer from the last in-order stream
sequence whenever it detects a gap or the server invalidates the consumer — so
ordering survives reconnects. (KV watch and Object Store reads are built on
it.) `OrderedConsumerOptions` selects the start position (`deliverPolicy`,
`optStartSeq`/`optStartTime`) and heartbeat/inactivity tuning:

```scala mdoc:silent
def auditLog(js: JetStream[IO]): IO[Unit] =
  js.subscribeOrdered(
      "ORDERS",
      filterSubject = Some("orders.>"),
      opts = OrderedConsumerOptions(deliverPolicy = DeliverPolicy.New)
    )
    .use(_.evalMap(m => process(m.payload)).compile.drain)
```

## Consumer configuration and management

`ConsumerConfig` controls far more than the durable name and filter: where to
start (`DeliverPolicy.All`/`Last`/`New`/`ByStartSequence`/`ByStartTime`/
`LastPerSubject`), how to ack (`AckPolicy.Explicit`/`All`/`None`), redelivery
(`ackWait`, `maxDeliver`, per-attempt `backoff` delays), multiple subject
filters (`filterSubjects`), replay pacing (`ReplayPolicy.Instant`/`Original`),
`headersOnly`, and flow limits (`maxAckPending`, `maxWaiting`). A consumer is
pull-based unless `deliverSubject` is set:

```scala mdoc:silent
val retryingWorkers = ConsumerConfig(
  durable = Some("workers-v2"),
  filterSubjects = List("orders.new", "orders.updated"),
  ackWait = Some(30.seconds),
  maxDeliver = 5,
  backoff = List(1.second, 10.seconds, 1.minute),
  maxAckPending = 500
)
```

Consumer admin mirrors the stream admin — create/update/inspect/delete plus
paginated listing:

```scala mdoc:silent
def consumerAdmin(js: JetStream[IO]): IO[Unit] =
  for
    _    <- js.addConsumer("ORDERS", retryingWorkers)
    info <- js.consumerInfo("ORDERS", "workers-v2")
    _    <- IO.println(s"pending=${info.numPending} redelivered=${info.numRedelivered}")
    all  <- js.listConsumers("ORDERS").compile.toList
    _    <- IO.println(s"${all.size} consumers on ORDERS")
    _    <- js.deleteConsumer("ORDERS", "workers-v2")
  yield ()
```

## Message metadata

Every delivered `JsMessage` exposes `subject`, `headers`, `payload` — and
`metadata`, parsed from the ack-reply subject: stream and consumer sequences,
delivery count, pending count and the stored timestamp. `numDelivered > 1`
(`isRedelivery`) is the hook for dedup and poison-message handling:

```scala mdoc:silent
def handle(m: JsMessage[IO]): IO[Unit] =
  if m.metadata.isRedelivery && m.metadata.numDelivered > 3
  then m.term
  else process(m.payload) *> m.ack
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

## Errors

Every JetStream API call can raise `NatsError.JetStreamApiError(code, errCode,
description)` — the server's error envelope with its HTTP-like code and
JetStream-specific `err_code`. Two more cases matter in practice:
`JetStreamNotEnabled` when `client.jetStream()` is acquired against a server
without JetStream, and `JetStreamPublishNoAck(subject)` when a publish goes to
a subject no stream captures (mapped from the no-responders reply).

Built on JetStream: [Key-Value](key-value.md) and [Object Store](object-store.md).
