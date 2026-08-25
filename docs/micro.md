# Micro Services

`fs2.nats.micro` implements the NATS micro services protocol (ADR-32): typed
request/reply endpoints with discovery and per-endpoint statistics. An endpoint
is described once as a plain `Rpc` value — subject pattern, request/response
payloads, typed error — and the same value is interpreted by the server
(`NatsService`) and the client (`Micro`).

The snippets below share these imports:

```scala mdoc:silent
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.nats.client.NatsClient
import fs2.nats.micro.*
```

## Shared endpoint definitions

Put the `Rpc` values in a module both sides depend on. `pattern` validates the
subject literal at compile time; wildcard captures (`*`, trailing `>`) are bound
to a type with a `TokenCodec`:

```scala mdoc:silent
object OrdersApi:

  /** orders.get.<id> — no request body, replies with the order text. */
  val get = Rpc(
    name = "get",
    subject = pattern["orders.get.*"].bind[String],
    in = Payload.empty,
    err = ServiceErr.plain,
    out = Payload.string
  )

  /** orders.add — request body is the order text, replies with the new id. */
  val add = Rpc(
    name = "add",
    subject = pattern["orders.add"],
    in = Payload.string,
    err = ServiceErr.plain,
    out = Payload.string
  )
```

`Payload` provides `empty`, `bytes`, `string` and `json` (jsoniter);
`ServiceErr.plain` passes the raw `(code, description)` pair through — map it to
your own error ADT with `ServiceErr.from`.

## Server side

Attach logic with `handle` (or `handleWithHeaders` for access to request
headers) and start the service as a `Resource`. Release unsubscribes and
cancels in-flight handlers:

```scala mdoc:silent
def server(client: NatsClient[IO]): IO[Unit] =
  Ref.of[IO, (Int, Map[String, String])]((1, Map.empty)).flatMap { store =>
    val handlers = List(
      OrdersApi.get.handle[IO] { (id, _) =>
        store.get.map { case (_, orders) =>
          orders.get(id).toRight((404, s"order '$id' not found"))
        }
      },
      OrdersApi.add.handle[IO] { (_, order) =>
        store.modify { case (next, orders) =>
          ((next + 1, orders.updated(next.toString, order)), Right(next.toString))
        }
      }
    )
    NatsService(client, ServiceConfig("orders", "1.0.0"), handlers)
      .use(_ => IO.never)
  }
```

## Client side

`Micro.call` fills the captures into the subject and decodes the reply. There
is an overload without a request argument for endpoints with `Payload.empty`:

```scala mdoc:silent
def orderClient(client: NatsClient[IO]): IO[Unit] =
  val micro = Micro(client)
  for
    added <- micro.call(OrdersApi.add)((), "2x flat white")
    found <- micro.call(OrdersApi.get)(added.getOrElse("1"))
    _     <- IO.println(s"add: $added, get: $found")
  yield ()
```

## Error semantics

- A handler's `Left(e)` is encoded via the endpoint's `ServiceErr` into the
  ADR-32 headers `Nats-Service-Error-Code` and `Nats-Service-Error` on an empty
  reply; the client decodes them back to `Left(E)`.
- A handler exception becomes a `500` with the exception message; a request
  whose subject captures or payload do not decode is answered with `400` before
  the handler runs.
- The client raises in `F` only transport failures — `NatsError.Timeout`,
  `NatsError.NoResponders` (no instance subscribed) and
  `NatsError.PayloadDecodeError` (a success reply whose body does not decode) —
  plus `IllegalArgumentException` when the params encode to an invalid subject
  token (a programmer error, not a service error).

## Discovery

Every instance answers the ADR-32 discovery subjects `$SRV.PING`, `$SRV.INFO`
and `$SRV.STATS` — each in the bare form, suffixed with the service name, and
suffixed with `<name>.<id>` — with the standard `io.nats.micro.v1.*` JSON
responses. Any plain request works, e.g. `nats req '$SRV.PING' ''` or the
`nats micro` CLI. `INFO` lists the endpoints with their wildcard subjects,
queue groups and metadata; payload schemas attached via `Payload.withSchema`
appear as `request_schema`/`response_schema`.

## Stats and reset

The `NatsService` handle exposes the same data locally: `info`, `stats`
(per-endpoint request/error counters, last error, processing times) and `reset`
(zero all counters and re-stamp `started`):

```scala mdoc:silent
def printStats(svc: NatsService[IO]): IO[Unit] =
  svc.stats.flatMap { s =>
    s.endpoints.traverse_ { e =>
      IO.println(s"${e.name}: ${e.numRequests} requests, ${e.numErrors} errors")
    }
  }
```

## Notes

- Subject patterns support at most 4 captures: 1 capture binds any `A` with a
  `TokenCodec`, 2–4 captures bind `Tuple2`..`Tuple4`.
- Endpoints join queue group `"q"` by default (per ADR-32), so multiple
  instances load-balance automatically. Override per service with
  `ServiceConfig.withQueueGroup` or per endpoint with `Rpc.withQueueGroup`.
- Success replies are published without headers; setting response headers on a
  successful reply is not supported yet.
