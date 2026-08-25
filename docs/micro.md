# Micro Services

`fs2.nats.micro` implements the NATS micro services protocol (ADR-32): typed
request/reply endpoints with discovery and per-endpoint statistics. An endpoint
is described once as a plain `Rpc` value — subject pattern, request/response
payloads, typed error — and the same value is interpreted by the server
(`NatsService`) and the client (`Micro`).

Two runnable examples accompany this page:
[`MicroExample.scala`](https://github.com/ThatScalaGuy/fs2-nats/blob/main/examples/MicroExample.scala)
covers the basics;
[`MicroAdvancedExample.scala`](https://github.com/ThatScalaGuy/fs2-nats/blob/main/examples/MicroAdvancedExample.scala)
runs everything from the advanced sections below — domain-typed captures, JSON
payloads, a typed error ADT, schemas, metadata and headers — against a live
server.

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

`Payload` provides `empty`, `bytes`, `string` and `json` (jsoniter), plus
`from`/`imap` for anything else — see the JSON payloads section below.
`ServiceErr.plain` passes the raw `(code, description)` pair through — map it to
your own error ADT with `ServiceErr.from`, covered in the typed errors section.

## Server side

Attach logic with `handle` (or `handleWithHeaders` to read the request headers
and set response headers) and start the service as a `Resource`. Release
unsubscribes and cancels in-flight handlers:

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

`ServiceConfig` carries more than name and version: `withDescription` sets the
human-readable description shown by `nats micro info`, and `withMaxConcurrent`
bounds how many handlers may run at once *per endpoint* (default 64) — further
requests wait on the subscription until a slot frees up:

```scala mdoc:silent
val tunedConfig = ServiceConfig("orders", "1.0.0")
  .withDescription("order management")
  .withMaxConcurrent(16)
```

`NatsService(...)` validates its inputs during acquisition and raises
`IllegalArgumentException` on violations: service and endpoint names must match
`[A-Za-z0-9-_]+`, the version must be SemVer, and the handler list must be
non-empty with unique endpoint names.

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

Calls time out after 5 seconds by default; `call` and `callWithHeaders` take a
`timeout` parameter to override that per call:

```scala mdoc:silent
import scala.concurrent.duration.*

def slowAdd(client: NatsClient[IO]): IO[Either[(Int, String), String]] =
  Micro(client).call(OrdersApi.add)((), "1x espresso", timeout = 30.seconds)
```

On a `Payload.empty` endpoint the parameterless sugar always wins overload
resolution, so there the deadline comes from `callWithHeaders` (pass
`Headers.empty` if you have nothing to send — see the headers section).

A call that misses its deadline raises `NatsError.Timeout` in `F` (see error
semantics below).

## Domain types in subject captures

`bind` accepts any type with a `TokenCodec` — instances for `String`, `Int`,
`Long` and `java.util.UUID` are provided. Wrap them with `imap` (or build one
from scratch with `TokenCodec.from`) to carry domain types instead of raw
strings; a pattern with 2–4 captures binds the matching tuple:

```scala mdoc:silent
final case class TenantId(value: String)
final case class OrderId(value: Long)

object ShopIds:
  given TokenCodec[TenantId] = TokenCodec.string.imap(TenantId.apply)(_.value)
  given TokenCodec[OrderId]  = TokenCodec.long.imap(OrderId.apply)(_.value)

import ShopIds.given

/** shop.<tenant>.orders.get.<id> — two captures decode to a tuple. */
val getForTenant = Rpc(
  name = "get-for-tenant",
  subject = pattern["shop.*.orders.get.*"].bind[(TenantId, OrderId)],
  in = Payload.empty,
  err = ServiceErr.plain,
  out = Payload.string
)

def find(client: NatsClient[IO]): IO[Either[(Int, String), String]] =
  Micro(client).call(getForTenant)((TenantId("acme"), OrderId(1)))
```

A request whose token does not decode — `shop.acme.orders.get.oops` here,
since `oops` is no `Long` — is answered with a `400` before any handler runs.

## JSON payloads and custom codecs

`Payload.json[A]` serializes with jsoniter-scala. It summons a
`JsonValueCodec[A]`, which you derive with `JsonCodecMaker.make` — add the
macros to your build; `compile-internal` keeps them off your runtime classpath:

```scala
libraryDependencies +=
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.40.1" % "compile-internal"
```

```scala mdoc:silent
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

final case class AddOrder(item: String, quantity: Int)
final case class Order(id: Long, item: String, quantity: Int)

object ShopJson:
  given JsonValueCodec[AddOrder] = JsonCodecMaker.make
  given JsonValueCodec[Order]    = JsonCodecMaker.make

import ShopJson.given

val addTyped = Rpc(
  name = "add-typed",
  subject = pattern["orders.add-typed"],
  in = Payload.json[AddOrder],
  err = ServiceErr.plain,
  out = Payload.json[Order]
)
```

jsoniter is not required, though: a `Payload` is just an encode/decode pair
over `Chunk[Byte]`, so any serialization plugs in via `Payload.from(enc, dec)`,
and `imap` refines an existing codec — its decode side may reject:

```scala mdoc:silent
val intPayload: Payload[Int] =
  Payload.string.imap(s => s.toIntOption.toRight(s"not an int: '$s'"))(_.toString)
```

## Typed errors

`ServiceErr.plain` exposes errors as the raw `(Int, String)` pair. A shared API
usually wants an ADT both sides pattern-match on; `ServiceErr.from` maps
between the two. Only the code and description travel on the wire, and `decode`
must be total — give it a catch-all case for codes this version of the client
does not know:

```scala mdoc:silent
enum OrderError:
  case NotFound(description: String)
  case InvalidQuantity(description: String)
  case Unknown(code: Int, description: String)

val orderErr: ServiceErr[OrderError] = ServiceErr.from(
  encode = {
    case OrderError.NotFound(d)        => (404, d)
    case OrderError.InvalidQuantity(d) => (422, d)
    case OrderError.Unknown(code, d)   => (code, d)
  },
  decode = (code, description) =>
    code match
      case 404   => OrderError.NotFound(description)
      case 422   => OrderError.InvalidQuantity(description)
      case other => OrderError.Unknown(other, description)
)
```

An endpoint defined with `err = orderErr` hands its handlers
`Left(OrderError.NotFound(...))` and returns the same value to callers — no
exceptions involved on either side.

## Headers

NATS message headers pass through the typed layer untouched, in both
directions — for tracing ids, tenant hints, cache markers and similar
cross-cutting data that does not belong in the payload. The client attaches
request headers with `callWithHeaders`; the handler reads them and answers with
a `Reply`, which is the response value plus the headers to publish with it:

```scala mdoc:silent
import fs2.nats.protocol.Headers

val tracedHandler = OrdersApi.get.handleWithHeaders[IO] { (id, headers, _) =>
  val trace = headers.get("X-Trace-Id").getOrElse("<none>")
  IO.println(s"get $id, trace=$trace")
    .as(Right(Reply(s"order $id", Headers("X-Trace-Id" -> trace))))
}

def tracedCall(client: NatsClient[IO], traceId: String): IO[Unit] =
  Micro(client)
    .callWithHeaders(OrdersApi.get)("1", (), Headers("X-Trace-Id" -> traceId))
    .flatMap {
      case Right(reply) =>
        IO.println(s"${reply.value} (trace ${reply.headers.get("X-Trace-Id")})")
      case Left(err) => IO.println(s"get failed: $err")
    }
```

`Reply(o)` is the no-headers case — it is what `handle` produces and what
`call` unwraps, so the plain `handle`/`call` pair keeps working on `O` and
never mentions `Reply`. Use `handleWithHeaders` whenever you want to set
response headers, even if you ignore the request headers.

Only successful replies carry custom headers: an error reply is the empty
ADR-32 body plus `Nats-Service-Error-Code` / `Nats-Service-Error`, so a
handler's `Left(e)` has nowhere to put them.

## Error semantics

- A handler's `Left(e)` is encoded via the endpoint's `ServiceErr` into the
  ADR-32 headers `Nats-Service-Error-Code` and `Nats-Service-Error` on an empty
  reply; the client decodes them back to `Left(E)`.
- A handler exception becomes a `500` with the exception message; a request
  whose subject captures or payload do not decode is answered with `400` before
  the handler runs.
- The error description is sanitized before publishing: only the first line
  survives, remaining control characters are blanked, and it is capped at 256
  characters — long or multi-line messages arrive truncated at the client.
- A throwing codec cannot kill an endpoint: a request `decode` that throws is
  answered with `400`, a response or error `encode` that throws becomes a
  `500`, and a failed reply publish is swallowed but recorded in the endpoint's
  stats. A request without a reply subject is processed, but no reply is
  published.
- The client raises in `F` only transport failures — `NatsError.Timeout`,
  `NatsError.NoResponders` (no instance subscribed) and
  `NatsError.PayloadDecodeError` (a success reply whose body does not decode) —
  plus `IllegalArgumentException` when the params encode to an invalid subject
  token (a programmer error, not a service error).

## Discovery

Every instance answers the ADR-32 discovery subjects `$SRV.PING`, `$SRV.INFO`
and `$SRV.STATS` — each in the bare form, suffixed with the service name, and
suffixed with `<name>.<id>` — with the standard `io.nats.micro.v1.*` JSON
responses. The instance id in that last form is available locally as
`NatsService#id`, e.g. to target one specific instance or correlate logs with
discovery output. Any plain request works, e.g. `nats req '$SRV.PING' ''` or the
`nats micro` CLI. `INFO` lists the endpoints with their wildcard subjects,
queue groups and metadata.

### Metadata

Both the service and each endpoint carry an immutable `Map[String, String]` of
metadata (ADR-33), set at definition time:

```scala mdoc:silent
val describedConfig = ServiceConfig("orders", "1.0.0")
  .withMetadata(Map("region" -> "eu-central", "owner" -> "orders-team"))

val describedRpc = Rpc(
  name = "get-described",
  subject = pattern["orders.get.*"].bind[String],
  in = Payload.empty,
  err = ServiceErr.plain,
  out = Payload.string
).withMetadata(Map("stability" -> "beta"))
```

Service metadata appears in the top level of all three discovery responses;
endpoint metadata appears in that endpoint's `INFO` entry (and locally via
`NatsService#info`). There is no way to change metadata on a running service —
endpoints are fixed values passed to `NatsService(...)` at construction.

### Publishing payload schemas

Attach a schema description to a payload with `Payload.withSchema`; the text is
free-form (JSON Schema, a URL, a type name):

```scala mdoc:silent
val addOrderSchema =
  """{"type":"object","properties":{"item":{"type":"string"},"quantity":{"type":"integer"}}}"""

val addWithSchema = Rpc(
  name = "add-documented",
  subject = pattern["orders.add"],
  in = Payload.withSchema(Payload.string, addOrderSchema),
  err = ServiceErr.plain,
  out = Payload.string
)
```

The service publishes these in the endpoint's `INFO` metadata under the keys
`request_schema` and `response_schema`, next to any explicit `Rpc.withMetadata`
entries (explicit keys win on collision). This is the current ADR-32 way to
expose schemas: early versions of the services API had a dedicated `$SRV.SCHEMA`
verb (old nats.go releases implemented it), but it was removed from the spec and
today's clients and the `nats` CLI only read the `INFO` metadata. Nothing is
derived automatically — `Payload.json` publishes no schema unless you attach
one.

## Stats and reset

The `NatsService` handle exposes the same data locally: `info`, `stats` and
`reset` (zero all counters and re-stamp `started`). Each `EndpointStats`
carries `name`, `subject`, `queueGroup`, the `numRequests`/`numErrors`
counters, the `lastError` description, and total and average processing times
as `FiniteDuration`:

```scala mdoc:silent
def printStats(svc: NatsService[IO]): IO[Unit] =
  svc.stats.flatMap { s =>
    IO.println(s"${s.name} (${s.id}) up since ${s.started}") *>
      s.endpoints.traverse_ { e =>
        IO.println(
          s"${e.name} @ ${e.subject}: ${e.numRequests} requests, " +
            s"${e.numErrors} errors (last: ${e.lastError.getOrElse("-")}), " +
            s"avg ${e.averageProcessingTime.toMicros}µs"
        )
      }
  }
```

## Notes

- Subject patterns support at most 4 captures: 1 capture binds any `A` with a
  `TokenCodec`, 2–4 captures bind `Tuple2`..`Tuple4`. The `pattern` macro
  rejects empty tokens, whitespace, wildcards embedded in a token and a
  non-final `>` — all at compile time. `SubjectPattern#render` returns the
  original wildcard literal, e.g. for logging.
- On the client, params are encoded back into subject tokens; a `*` capture
  whose encoded token is empty or contains `.`, whitespace, `*` or `>` raises
  `IllegalArgumentException` before anything is published. A `>` tail may
  contain dots (it spans multiple tokens); only an empty tail is rejected.
- Endpoints join queue group `"q"` by default (per ADR-32), so multiple
  instances load-balance automatically. Override per service with
  `ServiceConfig.withQueueGroup` or per endpoint with `Rpc.withQueueGroup`.
- Control characters in response header keys and values are replaced with
  spaces before the reply is published: a CR/LF there would start a new line in
  the header block and let a request forge headers in its own reply.
