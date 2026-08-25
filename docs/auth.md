# Authentication & TLS

`fs2-nats` supports every client-side NATS authentication mechanism. Choose one
by setting `ClientConfig.credentials`.

The snippets below share these imports and helpers:

```scala mdoc:silent
import cats.effect.IO
import com.comcast.ip4s.{Host, Port}
import scala.concurrent.duration.*
import fs2.io.file.Path
import fs2.io.net.Network
import fs2.nats.client.{BackoffConfig, ClientConfig, NatsClient, NatsCredentials, ServerAddress, SlowConsumerPolicy}

val host = Host.fromString("nats.example.com").get
val port = Port.fromInt(4222).get
```

## Token

```scala mdoc:silent
val tokenConfig =
  ClientConfig(host = host, port = port, credentials = Some(NatsCredentials.Token("s3cr3t")))
```

## Username / password

```scala mdoc:silent
val userPassConfig =
  ClientConfig(host = host, port = port, credentials = Some(NatsCredentials.UserPassword("user", "pass")))
```

## NKey (Ed25519)

Provide the NKey seed (an `S...` string); the client signs the server's nonce
and derives the public key from it:

```scala mdoc:silent
val nkeyConfig =
  ClientConfig(host = host, port = port, credentials = Some(NatsCredentials.NKey("SUAB...seed...")))
```

`NKey` also takes an optional JWT — `NatsCredentials.NKey(seed, jwt =
Some(userJwt))` — for operator-mode setups where the JWT and seed come from a
secrets manager rather than a `.creds` file.

## Decentralized JWT (`.creds` files)

Operator-mode deployments (NGS / Synadia Cloud / self-hosted with `nsc`) issue a
`.creds` file bundling a user JWT and an NKey seed. Load it directly:

```scala mdoc:silent
def withCreds: IO[Unit] =
  NatsCredentials.fromCredsFile[IO](Path("user.creds")).flatMap { creds =>
    NatsClient
      .connect[IO](ClientConfig(host = host, port = port, credentials = Some(creds)))
      .use(_ => IO.unit)
  }
```

`NatsCredentials.fromCreds(content)` parses an already-loaded string and
returns `Either[Throwable, NatsCredentials]`.

## TLS

Set `useTls = true` and supply a `TLSContext` — one is required; the client
never falls back to plaintext or a default context. `Network[F].tlsContext.system`
loads the system trust store and is itself effectful, so flat-map it before
connecting:

```scala mdoc:silent
def withTls: IO[Unit] =
  Network[IO].tlsContext.system.flatMap { tls =>
    NatsClient
      .connect[IO](ClientConfig(host = host, port = port, useTls = true), tlsContext = Some(tls))
      .use(_ => IO.unit)
  }
```

The client follows the standard NATS handshake: it reads the plaintext `INFO`,
then upgrades the connection to TLS. Servers configured with
`handshake_first: true` (TLS before `INFO`) are not supported.

## Mutual TLS

For mutual TLS, build the `TLSContext` from an `SSLContext` whose `KeyManager`
presents your client certificate (and whose `TrustManager` trusts the server's
CA). `fromSSLContext` is pure, so pass the result straight through:

```scala mdoc:silent
def mutualTls(sslContext: javax.net.ssl.SSLContext): IO[Unit] =
  val tls = Network[IO].tlsContext.fromSSLContext(sslContext)
  NatsClient
    .connect[IO](ClientConfig(host = host, port = port, useTls = true), tlsContext = Some(tls))
    .use(_ => IO.unit)
```

## Configuration

`ClientConfig` exposes the connection surface:

```scala mdoc:silent
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
    maxRetries = None // unlimited
  ),
  queueCapacity = 10000,
  slowConsumerPolicy = SlowConsumerPolicy.Block,
  verbose = false,
  pedantic = false,
  echo = true,
  servers = List(                      // additional cluster seed servers
    ServerAddress(Host.fromString("nats-2.example.com").get, Port.fromInt(4222).get)
  ),
  noRandomize = false,                 // true: try servers in configured order
  reconnectBufferSize = 8L * 1024 * 1024
)
```

### Clustering and reconnect buffering

`servers` seeds the server pool beyond `host`/`port`; peers advertised by the
cluster via `INFO connect_urls` are added at runtime, and on a lost connection
the client fails over across the pool (shuffled — keeping the first configured
seed first — unless `noRandomize` is set).
`ClientConfig.fromUrls` builds a config from a list of URLs directly.

While disconnected, publishes are buffered up to `reconnectBufferSize` bytes
(default 8 MiB) and replayed after the reconnect; beyond the limit they fail
with `NatsError.ReconnectBufferExceeded`. Set it to `0` to fail writes
immediately instead of buffering.

### Slow consumer policies

When a subscription queue fills up:

- `SlowConsumerPolicy.Block` — backpressure (default)
- `SlowConsumerPolicy.DropNew` — drop incoming messages
- `SlowConsumerPolicy.DropOldest` — drop oldest queued messages
- `SlowConsumerPolicy.ErrorAndDrop` — emit an event and drop

### Reconnect backoff

Reconnection always uses exponential backoff with full jitter;
`ClientConfig.backoff` tunes its parameters. Besides the default (100 ms base,
30 s max, unlimited retries) there are two presets:

```scala mdoc:silent
val quick   = BackoffConfig.fast          // 10ms base, 1s max — for tests
val patient = BackoffConfig.conservative  // 1s base, 5min max
```

(The `Backoff` object also offers `fixed`, `immediate` and
`decorrelatedJitter` policies, but those pair with the general-purpose
`Retry.withBackoff` utility for retrying your own effects — the client's
reconnect loop cannot be switched to them.)

### Transport and parser tuning

`NatsClient.connect` takes two further knobs. `TransportConfig` sets
socket-level behaviour — connect/write timeouts, the outbound write-queue
capacity, and TCP socket options — with `highThroughput` and `lowLatency`
presets. `ParserConfig` bounds inbound protocol parsing (control-line length,
payload limit, strictness):

```scala mdoc:silent
import fs2.nats.transport.TransportConfig

def tunedConnect: IO[Unit] =
  NatsClient
    .connect[IO](config, transportConfig = TransportConfig.highThroughput)
    .use(_ => IO.unit)
```
