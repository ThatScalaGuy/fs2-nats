/*
 * Copyright 2025 ThatScalaGuy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.nats.client

import scala.concurrent.duration.*
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port, SocketAddress}
import fs2.io.net.tls.TLSParameters

/** Policy for handling slow consumers when subscription queues fill up.
  */
enum SlowConsumerPolicy:
  /** Block the reader until queue space is available. This applies backpressure
    * to the server connection.
    */
  case Block

  /** Drop the oldest message in the queue to make room. Useful when only recent
    * messages matter.
    */
  case DropOldest

  /** Drop the new incoming message. Preserves message order in the queue.
    */
  case DropNew

  /** Emit a SlowConsumer error event but continue processing. Combines with
    * DropNew behavior.
    */
  case ErrorAndDrop

/** Credentials for NATS authentication.
  */
enum NatsCredentials:
  /** Username/password authentication.
    *
    * @param username
    *   The username
    * @param password
    *   The password
    */
  case UserPassword(username: String, password: String)

  /** Token-based authentication.
    *
    * @param token
    *   The authentication token
    */
  case Token(token: String)

  /** NKey authentication using a seed, with optional JWT.
    *
    * @param seed
    *   The NKey seed (an `S...` string, e.g. `SUA...`). This is the private key
    *   material used to sign the server nonce; the public NKey is derived from
    *   it. Keep it secret.
    * @param jwt
    *   Optional user JWT. When present, the CONNECT sends `jwt` + `sig`
    *   (decentralized/operator auth). When absent, it sends the derived public
    *   `nkey` + `sig`.
    */
  case NKey(seed: String, jwt: Option[String] = None)

object NatsCredentials:
  /** Parse the contents of a `.creds` file (the decentralized-JWT credential
    * format produced by `nsc`) into [[NatsCredentials.NKey]] credentials.
    *
    * @param content
    *   The full text of a `.creds` file
    * @return
    *   Either a parse error or the extracted JWT + seed credentials
    */
  def fromCreds(content: String): Either[Throwable, NatsCredentials] =
    fs2.nats.auth.Creds.parse(content)

  /** Load and parse a `.creds` file from disk into [[NatsCredentials.NKey]]
    * credentials.
    *
    * @param path
    *   Path to the `.creds` file
    */
  def fromCredsFile[F[_]: fs2.io.file.Files: cats.effect.kernel.Async](
      path: fs2.io.file.Path
  ): F[NatsCredentials] =
    fs2.nats.auth.Creds.fromFile(path)

/** Configuration for exponential backoff with jitter.
  *
  * @param baseDelay
  *   The initial delay before first retry
  * @param maxDelay
  *   The maximum delay between retries
  * @param factor
  *   The multiplier applied to delay after each retry
  * @param maxRetries
  *   Optional maximum number of retry attempts (None for unlimited)
  */
final case class BackoffConfig(
    baseDelay: FiniteDuration = 100.millis,
    maxDelay: FiniteDuration = 30.seconds,
    factor: Double = 2.0,
    maxRetries: Option[Int] = None
)

object BackoffConfig:
  /** Default backoff configuration: 100ms base, 30s max, factor 2.0, unlimited
    * retries
    */
  val default: BackoffConfig = BackoffConfig()

  /** Fast backoff for testing: 10ms base, 1s max */
  val fast: BackoffConfig = BackoffConfig(
    baseDelay = 10.millis,
    maxDelay = 1.second
  )

  /** Conservative backoff: 1s base, 5 minutes max */
  val conservative: BackoffConfig = BackoffConfig(
    baseDelay = 1.second,
    maxDelay = 5.minutes
  )

/** Address of a single NATS server. Used both for the configured seed list and
  * for peers discovered via the server's INFO `connect_urls`.
  *
  * @param host
  *   The server host
  * @param port
  *   The server port
  * @param useTls
  *   Whether to connect to this server over TLS. Seeds carry the scheme from
  *   their URL; peers discovered via `connect_urls` (which carry no scheme)
  *   inherit the scheme of the server they were learned from.
  */
final case class ServerAddress(host: Host, port: Port, useTls: Boolean = false):
  def socketAddress: SocketAddress[Host] = SocketAddress(host, port)

object ServerAddress:
  /** Parse a bare `host:port` entry as advertised in INFO `connect_urls`. Also
    * accepts a bracketed IPv6 form `[ipv6]:port` and a bare host (defaulting to
    * port 4222). Returns None for anything unparseable so a single malformed
    * gossip entry can be dropped without breaking discovery.
    *
    * @param s
    *   The `host:port` string
    */
  def fromHostPort(s: String): Option[ServerAddress] =
    val t = s.trim
    t.lastIndexOf(':') match
      case -1 =>
        Host.fromString(t).map(ServerAddress(_, Port.fromInt(4222).get))
      case i =>
        val raw = t.take(i)
        val host =
          if raw.startsWith("[") && raw.endsWith("]") then
            raw.drop(1).dropRight(1)
          else raw
        for
          h <- Host.fromString(host)
          p <- t.drop(i + 1).toIntOption.flatMap(Port.fromInt)
        yield ServerAddress(h, p)

/** Configuration for the NATS client connection.
  *
  * @param host
  *   The NATS server host
  * @param port
  *   The NATS server port (default: 4222)
  * @param useTls
  *   Whether to use TLS encryption
  * @param tlsParams
  *   Optional TLS parameters for custom configuration
  * @param name
  *   Optional client name for server identification
  * @param credentials
  *   Optional authentication credentials
  * @param backoff
  *   Backoff configuration for reconnection attempts
  * @param queueCapacity
  *   Default capacity for subscription message queues
  * @param slowConsumerPolicy
  *   Policy for handling full subscription queues
  * @param idleTimeout
  *   Optional timeout for idle connections
  * @param verbose
  *   Whether to enable verbose mode (+OK acknowledgements)
  * @param pedantic
  *   Whether to enable pedantic mode (stricter checking)
  * @param echo
  *   Whether the server should echo messages back to the publishing connection
  * @param servers
  *   Additional seed servers beyond `host`/`port`, used for cluster bootstrap
  *   and failover. The pool is also grown at runtime from INFO `connect_urls`.
  * @param noRandomize
  *   When false (default), the server pool is shuffled (the first configured
  *   seed is still tried first) to spread connection load across the cluster.
  *   Set true to try servers strictly in configured order.
  * @param reconnectBufferSize
  *   Maximum number of outbound bytes buffered while disconnected; on reconnect
  *   the buffer is replayed so publishes are not lost. A value of 0 disables
  *   buffering (writes during a disconnect fail immediately).
  */
final case class ClientConfig(
    host: Host,
    port: Port = Port.fromInt(4222).get,
    useTls: Boolean = false,
    tlsParams: Option[TLSParameters] = None,
    name: Option[String] = None,
    credentials: Option[NatsCredentials] = None,
    backoff: BackoffConfig = BackoffConfig.default,
    queueCapacity: Int = 10000,
    slowConsumerPolicy: SlowConsumerPolicy = SlowConsumerPolicy.Block,
    idleTimeout: Option[FiniteDuration] = None,
    verbose: Boolean = false,
    pedantic: Boolean = false,
    echo: Boolean = true,
    servers: List[ServerAddress] = Nil,
    noRandomize: Boolean = false,
    reconnectBufferSize: Long = 8L * 1024 * 1024
):
  /** The full seed pool: the primary `host`/`port` first, then any extra
    * `servers`, de-duplicated with order preserved.
    */
  def seedServers: List[ServerAddress] =
    (ServerAddress(host, port, useTls) :: servers).distinct

object ClientConfig:
  /** Create a minimal configuration for localhost.
    *
    * @param port
    *   The port to connect to (default: 4222)
    * @return
    *   ClientConfig for localhost connection
    */
  def localhost(port: Int = 4222): ClientConfig =
    ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(port).get
    )

  /** Create a configuration from a NATS URL. Supports: nats://host:port,
    * tls://host:port
    *
    * @param url
    *   The NATS URL
    * @return
    *   Either an error message or the parsed ClientConfig
    */
  def fromUrl(url: String): Either[String, ClientConfig] =
    val natsPattern = """^(nats|tls)://([^:]+):(\d+)$""".r
    val natsPatternNoPort = """^(nats|tls)://([^:]+)$""".r

    url match
      case natsPattern(scheme, hostStr, portStr) =>
        for
          host <- Host.fromString(hostStr).toRight(s"Invalid host: $hostStr")
          port <- portStr.toIntOption
            .flatMap(Port.fromInt)
            .toRight(s"Invalid port: $portStr")
        yield ClientConfig(
          host = host,
          port = port,
          useTls = scheme == "tls"
        )

      case natsPatternNoPort(scheme, hostStr) =>
        Host.fromString(hostStr) match
          case Some(host) =>
            Right(
              ClientConfig(
                host = host,
                port = Port.fromInt(4222).get,
                useTls = scheme == "tls"
              )
            )
          case None => Left(s"Invalid host: $hostStr")

      case _ =>
        Left(
          s"Invalid NATS URL format: $url. Expected: nats://host:port or tls://host:port"
        )

  /** Create a configuration from multiple NATS URLs for cluster bootstrap. The
    * first URL fills `host`/`port`/`useTls`; the remaining URLs become extra
    * seed `servers`, each carrying its own scheme (`tls://` vs `nats://`) as a
    * per-server `useTls`.
    *
    * @param urls
    *   The NATS URLs (at least one)
    * @return
    *   Either an error message or the parsed ClientConfig
    */
  def fromUrls(urls: List[String]): Either[String, ClientConfig] =
    urls match
      case Nil =>
        Left("fromUrls requires at least one URL")
      case head :: tail =>
        for
          base <- fromUrl(head)
          extra <- tail.traverse(parseServerAddress)
        yield base.copy(servers = extra)

  /** Parse a `nats://host:port` or `tls://host:port` URL into a ServerAddress,
    * carrying the scheme as `useTls`.
    */
  private def parseServerAddress(url: String): Either[String, ServerAddress] =
    fromUrl(url).map(c => ServerAddress(c.host, c.port, c.useTls))
