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

package fs2.nats.protocol

import io.circe.{Decoder, Encoder}

/** Server INFO message sent upon connection and asynchronously when cluster
  * topology changes. Contains server metadata and connection parameters.
  *
  * @param serverId
  *   The unique identifier of the server
  * @param serverName
  *   The name of the server
  * @param version
  *   The version string of the NATS server
  * @param proto
  *   Protocol version in use
  * @param go
  *   Go version used to build the server
  * @param host
  *   The host this server is bound to
  * @param port
  *   The port this server is bound to
  * @param headersSupported
  *   Whether the server supports headers (NATS 2.2+)
  * @param maxPayload
  *   Maximum payload size the server will accept
  * @param clientId
  *   The client ID assigned by the server
  * @param authRequired
  *   Whether authentication is required
  * @param tlsRequired
  *   Whether TLS is required
  * @param tlsVerify
  *   Whether TLS client certificate verification is required
  * @param tlsAvailable
  *   Whether TLS is available
  * @param connectUrls
  *   List of server URLs for cluster-aware clients
  * @param ldmMode
  *   Whether the server is in Lame Duck Mode
  * @param jetStream
  *   Whether JetStream is enabled (not used in this iteration)
  * @param nonce
  *   Authentication nonce for NKey authentication
  */
final case class Info(
    serverId: String,
    serverName: Option[String] = None,
    version: String,
    proto: Int,
    go: String,
    host: String,
    port: Int,
    headersSupported: Boolean = false,
    maxPayload: Long,
    clientId: Option[Long] = None,
    authRequired: Boolean = false,
    tlsRequired: Boolean = false,
    tlsVerify: Boolean = false,
    tlsAvailable: Boolean = false,
    connectUrls: Option[List[String]] = None,
    ldmMode: Boolean = false,
    jetStream: Boolean = false,
    nonce: Option[String] = None
)

object Info:
  given Decoder[Info] = Decoder.instance { c =>
    for
      serverId <- c.downField("server_id").as[String]
      serverName <- c.downField("server_name").as[Option[String]]
      version <- c.downField("version").as[String]
      proto <- c.downField("proto").as[Int]
      go <- c.downField("go").as[String]
      host <- c.downField("host").as[String]
      port <- c.downField("port").as[Int]
      headersSupported <- c
        .downField("headers")
        .as[Option[Boolean]]
        .map(_.getOrElse(false))
      maxPayload <- c.downField("max_payload").as[Long]
      clientId <- c.downField("client_id").as[Option[Long]]
      authRequired <- c
        .downField("auth_required")
        .as[Option[Boolean]]
        .map(_.getOrElse(false))
      tlsRequired <- c
        .downField("tls_required")
        .as[Option[Boolean]]
        .map(_.getOrElse(false))
      tlsVerify <- c
        .downField("tls_verify")
        .as[Option[Boolean]]
        .map(_.getOrElse(false))
      tlsAvailable <- c
        .downField("tls_available")
        .as[Option[Boolean]]
        .map(_.getOrElse(false))
      connectUrls <- c.downField("connect_urls").as[Option[List[String]]]
      ldmMode <- c.downField("ldm").as[Option[Boolean]].map(_.getOrElse(false))
      jetStream <- c
        .downField("jetstream")
        .as[Option[Boolean]]
        .map(_.getOrElse(false))
      nonce <- c.downField("nonce").as[Option[String]]
    yield Info(
      serverId = serverId,
      serverName = serverName,
      version = version,
      proto = proto,
      go = go,
      host = host,
      port = port,
      headersSupported = headersSupported,
      maxPayload = maxPayload,
      clientId = clientId,
      authRequired = authRequired,
      tlsRequired = tlsRequired,
      tlsVerify = tlsVerify,
      tlsAvailable = tlsAvailable,
      connectUrls = connectUrls,
      ldmMode = ldmMode,
      jetStream = jetStream,
      nonce = nonce
    )
  }

/** Client CONNECT message sent after receiving INFO from server. Contains
  * client metadata and authentication credentials.
  *
  * @param verbose
  *   If true, server sends +OK for each correctly parsed command
  * @param pedantic
  *   If true, server performs stricter checking
  * @param tlsRequired
  *   If true, client requires TLS connection
  * @param authToken
  *   Authentication token if required
  * @param user
  *   Username for user/password authentication
  * @param pass
  *   Password for user/password authentication
  * @param name
  *   Optional client name for identification
  * @param lang
  *   Client implementation language
  * @param version
  *   Client version
  * @param protocol
  *   Protocol version
  * @param echo
  *   If false, server won't echo messages published by this client
  * @param sig
  *   Signature for NKey authentication
  * @param jwt
  *   User JWT for JWT authentication
  * @param nkey
  *   Public NKey for NKey authentication
  * @param headersSupported
  *   Whether client supports headers
  * @param noResponders
  *   Whether to enable no-responders behavior
  */
final case class Connect(
    verbose: Boolean = false,
    pedantic: Boolean = false,
    tlsRequired: Boolean = false,
    authToken: Option[String] = None,
    user: Option[String] = None,
    pass: Option[String] = None,
    name: Option[String] = None,
    lang: String = "scala",
    version: String = "0.1.0",
    protocol: Int = 1,
    echo: Boolean = true,
    sig: Option[String] = None,
    jwt: Option[String] = None,
    nkey: Option[String] = None,
    headersSupported: Boolean = true,
    noResponders: Boolean = true
)

object Connect:
  given Encoder[Connect] = Encoder.instance { c =>
    import io.circe.Json

    val fields = List(
      Some("verbose" -> Json.fromBoolean(c.verbose)),
      Some("pedantic" -> Json.fromBoolean(c.pedantic)),
      Some("tls_required" -> Json.fromBoolean(c.tlsRequired)),
      c.authToken.map(t => "auth_token" -> Json.fromString(t)),
      c.user.map(u => "user" -> Json.fromString(u)),
      c.pass.map(p => "pass" -> Json.fromString(p)),
      c.name.map(n => "name" -> Json.fromString(n)),
      Some("lang" -> Json.fromString(c.lang)),
      Some("version" -> Json.fromString(c.version)),
      Some("protocol" -> Json.fromInt(c.protocol)),
      Some("echo" -> Json.fromBoolean(c.echo)),
      c.sig.map(s => "sig" -> Json.fromString(s)),
      c.jwt.map(j => "jwt" -> Json.fromString(j)),
      c.nkey.map(n => "nkey" -> Json.fromString(n)),
      Some("headers" -> Json.fromBoolean(c.headersSupported)),
      Some("no_responders" -> Json.fromBoolean(c.noResponders))
    ).flatten

    Json.obj(fields*)
  }

/** PUB command for publishing a message without headers.
  *
  * @param subject
  *   The subject to publish to
  * @param replyTo
  *   Optional reply-to subject for request/reply pattern
  * @param payloadLength
  *   The length of the payload in bytes
  */
final case class Pub(
    subject: String,
    replyTo: Option[String],
    payloadLength: Int
)

/** HPUB command for publishing a message with headers (NATS 2.2+).
  *
  * @param subject
  *   The subject to publish to
  * @param replyTo
  *   Optional reply-to subject for request/reply pattern
  * @param headerLength
  *   The length of the headers block in bytes
  * @param totalLength
  *   The total length of headers + payload in bytes
  */
final case class HPub(
    subject: String,
    replyTo: Option[String],
    headerLength: Int,
    totalLength: Int
)

/** SUB command for subscribing to a subject.
  *
  * @param subject
  *   The subject to subscribe to (may include wildcards)
  * @param queueGroup
  *   Optional queue group for load balancing
  * @param sid
  *   The subscription ID (unique per connection)
  */
final case class Sub(
    subject: String,
    queueGroup: Option[String],
    sid: Long
)

/** UNSUB command for unsubscribing from a subscription.
  *
  * @param sid
  *   The subscription ID to unsubscribe
  * @param maxMsgs
  *   Optional max messages to receive before auto-unsubscribing
  */
final case class Unsub(
    sid: Long,
    maxMsgs: Option[Int]
)

/** MSG delivery from server for a message without headers.
  *
  * @param subject
  *   The subject the message was published to
  * @param sid
  *   The subscription ID that matched
  * @param replyTo
  *   Optional reply-to subject
  * @param payloadLength
  *   The length of the payload in bytes
  */
final case class Msg(
    subject: String,
    sid: Long,
    replyTo: Option[String],
    payloadLength: Int
)

/** HMSG delivery from server for a message with headers (NATS 2.2+).
  *
  * @param subject
  *   The subject the message was published to
  * @param sid
  *   The subscription ID that matched
  * @param replyTo
  *   Optional reply-to subject
  * @param headerLength
  *   The length of the headers block in bytes
  * @param totalLength
  *   The total length of headers + payload in bytes
  */
final case class HMsg(
    subject: String,
    sid: Long,
    replyTo: Option[String],
    headerLength: Int,
    totalLength: Int
)

/** Control line variants that don't carry payloads.
  */
enum ControlLine:
  /** PING command from server or client */
  case Ping

  /** PONG response to PING */
  case Pong

  /** +OK acknowledgement from server (when verbose mode is enabled) */
  case Ok

  /** -ERR error message from server */
  case Err(message: String)
