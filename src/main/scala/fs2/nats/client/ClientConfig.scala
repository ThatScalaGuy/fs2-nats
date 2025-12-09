/*
 * Copyright 2024 fs2-nats contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.client

import scala.concurrent.duration.*
import com.comcast.ip4s.{Host, Port}
import fs2.io.net.tls.TLSParameters

/**
 * Policy for handling slow consumers when subscription queues fill up.
 */
enum SlowConsumerPolicy:
  /**
   * Block the reader until queue space is available.
   * This applies backpressure to the server connection.
   */
  case Block

  /**
   * Drop the oldest message in the queue to make room.
   * Useful when only recent messages matter.
   */
  case DropOldest

  /**
   * Drop the new incoming message.
   * Preserves message order in the queue.
   */
  case DropNew

  /**
   * Emit a SlowConsumer error event but continue processing.
   * Combines with DropNew behavior.
   */
  case ErrorAndDrop

/**
 * Credentials for NATS authentication.
 */
enum NatsCredentials:
  /**
   * Username/password authentication.
   *
   * @param username The username
   * @param password The password
   */
  case UserPassword(username: String, password: String)

  /**
   * Token-based authentication.
   *
   * @param token The authentication token
   */
  case Token(token: String)

  /**
   * NKey authentication with optional JWT.
   *
   * @param nkey The public NKey
   * @param jwt Optional user JWT
   */
  case NKey(nkey: String, jwt: Option[String] = None)

/**
 * Configuration for exponential backoff with jitter.
 *
 * @param baseDelay The initial delay before first retry
 * @param maxDelay The maximum delay between retries
 * @param factor The multiplier applied to delay after each retry
 * @param maxRetries Optional maximum number of retry attempts (None for unlimited)
 */
final case class BackoffConfig(
    baseDelay: FiniteDuration = 100.millis,
    maxDelay: FiniteDuration = 30.seconds,
    factor: Double = 2.0,
    maxRetries: Option[Int] = None
)

object BackoffConfig:
  /** Default backoff configuration: 100ms base, 30s max, factor 2.0, unlimited retries */
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

/**
 * Configuration for the NATS client connection.
 *
 * @param host The NATS server host
 * @param port The NATS server port (default: 4222)
 * @param useTls Whether to use TLS encryption
 * @param tlsParams Optional TLS parameters for custom configuration
 * @param name Optional client name for server identification
 * @param credentials Optional authentication credentials
 * @param backoff Backoff configuration for reconnection attempts
 * @param queueCapacity Default capacity for subscription message queues
 * @param slowConsumerPolicy Policy for handling full subscription queues
 * @param idleTimeout Optional timeout for idle connections
 * @param verbose Whether to enable verbose mode (+OK acknowledgements)
 * @param pedantic Whether to enable pedantic mode (stricter checking)
 * @param echo Whether the server should echo messages back to the publishing connection
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
    echo: Boolean = true
)

object ClientConfig:
  /**
   * Create a minimal configuration for localhost.
   *
   * @param port The port to connect to (default: 4222)
   * @return ClientConfig for localhost connection
   */
  def localhost(port: Int = 4222): ClientConfig =
    ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(port).get
    )

  /**
   * Create a configuration from a NATS URL.
   * Supports: nats://host:port, tls://host:port
   *
   * @param url The NATS URL
   * @return Either an error message or the parsed ClientConfig
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
        Left(s"Invalid NATS URL format: $url. Expected: nats://host:port or tls://host:port")
