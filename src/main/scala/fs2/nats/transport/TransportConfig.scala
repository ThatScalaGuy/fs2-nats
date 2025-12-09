/*
 * Copyright 2024 fs2-nats contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.transport

import scala.concurrent.duration.*

/**
 * Configuration for the NATS transport layer.
 *
 * @param readBufferSize Size of the read buffer in bytes (default 8192)
 * @param writeQueueCapacity Capacity of the outbound write queue (default 8192)
 * @param socketChunkSize Size of chunks when reading from socket (default 8192)
 * @param connectTimeout Timeout for initial connection (default 10 seconds)
 * @param writeTimeout Timeout for write operations (default 30 seconds)
 */
final case class TransportConfig(
    readBufferSize: Int = 8192,
    writeQueueCapacity: Int = 8192,
    socketChunkSize: Int = 8192,
    connectTimeout: FiniteDuration = 10.seconds,
    writeTimeout: FiniteDuration = 30.seconds
)

object TransportConfig:
  val default: TransportConfig = TransportConfig()

  val highThroughput: TransportConfig = TransportConfig(
    readBufferSize = 65536,
    writeQueueCapacity = 65536,
    socketChunkSize = 65536
  )

  val lowLatency: TransportConfig = TransportConfig(
    readBufferSize = 4096,
    writeQueueCapacity = 1024,
    socketChunkSize = 4096
  )
