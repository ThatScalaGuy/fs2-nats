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

package fs2.nats.jetstream.protocol

import io.circe.Decoder

/** Per-account JetStream resource limits. `-1` means unlimited. */
final case class AccountLimits(
    maxMemory: Long,
    maxStorage: Long,
    maxStreams: Int,
    maxConsumers: Int
)
object AccountLimits:
  given Decoder[AccountLimits] = Decoder.instance { c =>
    for
      maxMemory <- c
        .downField("max_memory")
        .as[Option[Long]]
        .map(_.getOrElse(0L))
      maxStorage <- c
        .downField("max_storage")
        .as[Option[Long]]
        .map(_.getOrElse(0L))
      maxStreams <- c
        .downField("max_streams")
        .as[Option[Int]]
        .map(_.getOrElse(-1))
      maxConsumers <- c
        .downField("max_consumers")
        .as[Option[Int]]
        .map(_.getOrElse(-1))
    yield AccountLimits(maxMemory, maxStorage, maxStreams, maxConsumers)
  }

/** Account-level JetStream usage and limits (`$JS.API.INFO`). */
final case class AccountInfo(
    memory: Long,
    storage: Long,
    streams: Int,
    consumers: Int,
    domain: Option[String],
    limits: AccountLimits
)
object AccountInfo:
  given Decoder[AccountInfo] = Decoder.instance { c =>
    for
      memory <- c.downField("memory").as[Option[Long]].map(_.getOrElse(0L))
      storage <- c.downField("storage").as[Option[Long]].map(_.getOrElse(0L))
      streams <- c.downField("streams").as[Option[Int]].map(_.getOrElse(0))
      consumers <- c.downField("consumers").as[Option[Int]].map(_.getOrElse(0))
      domain <- c.downField("domain").as[Option[String]]
      limits <- c
        .downField("limits")
        .as[Option[AccountLimits]]
        .map(_.getOrElse(AccountLimits(0L, 0L, -1, -1)))
    yield AccountInfo(memory, storage, streams, consumers, domain, limits)
  }
