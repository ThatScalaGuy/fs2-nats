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

package fs2.nats.kv

import fs2.nats.jetstream.protocol.{
  DiscardPolicy,
  StorageType,
  StoreCompression,
  StreamConfig
}

import scala.concurrent.duration.FiniteDuration

/** Configuration for a KV bucket.
  *
  * @param history
  *   number of historical revisions retained per key
  *   (1..[[KvConfig.MaxHistory]])
  * @param ttl
  *   maximum age of an entry before it expires
  * @param maxValueSize
  *   maximum size of a single value in bytes (`-1` == unlimited)
  * @param maxBytes
  *   maximum total size of the bucket in bytes (`-1` == unlimited)
  * @param allowDirect
  *   enable the JetStream Direct Get fast path for reads (recommended)
  */
final case class KvConfig(
    bucket: String,
    description: Option[String] = None,
    maxValueSize: Int = -1,
    history: Int = 1,
    ttl: Option[FiniteDuration] = None,
    maxBytes: Long = -1,
    storage: StorageType = StorageType.File,
    replicas: Int = 1,
    compression: StoreCompression = StoreCompression.None,
    duplicateWindow: Option[FiniteDuration] = None,
    allowDirect: Boolean = true
):

  /** Validate the bucket name and history depth. */
  def validate: Either[String, KvConfig] =
    KvNames.validateBucket(bucket).flatMap { _ =>
      if history < 1 || history > KvConfig.MaxHistory then
        Left(
          s"history must be between 1 and ${KvConfig.MaxHistory}, got $history"
        )
      else Right(this)
    }

  /** The backing JetStream stream configuration. */
  def toStreamConfig: StreamConfig =
    StreamConfig(
      name = KvNames.streamName(bucket),
      subjects = List(KvNames.allKeysSubject(bucket)),
      storage = storage,
      maxBytes = maxBytes,
      maxAge = ttl,
      maxMsgSize = maxValueSize,
      maxMsgsPerSubject = history.toLong,
      replicas = replicas,
      discard = DiscardPolicy.New,
      duplicateWindow = duplicateWindow,
      description = description,
      compression = compression,
      allowDirect = allowDirect,
      // KV relies on per-subject roll-over to retain only `history` revisions:
      // leave discard_new_per_subject off so a write past the limit drops the
      // oldest rather than being rejected. discard=New still bounds the bucket
      // by max_bytes. allow_rollup_hdrs powers purge; deny_delete blocks the
      // raw message-delete API.
      allowRollupHdrs = true,
      denyDelete = true
    )

object KvConfig:
  /** Maximum history depth supported by the server. */
  val MaxHistory: Int = 64
