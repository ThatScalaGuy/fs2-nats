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

package fs2.nats.objectstore

import fs2.nats.jetstream.protocol.{
  DiscardPolicy,
  StorageType,
  StoreCompression,
  StreamConfig
}

import scala.concurrent.duration.FiniteDuration

/** Configuration for an Object Store bucket.
  *
  * @param compression
  *   storage compression; defaults to S2 (NATS 2.10+), trading CPU for
  *   storage/bandwidth on large objects
  * @param allowDirect
  *   enable the JetStream Direct Get fast path for meta reads (recommended)
  */
final case class ObjConfig(
    bucket: String,
    description: Option[String] = None,
    ttl: Option[FiniteDuration] = None,
    maxBytes: Long = -1,
    storage: StorageType = StorageType.File,
    replicas: Int = 1,
    compression: StoreCompression = StoreCompression.S2,
    allowDirect: Boolean = true
):

  /** Validate the bucket name. */
  def validate: Either[String, ObjConfig] =
    ObjNames.validateBucket(bucket).map(_ => this)

  /** The backing JetStream stream configuration: chunk + meta subjects,
    * `discard=new`, rollup headers (for the meta rollup) and direct access.
    * Object Store does not set `deny_delete` because it purges chunk subjects.
    */
  def toStreamConfig: StreamConfig =
    StreamConfig(
      name = ObjNames.streamName(bucket),
      subjects = List(
        ObjNames.chunkStreamSubject(bucket),
        ObjNames.metaStreamSubject(bucket)
      ),
      storage = storage,
      maxBytes = maxBytes,
      maxAge = ttl,
      replicas = replicas,
      discard = DiscardPolicy.New,
      description = description,
      compression = compression,
      allowDirect = allowDirect,
      allowRollupHdrs = true
    )

object ObjConfig:
  /** Default object chunk size (128 KiB). */
  val DefaultChunkSize: Int = 128 * 1024
