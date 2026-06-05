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

import cats.effect.Resource
import fs2.{Chunk, Stream}
import fs2.nats.jetstream.protocol.{StorageType, StreamInfo}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** The operation that produced a KV entry. `Put` is the default (no marker
  * header); `Delete`/`Purge` are tombstone markers.
  */
enum KvOperation:
  case Put, Delete, Purge

/** A single value (or tombstone) stored under a key.
  *
  * @param revision
  *   the stream sequence assigned to this entry (monotonic per bucket)
  * @param created
  *   the server timestamp of the entry
  * @param delta
  *   distance from the latest revision at delivery time (0 == latest)
  */
final case class KvEntry(
    bucket: String,
    key: String,
    value: Chunk[Byte],
    revision: Long,
    created: Instant,
    delta: Long,
    operation: KvOperation
)

/** An event from a `watch`: either an entry or the one-shot marker emitted once
  * the initial snapshot has been fully delivered (live updates follow).
  */
enum KvWatchEvent:
  case Entry(value: KvEntry)
  case EndOfData

/** Options controlling a `watch`/`watchAll`.
  *
  * @param includeHistory
  *   replay every historical revision (`DeliverPolicy.All`) instead of only the
  *   latest per key
  * @param updatesOnly
  *   skip the initial snapshot and deliver only future updates
  *   (`DeliverPolicy.New`); mutually exclusive with `includeHistory`
  * @param ignoreDeletes
  *   drop `Delete`/`Purge` tombstones from the stream
  * @param metaOnly
  *   deliver entries without their values (headers only)
  */
final case class WatchOptions(
    includeHistory: Boolean = false,
    updatesOnly: Boolean = false,
    ignoreDeletes: Boolean = false,
    metaOnly: Boolean = false
)
object WatchOptions:
  val default: WatchOptions = WatchOptions()

/** Runtime status of a KV bucket. */
final case class KvStatus(
    bucket: String,
    values: Long,
    history: Long,
    ttl: Option[FiniteDuration],
    bytes: Long,
    storage: StorageType,
    replicas: Int
)
object KvStatus:
  /** Derive a bucket status from its backing stream info. */
  def from(bucket: String, info: StreamInfo): KvStatus =
    KvStatus(
      bucket = bucket,
      values = info.state.messages,
      history = info.config.maxMsgsPerSubject,
      ttl = info.config.maxAge,
      bytes = info.state.bytes,
      storage = info.config.storage,
      replicas = info.config.replicas
    )

/** A handle to a NATS Key-Value bucket, layered on JetStream.
  *
  * Reads use JetStream Direct Get when the bucket allows it (the value is the
  * raw message payload — no JSON/base64 on the hot path), falling back to
  * `STREAM.MSG.GET` otherwise. Writes go through the JetStream publish path and
  * its coalesced write/pipeline window. `keys`/`history`/`watch` stream from an
  * ephemeral, gap-resetting ordered consumer the handle creates and cleans up;
  * it recovers in-order across a reconnect mid-watch.
  */
trait KeyValue[F[_]]:

  /** The bucket name. */
  def bucket: String

  // ---- Reads ----

  /** Get the latest entry for `key`, or `None` if absent or deleted/purged. */
  def get(key: String): F[Option[KvEntry]]

  /** Get a specific revision of `key`, or `None` if it does not exist or the
    * sequence belongs to a different key.
    */
  def get(key: String, revision: Long): F[Option[KvEntry]]

  // ---- Writes ----

  /** Put a value and return its new revision. */
  def put(key: String, value: Chunk[Byte]): F[Long]

  /** Pipelined put: the outer effect completes once a publish-window slot is
    * taken; the inner effect yields the new revision (or fails).
    */
  def putAsync(key: String, value: Chunk[Byte]): F[F[Long]]

  /** Put only if the key does not currently exist; fail with
    * [[fs2.nats.errors.NatsError.KeyValueWrongLastSequence]] otherwise.
    */
  def create(key: String, value: Chunk[Byte]): F[Long]

  /** Put only if the key's latest revision equals `expectedRevision`
    * (optimistic concurrency); fail with
    * [[fs2.nats.errors.NatsError.KeyValueWrongLastSequence]] otherwise.
    */
  def update(key: String, value: Chunk[Byte], expectedRevision: Long): F[Long]

  /** Soft-delete a key (writes a `Delete` tombstone; history is retained). */
  def delete(key: String): F[Unit]

  /** Purge a key, collapsing its history to a single `Purge` tombstone. */
  def purge(key: String): F[Unit]

  // ---- Enumeration ----

  /** Stream the live (non-deleted) keys in the bucket. */
  def keys: Stream[F, String]

  /** All retained revisions of `key`, oldest first. */
  def history(key: String): F[List[KvEntry]]

  // ---- Watch ----

  /** Watch keys matching `keyPattern` (NATS wildcards allowed). The stream
    * emits the current snapshot, then a single `EndOfData`, then live updates.
    */
  def watch(
      keyPattern: String,
      opts: WatchOptions = WatchOptions.default
  ): Resource[F, Stream[F, KvWatchEvent]]

  /** Watch every key in the bucket. */
  def watchAll(
      opts: WatchOptions = WatchOptions.default
  ): Resource[F, Stream[F, KvWatchEvent]]

  // ---- Status ----

  /** Current bucket status. */
  def status: F[KvStatus]
