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

import fs2.{Chunk, Stream}
import fs2.io.file.Path
import fs2.nats.jetstream.protocol.{StorageType, StreamInfo}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** User-supplied metadata for a put. `name` is the object key; everything else
  * is optional.
  *
  * @param maxChunkSize
  *   bytes per stored chunk (default 128 KiB)
  */
final case class ObjectMeta(
    name: String,
    description: Option[String] = None,
    metadata: Map[String, String] = Map.empty,
    maxChunkSize: Int = ObjConfig.DefaultChunkSize
)

/** A link to another object (when `name` is set) or to a whole bucket (when
  * `name` is empty).
  */
final case class ObjectLink(bucket: String, name: Option[String] = None)

/** Server-side stored information about an object, returned by
  * `put`/`info`/`list`/`watch`.
  *
  * @param digest
  *   `"SHA-256=<url-base64>"` over the whole object
  * @param modified
  *   client-side mtime, taken from the meta message timestamp
  * @param link
  *   present when this entry is a link rather than a stored object
  * @param maxChunkSize
  *   the chunk size used when the object was written
  */
final case class ObjectInfo(
    name: String,
    bucket: String,
    nuid: String,
    size: Long,
    chunks: Long,
    digest: String,
    description: Option[String] = None,
    metadata: Map[String, String] = Map.empty,
    deleted: Boolean = false,
    modified: Instant = Instant.EPOCH,
    link: Option[ObjectLink] = None,
    maxChunkSize: Option[Int] = None
)

/** The result of a `get`: the object's [[ObjectInfo]] plus a streaming,
  * digest-verified byte source.
  *
  * Consuming `data` opens an ordered consumer over the object's chunks, streams
  * them in order without materializing the whole object, and verifies the
  * SHA-256 digest once all chunks have been read — raising
  * [[fs2.nats.errors.NatsError.ObjectDigestMismatch]] on mismatch. If the
  * caller stops early the digest is not checked.
  */
final case class ObjectResult[F[_]](info: ObjectInfo, data: Stream[F, Byte])

/** An event from a `watch`: an object meta update (including deletions, which
  * arrive as an [[ObjectInfo]] with `deleted = true`), or the one-shot marker
  * emitted once the initial snapshot has been delivered.
  */
enum ObjectWatchEvent:
  case Update(info: ObjectInfo)
  case EndOfData

/** Runtime status of an Object Store bucket. */
final case class ObjStatus(
    bucket: String,
    description: Option[String],
    ttl: Option[FiniteDuration],
    storage: StorageType,
    replicas: Int,
    size: Long,
    isSealed: Boolean
)
object ObjStatus:
  /** Derive a bucket status from its backing stream info. */
  def from(bucket: String, info: StreamInfo): ObjStatus =
    ObjStatus(
      bucket = bucket,
      description = info.config.description,
      ttl = info.config.maxAge,
      storage = info.config.storage,
      replicas = info.config.replicas,
      size = info.state.bytes,
      isSealed = info.config.sealedStream
    )

/** A handle to a NATS Object Store bucket, layered on JetStream.
  *
  * Objects are chunked across a JetStream stream: each object's chunks are
  * published (pipelined, coalesced) under a per-object NUID subject, and a
  * rolled-up meta message records its size/chunk-count/SHA-256 digest. Reads
  * stream the chunks back in order via a gap-resetting ordered consumer and
  * verify the digest at the end. Both `put` and `get` are fully streaming and
  * never materialize a whole object in memory.
  */
trait ObjectStore[F[_]]:

  /** The bucket name. */
  def bucket: String

  // ---- Put (streaming-first) ----

  /** Store an object from a streaming byte source and return its info. */
  def put(meta: ObjectMeta, data: Stream[F, Byte]): F[ObjectInfo]

  /** Store an in-memory object. */
  def putBytes(meta: ObjectMeta, data: Chunk[Byte]): F[ObjectInfo]

  /** Store a file's contents under `name` (streamed from disk). */
  def putFile(name: String, path: Path): F[ObjectInfo]

  // ---- Get (streaming-first, digest-verified) ----

  /** Get an object (following links), or `None` if absent or deleted. */
  def get(name: String): F[Option[ObjectResult[F]]]

  /** Get an object's bytes into memory (digest-verified), or `None`. */
  def getBytes(name: String): F[Option[Chunk[Byte]]]

  /** Stream an object to a file (digest-verified); returns its info or `None`.
    */
  def getToFile(name: String, path: Path): F[Option[ObjectInfo]]

  // ---- Metadata ----

  /** Get an object's info (following links), or `None` if absent or deleted. */
  def info(name: String): F[Option[ObjectInfo]]

  /** Update an object's description/metadata (and optionally its name, which
    * renames it) without re-uploading its bytes.
    */
  def updateMeta(name: String, meta: ObjectMeta): F[ObjectInfo]

  /** Rename an object without re-uploading its bytes. */
  def rename(from: String, to: String): F[ObjectInfo]

  // ---- Delete ----

  /** Delete an object: tombstone its meta and purge its chunks. */
  def delete(name: String): F[Unit]

  // ---- Links ----

  /** Create a link `linkName` pointing at `target` (another object). */
  def addLink(linkName: String, target: ObjectInfo): F[ObjectInfo]

  /** Create a link `linkName` pointing at another bucket. */
  def addBucketLink(linkName: String, target: ObjectStore[F]): F[ObjectInfo]

  // ---- Enumeration / watch ----

  /** Stream the current (non-deleted) objects in the bucket. */
  def list: Stream[F, ObjectInfo]

  /** Watch object meta changes: emits the current snapshot, then a single
    * `EndOfData`, then live updates (including deletions).
    */
  def watch: cats.effect.Resource[F, Stream[F, ObjectWatchEvent]]

  // ---- Bucket admin ----

  /** Seal the bucket, making it permanently read-only. */
  def seal: F[Unit]

  /** Current bucket status. */
  def status: F[ObjStatus]
