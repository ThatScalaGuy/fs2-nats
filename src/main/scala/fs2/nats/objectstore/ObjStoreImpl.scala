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

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.github.plokhotnyuk.jsoniter_scala.core.{readFromArray, writeToArray}
import fs2.io.file.{Files, Path}
import fs2.{Chunk, Stream}
import fs2.nats.client.NatsClient
import fs2.nats.errors.NatsError
import fs2.nats.jetstream.{
  ApiSubjects,
  JetStream,
  JetStreamConfig,
  OrderedConsumerOptions
}
import fs2.nats.jetstream.protocol.{DeliverPolicy, MessageGet, PurgeOptions}
import fs2.nats.objectstore.protocol.{ObjHeaders, WireObjectInfo, WireOptions}
import fs2.nats.protocol.Headers
import fs2.nats.util.Tokens

import java.security.MessageDigest
import java.time.Instant
import java.util.Base64

/** Implementation of the Object Store layer over a JetStream context.
  *
  * Built from inside the JetStream companion via
  * [[ObjStoreImpl.create]]/[[ObjStoreImpl.bind]]; only the public
  * [[ObjectStore]] type escapes.
  */
private[nats] object ObjStoreImpl:

  /** JetStream error code returned when no stored message matched a get. */
  private val NoMessageHttp = 404

  /** Create the bucket stream and return a handle. */
  def create[F[_]: Async](
      js: JetStream[F],
      client: NatsClient[F],
      subjects: ApiSubjects,
      config: JetStreamConfig,
      cfg: ObjConfig
  ): F[ObjectStore[F]] =
    Async[F]
      .fromEither(
        cfg.validate.leftMap(msg => NatsError.InvalidSubject(cfg.bucket, msg))
      )
      .flatMap { valid =>
        js.addStream(valid.toStreamConfig)
          .as(
            new Impl(
              js,
              client,
              subjects,
              config,
              valid.bucket,
              valid.allowDirect
            )
          )
      }

  /** Bind to an existing bucket, verifying its backing stream exists. */
  def bind[F[_]: Async](
      js: JetStream[F],
      client: NatsClient[F],
      subjects: ApiSubjects,
      config: JetStreamConfig,
      bucket: String
  ): F[ObjectStore[F]] =
    Async[F]
      .fromEither(
        ObjNames
          .validateBucket(bucket)
          .leftMap(msg => NatsError.InvalidSubject(bucket, msg))
      )
      .flatMap { b =>
        js.streamInfo(ObjNames.streamName(b))
          .map(info =>
            new Impl(js, client, subjects, config, b, info.config.allowDirect)
          )
      }

  private final class Impl[F[_]: Async](
      js: JetStream[F],
      client: NatsClient[F],
      subjects: ApiSubjects,
      config: JetStreamConfig,
      val bucket: String,
      allowDirect: Boolean
  ) extends ObjectStore[F]:

    private val F = Async[F]
    private val stream = ObjNames.streamName(bucket)

    // ---- Put ----

    override def put(meta: ObjectMeta, data: Stream[F, Byte]): F[ObjectInfo] =
      validName(meta.name) *>
        rawInfo(meta.name).flatMap { existing =>
          for
            nuid <- Tokens.randomInboxId[F](22)
            chunkSubj = ObjNames.chunkSubject(bucket, nuid)
            md <- F.delay(MessageDigest.getInstance("SHA-256"))
            // Rechunk to exact-size chunks; fold SHA-256 in order on this single
            // fiber, and pipeline each chunk publish through the bounded
            // publishAsync window (transport coalescing batches the writes).
            // Collect the in-flight PubAck effects to await durability.
            acks <- data
              .chunkN(meta.maxChunkSize, allowFewer = true)
              .evalMap { c =>
                (F.delay(md.update(c.toArray)) *> js.publishAsync(chunkSubj, c))
                  .map(ack => (c.size.toLong, ack))
              }
              .compile
              .toList
            _ <- acks.traverse_(_._2)
            size = acks.foldLeft(0L)(_ + _._1)
            chunks = acks.length.toLong
            digest = "SHA-256=" + Base64.getUrlEncoder
              .encodeToString(md.digest())
            now <- nowInstant
            info = ObjectInfo(
              name = meta.name,
              bucket = bucket,
              nuid = nuid,
              size = size,
              chunks = chunks,
              digest = digest,
              description = meta.description,
              metadata = meta.metadata,
              deleted = false,
              modified = now,
              link = None,
              maxChunkSize = Some(meta.maxChunkSize)
            )
            _ <- publishMeta(info)
            // Overwrite: purge the previous object's chunks after the new meta
            // is durable.
            _ <- existing match
              case Some(old) if !old.deleted && old.nuid != nuid =>
                js.purgeStream(
                  stream,
                  PurgeOptions(filter =
                    Some(ObjNames.chunkSubject(bucket, old.nuid))
                  )
                ).void
              case _ => F.unit
          yield info
        }

    override def putBytes(meta: ObjectMeta, data: Chunk[Byte]): F[ObjectInfo] =
      put(meta, Stream.chunk(data))

    override def putFile(name: String, path: Path): F[ObjectInfo] =
      put(ObjectMeta(name), Files.forAsync[F].readAll(path))

    // ---- Get ----

    override def get(name: String): F[Option[ObjectResult[F]]] =
      info(name).map(_.map(i => ObjectResult(i, dataStream(i))))

    override def getBytes(name: String): F[Option[Chunk[Byte]]] =
      get(name).flatMap(_.traverse(_.data.compile.to(Chunk)))

    override def getToFile(name: String, path: Path): F[Option[ObjectInfo]] =
      get(name).flatMap(
        _.traverse(r =>
          r.data
            .through(Files.forAsync[F].writeAll(path))
            .compile
            .drain
            .as(r.info)
        )
      )

    private def dataStream(i: ObjectInfo): Stream[F, Byte] =
      if i.size == 0 || i.chunks == 0 then Stream.empty
      else
        Stream
          .resource(
            js.subscribeOrdered(
              ObjNames.streamName(i.bucket),
              Some(ObjNames.chunkSubject(i.bucket, i.nuid)),
              OrderedConsumerOptions(deliverPolicy = DeliverPolicy.All)
            )
          )
          .flatMap { msgs =>
            Stream.eval(F.delay(MessageDigest.getInstance("SHA-256"))).flatMap {
              md =>
                msgs
                  .take(i.chunks)
                  .evalTap(m => F.delay(md.update(m.payload.toArray)))
                  .flatMap(m => Stream.chunk(m.payload)) ++
                  Stream.exec(verifyDigest(md, i))
            }
          }

    private def verifyDigest(md: MessageDigest, i: ObjectInfo): F[Unit] =
      F.delay("SHA-256=" + Base64.getUrlEncoder.encodeToString(md.digest()))
        .flatMap { actual =>
          if actual == i.digest then F.unit
          else
            F.raiseError(
              NatsError.ObjectDigestMismatch(i.name, i.digest, actual)
            )
        }

    // ---- Info (with link resolution) ----

    override def info(name: String): F[Option[ObjectInfo]] =
      rawInfo(name).flatMap {
        case Some(i) if i.deleted        => F.pure(None)
        case Some(i) if i.link.isDefined => resolveLink(i)
        case other                       => F.pure(other)
      }

    /** Read an object's meta without link resolution or deleted-filtering. */
    private def rawInfo(name: String): F[Option[ObjectInfo]] =
      validName(name) *> readMeta(bucket, name, allowDirect)

    private def readMeta(
        bkt: String,
        name: String,
        direct: Boolean
    ): F[Option[ObjectInfo]] =
      val strm = ObjNames.streamName(bkt)
      val metaSubj = ObjNames.metaSubject(bkt, name)
      if direct then
        client
          .request(
            subjects.directGetLastBySubject(strm, metaSubj),
            Chunk.empty,
            Headers.empty,
            config.timeout
          )
          .map(reply =>
            if reply.status.isDefined then None
            else
              Some(
                decodeInfo(
                  reply.payload,
                  parseInstant(reply.headers.get(ObjHeaders.TimeStamp))
                )
              )
          )
      else
        js.getMessage(strm, MessageGet.LastBySubject(metaSubj))
          .map(sm => Some(decodeInfo(sm.data, sm.time)))
          .recover { case NatsError.JetStreamApiError(NoMessageHttp, _, _) =>
            None
          }

    private def resolveLink(linkInfo: ObjectInfo): F[Option[ObjectInfo]] =
      linkInfo.link match
        case Some(ObjectLink(lb, Some(ln))) =>
          // Object link: resolve the target (cross-bucket reads use the
          // universal STREAM.MSG.GET path). A link to a link is rejected.
          readMeta(lb, ln, direct = false).flatMap {
            case Some(t) if t.link.isDefined =>
              F.raiseError(NatsError.ObjectIsLink(ln))
            case Some(t) if t.deleted => F.pure(None)
            case other                => F.pure(other)
          }
        case _ =>
          // Bucket link (or malformed): surface the link entry itself.
          F.pure(Some(linkInfo))

    // ---- Metadata updates ----

    override def updateMeta(name: String, meta: ObjectMeta): F[ObjectInfo] =
      rawInfo(name).flatMap {
        case Some(existing) if !existing.deleted && existing.link.isEmpty =>
          applyMeta(name, existing, meta)
        case _ => F.raiseError(NatsError.ObjectNotFound(bucket, name))
      }

    override def rename(from: String, to: String): F[ObjectInfo] =
      rawInfo(from).flatMap {
        case Some(existing) if !existing.deleted && existing.link.isEmpty =>
          applyMeta(
            from,
            existing,
            ObjectMeta(to, existing.description, existing.metadata)
          )
        case _ => F.raiseError(NatsError.ObjectNotFound(bucket, from))
      }

    private def applyMeta(
        oldName: String,
        existing: ObjectInfo,
        meta: ObjectMeta
    ): F[ObjectInfo] =
      val updated = existing.copy(
        name = meta.name,
        description = meta.description,
        metadata = meta.metadata
      )
      if meta.name == oldName then publishMeta(updated).as(updated)
      else
        rawInfo(meta.name).flatMap {
          case Some(t) if !t.deleted =>
            F.raiseError(NatsError.ObjectAlreadyExists(meta.name))
          case _ =>
            publishMeta(updated) *>
              publishMeta(tombstone(existing)).as(updated)
        }

    // ---- Delete ----

    override def delete(name: String): F[Unit] =
      rawInfo(name).flatMap {
        case Some(existing) if !existing.deleted =>
          publishMeta(tombstone(existing)) *>
            js.purgeStream(
              stream,
              PurgeOptions(filter =
                Some(ObjNames.chunkSubject(bucket, existing.nuid))
              )
            ).void
        case _ => F.raiseError(NatsError.ObjectNotFound(bucket, name))
      }

    // ---- Links ----

    override def addLink(
        linkName: String,
        target: ObjectInfo
    ): F[ObjectInfo] =
      if target.deleted then
        F.raiseError(NatsError.ObjectNotFound(target.bucket, target.name))
      else if target.link.isDefined then
        F.raiseError(NatsError.ObjectIsLink(target.name))
      else publishLink(linkName, ObjectLink(target.bucket, Some(target.name)))

    override def addBucketLink(
        linkName: String,
        target: ObjectStore[F]
    ): F[ObjectInfo] =
      publishLink(linkName, ObjectLink(target.bucket, None))

    private def publishLink(
        linkName: String,
        link: ObjectLink
    ): F[ObjectInfo] =
      validName(linkName) *>
        (Tokens.randomInboxId[F](22), nowInstant).flatMapN { (nuid, now) =>
          val info = ObjectInfo(
            name = linkName,
            bucket = bucket,
            nuid = nuid,
            size = 0L,
            chunks = 0L,
            digest = "",
            modified = now,
            link = Some(link)
          )
          publishMeta(info).as(info)
        }

    // ---- Enumeration / watch ----

    override def list: Stream[F, ObjectInfo] =
      Stream
        .resource(metaConsume)
        .flatMap { (pending, msgs) =>
          if pending == 0 then Stream.empty
          else msgs.take(pending).map(toInfo).filterNot(_.deleted)
        }

    override def watch: cats.effect.Resource[F, Stream[F, ObjectWatchEvent]] =
      metaConsume.map { (pending, msgs) =>
        val events = msgs.map(m => ObjectWatchEvent.Update(toInfo(m)))
        if pending == 0 then Stream.emit(ObjectWatchEvent.EndOfData) ++ events
        else
          events.zipWithIndex.flatMap { case (e, i) =>
            if i + 1 == pending then
              Stream.emits(List(e, ObjectWatchEvent.EndOfData))
            else Stream.emit(e)
          }
      }

    /** Ordered, last-per-subject consume over the meta subjects, yielding the
      * snapshot size and the decoded-message stream.
      */
    private def metaConsume: Resource[
      F,
      (Long, Stream[F, fs2.nats.jetstream.JsMessage[F]])
    ] =
      js.subscribeOrderedWithInfo(
        stream,
        Some(ObjNames.metaStreamSubject(bucket)),
        OrderedConsumerOptions(deliverPolicy = DeliverPolicy.LastPerSubject)
      ).map { case (info, msgs) => (info.numPending, msgs) }

    private def toInfo(m: fs2.nats.jetstream.JsMessage[F]): ObjectInfo =
      decodeInfo(m.payload, m.metadata.timestamp)

    // ---- Admin ----

    override def seal: F[Unit] =
      js.streamInfo(stream)
        .flatMap(i => js.updateStream(i.config.copy(sealedStream = true)))
        .void

    override def status: F[ObjStatus] =
      js.streamInfo(stream).map(ObjStatus.from(bucket, _))

    // ---- Shared helpers ----

    private def publishMeta(info: ObjectInfo): F[Unit] =
      val wire = WireObjectInfo(
        name = info.name,
        bucket = info.bucket,
        nuid = info.nuid,
        size = info.size,
        chunks = info.chunks,
        digest = info.digest,
        description = info.description,
        metadata = info.metadata,
        deleted = info.deleted,
        options =
          if info.link.isDefined || info.maxChunkSize.isDefined then
            Some(WireOptions(info.link, info.maxChunkSize))
          else None
      )
      js.publish(
        ObjNames.metaSubject(bucket, info.name),
        Chunk.array(writeToArray(wire)),
        Headers.empty.set(ObjHeaders.Rollup, ObjHeaders.RollupSubject)
      ).void

    private def decodeInfo(
        payload: Chunk[Byte],
        modified: Instant
    ): ObjectInfo =
      val w = readFromArray[WireObjectInfo](payload.toArray)
      ObjectInfo(
        name = w.name,
        bucket = w.bucket,
        nuid = w.nuid,
        size = w.size,
        chunks = w.chunks,
        digest = w.digest,
        description = w.description,
        metadata = w.metadata,
        deleted = w.deleted,
        modified = modified,
        link = w.options.flatMap(_.link),
        maxChunkSize = w.options.flatMap(_.maxChunkSize)
      )

    private def tombstone(info: ObjectInfo): ObjectInfo =
      info.copy(deleted = true, size = 0L, chunks = 0L, digest = "")

    private def validName(name: String): F[Unit] =
      F.fromEither(
        ObjNames
          .validateObjectName(name)
          .leftMap(msg => NatsError.InvalidSubject(name, msg))
          .void
      )

    private def nowInstant: F[Instant] =
      F.realTime.map(d => Instant.ofEpochMilli(d.toMillis))

    private def parseInstant(s: Option[String]): Instant =
      s.flatMap(v => scala.util.Try(Instant.parse(v)).toOption)
        .getOrElse(Instant.EPOCH)
