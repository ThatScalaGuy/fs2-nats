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

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.github.plokhotnyuk.jsoniter_scala.core.writeToArray
import fs2.{Chunk, Stream}
import fs2.nats.client.NatsClient
import fs2.nats.errors.NatsError
import fs2.nats.jetstream.{
  ApiSubjects,
  JetStream,
  JetStreamConfig,
  JsAckParser,
  PublishOptions,
  PushSignal,
  PushStatus
}
import fs2.nats.jetstream.protocol.{
  AckPolicy,
  ConsumerConfig,
  DeliverPolicy,
  MessageGet,
  StoredMessage
}
import fs2.nats.kv.protocol.{DirectGetRequest, KvHeaders}
import fs2.nats.protocol.Headers
import fs2.nats.subscriptions.NatsMessage
import fs2.nats.util.Tokens

import java.time.Instant
import scala.concurrent.duration.*

/** Implementation of the KV layer over a JetStream context.
  *
  * Built from inside the JetStream companion (where
  * `client`/`subjects`/`config` are in scope) via
  * [[KvImpl.create]]/[[KvImpl.bind]]; only the public [[KeyValue]] type
  * escapes.
  */
private[nats] object KvImpl:

  /** Wrong-last-sequence server error code (optimistic concurrency failure). */
  private val WrongLastSeqErr = 10071

  /** JetStream error code returned when no stored message matched a get. */
  private val NoMessageHttp = 404

  /** The server requires `flow_control` consumers to set an idle heartbeat. */
  private val WatchHeartbeat = 5.seconds

  /** Create the bucket stream and return a handle. */
  def create[F[_]: Async](
      js: JetStream[F],
      client: NatsClient[F],
      subjects: ApiSubjects,
      config: JetStreamConfig,
      cfg: KvConfig
  ): F[KeyValue[F]] =
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
  ): F[KeyValue[F]] =
    Async[F]
      .fromEither(
        KvNames
          .validateBucket(bucket)
          .leftMap(msg => NatsError.InvalidSubject(bucket, msg))
      )
      .flatMap { b =>
        js.streamInfo(KvNames.streamName(b))
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
  ) extends KeyValue[F]:

    private val F = Async[F]
    private val stream = KvNames.streamName(bucket)

    // ---- Reads ----

    override def get(key: String): F[Option[KvEntry]] =
      withValidKey(key) {
        val subject = KvNames.keySubject(bucket, key)
        if allowDirect then
          client
            .request(
              subjects.directGetLastBySubject(stream, subject),
              Chunk.empty,
              Headers.empty,
              config.timeout
            )
            .map(parseDirect(key, _))
        else
          js.getMessage(stream, MessageGet.LastBySubject(subject))
            .map(storedToEntry(key, _))
            .recover { case NatsError.JetStreamApiError(NoMessageHttp, _, _) =>
              None
            }
      }

    override def get(key: String, revision: Long): F[Option[KvEntry]] =
      withValidKey(key) {
        if allowDirect then
          client
            .request(
              subjects.directGet(stream),
              Chunk.array(writeToArray(DirectGetRequest(seq = Some(revision)))),
              Headers.empty,
              config.timeout
            )
            .map(parseDirect(key, _))
        else
          js.getMessage(stream, MessageGet.BySeq(revision))
            .map { sm =>
              if sm.subject == KvNames.keySubject(bucket, key) then
                storedToEntry(key, sm)
              else None
            }
            .recover { case NatsError.JetStreamApiError(NoMessageHttp, _, _) =>
              None
            }
      }

    // ---- Writes ----

    override def put(key: String, value: Chunk[Byte]): F[Long] =
      withValidKey(key)(
        js.publish(KvNames.keySubject(bucket, key), value).map(_.seq)
      )

    override def putAsync(key: String, value: Chunk[Byte]): F[F[Long]] =
      withValidKey(key)(
        js.publishAsync(KvNames.keySubject(bucket, key), value)
          .map(_.map(_.seq))
      )

    override def create(key: String, value: Chunk[Byte]): F[Long] =
      casPublish(key, value, expected = 0L)

    override def update(
        key: String,
        value: Chunk[Byte],
        expectedRevision: Long
    ): F[Long] =
      casPublish(key, value, expected = expectedRevision)

    private def casPublish(
        key: String,
        value: Chunk[Byte],
        expected: Long
    ): F[Long] =
      withValidKey(key)(
        js.publish(
          KvNames.keySubject(bucket, key),
          value,
          Headers.empty,
          PublishOptions(expectedLastSubjectSeq = Some(expected))
        ).map(_.seq)
          .adaptError {
            case NatsError.JetStreamApiError(_, WrongLastSeqErr, _) =>
              NatsError.KeyValueWrongLastSequence(key, expected)
          }
      )

    override def delete(key: String): F[Unit] =
      withValidKey(key)(
        js.publish(
          KvNames.keySubject(bucket, key),
          Chunk.empty,
          Headers.empty.set(KvHeaders.Operation, KvHeaders.OperationDelete)
        ).void
      )

    override def purge(key: String): F[Unit] =
      withValidKey(key)(
        js.publish(
          KvNames.keySubject(bucket, key),
          Chunk.empty,
          Headers.empty
            .set(KvHeaders.Operation, KvHeaders.OperationPurge)
            .set(KvHeaders.Rollup, KvHeaders.RollupSubject)
        ).void
      )

    // ---- Status ----

    override def status: F[KvStatus] =
      js.streamInfo(stream).map(KvStatus.from(bucket, _))

    // ---- Enumeration ----

    override def keys: Stream[F, String] =
      Stream
        .resource(
          kvConsume(
            KvNames.allKeysSubject(bucket),
            DeliverPolicy.LastPerSubject,
            headersOnly = true
          )
        )
        .flatMap { (pending, msgs) =>
          if pending == 0 then Stream.empty
          else
            msgs
              .take(pending)
              .map(m =>
                (
                  KvNames.keyFromSubject(bucket, m.subject),
                  operation(m.headers)
                )
              )
              .collect { case (k, KvOperation.Put) => k }
        }

    override def history(key: String): F[List[KvEntry]] =
      withValidKey(key) {
        Stream
          .resource(
            kvConsume(
              KvNames.keySubject(bucket, key),
              DeliverPolicy.All,
              headersOnly = false
            )
          )
          .flatMap { (pending, msgs) =>
            if pending == 0 then Stream.empty
            else msgs.take(pending).map(buildEntry(key, _))
          }
          .compile
          .toList
      }

    // ---- Watch ----

    override def watch(
        keyPattern: String,
        opts: WatchOptions
    ): Resource[F, Stream[F, KvWatchEvent]] =
      Resource.eval(validatePattern(keyPattern)) >>
        kvConsume(
          KvNames.patternSubject(bucket, keyPattern),
          watchPolicy(opts),
          opts.metaOnly
        ).map { (pending, msgs) => watchStream(pending, msgs, opts) }

    override def watchAll(
        opts: WatchOptions
    ): Resource[F, Stream[F, KvWatchEvent]] =
      watch(">", opts)

    private def watchStream(
        pending: Long,
        msgs: Stream[F, NatsMessage],
        opts: WatchOptions
    ): Stream[F, KvWatchEvent] =
      if pending == 0 then
        Stream.emit(KvWatchEvent.EndOfData) ++ msgs.map(toEvent(opts)).unNone
      else
        msgs.zipWithIndex.flatMap { case (m, i) =>
          val entry =
            toEvent(opts)(m).fold(Chunk.empty[KvWatchEvent])(Chunk.singleton)
          if i + 1 == pending then
            Stream.chunk(entry ++ Chunk.singleton(KvWatchEvent.EndOfData))
          else Stream.chunk(entry)
        }

    private def toEvent(opts: WatchOptions)(
        m: NatsMessage
    ): Option[KvWatchEvent] =
      val entry = buildEntry(KvNames.keyFromSubject(bucket, m.subject), m)
      if opts.ignoreDeletes && entry.operation != KvOperation.Put then None
      else Some(KvWatchEvent.Entry(entry))

    private def watchPolicy(opts: WatchOptions): DeliverPolicy =
      if opts.updatesOnly then DeliverPolicy.New
      else if opts.includeHistory then DeliverPolicy.All
      else DeliverPolicy.LastPerSubject

    /** Create an ephemeral push consumer over `filter` and stream its raw
      * deliveries, returning the snapshot size (`numPending` at creation) so
      * the caller can detect "caught up". Heartbeats are dropped and
      * flow-control is answered; the consumer is deleted on release.
      */
    private def kvConsume(
        filter: String,
        deliverPolicy: DeliverPolicy,
        headersOnly: Boolean
    ): Resource[F, (Long, Stream[F, NatsMessage])] =
      for
        inbox <- Resource.eval(
          Tokens.randomInboxId[F]().map(id => s"_INBOX.$id")
        )
        // Subscribe before creating the consumer so no early delivery is missed.
        raw <- client.subscribe(inbox)
        info <- Resource.make(
          js.addConsumer(
            stream,
            ConsumerConfig(
              deliverPolicy = deliverPolicy,
              ackPolicy = AckPolicy.None,
              filterSubject = Some(filter),
              headersOnly = headersOnly,
              deliverSubject = Some(inbox),
              flowControl = true,
              idleHeartbeat = Some(WatchHeartbeat),
              inactiveThreshold = Some(5.minutes)
            )
          )
        )(info => js.deleteConsumer(stream, info.name).attempt.void)
      yield (info.numPending, raw.evalMapFilter(classify))

    private def classify(m: NatsMessage): F[Option[NatsMessage]] =
      PushStatus.classify(m.status, m.replyTo, m.statusDescription) match
        case PushSignal.Data               => F.pure(Some(m))
        case PushSignal.Heartbeat          => F.pure(None)
        case PushSignal.FlowControl(reply) =>
          client.publish(reply, Chunk.empty).as(None)
        case PushSignal.Fail(code, desc) =>
          F.raiseError(NatsError.JetStreamApiError(code, 0, desc))

    // ---- Shared parsing ----

    private def parseDirect(key: String, reply: NatsMessage): Option[KvEntry] =
      if reply.status.isDefined then None // 404 (or any status) => not found
      else if reply.headers
          .get(KvHeaders.Subject)
          .exists(_ != KvNames.keySubject(bucket, key))
      then None // a by-sequence lookup landed on a different key
      else
        operation(reply.headers) match
          case KvOperation.Put =>
            Some(
              KvEntry(
                bucket,
                key,
                reply.payload,
                parseLong(reply.headers.get(KvHeaders.Sequence)),
                parseInstant(reply.headers.get(KvHeaders.TimeStamp)),
                0L,
                KvOperation.Put
              )
            )
          case _ => None

    private def storedToEntry(key: String, sm: StoredMessage): Option[KvEntry] =
      operation(sm.headers) match
        case KvOperation.Put =>
          Some(
            KvEntry(bucket, key, sm.data, sm.seq, sm.time, 0L, KvOperation.Put)
          )
        case _ => None

    private def buildEntry(key: String, m: NatsMessage): KvEntry =
      val reply = m.replyTo.getOrElse("")
      val (revision, created, delta) =
        if reply.startsWith("$JS.ACK") then
          JsAckParser.parse(reply) match
            case Right(meta) =>
              (meta.streamSeq, meta.timestamp, meta.numPending)
            case Left(_) => (0L, Instant.EPOCH, 0L)
        else (0L, Instant.EPOCH, 0L)
      KvEntry(
        bucket,
        key,
        m.payload,
        revision,
        created,
        delta,
        operation(m.headers)
      )

    private def operation(headers: Headers): KvOperation =
      headers.get(KvHeaders.Operation) match
        case Some(KvHeaders.OperationDelete) => KvOperation.Delete
        case Some(KvHeaders.OperationPurge)  => KvOperation.Purge
        case _                               => KvOperation.Put

    private def parseLong(s: Option[String]): Long =
      s.flatMap(_.toLongOption).getOrElse(0L)

    private def parseInstant(s: Option[String]): Instant =
      s.flatMap(v => scala.util.Try(Instant.parse(v)).toOption)
        .getOrElse(Instant.EPOCH)

    private def withValidKey[A](key: String)(f: => F[A]): F[A] =
      KvNames.validateKey(key) match
        case Right(_)  => f
        case Left(msg) => F.raiseError(NatsError.InvalidSubject(key, msg))

    private def validatePattern(pattern: String): F[Unit] =
      KvNames.validateKeyPattern(pattern) match
        case Right(_)  => F.unit
        case Left(msg) => F.raiseError(NatsError.InvalidSubject(pattern, msg))
