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

package fs2.nats.jetstream

import cats.effect.{Async, Deferred, Ref, Resource}
import cats.effect.std.{Semaphore, Supervisor}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.nats.client.NatsClient
import fs2.nats.errors.NatsError
import fs2.nats.jetstream.protocol.*
import fs2.nats.protocol.Headers
import fs2.nats.subscriptions.NatsMessage
import fs2.nats.util.Tokens
import com.github.plokhotnyuk.jsoniter_scala.core.*

import scala.concurrent.duration.*

/** Configuration for a JetStream context.
  *
  * @param apiPrefix
  *   The `$JS.API.` subject prefix (ignored when `domain` is set)
  * @param domain
  *   Optional JetStream domain; when set the prefix becomes `$JS.<domain>.API.`
  * @param timeout
  *   Default request timeout for API calls
  * @param publishAsyncMaxPending
  *   Maximum number of in-flight `publishAsync` requests (window size)
  */
final case class JetStreamConfig(
    apiPrefix: String = "$JS.API.",
    domain: Option[String] = None,
    timeout: FiniteDuration = 5.seconds,
    publishAsyncMaxPending: Int = 256
)
object JetStreamConfig:
  val default: JetStreamConfig = JetStreamConfig()

/** Options for a JetStream publish: de-duplication via `Nats-Msg-Id` and
  * optimistic-concurrency via the `Nats-Expected-*` headers.
  */
final case class PublishOptions(
    msgId: Option[String] = None,
    expectedStream: Option[String] = None,
    expectedLastSeq: Option[Long] = None,
    expectedLastSubjectSeq: Option[Long] = None,
    expectedLastMsgId: Option[String] = None,
    timeout: Option[FiniteDuration] = None
)
object PublishOptions:
  val default: PublishOptions = PublishOptions()

/** A JetStream context layered on a [[fs2.nats.client.NatsClient]]. Provides
  * persistent publish (with `PubAck`) and stream management. Consumer
  * management and consumption are added in later layers.
  */
trait JetStream[F[_]]:

  // ---- Publish ----

  /** Publish a message to a stream-bound subject and await the `PubAck`. */
  def publish(
      subject: String,
      payload: Chunk[Byte],
      headers: Headers = Headers.empty,
      opts: PublishOptions = PublishOptions.default
  ): F[PubAck]

  /** Pipelined publish: the outer effect completes once an in-flight window
    * slot is taken; the inner effect yields the `PubAck` (or fails).
    */
  def publishAsync(
      subject: String,
      payload: Chunk[Byte],
      headers: Headers = Headers.empty,
      opts: PublishOptions = PublishOptions.default
  ): F[F[PubAck]]

  // ---- Stream management ----

  def addStream(config: StreamConfig): F[StreamInfo]
  def updateStream(config: StreamConfig): F[StreamInfo]
  def streamInfo(name: String): F[StreamInfo]
  def deleteStream(name: String): F[Unit]
  def purgeStream(
      name: String,
      opts: PurgeOptions = PurgeOptions.all
  ): F[PurgeResponse]
  def streamNames: Stream[F, String]
  def listStreams: Stream[F, StreamInfo]
  def getMessage(stream: String, query: MessageGet): F[StoredMessage]
  def deleteMessage(stream: String, seq: Long, erase: Boolean = true): F[Unit]
  def accountInfo: F[AccountInfo]

  // ---- Consumer management ----

  def addConsumer(stream: String, config: ConsumerConfig): F[ConsumerInfo]
  def updateConsumer(stream: String, config: ConsumerConfig): F[ConsumerInfo]
  def consumerInfo(stream: String, consumer: String): F[ConsumerInfo]
  def deleteConsumer(stream: String, consumer: String): F[Unit]
  def consumerNames(stream: String): Stream[F, String]
  def listConsumers(stream: String): Stream[F, ConsumerInfo]

  // ---- Consumer handle factories ----

  /** Attach to an existing consumer (verifies it exists). */
  def consumer(stream: String, consumer: String): F[JsConsumer[F]]

  /** Create (or update) a consumer and return a handle to it. */
  def createConsumer(
      stream: String,
      config: ConsumerConfig
  ): F[JsConsumer[F]]

  // ---- Push consume ----

  /** Create a push consumer and stream its deliveries. A `deliverSubject` is
    * generated if not set in `config`; `deliverGroup` enables queue-group load
    * balancing. The `Resource` owns the delivery subscription and deletes
    * ephemeral (non-durable) consumers on release. Idle heartbeats are filtered
    * and flow-control requests are answered automatically.
    */
  def subscribePush(
      stream: String,
      config: ConsumerConfig
  ): Resource[F, Stream[F, JsMessage[F]]]

object JetStream:

  /** Build a JetStream context over a connected client. */
  def apply[F[_]: Async](
      client: NatsClient[F],
      config: JetStreamConfig
  ): Resource[F, JetStream[F]] =
    for
      window <- Resource.eval(
        Semaphore[F](math.max(1, config.publishAsyncMaxPending).toLong)
      )
      supervisor <- Supervisor[F]
    yield new JetStreamImpl[F](
      client,
      config,
      ApiSubjects(config.apiPrefix, config.domain),
      window,
      supervisor
    )

  private final class JetStreamImpl[F[_]: Async](
      client: NatsClient[F],
      config: JetStreamConfig,
      subjects: ApiSubjects,
      window: Semaphore[F],
      supervisor: Supervisor[F]
  ) extends JetStream[F]:

    private val F = Async[F]

    // ---- Publish ----

    override def publish(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        opts: PublishOptions
    ): F[PubAck] =
      val merged = mergePublishHeaders(headers, opts)
      val timeout = opts.timeout.getOrElse(config.timeout)
      client
        .request(subject, payload, merged, timeout)
        .adaptError { case NatsError.NoResponders(_) =>
          NatsError.JetStreamPublishNoAck(subject)
        }
        .flatMap(reply => decode[PubAck](reply.payload))

    override def publishAsync(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        opts: PublishOptions
    ): F[F[PubAck]] =
      for
        _ <- window.acquire
        slot <- Deferred[F, Either[Throwable, PubAck]]
        _ <- supervisor.supervise(
          publish(subject, payload, headers, opts).attempt
            .flatMap(slot.complete)
            .guarantee(window.release)
        )
      yield slot.get.rethrow

    private def mergePublishHeaders(
        headers: Headers,
        opts: PublishOptions
    ): Headers =
      List(
        opts.msgId.map(JsHeaders.MsgId -> _),
        opts.expectedStream.map(JsHeaders.ExpectedStream -> _),
        opts.expectedLastSeq.map(v => JsHeaders.ExpectedLastSeq -> v.toString),
        opts.expectedLastSubjectSeq.map(v =>
          JsHeaders.ExpectedLastSubjectSeq -> v.toString
        ),
        opts.expectedLastMsgId.map(JsHeaders.ExpectedLastMsgId -> _)
      ).flatten.foldLeft(headers) { case (h, (k, v)) => h.set(k, v) }

    // ---- Stream management ----

    override def addStream(streamConfig: StreamConfig): F[StreamInfo] =
      apiRequest[StreamInfo](
        subjects.streamCreate(streamConfig.name),
        jsonBody(streamConfig)
      )

    override def updateStream(streamConfig: StreamConfig): F[StreamInfo] =
      apiRequest[StreamInfo](
        subjects.streamUpdate(streamConfig.name),
        jsonBody(streamConfig)
      )

    override def streamInfo(name: String): F[StreamInfo] =
      apiRequest[StreamInfo](subjects.streamInfo(name), Chunk.empty)

    override def deleteStream(name: String): F[Unit] =
      apiRequest[SuccessResponse](
        subjects.streamDelete(name),
        Chunk.empty
      ).void

    override def purgeStream(
        name: String,
        opts: PurgeOptions
    ): F[PurgeResponse] =
      apiRequest[PurgeResponse](
        subjects.streamPurge(name),
        jsonBody(PurgeRequest(opts.filter, opts.seq, opts.keep))
      )

    override def streamNames: Stream[F, String] =
      paginate { offset =>
        apiRequest[StreamNamesResponse](
          subjects.streamNames,
          jsonBody(OffsetRequest(offset))
        ).map(r => (r.streams, r.total))
      }

    override def listStreams: Stream[F, StreamInfo] =
      paginate { offset =>
        apiRequest[StreamListResponse](
          subjects.streamList,
          jsonBody(OffsetRequest(offset))
        ).map(r => (r.streams, r.total))
      }

    override def getMessage(
        stream: String,
        query: MessageGet
    ): F[StoredMessage] =
      val body = query match
        case MessageGet.BySeq(seq) =>
          MsgGetRequest(seq = Some(seq))
        case MessageGet.LastBySubject(subject) =>
          MsgGetRequest(lastBySubj = Some(subject))
      client
        .request(
          subjects.msgGet(stream),
          jsonBody(body),
          Headers.empty,
          config.timeout
        )
        .flatMap(reply => decodeMessage(reply.payload))

    override def deleteMessage(
        stream: String,
        seq: Long,
        erase: Boolean
    ): F[Unit] =
      apiRequest[SuccessResponse](
        subjects.msgDelete(stream),
        jsonBody(DeleteMsgRequest(seq, noErase = !erase))
      ).void

    override def accountInfo: F[AccountInfo] =
      apiRequest[AccountInfo](subjects.accountInfo, Chunk.empty)

    // ---- Consumer management ----

    override def addConsumer(
        stream: String,
        consumerConfig: ConsumerConfig
    ): F[ConsumerInfo] =
      apiRequest[ConsumerInfo](
        createSubject(stream, consumerConfig),
        consumerCreateBody(stream, consumerConfig, "create")
      )

    override def updateConsumer(
        stream: String,
        consumerConfig: ConsumerConfig
    ): F[ConsumerInfo] =
      apiRequest[ConsumerInfo](
        createSubject(stream, consumerConfig),
        consumerCreateBody(stream, consumerConfig, "update")
      )

    override def consumerInfo(
        stream: String,
        consumer: String
    ): F[ConsumerInfo] =
      apiRequest[ConsumerInfo](
        subjects.consumerInfo(stream, consumer),
        Chunk.empty
      )

    override def deleteConsumer(stream: String, consumer: String): F[Unit] =
      apiRequest[SuccessResponse](
        subjects.consumerDelete(stream, consumer),
        Chunk.empty
      ).void

    override def consumerNames(stream: String): Stream[F, String] =
      paginate { offset =>
        apiRequest[ConsumerNamesResponse](
          subjects.consumerNames(stream),
          jsonBody(OffsetRequest(offset))
        ).map(r => (r.consumers, r.total))
      }

    override def listConsumers(stream: String): Stream[F, ConsumerInfo] =
      paginate { offset =>
        apiRequest[ConsumerListResponse](
          subjects.consumerList(stream),
          jsonBody(OffsetRequest(offset))
        ).map(r => (r.consumers, r.total))
      }

    override def consumer(
        stream: String,
        consumer: String
    ): F[JsConsumer[F]] =
      consumerInfo(stream, consumer).as(new JsConsumerHandle(stream, consumer))

    override def createConsumer(
        stream: String,
        consumerConfig: ConsumerConfig
    ): F[JsConsumer[F]] =
      apiRequest[ConsumerInfo](
        createSubject(stream, consumerConfig),
        consumerCreateBody(stream, consumerConfig, "")
      ).map(info => new JsConsumerHandle(info.stream, info.name))

    private def createSubject(
        stream: String,
        cfg: ConsumerConfig
    ): String =
      ConsumerCreate.subject(subjects, stream, cfg)

    private def consumerCreateBody(
        stream: String,
        cfg: ConsumerConfig,
        action: String
    ): Chunk[Byte] =
      jsonBody(ConsumerCreateRequest(stream, cfg, action))

    private def newInbox: F[String] =
      Tokens.randomInboxId[F]().map(id => s"_INBOX.$id")

    // ---- Push consume ----

    override def subscribePush(
        stream: String,
        pushConfig: ConsumerConfig
    ): Resource[F, Stream[F, JsMessage[F]]] =
      for
        deliverSubject <- Resource.eval(
          pushConfig.deliverSubject.fold(newInbox)(F.pure)
        )
        effective = pushConfig.copy(deliverSubject = Some(deliverSubject))
        // Create-or-update the consumer; clean up ephemeral ones on release.
        _ <- Resource.make(
          apiRequest[ConsumerInfo](
            createSubject(stream, effective),
            consumerCreateBody(stream, effective, "")
          )
        )(info =>
          if effective.durable.isEmpty then
            deleteConsumer(stream, info.name).attempt.void
          else F.unit
        )
        delivery <- client.subscribe(deliverSubject, effective.deliverGroup)
      yield delivery.evalMapFilter(handlePushMessage)

    private def handlePushMessage(m: NatsMessage): F[Option[JsMessage[F]]] =
      PushStatus.classify(m.status, m.replyTo, m.statusDescription) match
        case PushSignal.Data               => buildJsMessage(m).map(Some(_))
        case PushSignal.Heartbeat          => F.pure(None)
        case PushSignal.FlowControl(reply) =>
          // Echo an empty message back to pace the server. In stream order this
          // runs after the messages the flow-control request is pacing.
          client.publish(reply, Chunk.empty).as(None)
        case PushSignal.Fail(code, desc) =>
          F.raiseError(NatsError.JetStreamApiError(code, 0, desc))

    // ---- Shared message construction + ack ----

    // Shared placeholder for messages without a $JS.ACK reply (e.g. an
    // AckPolicy.None push consumer): metadata is unavailable and acks are
    // no-ops, so a single immutable instance is reused.
    private val EmptyMetadata: JsMetadata =
      JsMetadata("", "", 0L, 0L, 0L, 0L, java.time.Instant.EPOCH, None)

    private def buildJsMessage(m: NatsMessage): F[JsMessage[F]] =
      val reply = m.replyTo.getOrElse("")
      if reply.startsWith("$JS.ACK") then
        JsAckParser.parse(reply) match
          case Right(meta) => F.pure(new JsMessageImpl(m, meta))
          case Left(err)   =>
            F.raiseError(
              NatsError.ProtocolParseError(
                s"Invalid JetStream ACK subject: $err"
              )
            )
      else F.pure(new JsMessageImpl(m, EmptyMetadata))

    private final class JsMessageImpl(
        raw: NatsMessage,
        val metadata: JsMetadata
    ) extends JsMessage[F]:
      def subject: String = raw.subject
      def headers: Headers = raw.headers
      def payload: Chunk[Byte] = raw.payload
      def payloadAsString: String = raw.payloadAsString
      def ackReply: String = raw.replyTo.getOrElse("")

      private val canAck: Boolean = ackReply.startsWith("$JS.ACK")
      // Once-guard for finalizing acks; cheaper per message than a Ref.
      private val acked = new java.util.concurrent.atomic.AtomicBoolean(false)

      def ack: F[Unit] = finalizeAck(AckBytes.Ack)
      def nak: F[Unit] = finalizeAck(AckBytes.Nak)
      def nakWithDelay(delay: FiniteDuration): F[Unit] =
        finalizeAck(AckBytes.nakWithDelay(delay))
      def term: F[Unit] = finalizeAck(AckBytes.Term)
      def termWith(reason: String): F[Unit] =
        finalizeAck(AckBytes.termWith(reason))

      // Non-finalizing; repeatable.
      def inProgress: F[Unit] =
        if canAck then client.publish(ackReply, AckBytes.InProgress)
        else F.unit

      def ackSync: F[Unit] =
        if !canAck then F.unit
        else
          F.defer {
            if acked.compareAndSet(false, true) then
              client
                .request(ackReply, AckBytes.Ack, Headers.empty, config.timeout)
                .void
            else F.unit
          }

      private def finalizeAck(bytes: Chunk[Byte]): F[Unit] =
        if !canAck then F.unit
        else
          F.defer {
            if acked.compareAndSet(false, true) then
              client.publish(ackReply, bytes)
            else F.unit
          }

    private final class JsConsumerHandle(
        val stream: String,
        val name: String
    ) extends JsConsumer[F]:

      def info: F[ConsumerInfo] = consumerInfo(stream, name)

      private val nextSubject = subjects.msgNext(stream, name)

      def fetch(batch: Int, maxWait: FiniteDuration): F[Chunk[JsMessage[F]]] =
        collect(
          PullRequest(batch, maxWait, noWait = false),
          batch,
          maxWait + 1.second
        )

      def fetchNoWait(batch: Int): F[Chunk[JsMessage[F]]] =
        collect(
          PullRequest(batch, config.timeout, noWait = true),
          batch,
          config.timeout
        )

      private def collect(
          req: PullRequest,
          batch: Int,
          deadline: FiniteDuration
      ): F[Chunk[JsMessage[F]]] =
        newInbox.flatMap { inbox =>
          client.subscribe(inbox).use { inboxStream =>
            issuePull(inbox, req) *>
              inboxStream
                .evalMap(classifyToOption)
                .unNoneTerminate
                .collect { case Some(m) => m }
                .take(batch.toLong)
                .interruptAfter(deadline)
                .evalMap(buildJsMessage)
                .compile
                .toList
                .map(Chunk.from)
          }
        }

      def consume(
          opts: ConsumeOptions
      ): Resource[F, Stream[F, JsMessage[F]]] =
        for
          inbox <- Resource.eval(newInbox)
          inboxStream <- client.subscribe(inbox)
          received <- Resource.eval(Ref.of[F, Int](0))
          // The server requires idle_heartbeat < expires/2; clamp to 40% of
          // expires so a small `expires` can't produce an invalid request.
          heartbeatCap = (opts.expires.toNanos * 2 / 5).nanos
          req = PullRequest(
            opts.maxMessages,
            opts.expires,
            opts.maxBytes,
            noWait = false,
            Some(opts.idleHeartbeat.min(heartbeatCap))
          )
        yield
          val dataLoop = Stream.exec(issuePull(inbox, req)) ++
            inboxStream.evalMapFilter { m =>
              PullStatus.classify(m.status, m.statusDescription) match
                case PullSignal.Data =>
                  received
                    .modify(n =>
                      if n + 1 >= opts.maxMessages then (0, true)
                      else (n + 1, false)
                    )
                    .flatMap { rePull =>
                      (if rePull then issuePull(inbox, req) else F.unit) *>
                        buildJsMessage(m).map(Some(_))
                    }
                case PullSignal.Heartbeat => F.pure(None)
                case PullSignal.Complete  =>
                  received.set(0) *> issuePull(inbox, req).as(None)
                case PullSignal.Fail(code, desc) =>
                  F.raiseError(NatsError.JetStreamApiError(code, 0, desc))
            }
          // Liveness/reconnect safety net: re-issue a pull every `expires` so
          // the loop recovers even when an in-flight request is lost (e.g. on
          // reconnect) and no terminating status arrives to drive a re-pull.
          val keepAlive =
            Stream
              .awakeEvery[F](opts.expires)
              .evalMap(_ => issuePull(inbox, req))
              .drain
          dataLoop.concurrently(keepAlive)

      private def classifyToOption(
          m: NatsMessage
      ): F[Option[Option[NatsMessage]]] =
        PullStatus.classify(m.status, m.statusDescription) match
          case PullSignal.Data             => F.pure(Some(Some(m)))
          case PullSignal.Heartbeat        => F.pure(Some(None))
          case PullSignal.Complete         => F.pure(None)
          case PullSignal.Fail(code, desc) =>
            F.raiseError(NatsError.JetStreamApiError(code, 0, desc))

      private def issuePull(inbox: String, req: PullRequest): F[Unit] =
        client.publish(
          nextSubject,
          jsonBody(req),
          Headers.empty,
          Some(inbox)
        )

    // ---- Internal helpers ----

    private def jsonBody[A](a: A)(using JsonValueCodec[A]): Chunk[Byte] =
      Chunk.array(writeToArray(a))

    private def apiRequest[A: JsonValueCodec](
        subject: String,
        body: Chunk[Byte]
    ): F[A] =
      client
        .request(subject, body, Headers.empty, config.timeout)
        .flatMap(reply => decode[A](reply.payload))

    /** Decode an API response: raise [[NatsError.JetStreamApiError]] when the
      * payload carries an `error` envelope; otherwise decode `A`. The error
      * envelope is read in a first pass (jsoniter skips the success fields);
      * `A` is then read from the same bytes.
      */
    private def decode[A: JsonValueCodec](payload: Chunk[Byte]): F[A] =
      F.delay(payload.toArray)
        .flatMap { bytes =>
          F.delay(readFromArray[ApiErrorEnvelope](bytes).error).flatMap {
            case Some(err) =>
              F.raiseError[A](
                NatsError
                  .JetStreamApiError(err.code, err.errCode, err.description)
              )
            case None => F.delay(readFromArray[A](bytes))
          }
        }
        .adaptError { case e: JsonReaderException =>
          NatsError.ProtocolParseError(
            s"JetStream response decode failed: ${e.getMessage}"
          )
        }

    /** Like [[decode]] but the success value lives under `message`. */
    private def decodeMessage(payload: Chunk[Byte]): F[StoredMessage] =
      F.delay(payload.toArray)
        .flatMap { bytes =>
          F.delay(readFromArray[ApiErrorEnvelope](bytes).error).flatMap {
            case Some(err) =>
              F.raiseError[StoredMessage](
                NatsError
                  .JetStreamApiError(err.code, err.errCode, err.description)
              )
            case None => F.delay(readFromArray[GetMsgResponse](bytes).message)
          }
        }
        .adaptError { case e: JsonReaderException =>
          NatsError.ProtocolParseError(
            s"JetStream response decode failed: ${e.getMessage}"
          )
        }

    /** Lazily page through an offset-paginated API list. */
    private def paginate[A](
        page: Int => F[(List[A], Int)]
    ): Stream[F, A] =
      Stream.unfoldChunkEval(Option(0)) {
        case None         => F.pure(None)
        case Some(offset) =>
          page(offset).map { case (items, total) =>
            val nextOffset = offset + items.length
            val cont =
              if items.nonEmpty && nextOffset < total then Some(nextOffset)
              else None
            Some((Chunk.from(items), cont))
          }
      }
