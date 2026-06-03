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

import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Consumer configuration. Push-only fields (`deliverSubject`/`deliverGroup`/
  * `flowControl`/`idleHeartbeat`) take effect only when `deliverSubject` is
  * set; otherwise the consumer is pull-based.
  */
final case class ConsumerConfig(
    durable: Option[String] = None,
    name: Option[String] = None,
    description: Option[String] = None,
    deliverPolicy: DeliverPolicy = DeliverPolicy.All,
    optStartSeq: Option[Long] = None,
    optStartTime: Option[Instant] = None,
    ackPolicy: AckPolicy = AckPolicy.Explicit,
    ackWait: Option[FiniteDuration] = None,
    maxDeliver: Int = -1,
    backoff: List[FiniteDuration] = Nil,
    filterSubject: Option[String] = None,
    filterSubjects: List[String] = Nil,
    replayPolicy: ReplayPolicy = ReplayPolicy.Instant,
    headersOnly: Boolean = false,
    maxAckPending: Int = -1,
    maxWaiting: Int = -1,
    inactiveThreshold: Option[FiniteDuration] = None,
    numReplicas: Option[Int] = None,
    deliverSubject: Option[String] = None,
    deliverGroup: Option[String] = None,
    flowControl: Boolean = false,
    idleHeartbeat: Option[FiniteDuration] = None
)

object ConsumerConfig:
  given Encoder[ConsumerConfig] = Encoder.instance { c =>
    // filter_subject and filter_subjects are mutually exclusive; prefer the
    // explicit list when present.
    val singleFilter =
      if c.filterSubjects.isEmpty then c.filterSubject else None
    JsWire.obj(
      c.durable.map("durable_name" -> Json.fromString(_)),
      c.name.map("name" -> Json.fromString(_)),
      c.description.map("description" -> Json.fromString(_)),
      Some("deliver_policy" -> c.deliverPolicy.asJson),
      c.optStartSeq.map(s => "opt_start_seq" -> Json.fromLong(s)),
      c.optStartTime.map(t => "opt_start_time" -> JsWire.instantToJson(t)),
      Some("ack_policy" -> c.ackPolicy.asJson),
      c.ackWait.map(d => "ack_wait" -> JsWire.durationToNanos(d)),
      Option.when(c.maxDeliver != -1)(
        "max_deliver" -> Json.fromInt(c.maxDeliver)
      ),
      Option.when(c.backoff.nonEmpty)(
        "backoff" -> Json.fromValues(c.backoff.map(JsWire.durationToNanos))
      ),
      singleFilter.map("filter_subject" -> Json.fromString(_)),
      Option.when(c.filterSubjects.nonEmpty)(
        "filter_subjects" -> Json.fromValues(
          c.filterSubjects.map(Json.fromString)
        )
      ),
      Some("replay_policy" -> c.replayPolicy.asJson),
      Option.when(c.headersOnly)("headers_only" -> Json.fromBoolean(true)),
      Option.when(c.maxAckPending != -1)(
        "max_ack_pending" -> Json.fromInt(c.maxAckPending)
      ),
      Option.when(c.maxWaiting != -1)(
        "max_waiting" -> Json.fromInt(c.maxWaiting)
      ),
      c.inactiveThreshold.map(d =>
        "inactive_threshold" -> JsWire.durationToNanos(d)
      ),
      c.numReplicas.map(n => "num_replicas" -> Json.fromInt(n)),
      c.deliverSubject.map("deliver_subject" -> Json.fromString(_)),
      c.deliverGroup.map("deliver_group" -> Json.fromString(_)),
      Option.when(c.flowControl)("flow_control" -> Json.fromBoolean(true)),
      c.idleHeartbeat.map(d => "idle_heartbeat" -> JsWire.durationToNanos(d))
    )
  }

  given Decoder[ConsumerConfig] =
    import JsWire.given
    Decoder.instance { c =>
      for
        durable <- c.downField("durable_name").as[Option[String]]
        name <- c.downField("name").as[Option[String]]
        description <- c.downField("description").as[Option[String]]
        deliverPolicy <- c
          .downField("deliver_policy")
          .as[Option[DeliverPolicy]]
          .map(_.getOrElse(DeliverPolicy.All))
        optStartSeq <- c.downField("opt_start_seq").as[Option[Long]]
        optStartTime <- c.downField("opt_start_time").as[Option[Instant]]
        ackPolicy <- c
          .downField("ack_policy")
          .as[Option[AckPolicy]]
          .map(_.getOrElse(AckPolicy.Explicit))
        ackWait <- JsWire.optDurationNanos(c.downField("ack_wait"))
        maxDeliver <- c
          .downField("max_deliver")
          .as[Option[Int]]
          .map(_.getOrElse(-1))
        backoff <- c
          .downField("backoff")
          .as[Option[List[Long]]]
          .map(_.getOrElse(Nil).map(JsWire.nanosToDuration))
        filterSubject <- c.downField("filter_subject").as[Option[String]]
        filterSubjects <- c
          .downField("filter_subjects")
          .as[Option[List[String]]]
          .map(_.getOrElse(Nil))
        replayPolicy <- c
          .downField("replay_policy")
          .as[Option[ReplayPolicy]]
          .map(_.getOrElse(ReplayPolicy.Instant))
        headersOnly <- c
          .downField("headers_only")
          .as[Option[Boolean]]
          .map(_.getOrElse(false))
        maxAckPending <- c
          .downField("max_ack_pending")
          .as[Option[Int]]
          .map(_.getOrElse(-1))
        maxWaiting <- c
          .downField("max_waiting")
          .as[Option[Int]]
          .map(_.getOrElse(-1))
        inactiveThreshold <- JsWire.optDurationNanos(
          c.downField("inactive_threshold")
        )
        numReplicas <- c.downField("num_replicas").as[Option[Int]]
        deliverSubject <- c.downField("deliver_subject").as[Option[String]]
        deliverGroup <- c.downField("deliver_group").as[Option[String]]
        flowControl <- c
          .downField("flow_control")
          .as[Option[Boolean]]
          .map(_.getOrElse(false))
        idleHeartbeat <- JsWire.optDurationNanos(c.downField("idle_heartbeat"))
      yield ConsumerConfig(
        durable,
        name,
        description,
        deliverPolicy,
        optStartSeq,
        optStartTime,
        ackPolicy,
        ackWait,
        maxDeliver,
        backoff,
        filterSubject,
        filterSubjects,
        replayPolicy,
        headersOnly,
        maxAckPending,
        maxWaiting,
        inactiveThreshold,
        numReplicas,
        deliverSubject,
        deliverGroup,
        flowControl,
        idleHeartbeat
      )
    }

/** A consumer/stream sequence pair. */
final case class SequencePair(consumerSeq: Long, streamSeq: Long)
object SequencePair:
  given Decoder[SequencePair] = Decoder.instance { c =>
    for
      consumerSeq <- c
        .downField("consumer_seq")
        .as[Option[Long]]
        .map(_.getOrElse(0L))
      streamSeq <- c
        .downField("stream_seq")
        .as[Option[Long]]
        .map(_.getOrElse(0L))
    yield SequencePair(consumerSeq, streamSeq)
  }

/** Consumer information returned by create/update/info. */
final case class ConsumerInfo(
    stream: String,
    name: String,
    config: ConsumerConfig,
    delivered: SequencePair,
    ackFloor: SequencePair,
    numPending: Long,
    numAckPending: Long,
    numRedelivered: Long,
    numWaiting: Int,
    created: Instant
)
object ConsumerInfo:
  given Decoder[ConsumerInfo] =
    import JsWire.given
    Decoder.instance { c =>
      for
        stream <- c.downField("stream_name").as[String]
        name <- c.downField("name").as[String]
        config <- c.downField("config").as[ConsumerConfig]
        delivered <- c.downField("delivered").as[SequencePair]
        ackFloor <- c.downField("ack_floor").as[SequencePair]
        numPending <- c
          .downField("num_pending")
          .as[Option[Long]]
          .map(_.getOrElse(0L))
        numAckPending <- c
          .downField("num_ack_pending")
          .as[Option[Long]]
          .map(_.getOrElse(0L))
        numRedelivered <- c
          .downField("num_redelivered")
          .as[Option[Long]]
          .map(_.getOrElse(0L))
        numWaiting <- c
          .downField("num_waiting")
          .as[Option[Int]]
          .map(_.getOrElse(0))
        created <- c.downField("created").as[Instant]
      yield ConsumerInfo(
        stream,
        name,
        config,
        delivered,
        ackFloor,
        numPending,
        numAckPending,
        numRedelivered,
        numWaiting,
        created
      )
    }

/** Page of consumer names (paginated `CONSUMER.NAMES`). */
private[jetstream] final case class ConsumerNamesResponse(
    consumers: List[String],
    total: Int,
    offset: Int
)
private[jetstream] object ConsumerNamesResponse:
  given Decoder[ConsumerNamesResponse] = Decoder.instance { c =>
    for
      consumers <- c
        .downField("consumers")
        .as[Option[List[String]]]
        .map(_.getOrElse(Nil))
      total <- c.downField("total").as[Option[Int]].map(_.getOrElse(0))
      offset <- c.downField("offset").as[Option[Int]].map(_.getOrElse(0))
    yield ConsumerNamesResponse(consumers, total, offset)
  }

/** Page of full consumer info (paginated `CONSUMER.LIST`). */
private[jetstream] final case class ConsumerListResponse(
    consumers: List[ConsumerInfo],
    total: Int,
    offset: Int
)
private[jetstream] object ConsumerListResponse:
  given Decoder[ConsumerListResponse] = Decoder.instance { c =>
    for
      consumers <- c
        .downField("consumers")
        .as[Option[List[ConsumerInfo]]]
        .map(_.getOrElse(Nil))
      total <- c.downField("total").as[Option[Int]].map(_.getOrElse(0))
      offset <- c.downField("offset").as[Option[Int]].map(_.getOrElse(0))
    yield ConsumerListResponse(consumers, total, offset)
  }
