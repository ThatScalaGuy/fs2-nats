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

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

import java.time.Instant
import scala.concurrent.duration.*

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
  // Hand-written: `filter_subject`/`filter_subjects` are mutually exclusive,
  // the `-1` sentinels / `false` flags / empty lists are omitted, and durations
  // use the `0 == None` convention on decode — none macro-expressible.
  given JsonValueCodec[ConsumerConfig] = new JsonValueCodec[ConsumerConfig]:
    private val deliverC = summon[JsonValueCodec[DeliverPolicy]]
    private val ackC = summon[JsonValueCodec[AckPolicy]]
    private val replayC = summon[JsonValueCodec[ReplayPolicy]]

    def nullValue: ConsumerConfig = null

    def encodeValue(c: ConsumerConfig, out: JsonWriter): Unit =
      // filter_subject and filter_subjects are mutually exclusive; prefer the
      // explicit list when present.
      val singleFilter =
        if c.filterSubjects.isEmpty then c.filterSubject else None
      out.writeObjectStart()
      c.durable.foreach { v => out.writeKey("durable_name"); out.writeVal(v) }
      c.name.foreach { v => out.writeKey("name"); out.writeVal(v) }
      c.description.foreach { v =>
        out.writeKey("description"); out.writeVal(v)
      }
      out.writeKey("deliver_policy"); deliverC.encodeValue(c.deliverPolicy, out)
      c.optStartSeq.foreach { v =>
        out.writeKey("opt_start_seq"); out.writeVal(v)
      }
      c.optStartTime.foreach { v =>
        out.writeKey("opt_start_time"); out.writeVal(v)
      }
      out.writeKey("ack_policy"); ackC.encodeValue(c.ackPolicy, out)
      c.ackWait.foreach { d =>
        out.writeKey("ack_wait"); out.writeVal(d.toNanos)
      }
      if c.maxDeliver != -1 then
        out.writeKey("max_deliver"); out.writeVal(c.maxDeliver)
      if c.backoff.nonEmpty then
        out.writeKey("backoff")
        out.writeArrayStart()
        c.backoff.foreach(d => out.writeVal(d.toNanos))
        out.writeArrayEnd()
      singleFilter.foreach { v =>
        out.writeKey("filter_subject"); out.writeVal(v)
      }
      if c.filterSubjects.nonEmpty then
        out.writeKey("filter_subjects")
        out.writeArrayStart()
        c.filterSubjects.foreach(out.writeVal)
        out.writeArrayEnd()
      out.writeKey("replay_policy"); replayC.encodeValue(c.replayPolicy, out)
      if c.headersOnly then
        out.writeKey("headers_only")
        out.writeVal(true)
      if c.maxAckPending != -1 then
        out.writeKey("max_ack_pending"); out.writeVal(c.maxAckPending)
      if c.maxWaiting != -1 then
        out.writeKey("max_waiting"); out.writeVal(c.maxWaiting)
      c.inactiveThreshold.foreach { d =>
        out.writeKey("inactive_threshold"); out.writeVal(d.toNanos)
      }
      c.numReplicas.foreach { v =>
        out.writeKey("num_replicas"); out.writeVal(v)
      }
      c.deliverSubject.foreach { v =>
        out.writeKey("deliver_subject"); out.writeVal(v)
      }
      c.deliverGroup.foreach { v =>
        out.writeKey("deliver_group"); out.writeVal(v)
      }
      if c.flowControl then
        out.writeKey("flow_control")
        out.writeVal(true)
      c.idleHeartbeat.foreach { d =>
        out.writeKey("idle_heartbeat"); out.writeVal(d.toNanos)
      }
      out.writeObjectEnd()

    def decodeValue(in: JsonReader, default: ConsumerConfig): ConsumerConfig =
      if in.isNextToken('{') then
        var durable: Option[String] = None
        var name: Option[String] = None
        var description: Option[String] = None
        var deliverPolicy = DeliverPolicy.All
        var optStartSeq: Option[Long] = None
        var optStartTime: Option[Instant] = None
        var ackPolicy = AckPolicy.Explicit
        var ackWait: Option[FiniteDuration] = None
        var maxDeliver = -1
        var backoff: List[FiniteDuration] = Nil
        var filterSubject: Option[String] = None
        var filterSubjects: List[String] = Nil
        var replayPolicy = ReplayPolicy.Instant
        var headersOnly = false
        var maxAckPending = -1
        var maxWaiting = -1
        var inactiveThreshold: Option[FiniteDuration] = None
        var numReplicas: Option[Int] = None
        var deliverSubject: Option[String] = None
        var deliverGroup: Option[String] = None
        var flowControl = false
        var idleHeartbeat: Option[FiniteDuration] = None
        if !in.isNextToken('}') then
          in.rollbackToken()
          var cont = true
          while cont do
            val l = in.readKeyAsCharBuf()
            if in.isCharBufEqualsTo(l, "durable_name") then
              durable = Some(in.readString(null))
            else if in.isCharBufEqualsTo(l, "name") then
              name = Some(in.readString(null))
            else if in.isCharBufEqualsTo(l, "description") then
              description = Some(in.readString(null))
            else if in.isCharBufEqualsTo(l, "deliver_policy") then
              deliverPolicy = deliverC.decodeValue(in, deliverPolicy)
            else if in.isCharBufEqualsTo(l, "opt_start_seq") then
              optStartSeq = Some(in.readLong())
            else if in.isCharBufEqualsTo(l, "opt_start_time") then
              optStartTime = Some(in.readInstant(null))
            else if in.isCharBufEqualsTo(l, "ack_policy") then
              ackPolicy = ackC.decodeValue(in, ackPolicy)
            else if in.isCharBufEqualsTo(l, "ack_wait") then
              ackWait = JsRead.optDurationNanos(in)
            else if in.isCharBufEqualsTo(l, "max_deliver") then
              maxDeliver = in.readInt()
            else if in.isCharBufEqualsTo(l, "backoff") then
              backoff = JsRead.durationListNanos(in)
            else if in.isCharBufEqualsTo(l, "filter_subject") then
              filterSubject = Some(in.readString(null))
            else if in.isCharBufEqualsTo(l, "filter_subjects") then
              filterSubjects = JsRead.stringList(in)
            else if in.isCharBufEqualsTo(l, "replay_policy") then
              replayPolicy = replayC.decodeValue(in, replayPolicy)
            else if in.isCharBufEqualsTo(l, "headers_only") then
              headersOnly = in.readBoolean()
            else if in.isCharBufEqualsTo(l, "max_ack_pending") then
              maxAckPending = in.readInt()
            else if in.isCharBufEqualsTo(l, "max_waiting") then
              maxWaiting = in.readInt()
            else if in.isCharBufEqualsTo(l, "inactive_threshold") then
              inactiveThreshold = JsRead.optDurationNanos(in)
            else if in.isCharBufEqualsTo(l, "num_replicas") then
              numReplicas = Some(in.readInt())
            else if in.isCharBufEqualsTo(l, "deliver_subject") then
              deliverSubject = Some(in.readString(null))
            else if in.isCharBufEqualsTo(l, "deliver_group") then
              deliverGroup = Some(in.readString(null))
            else if in.isCharBufEqualsTo(l, "flow_control") then
              flowControl = in.readBoolean()
            else if in.isCharBufEqualsTo(l, "idle_heartbeat") then
              idleHeartbeat = JsRead.optDurationNanos(in)
            else in.skip()
            cont = in.isNextToken(',')
          if !in.isCurrentToken('}') then in.objectEndOrCommaError()
        ConsumerConfig(
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
      else in.readNullOrTokenError(default, '{')

/** A consumer/stream sequence pair. */
final case class SequencePair(consumerSeq: Long = 0L, streamSeq: Long = 0L)
object SequencePair:
  given JsonValueCodec[SequencePair] = JsonCodecMaker.make(JsWire.snake)

/** Consumer information returned by create/update/info. */
final case class ConsumerInfo(
    @named("stream_name") stream: String,
    name: String,
    config: ConsumerConfig,
    delivered: SequencePair,
    ackFloor: SequencePair,
    numPending: Long = 0L,
    numAckPending: Long = 0L,
    numRedelivered: Long = 0L,
    numWaiting: Int = 0,
    created: Instant
)
object ConsumerInfo:
  given JsonValueCodec[ConsumerInfo] = JsonCodecMaker.make(JsWire.snake)

/** Page of consumer names (paginated `CONSUMER.NAMES`). */
private[jetstream] final case class ConsumerNamesResponse(
    consumers: List[String] = Nil,
    total: Int = 0,
    offset: Int = 0
)
private[jetstream] object ConsumerNamesResponse:
  given JsonValueCodec[ConsumerNamesResponse] =
    JsonCodecMaker.make(JsWire.snake)

/** Page of full consumer info (paginated `CONSUMER.LIST`). */
private[jetstream] final case class ConsumerListResponse(
    consumers: List[ConsumerInfo] = Nil,
    total: Int = 0,
    offset: Int = 0
)
private[jetstream] object ConsumerListResponse:
  given JsonValueCodec[ConsumerListResponse] = JsonCodecMaker.make(JsWire.snake)
