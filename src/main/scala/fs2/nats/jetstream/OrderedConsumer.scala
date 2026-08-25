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

import fs2.nats.jetstream.protocol.DeliverPolicy

import java.time.Instant
import scala.concurrent.duration.*

/** Options for an ordered push consumer (see [[JetStream.subscribeOrdered]]).
  *
  * An ordered consumer is an ephemeral, no-ack, flow-controlled push consumer
  * over a single filter subject. The client tracks the per-consumer delivery
  * sequence and, on a detected gap (a missed delivery) or a server-side
  * invalidation (the consumer was deleted/expired, e.g. after a reconnect),
  * transparently deletes and recreates the consumer starting just after the
  * last in-order stream sequence — so the delivered stream is gap-free and
  * in-order even across reconnects.
  *
  * @param deliverPolicy
  *   where the first (and any from-scratch) delivery cycle starts; `All` reads
  *   the whole stream in order (the common case, e.g. Object Store chunk reads)
  * @param optStartSeq
  *   honored when `deliverPolicy = ByStartSequence`
  * @param optStartTime
  *   honored when `deliverPolicy = ByStartTime`
  * @param headersOnly
  *   deliver message headers without payloads
  * @param idleHeartbeat
  *   server idle-heartbeat interval (required for a flow-control consumer)
  * @param inactiveThreshold
  *   server-side cleanup delay for the ephemeral consumer after the last
  *   delivery/interest
  */
final case class OrderedConsumerOptions(
    deliverPolicy: DeliverPolicy = DeliverPolicy.All,
    optStartSeq: Option[Long] = None,
    optStartTime: Option[Instant] = None,
    headersOnly: Boolean = false,
    idleHeartbeat: FiniteDuration = 5.seconds,
    inactiveThreshold: FiniteDuration = 5.minutes
)
object OrderedConsumerOptions:
  val default: OrderedConsumerOptions = OrderedConsumerOptions()

/** Mutable tracking state for an ordered consumer's delivery loop.
  *
  * The consumer name lives here with the sequence counters rather than in a
  * second `Ref`: the delivery path needs all of them together, so two `Ref`s
  * cost two effect nodes per message just to read, and a recreate that
  * publishes the new name and the reset counters as two separate stores leaves
  * a window in which a reader pairs a name from this cycle with counters from
  * the last one.
  *
  * @param expectedConsumerSeq
  *   the next per-consumer delivery sequence expected from the *current*
  *   consumer cycle (resets to 1 on each recreate)
  * @param lastStreamSeq
  *   the highest in-order stream sequence emitted so far (0 = none yet); a
  *   recreate resumes from `lastStreamSeq + 1`
  * @param cycle
  *   increments on each recreate (diagnostic)
  * @param consumer
  *   name of the consumer delivering the current cycle (`""` until the first
  *   create completes); a delivery naming any other consumer belongs to a
  *   superseded cycle and is dropped
  */
private[jetstream] final case class OrderedState(
    expectedConsumerSeq: Long,
    lastStreamSeq: Long,
    cycle: Long,
    consumer: String
)

private[jetstream] object OrderedConsumer:

  /** What to do with a delivered data message, given the expected next
    * per-consumer sequence and the highest in-order stream sequence so far.
    *
    *   - `consumerSeq == expected` → in order: [[Decision.Emit]].
    *   - `consumerSeq >  expected` → a delivery was missed (gap):
    *     [[Decision.Recreate]] from `lastStreamSeq + 1`.
    *   - `consumerSeq <  expected` → a duplicate/older delivery from the
    *     current cycle: [[Decision.DropStale]].
    *
    * Deliveries from a *previous* cycle (after a recreate) are filtered earlier
    * by comparing the delivering consumer name, so they never reach `decide`.
    */
  def decide(
      expectedConsumerSeq: Long,
      lastStreamSeq: Long,
      msgConsumerSeq: Long
  ): Decision =
    if msgConsumerSeq == expectedConsumerSeq then Decision.Emit
    else if msgConsumerSeq > expectedConsumerSeq then
      Decision.Recreate(lastStreamSeq + 1)
    else Decision.DropStale

  /** The whole per-message state transition, as one pure function, so the
    * delivery path can run it inside a single `Ref.modify` -- and so it can be
    * unit-tested, which the `Ref`-and-closure version it replaces could not be.
    *
    * A delivery naming a consumer other than the current cycle's was queued
    * before a recreate and is dropped here; everything else is [[decide]].
    */
  def step(
      st: OrderedState,
      msgConsumer: String,
      msgConsumerSeq: Long,
      msgStreamSeq: Long
  ): (OrderedState, Step) =
    if msgConsumer != st.consumer then (st, Step.Drop)
    else
      decide(st.expectedConsumerSeq, st.lastStreamSeq, msgConsumerSeq) match
        case Decision.Emit =>
          (
            st.copy(
              expectedConsumerSeq = st.expectedConsumerSeq + 1,
              lastStreamSeq = msgStreamSeq
            ),
            Step.Deliver
          )
        case Decision.Recreate(_) => (st, Step.Recreate)
        case Decision.DropStale   => (st, Step.Drop)

  enum Decision:
    case Emit
    case Recreate(fromStreamSeq: Long)
    case DropStale

  /** Outcome of [[step]]. All three cases are parameterless, hence singletons,
    * so carrying the decision out of the `Ref.modify` allocates nothing.
    */
  enum Step:
    case Deliver
    case Drop
    case Recreate
