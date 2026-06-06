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
  * @param expectedConsumerSeq
  *   the next per-consumer delivery sequence expected from the *current*
  *   consumer cycle (resets to 1 on each recreate)
  * @param lastStreamSeq
  *   the highest in-order stream sequence emitted so far (0 = none yet); a
  *   recreate resumes from `lastStreamSeq + 1`
  * @param cycle
  *   increments on each recreate (diagnostic)
  */
private[jetstream] final case class OrderedState(
    expectedConsumerSeq: Long,
    lastStreamSeq: Long,
    cycle: Long
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

  enum Decision:
    case Emit
    case Recreate(fromStreamSeq: Long)
    case DropStale
