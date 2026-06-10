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

package fs2.nats.subscriptions

import cats.effect.{Async, Ref}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.nats.client.{ClientEvent, SlowConsumerPolicy}
import fs2.nats.protocol.Headers

/** Handle for managing a subscription. Provides methods to unsubscribe and
  * configure message delivery.
  *
  * @tparam F
  *   The effect type
  */
trait SubscriptionHandle[F[_]]:

  /** Unsubscribe from this subscription. No more messages will be delivered
    * after unsubscribe completes.
    *
    * @return
    *   Effect that completes when unsubscribed
    */
  def unsubscribe: F[Unit]

  /** Unsubscribe after receiving a maximum number of messages. Useful for
    * request/reply patterns.
    *
    * @param maxMsgs
    *   Maximum messages to receive before auto-unsubscribing
    * @return
    *   Effect that completes when the UNSUB is sent
    */
  def unsubscribeAfter(maxMsgs: Int): F[Unit]

  /** The subscription ID.
    */
  def sid: Long

  /** The subject this subscription is for.
    */
  def subject: String

  /** The queue group, if any.
    */
  def queueGroup: Option[String]

/** Internal subscription state.
  */
private[subscriptions] final case class SubscriptionState(
    sid: Long,
    subject: String,
    queueGroup: Option[String],
    queue: Queue[cats.effect.IO, NatsMessage],
    active: Boolean,
    remainingMessages: Option[Int]
)

/** Manages active subscriptions and message routing.
  *
  * Responsible for:
  *   - Creating bounded per-subscription message queues
  *   - Routing incoming MSG/HMSG frames to appropriate subscriptions
  *   - Handling UNSUB and auto-unsubscribe by count
  *   - Applying SlowConsumerPolicy when queues are full
  *
  * @tparam F
  *   The effect type
  */
trait SubscriptionManager[F[_]]:

  /** Register a new subscription.
    *
    * @param sid
    *   The subscription ID
    * @param subject
    *   The subject to subscribe to
    * @param queueGroup
    *   Optional queue group name
    * @return
    *   A tuple of (message stream, subscription handle)
    */
  def register(
      sid: Long,
      subject: String,
      queueGroup: Option[String] = None
  ): F[(Stream[F, NatsMessage], SubscriptionHandle[F])]

  /** Route an already-built message (constructed by the parser on the data
    * path) to the appropriate subscription.
    *
    * @param msg
    *   The message to deliver
    * @return
    *   Effect that completes when the message is routed or policy applied
    */
  def routeMessage(msg: NatsMessage): F[Option[ClientEvent.SlowConsumer]]

  /** Get all active subscriptions for reconnection.
    *
    * @return
    *   List of (sid, subject, queueGroup) for all active subscriptions
    */
  def activeSubscriptions: F[List[(Long, String, Option[String])]]

  /** Mark a subscription as unsubscribed (internal use).
    *
    * @param sid
    *   The subscription ID to remove
    * @return
    *   Effect that completes when the subscription is removed
    */
  def remove(sid: Long): F[Unit]

  /** Close all subscriptions.
    *
    * @return
    *   Effect that completes when all subscriptions are closed
    */
  def closeAll: F[Unit]

object SubscriptionManager:

  /** Create a new SubscriptionManager.
    *
    * @param queueCapacity
    *   Default capacity for subscription message queues
    * @param policy
    *   Policy for handling slow consumers
    * @param sidAllocator
    *   The SID allocator to use
    * @param onUnsubscribe
    *   Callback to send UNSUB command to server
    * @tparam F
    *   The effect type
    * @return
    *   A new SubscriptionManager instance
    */
  def apply[F[_]: Async](
      queueCapacity: Int,
      policy: SlowConsumerPolicy,
      sidAllocator: SidAllocator[F],
      onUnsubscribe: (Long, Option[Int]) => F[Unit]
  ): F[SubscriptionManager[F]] =
    for subsRef <- Ref.of[F, Map[Long, InternalSubscription[F]]](Map.empty)
    yield new SubscriptionManagerImpl[F](
      subsRef,
      queueCapacity,
      policy,
      sidAllocator,
      onUnsubscribe
    )

  /** Per-subscription hot state, read once per delivered message. `remaining` <
    * 0 means unlimited (no auto-unsubscribe count); a non-negative value counts
    * down toward auto-unsubscribe.
    */
  private final class SubState(val active: Boolean, val remaining: Int)

  /** Sentinel enqueued to terminate a subscription stream. Compared by
    * reference identity (`ne`), so delivered messages need not be boxed in an
    * Option just to carry an out-of-band end-of-stream signal.
    */
  private val PoisonPill: NatsMessage =
    NatsMessage("", None, Headers.empty, Chunk.empty, -1L)

  private class InternalSubscription[F[_]](
      val sid: Long,
      val subject: String,
      val queueGroup: Option[String],
      val queue: Queue[F, NatsMessage],
      val stateRef: Ref[F, SubState]
  )

  private class SubscriptionManagerImpl[F[_]: Async](
      subsRef: Ref[F, Map[Long, InternalSubscription[F]]],
      queueCapacity: Int,
      policy: SlowConsumerPolicy,
      sidAllocator: SidAllocator[F],
      onUnsubscribe: (Long, Option[Int]) => F[Unit]
  ) extends SubscriptionManager[F]:

    override def register(
        sid: Long,
        subject: String,
        queueGroup: Option[String]
    ): F[(Stream[F, NatsMessage], SubscriptionHandle[F])] =
      for
        queue <- Queue.bounded[F, NatsMessage](queueCapacity)
        stateRef <- Ref.of[F, SubState](new SubState(true, -1))
        sub = new InternalSubscription[F](
          sid,
          subject,
          queueGroup,
          queue,
          stateRef
        )
        _ <- subsRef.update(_ + (sid -> sub))
      yield
        val stream =
          Stream.fromQueueUnterminated(queue).takeWhile(_ ne PoisonPill)
        val handle = new SubscriptionHandleImpl[F](
          sid,
          subject,
          queueGroup,
          stateRef,
          onUnsubscribe,
          this
        )
        (stream, handle)

    override def routeMessage(
        msg: NatsMessage
    ): F[Option[ClientEvent.SlowConsumer]] =
      deliverMessage(msg.sid, msg)

    private def deliverMessage(
        sid: Long,
        msg: NatsMessage
    ): F[Option[ClientEvent.SlowConsumer]] =
      subsRef.get.flatMap { subs =>
        subs.get(sid) match
          case None =>
            Async[F].pure(None)

          case Some(sub) =>
            sub.stateRef.get.flatMap { state =>
              if !state.active then Async[F].pure(None)
              else if state.remaining < 0 then
                // Unlimited subscription (the common case): deliver with a single
                // state read, no compare-and-set.
                tryEnqueue(sub, msg)
              else
                sub.stateRef
                  .modify { cur =>
                    if !cur.active then (cur, false)
                    else if cur.remaining > 0 then
                      (new SubState(true, cur.remaining - 1), true)
                    else (new SubState(false, 0), false)
                  }
                  .flatMap { shouldDeliver =>
                    if !shouldDeliver then
                      sub.stateRef.set(new SubState(false, 0)) *>
                        sub.queue.offer(PoisonPill) *>
                        sidAllocator.release(sid) *>
                        subsRef.update(_ - sid) *>
                        Async[F].pure(None)
                    else tryEnqueue(sub, msg)
                  }
            }
      }

    private def tryEnqueue(
        sub: InternalSubscription[F],
        msg: NatsMessage
    ): F[Option[ClientEvent.SlowConsumer]] =
      policy match
        case SlowConsumerPolicy.Block =>
          sub.queue.offer(msg).as(None)

        case SlowConsumerPolicy.DropNew =>
          sub.queue.tryOffer(msg).flatMap {
            case true  => Async[F].pure(None)
            case false =>
              Async[F].pure(
                Some(ClientEvent.SlowConsumer(sub.sid, sub.subject, 1))
              )
          }

        case SlowConsumerPolicy.DropOldest =>
          sub.queue.tryOffer(msg).flatMap {
            case true  => Async[F].pure(None)
            case false =>
              sub.queue.tryTake *>
                sub.queue.tryOffer(msg) *>
                Async[F].pure(
                  Some(ClientEvent.SlowConsumer(sub.sid, sub.subject, 1))
                )
          }

        case SlowConsumerPolicy.ErrorAndDrop =>
          sub.queue.tryOffer(msg).flatMap {
            case true  => Async[F].pure(None)
            case false =>
              Async[F].pure(
                Some(ClientEvent.SlowConsumer(sub.sid, sub.subject, 1))
              )
          }

    override def activeSubscriptions: F[List[(Long, String, Option[String])]] =
      subsRef.get.flatMap { subs =>
        subs.toList
          .traverse { case (sid, sub) =>
            sub.stateRef.get.map { state =>
              if state.active then Some((sid, sub.subject, sub.queueGroup))
              else None
            }
          }
          .map(_.flatten)
      }

    override def remove(sid: Long): F[Unit] =
      subsRef.get.flatMap { subs =>
        subs.get(sid) match
          case None      => Async[F].unit
          case Some(sub) =>
            sub.stateRef.set(new SubState(false, 0)) *>
              sub.queue.offer(PoisonPill) *>
              sidAllocator.release(sid) *>
              subsRef.update(_ - sid)
      }

    override def closeAll: F[Unit] =
      subsRef.get.flatMap { subs =>
        subs.values.toList.traverse_ { sub =>
          sub.stateRef.set(new SubState(false, 0)) *>
            sub.queue.offer(PoisonPill) *>
            sidAllocator.release(sub.sid)
        }
      } *> subsRef.set(Map.empty)

  private class SubscriptionHandleImpl[F[_]: Async](
      val sid: Long,
      val subject: String,
      val queueGroup: Option[String],
      stateRef: Ref[F, SubState],
      onUnsubscribe: (Long, Option[Int]) => F[Unit],
      manager: SubscriptionManager[F]
  ) extends SubscriptionHandle[F]:

    override def unsubscribe: F[Unit] =
      stateRef.get.flatMap { state =>
        if state.active then
          onUnsubscribe(sid, None) *>
            manager.remove(sid)
        else Async[F].unit
      }

    override def unsubscribeAfter(maxMsgs: Int): F[Unit] =
      onUnsubscribe(sid, Some(maxMsgs))
