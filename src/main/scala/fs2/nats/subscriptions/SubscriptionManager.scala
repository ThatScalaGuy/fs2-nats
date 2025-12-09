/*
 * Copyright 2024 fs2-nats contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.subscriptions

import cats.effect.{Async, Ref, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.Stream
import fs2.nats.client.{ClientEvent, SlowConsumerPolicy}
import fs2.nats.protocol.{Headers, NatsFrame}
import cats.effect.std.MapRef

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

  /** Route an incoming message frame to the appropriate subscription.
    *
    * @param frame
    *   The MSG or HMSG frame
    * @return
    *   Effect that completes when the message is routed or policy applied
    */
  def routeMessage(frame: NatsFrame): F[Option[ClientEvent.SlowConsumer]]

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

  private class InternalSubscription[F[_]](
      val sid: Long,
      val subject: String,
      val queueGroup: Option[String],
      val queue: Queue[F, Option[NatsMessage]],
      val remainingRef: Ref[F, Option[Int]],
      val activeRef: Ref[F, Boolean]
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
        queue <- Queue.bounded[F, Option[NatsMessage]](queueCapacity)
        remainingRef <- Ref.of[F, Option[Int]](None)
        activeRef <- Ref.of[F, Boolean](true)
        sub = new InternalSubscription[F](
          sid,
          subject,
          queueGroup,
          queue,
          remainingRef,
          activeRef
        )
        _ <- subsRef.update(_ + (sid -> sub))
      yield
        val stream = Stream.fromQueueNoneTerminated(queue)
        val handle = new SubscriptionHandleImpl[F](
          sid,
          subject,
          queueGroup,
          activeRef,
          onUnsubscribe,
          this
        )
        (stream, handle)

    override def routeMessage(
        frame: NatsFrame
    ): F[Option[ClientEvent.SlowConsumer]] =
      frame match
        case NatsFrame.MsgFrame(subject, sid, replyTo, payload) =>
          val msg = NatsMessage(subject, replyTo, Headers.empty, payload, sid)
          deliverMessage(sid, subject, msg)

        case NatsFrame.HMsgFrame(subject, sid, replyTo, headers, _, payload) =>
          val msg = NatsMessage(subject, replyTo, headers, payload, sid)
          deliverMessage(sid, subject, msg)

        case _ =>
          Async[F].pure(None)

    private def deliverMessage(
        sid: Long,
        subject: String,
        msg: NatsMessage
    ): F[Option[ClientEvent.SlowConsumer]] =
      subsRef.get.flatMap { subs =>
        subs.get(sid) match
          case None =>
            Async[F].pure(None)

          case Some(sub) =>
            sub.activeRef.get.flatMap { active =>
              if !active then Async[F].pure(None)
              else
                sub.remainingRef
                  .modify {
                    case Some(n) if n > 0 => (Some(n - 1), true)
                    case Some(_) => (Some(0), false) // Covers 0 and negative
                    case None    => (None, true)
                  }
                  .flatMap { shouldDeliver =>
                    if !shouldDeliver then
                      sub.activeRef.set(false) *>
                        sub.queue.offer(None) *>
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
          sub.queue.offer(Some(msg)).as(None)

        case SlowConsumerPolicy.DropNew =>
          sub.queue.tryOffer(Some(msg)).flatMap {
            case true  => Async[F].pure(None)
            case false =>
              Async[F].pure(
                Some(ClientEvent.SlowConsumer(sub.sid, sub.subject, 1))
              )
          }

        case SlowConsumerPolicy.DropOldest =>
          sub.queue.tryOffer(Some(msg)).flatMap {
            case true  => Async[F].pure(None)
            case false =>
              sub.queue.tryTake *>
                sub.queue.tryOffer(Some(msg)) *>
                Async[F].pure(
                  Some(ClientEvent.SlowConsumer(sub.sid, sub.subject, 1))
                )
          }

        case SlowConsumerPolicy.ErrorAndDrop =>
          sub.queue.tryOffer(Some(msg)).flatMap {
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
            sub.activeRef.get.map { active =>
              if active then Some((sid, sub.subject, sub.queueGroup))
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
            sub.activeRef.set(false) *>
              sub.queue.offer(None) *>
              sidAllocator.release(sid) *>
              subsRef.update(_ - sid)
      }

    override def closeAll: F[Unit] =
      subsRef.get.flatMap { subs =>
        subs.values.toList.traverse_ { sub =>
          sub.activeRef.set(false) *>
            sub.queue.offer(None) *>
            sidAllocator.release(sub.sid)
        }
      } *> subsRef.set(Map.empty)

  private class SubscriptionHandleImpl[F[_]: Async](
      val sid: Long,
      val subject: String,
      val queueGroup: Option[String],
      activeRef: Ref[F, Boolean],
      onUnsubscribe: (Long, Option[Int]) => F[Unit],
      manager: SubscriptionManager[F]
  ) extends SubscriptionHandle[F]:

    override def unsubscribe: F[Unit] =
      activeRef.get.flatMap { active =>
        if active then
          onUnsubscribe(sid, None) *>
            manager.remove(sid)
        else Async[F].unit
      }

    override def unsubscribeAfter(maxMsgs: Int): F[Unit] =
      onUnsubscribe(sid, Some(maxMsgs))
