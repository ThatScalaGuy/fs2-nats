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

package fs2.nats.util

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.annotation.tailrec

import cats.Monad
import cats.effect.{Async, Deferred}
import cats.effect.std.Queue
import cats.syntax.all.*

/** Constructors for the bounded queues on the client's hot paths. */
private[nats] object Queues:

  /** Capacity bound (exclusive) below which `Queue.bounded` uses its fast
    * lock-free implementation. At or above it, cats-effect silently falls back
    * to a CAS-over-immutable-state queue (the ring buffer of the fast one is
    * allocated eagerly, so cats-effect caps it at `Short.MaxValue * 2`).
    */
  private[util] val FastPathLimit: Int = Short.MaxValue.toInt * 2 // 65534

  /** A bounded queue that stays on cats-effect's lock-free implementations at
    * any capacity.
    *
    * For `capacity < 65534` this is exactly `Queue.bounded`. Above that,
    * `Queue.bounded` would degrade to an implementation whose dequeues re-run
    * an O(n) list reversal on every contended CAS retry — measured at >30% of
    * client CPU under sustained receive load. Instead, large capacities get a
    * fast unbounded queue bounded externally by an atomic slot count, with the
    * same `offer`-blocks-when-full / `tryOffer`-fails-when-full semantics.
    */
  def bounded[F[_], A](capacity: Int)(implicit F: Async[F]): F[Queue[F, A]] =
    if capacity >= FastPathLimit then largeBounded(capacity)
    else Queue.bounded[F, A](capacity)

  private[util] def largeBounded[F[_], A](
      capacity: Int
  )(implicit F: Async[F]): F[Queue[F, A]] =
    Queue.unbounded[F, A].map(new LargeBoundedQueue(capacity, _))

  private final class LargeBoundedQueue[F[_], A](
      capacity: Int,
      underlying: Queue[F, A]
  )(implicit F: Async[F])
      extends Queue[F, A]:

    /** Slots in use. Incremented before the element reaches `underlying`,
      * decremented after it leaves, so the bound is never under-counted.
      */
    private val count = new AtomicInteger(0)

    /** Single shared "a slot was freed" signal. Blocked offerers park on it; a
      * take that frees a slot consumes-and-completes it. Offerers always
      * re-check `count` after waking, so spurious completions are harmless.
      * Lost wakeups are impossible: the parker installs the signal before
      * re-reading `count`, and the releaser decrements `count` before reading
      * the signal, so one of the two always observes the other.
      */
    private val roomSignal = new AtomicReference[Deferred[F, Unit]](null)

    @tailrec
    private def tryAcquireSlot(): Boolean =
      val c = count.get()
      if c >= capacity then false
      else if count.compareAndSet(c, c + 1) then true
      else tryAcquireSlot()

    private def releaseSlots(n: Int): F[Unit] =
      F.delay {
        count.addAndGet(-n)
        if roomSignal.get() ne null then roomSignal.getAndSet(null) else null
      }.flatMap { d =>
        if d ne null then d.complete(()).void else F.unit
      }

    @tailrec
    private def installSignal(fresh: Deferred[F, Unit]): Deferred[F, Unit] =
      val cur = roomSignal.get()
      if cur ne null then cur
      else if roomSignal.compareAndSet(null, fresh) then fresh
      else installSignal(fresh)

    private def awaitRoom: F[Unit] =
      Deferred[F, Unit].flatMap { fresh =>
        F.delay(installSignal(fresh)).flatMap { d =>
          F.delay(count.get() < capacity).flatMap {
            case true  => F.unit // a slot was freed while parking; retry now
            case false => d.get
          }
        }
      }

    override def offer(a: A): F[Unit] =
      F.uncancelable { poll =>
        def loop: F[Unit] =
          F.delay(tryAcquireSlot()).flatMap {
            case true  => underlying.offer(a)
            case false => poll(awaitRoom) >> loop
          }
        loop
      }

    override def tryOffer(a: A): F[Boolean] =
      F.uncancelable { _ =>
        F.delay(tryAcquireSlot()).flatMap {
          case true  => underlying.offer(a).as(true)
          case false => F.pure(false)
        }
      }

    override def take: F[A] =
      F.uncancelable { poll =>
        poll(underlying.take).flatMap(a => releaseSlots(1).as(a))
      }

    override def tryTake: F[Option[A]] =
      F.uncancelable { _ =>
        underlying.tryTake.flatMap {
          case some @ Some(_) => releaseSlots(1).as(some)
          case None           => F.pure(None)
        }
      }

    override def tryTakeN(
        maxN: Option[Int]
    )(implicit ev: Monad[F]): F[List[A]] =
      F.uncancelable { _ =>
        underlying.tryTakeN(maxN).flatMap {
          case Nil => F.pure(List.empty[A])
          case xs  => releaseSlots(xs.size).as(xs)
        }
      }

    override def size: F[Int] = F.delay(count.get())
