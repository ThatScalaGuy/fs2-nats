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

package fs2.nats.client

import cats.effect.{Async, Deferred}
import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.nats.errors.NatsError
import fs2.nats.protocol.Headers
import fs2.nats.publish.{Publisher, SerializationUtils}
import fs2.nats.subscriptions.{NatsMessage, SidAllocator, SubscriptionManager}
import fs2.nats.util.Tokens

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration.*

/** Core request/reply primitive built on a single shared response inbox.
  *
  * A single wildcard subscription `_INBOX.<inboxId>.*` is registered for the
  * connection lifetime (the modern nats.go `respMux` style). Each request
  * inserts a `Deferred` into a correlation map keyed by a unique token and
  * publishes the request with `replyTo = _INBOX.<inboxId>.<token>`. Replies are
  * routed back through the normal subscription path; a background fiber drains
  * the inbox subscription, extracts the trailing token, and completes the
  * matching `Deferred`.
  *
  * Because the inbox is registered through the
  * [[fs2.nats.subscriptions.SubscriptionManager]], it is part of
  * `activeSubscriptions` and is replayed automatically on reconnect.
  */
trait Requestor[F[_]]:

  /** Send a request and await a single reply.
    *
    * @param subject
    *   The subject to send the request to
    * @param payload
    *   The request payload
    * @param headers
    *   Optional request headers (default empty)
    * @param timeout
    *   How long to wait for a reply before failing with
    *   [[fs2.nats.errors.NatsError.Timeout]]
    * @return
    *   The reply message, or a failed effect with
    *   [[fs2.nats.errors.NatsError.NoResponders]] if the server reported that
    *   no subscribers are listening (503)
    */
  def request(
      subject: String,
      payload: Chunk[Byte],
      headers: Headers,
      timeout: FiniteDuration
  ): F[NatsMessage]

  /** Split-phase request: publish now, await later. Internal SPI backing the
    * pipelined JetStream publish path.
    *
    * The outer effect registers the correlation slot and puts the request on
    * the wire; it therefore fails for anything that makes the publish itself
    * impossible, and it can back-pressure on the bounded write queue. The
    * returned inner effect awaits the reply against a deadline that only starts
    * once the request is on the wire, so its clock origin is the publish, not
    * the await, and a back-pressured publish does not eat into the reply
    * budget. It holds no finalizer and is re-runnable: running it again after
    * it has settled replays the stored outcome.
    *
    * `onSettle` is the hook that hands the caller's resource back. It runs
    * exactly once, on whichever fiber settles the request - the inbox drain
    * fiber when a reply arrives, the awaiting caller when its deadline passes,
    * or the connection sweeper when nobody ever awaits - and it is deliberately
    * NOT run when the outer effect itself fails or is cancelled, because on
    * those paths the caller still owns the resource and releases it itself.
    * Those two parties are independent: a cancellation of the outer effect can
    * be observed after the request has already settled, so a caller that
    * releases on its own failure/cancellation path must make that release
    * one-shot against the hook.
    *
    * @param subject
    *   The subject to send the request to
    * @param payload
    *   The request payload
    * @param headers
    *   Optional request headers
    * @param timeout
    *   How long the reply may take before the request settles with
    *   [[fs2.nats.errors.NatsError.Timeout]]
    * @param onSettle
    *   Run exactly once when the request settles (see above)
    * @return
    *   An effect that publishes and yields the effect awaiting the reply
    */
  private[nats] def requestAsync(
      subject: String,
      payload: Chunk[Byte],
      headers: Headers,
      timeout: FiniteDuration,
      onSettle: F[Unit]
  ): F[F[NatsMessage]]

object Requestor:

  /** Construct a Requestor, register its shared inbox subscription, and start
    * the supervised drain fiber.
    *
    * @param subManager
    *   The subscription manager used to register the inbox (so it rides
    *   reconnect replay for free)
    * @param sidAllocator
    *   Allocates the inbox subscription id
    * @param publisher
    *   Used to publish requests with a reply-to inbox
    * @param send
    *   Sends raw protocol bytes (the inbox SUB frame)
    * @param supervisor
    *   Supervises the background inbox drain fiber and the expiry sweeper
    */
  def apply[F[_]: Async](
      subManager: SubscriptionManager[F],
      sidAllocator: SidAllocator[F],
      publisher: Publisher[F],
      send: Chunk[Byte] => F[Unit],
      supervisor: Supervisor[F]
  ): F[Requestor[F]] =
    for
      inboxId <- Tokens.randomInboxId[F]()
      inboxPrefix = s"_INBOX.$inboxId"
      wildcard = s"$inboxPrefix.*"
      sid <- sidAllocator.next
      registered <- subManager.register(sid, wildcard, None)
      (stream, _) = registered
      _ <- send(SerializationUtils.buildSub(wildcard, None, sid))
      impl = new RequestorImpl[F](inboxPrefix, publisher)
      _ <- supervisor.supervise(impl.drain(stream)).void
      _ <- supervisor.supervise(impl.expireLoop).void
    yield impl

  private class RequestorImpl[F[_]: Async](
      inboxPrefix: String,
      publisher: Publisher[F]
  ) extends Requestor[F]:

    // Replies arrive on "<inboxPrefix>.<token>"; this is where the token starts.
    private val tokenStart = inboxPrefix.length + 1

    // Deadline of a request that has not gone out yet. Compared by equality,
    // never by subtraction: `monotonic` is `System.nanoTime`, whose origin is
    // arbitrary, so a sentinel is not safely orderable against it.
    private val Unarmed: Long = Long.MaxValue

    /** One in-flight request. Carries the settle hook and an absolute deadline
      * so a request can be settled by whoever gets there first without a
      * per-request fiber holding the timeout.
      */
    private final class Pending(
        val slot: Deferred[F, Either[Throwable, NatsMessage]],
        val subject: String,
        val timeoutMillis: Long,
        val timeoutNanos: Long,
        val onSettle: F[Unit]
    ):
      // Armed by `arm` once the request is on the wire. The slot has to be in
      // the map before the publish (a reply may beat it), but the clock must
      // not start before it: a publish that back-pressures on the bounded write
      // queue would otherwise burn the reply budget, and the sweeper would
      // settle - and release the caller's resource for - a request the caller
      // is still publishing. Written once by the publishing fiber, read by the
      // sweeper and by the awaiting caller.
      @volatile var deadlineNanos: Long = Unarmed

    // Correlation state as plain concurrent primitives rather than Refs: every
    // request otherwise costs four compare-and-set loops on two shared cells
    // (counter draw, insert, reply-side take, guaranteed remove), and a lost
    // CAS on the pending map re-copies the whole immutable map before retrying.
    // The cells are created per connection and only ever touched from inside a
    // running effect (a delay, or a flatMap continuation the runtime re-enters
    // on every run), so the mutation is not observable as a side effect through
    // the pure API. Sized for the pipelined-publish window (publishAsync allows
    // 256 in flight by default) so a long-lived connection does not walk the
    // default 16 -> 512 resize ladder.
    private val pending = new ConcurrentHashMap[String, Pending](256)
    private val counter = new AtomicLong(0L)

    // One coarse tick per connection, not per request: this only ever settles
    // requests nobody is awaiting (see `sweepExpired`).
    private val sweepInterval: FiniteDuration = 1.second

    /** Drain the inbox subscription, completing the pending `Deferred` that
      * matches each reply's token. Runs until the subscription stream ends.
      */
    def drain(stream: Stream[F, NatsMessage]): F[Unit] =
      stream.evalMap(handleReply).compile.drain

    /** Bound the lifetime of requests whose wait effect is never run. The
      * pipelined publish path returns the wait effect to the caller and may
      * never run it; without this sweep such a request would hold its window
      * permit forever against a server that stops replying. Every caller that
      * does await gets the exact deadline from `await`, so the sweep's
      * granularity only bounds the abandoned case (timeout + one tick).
      */
    def expireLoop: F[Unit] =
      (Async[F].sleep(sweepInterval) *> sweepExpired).foreverM

    // `remove` is the atomic get-and-remove the `Ref.modify` provided: it is
    // what keeps completion at-most-once across the drain fiber, an awaiting
    // caller's deadline and the sweeper. A ConcurrentHashMap holds no null
    // values, so a null return is an unambiguous miss.
    private def take(token: String): F[Option[Pending]] =
      Async[F].delay(Option(pending.remove(token)))

    // Removes WITHOUT running the settle hook - only for paths where the caller
    // still owns the resource and releases it itself.
    private def drop(token: String): F[Unit] =
      Async[F].delay(pending.remove(token)).void

    // Always called from inside an uncancelable region (see `expire` and
    // `handleReply`): the removal that elects the settler and the settling
    // itself have to be one atomic step.
    private def complete(
        p: Pending,
        result: Either[Throwable, NatsMessage]
    ): F[Unit] =
      // The hook runs on whichever fiber settles the request, which for a reply
      // is the connection-wide drain fiber: a raise there would stop correlating
      // every other request. `Semaphore.release` is total, so swallowing here
      // can only ever hide a bug in a hook, never a real failure.
      p.slot.complete(result).void *> p.onSettle.handleError(_ => ())

    private def handleReply(msg: NatsMessage): F[Unit] =
      // Masked for the same reason as `expire`: the drain fiber is cancelled
      // when the connection shuts down and must not stop between taking a
      // request out of the map and settling it.
      Async[F].uncancelable { _ =>
        Async[F]
          .delay {
            val token =
              if msg.subject.length > tokenStart then
                msg.subject.substring(tokenStart)
              else ""
            // Token extraction and the arbitrating `remove` (see `take`) stay
            // in one delay: this runs for every reply on the connection.
            Option(pending.remove(token))
          }
          .flatMap {
            case None    => Async[F].unit
            case Some(p) => complete(p, Right(msg))
          }
      }

    private def timedOut(p: Pending): Throwable =
      NatsError.Timeout(s"request to '${p.subject}'", p.timeoutMillis)

    private def expire(token: String): F[Unit] =
      // Uncancelable as one step: `take` is the arbitrating removal, so a
      // cancel landing between it and `complete` would leave the slot
      // uncompleted with nobody left holding it and would swallow the
      // `onSettle` hook - permanently losing whatever resource the caller
      // handed to it. This runs on the awaiting caller's own (cancellable)
      // fiber as well as on the sweeper's.
      Async[F].uncancelable { _ =>
        take(token).flatMap {
          case None    => Async[F].unit
          case Some(p) => complete(p, Left(timedOut(p)))
        }
      }

    private def sweepExpired: F[Unit] =
      Async[F].monotonic.flatMap { now =>
        Async[F]
          .delay {
            val nowNanos = now.toNanos
            var expired = List.empty[String]
            val it = pending.entrySet().iterator()
            while it.hasNext do
              val e = it.next()
              val deadline = e.getValue.deadlineNanos
              // An unarmed request is still being published; its owner has not
              // handed it over yet.
              if deadline != Unarmed && deadline - nowNanos <= 0L then
                expired = e.getKey :: expired
            expired
          }
          .flatMap(_.traverse_(expire))
      }

    /** Draw a token and register the correlation slot. Registration strictly
      * precedes any publish so a reply can never arrive before its slot exists;
      * the deadline is armed afterwards by `arm`.
      */
    private def start(
        subject: String,
        timeout: FiniteDuration,
        onSettle: F[Unit]
    ): F[(String, Pending)] =
      for
        n <- Async[F].delay(counter.getAndIncrement())
        token = Tokens.base62(n)
        slot <- Deferred[F, Either[Throwable, NatsMessage]]
        p = new Pending(
          slot,
          subject,
          timeout.toMillis,
          timeout.toNanos,
          onSettle
        )
        _ <- Async[F].delay(pending.put(token, p)).void
      yield (token, p)

    /** Start the reply budget, once the request is actually on the wire. */
    private def arm(p: Pending): F[Unit] =
      Async[F].monotonic.flatMap { now =>
        Async[F].delay(p.deadlineNanos = now.toNanos + p.timeoutNanos)
      }

    /** Await the reply against the deadline `arm` set at the publish, so the
      * clock origin stays the publish and a re-run of an already-settled wait
      * returns the stored outcome immediately.
      */
    private def await(token: String, p: Pending): F[NatsMessage] =
      p.slot.tryGet
        .flatMap {
          case Some(result) => Async[F].fromEither(result)
          case None         =>
            Async[F].monotonic.flatMap { now =>
              val remaining =
                math.max(0L, p.deadlineNanos - now.toNanos).nanos
              // The fallback re-reads the slot because a reply may have won the
              // `remove` while the timer fired. It never waits again: `expire`
              // settles the slot itself unless another settler already took the
              // request, and the deadline has passed either way, so Timeout is
              // the honest answer rather than a second unbounded park.
              p.slot.get
                .timeoutTo(
                  remaining,
                  expire(token) *> p.slot.tryGet
                    .map(_.getOrElse(Left(timedOut(p))))
                )
                .flatMap(Async[F].fromEither)
            }
        }
        .flatMap { msg =>
          // NoResponders names the REQUEST subject, not the inbox it replied to.
          if msg.status.contains(503) then
            Async[F].raiseError[NatsMessage](NatsError.NoResponders(p.subject))
          else Async[F].pure(msg)
        }

    override def request(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        timeout: FiniteDuration
    ): F[NatsMessage] =
      // Registration is masked so a cancel cannot orphan a map entry; the
      // publish stays pollable because it can block on the bounded write queue.
      // `drop` (not `expire`) on the way out: this path's hook is a no-op and
      // the caller owns nothing.
      Async[F].uncancelable { poll =>
        start(subject, timeout, Async[F].unit).flatMap { case (token, p) =>
          poll(
            publishRequest(subject, payload, headers, s"$inboxPrefix.$token") *>
              arm(p) *> await(token, p)
          ).guarantee(drop(token))
        }
      }

    override private[nats] def requestAsync(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        timeout: FiniteDuration,
        onSettle: F[Unit]
    ): F[F[NatsMessage]] =
      Async[F].uncancelable { poll =>
        start(subject, timeout, onSettle).flatMap { case (token, p) =>
          poll(
            publishRequest(subject, payload, headers, s"$inboxPrefix.$token")
          )
            .onError { case _ => drop(token) }
            .onCancel(drop(token))
            // Masked, so the request is never visible to the sweeper before its
            // deadline exists.
            .productR(arm(p))
            .as(await(token, p))
        }
      }

    private def publishRequest(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        reply: String
    ): F[Unit] =
      if headers.isEmpty then publisher.publish(subject, payload, Some(reply))
      else publisher.publishWithHeaders(subject, payload, headers, Some(reply))
