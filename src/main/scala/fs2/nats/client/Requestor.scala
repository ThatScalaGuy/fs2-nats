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

import scala.concurrent.duration.FiniteDuration

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
    *   Supervises the background inbox drain fiber
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
    yield impl

  private class RequestorImpl[F[_]: Async](
      inboxPrefix: String,
      publisher: Publisher[F]
  ) extends Requestor[F]:

    // Replies arrive on "<inboxPrefix>.<token>"; this is where the token starts.
    private val tokenStart = inboxPrefix.length + 1

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
    private val pending =
      new ConcurrentHashMap[String, Deferred[F, NatsMessage]](256)
    private val counter = new AtomicLong(0L)

    /** Drain the inbox subscription, completing the pending `Deferred` that
      * matches each reply's token. Runs until the subscription stream ends.
      */
    def drain(stream: Stream[F, NatsMessage]): F[Unit] =
      stream.evalMap(handleReply).compile.drain

    private def handleReply(msg: NatsMessage): F[Unit] =
      Async[F]
        .delay {
          val token =
            if msg.subject.length > tokenStart then
              msg.subject.substring(tokenStart)
            else ""
          // `remove` is the atomic get-and-remove the `Ref.modify` provided:
          // it is what keeps completion at-most-once for a duplicate or late
          // reply. A ConcurrentHashMap holds no null values, so a null return
          // is an unambiguous miss.
          Option(pending.remove(token))
        }
        .flatMap {
          case None    => Async[F].unit
          case Some(d) => d.complete(msg).void
        }

    override def request(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        timeout: FiniteDuration
    ): F[NatsMessage] =
      for
        n <- Async[F].delay(counter.getAndIncrement())
        token = Tokens.base62(n)
        reply = s"$inboxPrefix.$token"
        d <- Deferred[F, NatsMessage]
        _ <- Async[F].delay(pending.put(token, d)).void
        result <-
          (publishRequest(subject, payload, headers, reply) *>
            d.get.timeoutTo(
              timeout,
              Async[F].raiseError[NatsMessage](
                NatsError.Timeout(s"request to '$subject'", timeout.toMillis)
              )
            )).guarantee(Async[F].delay(pending.remove(token)).void)
        msg <-
          if result.status.contains(503) then
            Async[F].raiseError[NatsMessage](NatsError.NoResponders(subject))
          else Async[F].pure(result)
      yield msg

    private def publishRequest(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        reply: String
    ): F[Unit] =
      if headers.isEmpty then publisher.publish(subject, payload, Some(reply))
      else publisher.publishWithHeaders(subject, payload, headers, Some(reply))
