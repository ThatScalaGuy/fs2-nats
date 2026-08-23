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

import cats.effect.{IO, Resource}
import cats.effect.kernel.Ref
import cats.effect.std.Supervisor
import cats.syntax.all.*
import fs2.Chunk
import fs2.nats.errors.NatsError
import fs2.nats.protocol.Headers
import fs2.nats.publish.Publisher
import fs2.nats.subscriptions.{NatsMessage, SidAllocator, SubscriptionManager}
import munit.CatsEffectSuite

import scala.concurrent.duration.*

/** Broker-free coverage of the request/reply correlation path: the pending map,
  * the token counter and the two removal paths are all private, so they are
  * pinned down through behaviour only.
  */
class RequestorSpec extends CatsEffectSuite:

  private def chunk(s: String): Chunk[Byte] = Chunk.array(s.getBytes)

  private case class PublishCall(
      subject: String,
      payload: Chunk[Byte],
      replyTo: Option[String],
      withHeaders: Boolean
  ):
    def payloadAsString: String = new String(payload.toArray)
    def reply: String =
      replyTo.getOrElse(fail("request published without reply"))

  /** Records every publish and then hands the call to a swappable hook, so a
    * test can either answer inline or capture the reply subject and answer
    * later.
    */
  private class StubPublisher(
      calls: Ref[IO, Vector[PublishCall]],
      hook: Ref[IO, PublishCall => IO[Unit]]
  ) extends Publisher[IO]:

    private def record(call: PublishCall): IO[Unit] =
      calls.update(_ :+ call) *> hook.get.flatMap(_(call))

    override def publish(
        subject: String,
        payload: Chunk[Byte],
        replyTo: Option[String]
    ): IO[Unit] =
      record(PublishCall(subject, payload, replyTo, withHeaders = false))

    override def publishWithHeaders(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        replyTo: Option[String]
    ): IO[Unit] =
      record(PublishCall(subject, payload, replyTo, withHeaders = true))

    override def updateMaxPayload(maxPayload: Long): IO[Unit] = IO.unit

  private case class Harness(
      requestor: Requestor[IO],
      subManager: SubscriptionManager[IO],
      inboxSid: Long,
      inboxPrefix: String,
      calls: Ref[IO, Vector[PublishCall]],
      hook: Ref[IO, PublishCall => IO[Unit]]
  ):
    def answerInline(reply: PublishCall => Chunk[Byte]): IO[Unit] =
      hook.set(c => routeReply(c.reply, reply(c)))

    def routeReply(subject: String, payload: Chunk[Byte]): IO[Unit] =
      subManager
        .routeMessage(
          NatsMessage.parserBuilder.msg(subject, inboxSid, None, payload)
        )
        .void

    def routeStatus(
        subject: String,
        code: Int,
        description: String
    ): IO[Unit] =
      subManager
        .routeMessage(
          NatsMessage.parserBuilder.hmsg(
            subject,
            inboxSid,
            None,
            Headers.empty,
            Some(code),
            Some(description),
            Chunk.empty
          )
        )
        .void

    /** Wait until at least `n` publishes have been recorded and return the n-th
      * (1-based). Used when the request runs in a started fiber.
      */
    def awaitCall(n: Int): IO[PublishCall] =
      calls.get
        .flatMap { cs =>
          if cs.length >= n then IO.pure(cs(n - 1))
          else IO.sleep(1.millis) *> awaitCall(n)
        }
        .timeout(5.seconds)

    /** Proves the supervised drain fiber is still running: a fresh request can
      * only complete if replies are still being correlated.
      */
    def assertDrainAlive(marker: String): IO[Unit] =
      answerInline(_ => chunk(marker)) *>
        requestor
          .request("probe.subject", chunk("probe"), Headers.empty, 5.seconds)
          .map(m => assertEquals(m.payloadAsString, marker))

  private val harness: Resource[IO, Harness] =
    Supervisor[IO].evalMap { supervisor =>
      for
        sidAllocator <- SidAllocator[IO]
        subManager <- SubscriptionManager[IO](
          100,
          SlowConsumerPolicy.Block,
          sidAllocator,
          (_, _) => IO.unit
        )
        calls <- Ref.of[IO, Vector[PublishCall]](Vector.empty)
        hook <- Ref.of[IO, PublishCall => IO[Unit]](_ => IO.unit)
        publisher = new StubPublisher(calls, hook)
        requestor <- Requestor[IO](
          subManager,
          sidAllocator,
          publisher,
          _ => IO.unit,
          supervisor
        )
        subs <- subManager.activeSubscriptions
        (inboxSid, wildcard, _) = subs.head
        // The registered inbox subscription is "<prefix>.*".
        inboxPrefix = wildcard.dropRight(2)
      yield Harness(
        requestor,
        subManager,
        inboxSid,
        inboxPrefix,
        calls,
        hook
      )
    }

  test("each concurrent request receives its own reply") {
    harness.use { h =>
      for
        _ <- h.answerInline(c => chunk("reply-" + c.payloadAsString))
        results <- (1 to 50).toList.parTraverse { i =>
          h.requestor
            .request("svc.echo", chunk(s"req-$i"), Headers.empty, 5.seconds)
            .map(_.payloadAsString)
        }
      yield assertEquals(results, (1 to 50).toList.map(i => s"reply-req-$i"))
    }
  }

  test("a duplicate reply for the same token completes the request once") {
    harness.use { h =>
      for
        fiber <- h.requestor
          .request("svc.dup", chunk("q"), Headers.empty, 5.seconds)
          .start
        call <- h.awaitCall(1)
        _ <- h.routeReply(call.reply, chunk("first"))
        _ <- h.routeReply(call.reply, chunk("second"))
        msg <- fiber.joinWithNever
        _ = assertEquals(msg.payloadAsString, "first")
        _ <- h.assertDrainAlive("after-duplicate")
      yield ()
    }
  }

  test("replies with no token do not kill the drain fiber") {
    harness.use { h =>
      for
        // Subject shorter than the token start, and a subject with an empty
        // token: both must miss the correlation map without raising.
        _ <- h.routeReply(h.inboxPrefix, chunk("stray"))
        _ <- h.routeReply(h.inboxPrefix + ".", chunk("stray"))
        _ <- h.routeReply("_INBOX", chunk("stray"))
        _ <- h.assertDrainAlive("after-stray")
      yield ()
    }
  }

  test("a 503 reply fails with NoResponders carrying the request subject") {
    harness.use { h =>
      for
        _ <- h.hook.set(c =>
          h.routeStatus(c.reply, 503, "No Responders Available For Request")
        )
        attempt <- h.requestor
          .request("svc.absent", chunk("q"), Headers.empty, 5.seconds)
          .attempt
        _ = assertEquals(
          attempt.left.toOption,
          Some(NatsError.NoResponders("svc.absent"))
        )
        _ <- h.assertDrainAlive("after-503")
      yield ()
    }
  }

  test("a non-503 status reply is returned as a value") {
    harness.use { h =>
      for
        _ <- h.hook.set(c => h.routeStatus(c.reply, 404, "Message Not Found"))
        msg <- h.requestor
          .request("svc.kv", chunk("q"), Headers.empty, 5.seconds)
        _ = assertEquals(msg.status, Some(404))
        _ = assertEquals(msg.statusDescription, Some("Message Not Found"))
      yield ()
    }
  }

  test("a request times out and its late reply is dropped silently") {
    harness.use { h =>
      for
        attempt <- h.requestor
          .request("svc.slow", chunk("q"), Headers.empty, 100.millis)
          .attempt
        call <- h.awaitCall(1)
        _ = assert(
          attempt.left.toOption.exists {
            case NatsError.Timeout(op, _) => op == "request to 'svc.slow'"
            case _                        => false
          },
          s"expected Timeout, got $attempt"
        )
        // The token is gone from the correlation map; the reply must be a no-op.
        _ <- h.routeReply(call.reply, chunk("late"))
        _ <- h.assertDrainAlive("after-timeout")
      yield ()
    }
  }

  test("a cancelled request drops its reply and leaves the client usable") {
    harness.use { h =>
      for
        fiber <- h.requestor
          .request("svc.cancel", chunk("q"), Headers.empty, 5.seconds)
          .start
        call <- h.awaitCall(1)
        _ <- fiber.cancel
        _ <- h.routeReply(call.reply, chunk("late"))
        _ <- h.assertDrainAlive("after-cancel")
      yield ()
    }
  }

  test("headers select publishWithHeaders and the reply-to is the inbox") {
    harness.use { h =>
      for
        _ <- h.answerInline(_ => chunk("ok"))
        _ <- h.requestor
          .request("svc.plain", chunk("q"), Headers.empty, 5.seconds)
        _ <- h.requestor.request(
          "svc.hdrs",
          chunk("q"),
          Headers.empty.add("K", "V"),
          5.seconds
        )
        calls <- h.calls.get
        _ = assertEquals(calls.length, 2)
        _ = assertEquals(calls(0).withHeaders, false)
        _ = assertEquals(calls(1).withHeaders, true)
        // Tokens are drawn from a counter starting at zero, so the first two
        // requests on a connection get base62(0) and base62(1).
        _ = assertEquals(calls(0).replyTo, Some(s"${h.inboxPrefix}.0"))
        _ = assertEquals(calls(1).replyTo, Some(s"${h.inboxPrefix}.1"))
      yield ()
    }
  }
