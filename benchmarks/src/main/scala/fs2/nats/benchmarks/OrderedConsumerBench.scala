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

package fs2.nats.benchmarks

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.{Chunk, Stream}
import fs2.nats.client.{ClientConfig, NatsClient, SlowConsumerPolicy}
import fs2.nats.jetstream.*
import fs2.nats.jetstream.protocol.*
import scala.concurrent.duration.*

/** End-to-end harness for the gap-resetting ordered push consumer
  * (`JetStream.subscribeOrdered`) against a live, JetStream-enabled NATS
  * server. KV `keys`/`history`/`watch` and Object Store `list`/`get`/`watch`
  * are all built on this path, and none of them had a benchmark.
  *
  * Publishes `n` messages to a stream and then drains them back with two
  * consumers, two passes each, interleaved `ordered, push, ordered, push`:
  *   - through `subscribeOrdered` — the path under measurement, whose delivery
  *     loop merges liveness ticks into the data stream and tracks the
  *     per-consumer sequence per message;
  *   - through a plain `subscribePush` consumer over the same data, configured
  *     identically (no ack, flow control, 5 s heartbeat) — the **control**.
  *     Push already uses the direct `delivery.evalMapFilter(...)` shape, so
  *     `push - ordered` bounds what reshaping the ordered loop can recover. If
  *     the two converge after a change, the loop was the whole gap.
  *
  * Each consumer runs twice and both passes are reported, because they read the
  * same messages back to back: a first reader that paid a cold-cache or
  * server-side warm-up penalty would otherwise look slower than the shape it is
  * being compared against, for a reason that has nothing to do with the shape.
  *
  * It is also a correctness gate, which on this path matters more than the
  * number: each payload carries its own index, and the consumer asserts a
  * strictly increasing, gap-free run of exactly `n` messages. Losing,
  * duplicating or reordering a delivery is exactly what a restructure of the
  * ordered loop can introduce and exactly what a throughput figure hides, so
  * such a run fails loudly instead of quietly looking faster. The check reads
  * the index straight out of the payload bytes rather than going through
  * `payloadAsString`, to keep the verification off the measured loop's
  * allocation budget.
  *
  * The index is verified rather than the stream sequence on purpose: it
  * survives a transparent consumer recreate, so a run that healed correctly
  * still passes while a run that dropped a message does not.
  *
  * Note on interpretation: an ordered consumer is `AckPolicy.None` with flow
  * control, so the server pushes at line rate. A client that falls far enough
  * behind to miss a delivery recreates its consumer from the last in-order
  * stream sequence — correct, but it shows up as a throughput cliff rather than
  * an error. Compare runs at the same `n` and payload size, and prefer the
  * median of several runs; a loopback JetStream round trip is noisy.
  *
  * Prerequisites: a JetStream-enabled NATS server, e.g. `docker compose up -d`.
  *
  * Run:
  * {{{
  *   sbt "benchmarks/runMain fs2.nats.benchmarks.OrderedConsumerBench [numMessages] [payloadBytes] [host] [port]"
  *   # defaults: 100000 16 localhost 4222
  * }}}
  */
object OrderedConsumerBench extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val n = args.lift(0).flatMap(_.toIntOption).getOrElse(100000)
    val payloadSize = args.lift(1).flatMap(_.toIntOption).getOrElse(16)
    val host = args.lift(2).getOrElse("localhost")
    val port = args.lift(3).flatMap(_.toIntOption).getOrElse(4222)

    val config = ClientConfig(
      host = Host.fromString(host).get,
      port = Port.fromInt(port).get,
      queueCapacity = 1 << 16,
      slowConsumerPolicy = SlowConsumerPolicy.Block
    )
    val warm = math.min(n, 20000)

    NatsClient.connect[IO](config).use { client =>
      client.jetStream().use { js =>
        for
          _ <- IO.println(
            s"fs2-nats ordered-consumer bench -> $host:$port  (n=$n, payload=${payloadSize}B)"
          )
          _ <- IO.println(s"  warmup ($warm msgs)...")
          _ <- round(js, "ORDBENCHWARM", payloadSize, warm, report = false)
          _ <- IO.println(s"  measure ($n msgs)...")
          _ <- round(js, "ORDBENCH", payloadSize, n, report = true)
        yield ExitCode.Success
      }
    }

  /** Publish `n` indexed messages, then drain them back twice with each
    * consumer, interleaved.
    *
    * Two passes because both consumers read the same messages back to back: if
    * the first reader paid a cold-cache or server-side warm-up penalty that the
    * second did not, the two passes disagree and the comparison is void. They
    * are reported separately rather than averaged for exactly that reason.
    */
  private def round(
      js: JetStream[IO],
      streamName: String,
      payloadSize: Int,
      n: Int,
      report: Boolean
  ): IO[Unit] =
    val subject = s"$streamName.x"
    val setup = js.addStream(
      StreamConfig(name = streamName, subjects = List(s"$streamName.>"))
    )
    val teardown = js.deleteStream(streamName).attempt.void

    // The path under measurement.
    val ordered = js
      .subscribeOrdered(
        streamName,
        Some(subject),
        OrderedConsumerOptions(deliverPolicy = DeliverPolicy.All)
      )
      .use(drainVerified(_, n))

    // Control: same delivery guarantees (no ack, flow control, 5 s heartbeat),
    // but the plain push consumer's direct stream shape.
    val push = js
      .subscribePush(
        streamName,
        ConsumerConfig(
          deliverPolicy = DeliverPolicy.All,
          ackPolicy = AckPolicy.None,
          filterSubject = Some(subject),
          flowControl = true,
          idleHeartbeat = Some(5.seconds),
          inactiveThreshold = Some(5.minutes)
        )
      )
      .use(drainVerified(_, n))

    (setup *> {
      for
        _ <- publishIndexed(js, subject, payloadSize, n)
        o1 <- timed(ordered)
        p1 <- timed(push)
        o2 <- timed(ordered)
        p2 <- timed(push)
        _ <-
          if report then
            reportRate("subscribeOrdered        pass 1", n, payloadSize, o1) *>
              reportRate(
                "subscribeOrdered        pass 2",
                n,
                payloadSize,
                o2
              ) *>
              reportRate(
                "subscribePush (control) pass 1",
                n,
                payloadSize,
                p1
              ) *>
              reportRate("subscribePush (control) pass 2", n, payloadSize, p2)
          else IO.unit
      yield ()
    }).guarantee(teardown)

  private def timed(fa: IO[Unit]): IO[FiniteDuration] =
    (IO.monotonic, fa, IO.monotonic).mapN((t0, _, t1) => t1 - t0)

  /** Pipelined publish of `n` messages whose payload starts with their own
    * index, zero-padded and comma-terminated, then padded out to `payloadSize`.
    */
  private def publishIndexed(
      js: JetStream[IO],
      subject: String,
      payloadSize: Int,
      n: Int
  ): IO[Unit] =
    val pad = math.max(payloadSize, IndexWidth + 1)
    val filler = "x" * (pad - IndexWidth - 1)
    def body(i: Int): Chunk[Byte] =
      Chunk.array(s"%0${IndexWidth}d,%s".format(i, filler).getBytes)
    (0 until n).toList
      .traverse(i => js.publishAsync(subject, body(i)))
      .flatMap(_.sequence_)

  private val IndexWidth = 11

  /** Take exactly `n` messages and assert they are the indices `0..n-1`, in
    * order and without gaps.
    */
  private def drainVerified(msgs: Stream[IO, JsMessage[IO]], n: Int): IO[Unit] =
    IO(new SequenceCheck(n)).flatMap { check =>
      msgs
        .take(n.toLong)
        .chunks
        .foreach(c => IO(check.accept(c)))
        .compile
        .drain
        .timeout(2.minutes) *> IO(check.finish())
    }

  /** Verifier for the published index sequence. Plainly mutable: a compiled fs2
    * stream pulls sequentially, so exactly one chunk is in `accept` at a time.
    */
  private final class SequenceCheck(expected: Int):
    private var next = 0L

    def accept(chunk: Chunk[JsMessage[IO]]): Unit =
      var i = 0
      while i < chunk.size do
        val idx = indexOf(chunk(i).payload)
        if idx != next then
          throw new AssertionError(
            s"ordered consumer broke: expected index $next, got $idx"
          )
        next += 1
        i += 1

    def finish(): Unit =
      if next != expected.toLong then
        throw new AssertionError(s"expected $expected messages, got $next")

    private def indexOf(payload: Chunk[Byte]): Long =
      var i = 0
      var acc = 0L
      while i < payload.size && payload(i) != ','.toByte do
        acc = acc * 10 + (payload(i) - '0'.toByte)
        i += 1
      acc

  private def reportRate(
      label: String,
      n: Int,
      size: Int,
      elapsed: FiniteDuration
  ): IO[Unit] =
    val secs = elapsed.toNanos / 1e9
    val rate = n / secs
    val mb = (n.toLong * size).toDouble / (1024 * 1024) / secs
    IO.println(
      f"[$label] $n%,d msgs in $secs%.3f s => $rate%,.0f msgs/s, $mb%.1f MB/s"
    )
