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
import fs2.Chunk
import fs2.nats.client.{ClientConfig, NatsClient, SlowConsumerPolicy}
import fs2.nats.jetstream.*
import fs2.nats.jetstream.protocol.*
import scala.concurrent.duration.*

/** End-to-end JetStream throughput and latency harness against a live,
  * JetStream-enabled NATS server.
  *
  * Measures three data-plane paths layered on the core client:
  *   - pipelined publish (`publishAsync`) throughput — PubAck per message
  *   - pull `consume` throughput, including a fire-and-forget ack per message
  *   - synchronous `publish` round-trip latency (publish -> PubAck)
  *
  * Prerequisites: a JetStream-enabled NATS server, e.g. `docker compose up -d`
  * (the compose file runs `nats -js`).
  *
  * Run:
  * {{{
  *   sbt "benchmarks/runMain fs2.nats.benchmarks.JetStreamBench [numMessages] [payloadBytes] [host] [port]"
  *   # defaults: 100000 16 localhost 4222
  * }}}
  */
object JetStreamBench extends IOApp:

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
    val payload = Chunk.array(Array.fill[Byte](payloadSize)('x'.toByte))

    NatsClient.connect[IO](config).use { client =>
      client.jetStream().use { js =>
        for
          _ <- IO.println(
            s"fs2-nats JetStream bench -> $host:$port  (n=$n, payload=${payloadSize}B)"
          )
          // Warm the JIT on a throwaway stream, then measure.
          _ <- IO.println(s"  warmup (${math.min(n, 20000)} msgs)...")
          _ <- pubConsumeRound(js, "BENCHWARM", payload, math.min(n, 20000))
          _ <- IO.println(s"  measure ($n msgs)...")
          _ <- pubConsumeRound(js, "BENCH", payload, n, report = true)
          _ <- publishLatency(js, payload, math.min(n, 20000))
        yield ExitCode.Success
      }
    }

  /** Create a stream, pipeline-publish `n` messages, then pull-consume and ack
    * all `n`, timing each phase independently.
    */
  private def pubConsumeRound(
      js: JetStream[IO],
      streamName: String,
      payload: Chunk[Byte],
      n: Int,
      report: Boolean = false
  ): IO[Unit] =
    val subject = s"$streamName.x"
    val setup = js.addStream(
      StreamConfig(name = streamName, subjects = List(s"$streamName.>"))
    )
    val teardown = js.deleteStream(streamName).attempt.void

    (setup *> {
      for
        // ---- pipelined publish (PubAck per message) ----
        p0 <- IO.monotonic
        acks <- (0 until n).toList.traverse(_ =>
          js.publishAsync(subject, payload)
        )
        _ <- acks.sequence_
        p1 <- IO.monotonic

        // ---- pull consume + ack ----
        consumer <- js.createConsumer(
          streamName,
          ConsumerConfig(
            durable = Some("bench"),
            ackPolicy = AckPolicy.Explicit
          )
        )
        c0 <- IO.monotonic
        _ <- consumer
          .consume(
            ConsumeOptions(
              maxMessages = 1000,
              expires = 10.seconds,
              idleHeartbeat = 2.seconds
            )
          )
          .use(_.evalMap(_.ack).take(n.toLong).compile.drain)
        c1 <- IO.monotonic

        _ <-
          if report then
            reportRate("publishAsync", n, payload.size, p1 - p0) *>
              reportRate("consume+ack", n, payload.size, c1 - c0)
          else IO.unit
      yield ()
    }).guarantee(teardown)

  /** Synchronous `publish` round-trip latency (publish -> PubAck). */
  private def publishLatency(
      js: JetStream[IO],
      payload: Chunk[Byte],
      iterations: Int
  ): IO[Unit] =
    val streamName = "BENCHLAT"
    val subject = s"$streamName.x"
    js.addStream(
      StreamConfig(name = streamName, subjects = List(s"$streamName.>"))
    ).bracket { _ =>
      val one = IO.monotonic
        .flatMap(t0 => js.publish(subject, payload) *> IO.monotonic.map(_ - t0))
        .map(_.toNanos)
      for
        _ <- one.replicateA_(200) // warmup
        samples <- (0 until iterations).toVector.traverse(_ => one)
        _ <- reportLatency(samples)
      yield ()
    }(_ => js.deleteStream(streamName).attempt.void)

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

  private def reportLatency(samplesNanos: Vector[Long]): IO[Unit] =
    if samplesNanos.isEmpty then IO.unit
    else
      val sorted = samplesNanos.sorted
      val count = sorted.size
      def at(p: Double): Long =
        sorted(math.min(count - 1, math.max(0, (p / 100.0 * count).toInt)))
      def us(nanos: Long): Double = nanos / 1000.0
      val meanUs = sorted.sum.toDouble / count / 1000.0
      IO.println(
        f"[publish latency] n=$count%,d mean=$meanUs%.1fµs p50=${us(at(50))}%.1fµs " +
          f"p99=${us(at(99))}%.1fµs p99.9=${us(at(99.9))}%.1fµs max=${us(sorted.last)}%.1fµs"
      )
