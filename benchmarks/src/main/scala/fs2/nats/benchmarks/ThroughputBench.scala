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
import scala.concurrent.duration.*

/** End-to-end throughput and latency harness against a live NATS server.
  *
  * Exercises the full publish path (serialize -> enqueue -> coalesced write) and
  * receive path (parse -> route -> deliver) over a real socket, using the
  * server's echo so a single connection both publishes and consumes.
  *
  * Prerequisites: a NATS server on the target host (e.g. `docker compose up -d`).
  *
  * Run:
  * {{{
  *   sbt "benchmarks/runMain fs2.nats.benchmarks.ThroughputBench [numMessages] [payloadBytes] [host] [port]"
  *   # defaults: 200000 16 localhost 4222
  * }}}
  *
  * Reports throughput (msgs/s, MB/s) and round-trip latency percentiles. To
  * compare against the pre-optimization baseline, run on `main` vs this branch.
  */
object ThroughputBench extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val n = args.lift(0).flatMap(_.toIntOption).getOrElse(200000)
    val payloadSize = args.lift(1).flatMap(_.toIntOption).getOrElse(16)
    val host = args.lift(2).getOrElse("localhost")
    val port = args.lift(3).flatMap(_.toIntOption).getOrElse(4222)

    val config = ClientConfig(
      host = Host.fromString(host).get,
      port = Port.fromInt(port).get,
      // large buffer + backpressure so the receive side never silently drops
      queueCapacity = 1 << 16,
      slowConsumerPolicy = SlowConsumerPolicy.Block
    )
    val payload = Chunk.array(Array.fill[Byte](payloadSize)('x'.toByte))

    NatsClient.connect[IO](config).use { client =>
      for
        _ <- IO.println(
          s"fs2-nats bench -> $host:$port  (n=$n, payload=${payloadSize}B)"
        )
        _ <- throughput(client, payload, n)
        _ <- latency(client, payload, math.min(n, 20000))
      yield ExitCode.Success
    }

  /** Publish k messages while concurrently consuming k, timing the whole round. */
  private def runRound(
      client: NatsClient[IO],
      stream: Stream[IO, ?],
      subject: String,
      payload: Chunk[Byte],
      k: Int
  ): IO[FiniteDuration] =
    for
      consumer <- stream.take(k.toLong).compile.drain.start
      start <- IO.monotonic
      _ <- Stream
        .repeatEval(client.publish(subject, payload))
        .take(k.toLong)
        .compile
        .drain
      _ <- consumer.join
      end <- IO.monotonic
    yield end - start

  private def throughput(
      client: NatsClient[IO],
      payload: Chunk[Byte],
      n: Int
  ): IO[Unit] =
    val subject = s"bench.tput.${System.nanoTime()}"
    client.subscribe(subject).use { stream =>
      val warmupN = math.min(n, 20000)
      for
        _ <- IO.sleep(250.millis) // let the SUB register server-side
        _ <- IO.println(s"  throughput warmup ($warmupN msgs)...")
        _ <- runRound(client, stream, subject, payload, warmupN)
        _ <- IO.println(s"  throughput measure ($n msgs)...")
        elapsed <- runRound(client, stream, subject, payload, n)
        _ <- report("throughput", n, payload.size, elapsed)
      yield ()
    }

  private def latency(
      client: NatsClient[IO],
      payload: Chunk[Byte],
      iterations: Int
  ): IO[Unit] =
    val subject = s"bench.rtt.${System.nanoTime()}"
    client.subscribe(subject).use { stream =>
      for
        _ <- IO.sleep(250.millis)
        _ <- IO.println("  latency warmup...")
        _ <- roundTrip(client, stream, subject, payload).replicateA_(200)
        _ <- IO.println(s"  latency measure ($iterations round-trips)...")
        samples <- (0 until iterations).toVector.traverse(_ =>
          roundTrip(client, stream, subject, payload)
        )
        _ <- reportLatency(samples)
      yield ()
    }

  /** One synchronous publish -> receive round trip, returning the elapsed nanos. */
  private def roundTrip(
      client: NatsClient[IO],
      stream: Stream[IO, ?],
      subject: String,
      payload: Chunk[Byte]
  ): IO[Long] =
    for
      t0 <- IO.monotonic
      _ <- client.publish(subject, payload)
      _ <- stream.head.compile.lastOrError
      t1 <- IO.monotonic
    yield (t1 - t0).toNanos

  private def report(
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
        f"[latency] n=$count%,d mean=$meanUs%.1fµs p50=${us(at(50))}%.1fµs " +
          f"p99=${us(at(99))}%.1fµs p99.9=${us(at(99.9))}%.1fµs max=${us(sorted.last)}%.1fµs"
      )
