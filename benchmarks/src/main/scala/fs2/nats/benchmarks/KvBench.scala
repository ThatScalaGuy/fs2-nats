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
import fs2.nats.kv.*
import scala.concurrent.duration.*

/** End-to-end KV throughput and latency harness against a live,
  * JetStream-enabled NATS server.
  *
  * Measures three KV paths:
  *   - pipelined `putAsync` throughput (PubAck/revision per write)
  *   - synchronous `put` round-trip latency
  *   - `get` latency on the Direct Get fast path (`allow_direct = true`) versus
  *     the `STREAM.MSG.GET` fallback (`allow_direct = false`) — the headline KV
  *     read number
  *
  * Prerequisites: a JetStream-enabled NATS server, e.g. `docker compose up -d`.
  *
  * Run:
  * {{{
  *   sbt "benchmarks/runMain fs2.nats.benchmarks.KvBench [numMessages] [payloadBytes] [host] [port]"
  *   # defaults: 50000 16 localhost 4222
  * }}}
  */
object KvBench extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val n = args.lift(0).flatMap(_.toIntOption).getOrElse(50000)
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
    val warm = math.min(n, 10000)
    val latIters = math.min(n, 20000)

    NatsClient.connect[IO](config).use { client =>
      client.jetStream().use { js =>
        for
          _ <- IO.println(
            s"fs2-nats KV bench -> $host:$port  (n=$n, payload=${payloadSize}B)"
          )
          _ <- IO.println(s"  warmup ($warm writes)...")
          _ <- putThroughput(js, "KVBENCHWARM", payload, warm, report = false)
          _ <- IO.println(s"  measure ($n writes)...")
          _ <- putThroughput(js, "KVBENCH", payload, n, report = true)
          _ <- putLatency(js, payload, latIters)
          _ <- getLatency(js, payload, latIters, allowDirect = true)
          _ <- getLatency(js, payload, latIters, allowDirect = false)
        yield ExitCode.Success
      }
    }

  /** Pipeline `putAsync` `n` distinct keys, awaiting all revisions. */
  private def putThroughput(
      js: JetStream[IO],
      bucket: String,
      payload: Chunk[Byte],
      n: Int,
      report: Boolean
  ): IO[Unit] =
    js.createKeyValue(KvConfig(bucket)).flatMap { kv =>
      (for
        p0 <- IO.monotonic
        revs <- (0 until n).toList.traverse(i => kv.putAsync(s"k$i", payload))
        _ <- revs.sequence_
        p1 <- IO.monotonic
        _ <-
          if report then reportRate("putAsync", n, payload.size, p1 - p0)
          else IO.unit
      yield ()).guarantee(js.deleteKeyValue(bucket).attempt.void)
    }

  /** Synchronous `put` round-trip latency (write -> revision). */
  private def putLatency(
      js: JetStream[IO],
      payload: Chunk[Byte],
      iterations: Int
  ): IO[Unit] =
    js.createKeyValue(KvConfig("KVBENCHPUTLAT")).flatMap { kv =>
      val one = IO.monotonic
        .flatMap(t0 => kv.put("lat", payload) *> IO.monotonic.map(_ - t0))
        .map(_.toNanos)
      (for
        _ <- one.replicateA_(200)
        samples <- (0 until iterations).toVector.traverse(_ => one)
        _ <- reportLatency("put latency", samples)
      yield ()).guarantee(js.deleteKeyValue("KVBENCHPUTLAT").attempt.void)
    }

  /** `get` latency on the Direct Get fast path vs the `STREAM.MSG.GET`
    * fallback, selected by the bucket's `allow_direct` flag.
    */
  private def getLatency(
      js: JetStream[IO],
      payload: Chunk[Byte],
      iterations: Int,
      allowDirect: Boolean
  ): IO[Unit] =
    val bucket = if allowDirect then "KVBENCHGETD" else "KVBENCHGETM"
    val label = if allowDirect then "get latency (direct)" else "get latency (msg.get)"
    js.createKeyValue(KvConfig(bucket, allowDirect = allowDirect)).flatMap { kv =>
      val one = IO.monotonic
        .flatMap(t0 => kv.get("lat") *> IO.monotonic.map(_ - t0))
        .map(_.toNanos)
      (for
        _ <- kv.put("lat", payload)
        _ <- one.replicateA_(200)
        samples <- (0 until iterations).toVector.traverse(_ => one)
        _ <- reportLatency(label, samples)
      yield ()).guarantee(js.deleteKeyValue(bucket).attempt.void)
    }

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
      f"[$label] $n%,d writes in $secs%.3f s => $rate%,.0f writes/s, $mb%.1f MB/s"
    )

  private def reportLatency(
      label: String,
      samplesNanos: Vector[Long]
  ): IO[Unit] =
    if samplesNanos.isEmpty then IO.unit
    else
      val sorted = samplesNanos.sorted
      val count = sorted.size
      def at(p: Double): Long =
        sorted(math.min(count - 1, math.max(0, (p / 100.0 * count).toInt)))
      def us(nanos: Long): Double = nanos / 1000.0
      val meanUs = sorted.sum.toDouble / count / 1000.0
      IO.println(
        f"[$label] n=$count%,d mean=$meanUs%.1fµs p50=${us(at(50))}%.1fµs " +
          f"p99=${us(at(99))}%.1fµs p99.9=${us(at(99.9))}%.1fµs max=${us(sorted.last)}%.1fµs"
      )
