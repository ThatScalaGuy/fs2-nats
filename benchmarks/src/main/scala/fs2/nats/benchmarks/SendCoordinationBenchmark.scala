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

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import fs2.Chunk
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit

/** Isolates the per-publish connection-phase coordination in
  * `ConnectionManager.send`.
  *
  * The old path ran `Ref.modify` returning an `Either[_, Option[Transport]]`,
  * allocating a tuple + `Right` + `Some` (and a no-op CAS write-back) on EVERY
  * Online send. The new path does a plain `Ref.get` — a single volatile read
  * with no allocation — and only falls back to `modify` when not Online.
  *
  * The `Phase` ADT here mirrors the shape of the (package-private) `ConnPhase`
  * so the only difference between the two benchmark methods is exactly the
  * coordination change. Run with `-prof gc` and compare
  * `gc.alloc.rate.norm` (bytes/op): the delta is the garbage removed from the
  * publish hot path.
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*SendCoordinationBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class SendCoordinationBenchmark:

  // Mirror of ConnPhase[F] (which is private[client]); same allocation shape.
  private sealed trait Phase
  private final case class Online(send: Chunk[Byte] => IO[Unit]) extends Phase
  private final case class Offline(pending: Vector[Chunk[Byte]], bytes: Long)
      extends Phase
  private final case class Closed() extends Phase

  private val bytes: Chunk[Byte] =
    Chunk.array("hello nats hello".getBytes)

  // Transport.send stand-in: a no-op so the benchmark measures only the
  // coordination wrapper, not socket/queue work (common to both methods).
  private val noop: Chunk[Byte] => IO[Unit] = _ => IO.unit

  private var ref: Ref[IO, Phase] = _

  @Setup
  def setup(): Unit =
    ref = Ref.of[IO, Phase](Online(noop)).unsafeRunSync()

  /** Old hot path: modify-with-Either, even though Online never changes state. */
  @Benchmark
  def modifyPath(bh: Blackhole): Unit =
    val io = ref
      .modify[Either[Throwable, Option[Chunk[Byte] => IO[Unit]]]] {
        case o @ Online(s) =>
          (o, Right(Some(s)))
        case Closed() =>
          (Closed(), Left(new RuntimeException("closed")))
        case off @ Offline(buf, sz) =>
          (Offline(buf :+ bytes, sz + bytes.size), Right(None))
      }
      .flatMap {
        case Right(Some(s)) => s(bytes)
        case Right(None)    => IO.unit
        case Left(err)      => IO.raiseError(err)
      }
    bh.consume(io.unsafeRunSync())

  /** New hot path: a single `get`, dispatch straight to the transport. */
  @Benchmark
  def getPath(bh: Blackhole): Unit =
    val io = ref.get.flatMap {
      case Online(s) => s(bytes)
      case _         => IO.unit // Offline/Closed slow path (unused while Online)
    }
    bh.consume(io.unsafeRunSync())
