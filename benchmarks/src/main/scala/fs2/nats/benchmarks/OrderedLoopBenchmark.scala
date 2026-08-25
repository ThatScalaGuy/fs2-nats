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
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.nats.protocol.Headers
import fs2.nats.subscriptions.NatsMessage
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.*

/** Mirror of `fs2.nats.jetstream.OrderedState` (which is `private[jetstream]`
  * and therefore invisible here).
  */
private[benchmarks] final case class LoopState(
    expectedConsumerSeq: Long,
    lastStreamSeq: Long,
    cycle: Long
)

/** The same state with the current cycle's consumer name folded in — the
  * one-`Ref` variant the change proposes.
  */
private[benchmarks] final case class NamedLoopState(
    expectedConsumerSeq: Long,
    lastStreamSeq: Long,
    cycle: Long,
    consumer: String
)

/** Outcome of the one-`Ref` state transition. All cases are parameterless,
  * hence singletons, so carrying the decision out of a `Ref.modify` allocates
  * nothing — which is the point of returning it rather than closing over the
  * effect inside the `modify`.
  */
private[benchmarks] enum LoopStep:
  case Deliver, Drop, Recreate

/** Isolates the *delivery loop shape* of the ordered push consumer
  * (`JetStream.subscribeOrderedWithInfo`) — no server, no JetStream, no
  * `buildJsMessage`.
  *
  * The loop today is
  * {{{
  * delivery.map(Option(_)).mergeHaltL(ticks).evalMapFilter {
  *   case Some(m) => onMessage(m)   // 3 Ref ops per message
  *   case None    => livenessCheck.as(None)
  * }
  * }}}
  * so that the liveness tick and the data share one sequential pull and need no
  * further synchronization. In fs2 3.13.0 `mergeHaltL` expands to
  * `noneTerminate.merge(that.map(Some(_))).unNoneTerminate`, so the bill is
  * **two** `Option` allocations plus an unwrap per element, and per *chunk*
  * three fresh `Chunk`s, an `indexWhere` scan, a `Channel.synchronous`
  * rendezvous and a `Semaphore` round trip — plus three fibers per materialized
  * stream. KV `watch`/`keys`/`history` and Object Store `get`/`list`/`watch`
  * all run through it.
  *
  * The four shapes are laid out so each step of the proposed change is
  * attributable from one JVM run (on this machine a before/after commit pair is
  * not comparable — throughput error bars reach ±40%):
  *
  *   - `mergeOptionTwoRefs` — what ships today.
  *   - `mergeSentinelTwoRefs` — drops only the explicit `.map(Option(_))` in
  *     favour of a reference-identity tick sentinel (the trick
  *     `SubscriptionManager.PoisonPill` already uses), keeping the merge and
  *     both `Ref`s. The delta to the previous shape is the `Option` wrap alone.
  *   - `mergeSentinelSingleRef` — also folds `nameRef` into the state `Ref` and
  *     makes the per-message transition one atomic `modify` instead of a
  *     `get`/`get`/`set` triple, and keeps the liveness timestamp in a plain
  *     `AtomicLong` (it is written and read from the same sequential pull). The
  *     delta is the `Ref` traffic alone.
  *   - `concurrentlySingleRef` — the alternative the issue suggests: no merge
  *     at all, liveness moved to a `.concurrently` fiber. The delta to the
  *     previous shape is what the merge machinery itself costs, i.e. the
  *     ceiling on what giving up the sequential loop could buy.
  *   - `directNoLiveness` — floor: the same handler with no liveness at all.
  *
  * `chunkSize` is the number of deliveries fs2 hands over per pull. It matters
  * because the merge is charged per *chunk* while the `Option` wrap and the
  * `Ref` traffic are charged per *element*: the delivery stream is
  * `Stream.fromQueueUnterminated` (`take` then `tryTakeN`), so its chunk size
  * self-tunes upward with load and the rendezvous amortizes precisely when
  * throughput is high. `1` is an idle or slow subject; `64` is a saturated one.
  *
  * ==Why the tick interval is one hour==
  * `Stream.awakeEvery` makes the naive version of this benchmark wall-clock
  * dependent: ticks per op = op duration / interval, so a loaded machine or a
  * larger `msgCount` silently injects more tick work and the delta between the
  * shapes stops being a property of the code. At `1.hour` the merge machinery,
  * the fibers and the timer registration are all still built and torn down
  * every op, but zero ticks ever fire. That is also the production ratio: a 5 s
  * heartbeat against thousands of messages per second puts ticks below 0.001%
  * of elements, so what is being measured is what liveness costs the *data*
  * path, which is the whole question.
  *
  * Each op resets the state and asserts it saw exactly `msgCount` messages, so
  * a shape that silently drops deliveries fails instead of posting a fake win.
  *
  * Run with `-prof gc` and read `gc.alloc.rate.norm` (bytes/op): as in
  * `FrameDispatchBenchmark`, `unsafeRunSync` dominates wall time and carries a
  * fixed fiber+latch floor, so compare deltas between the shapes, not absolute
  * numbers.
  *
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*OrderedLoopBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class OrderedLoopBenchmark:

  @Param(Array("1", "8", "64"))
  var chunkSize: Int = 8

  private val msgCount = 2000

  /** Deliberately far longer than any op — see the class Scaladoc. */
  private val tickInterval: FiniteDuration = 1.hour

  private val consumerName = "ordered-consumer-XYZ123"

  /** Sentinel tick, distinguished by reference identity only. */
  private val livenessTick: NatsMessage =
    NatsMessage("", None, Headers.empty, Chunk.empty, -1L)

  private var chunks: List[Chunk[NatsMessage]] = Nil

  private var state: Ref[IO, LoopState] = null
  private var nameRef: Ref[IO, String] = null
  private var lastSeenRef: Ref[IO, FiniteDuration] = null

  private var namedState: Ref[IO, NamedLoopState] = null
  private val lastSeen = new AtomicLong(0L)

  @Setup
  def setup(): Unit =
    val payload = Chunk.array(Array.fill[Byte](16)('x'.toByte))
    // `subject` stands in for the delivering consumer's name and `sid` for both
    // the per-consumer and the stream sequence, so every message takes the
    // in-order Emit branch — the realistic branch mix — without paying for the
    // reply-to metadata parse, which is identical in every shape and would only
    // dilute the delta.
    val msgs = (0 until msgCount).toList.map(i =>
      NatsMessage(consumerName, None, Headers.empty, payload, (i + 1).toLong)
    )
    chunks = msgs.grouped(chunkSize).map(Chunk.from).toList

    state = Ref.unsafe[IO, LoopState](initialState)
    // A distinct-but-equal String, so the name check is a real character
    // comparison rather than the reference short-circuit inside
    // `String.equals`; the live code compares a freshly parsed name too.
    nameRef = Ref.unsafe[IO, String](new String(consumerName.toCharArray))
    lastSeenRef = Ref.unsafe[IO, FiniteDuration](0.nanos)
    namedState = Ref.unsafe[IO, NamedLoopState](initialNamedState)

  private def initialState: LoopState = LoopState(1L, 0L, 0L)

  private def initialNamedState: NamedLoopState =
    NamedLoopState(1L, 0L, 0L, new String(consumerName.toCharArray))

  private def source: Stream[IO, NatsMessage] =
    Stream.emits(chunks).unchunks

  /** Stand-in for the real `recreate`, which deletes and recreates the
    * server-side consumer. Never reached here (no gaps are injected); present
    * so both handlers keep the same branch structure.
    */
  private val recreate: IO[Unit] = IO.unit

  private val noMessage: IO[Option[NatsMessage]] = IO.pure(None)

  // ------------------------------------------------------------- handlers

  /** Today's per-message work: refresh the liveness timestamp, then
    * `nameRef.get` + `state.get` + `state.set`.
    */
  private def onDataTwoRefs(m: NatsMessage): IO[Option[NatsMessage]] =
    IO.monotonic.flatMap(lastSeenRef.set) *>
      (nameRef.get, state.get).flatMapN { (curName, st) =>
        // Drop deliveries from a previous cycle (arriving after a recreate).
        if m.subject != curName then noMessage
        else if m.sid == st.expectedConsumerSeq then
          state
            .set(
              st.copy(
                expectedConsumerSeq = st.expectedConsumerSeq + 1,
                lastStreamSeq = m.sid
              )
            )
            .as(Some(m))
        else if m.sid > st.expectedConsumerSeq then recreate.as(None)
        else noMessage
      }

  /** One `Ref`, one atomic `modify`, and the liveness timestamp as a plain
    * volatile store rather than a second effect node.
    */
  private def onDataSingleRef(m: NatsMessage): IO[Option[NatsMessage]] =
    IO.monotonic.flatMap { now =>
      lastSeen.set(now.toNanos)
      namedState
        .modify(st => step(st, m.subject, m.sid, m.sid))
        .flatMap {
          case LoopStep.Deliver  => IO.pure(Some(m))
          case LoopStep.Drop     => noMessage
          case LoopStep.Recreate => recreate.as(None)
        }
    }

  /** The whole per-message transition as one pure function, so it fits in a
    * single `Ref.modify`.
    */
  private def step(
      st: NamedLoopState,
      msgConsumer: String,
      msgConsumerSeq: Long,
      msgStreamSeq: Long
  ): (NamedLoopState, LoopStep) =
    if msgConsumer != st.consumer then (st, LoopStep.Drop)
    else if msgConsumerSeq == st.expectedConsumerSeq then
      (
        st.copy(
          expectedConsumerSeq = st.expectedConsumerSeq + 1,
          lastStreamSeq = msgStreamSeq
        ),
        LoopStep.Deliver
      )
    else if msgConsumerSeq > st.expectedConsumerSeq then (st, LoopStep.Recreate)
    else (st, LoopStep.Drop)

  // ---------------------------------------------------------- liveness

  private val livenessTimeout: FiniteDuration = 11.seconds

  // `def`, not `val`: the `Ref`s these close over are built in `@Setup`, which
  // runs after the instance is constructed. They are never actually invoked (no
  // tick fires), so rebuilding the effect costs nothing.
  private def livenessCheckTwoRefs: IO[Unit] =
    (IO.monotonic, lastSeenRef.get).flatMapN { (now, seen) =>
      if now - seen > livenessTimeout then
        recreate *> IO.monotonic.flatMap(lastSeenRef.set)
      else IO.unit
    }

  private def livenessCheckSingleRef: IO[Unit] =
    IO.monotonic.flatMap { now =>
      if now.toNanos - lastSeen.get() > livenessTimeout.toNanos then
        recreate *> IO.monotonic.flatMap(n => IO(lastSeen.set(n.toNanos)))
      else IO.unit
    }

  // ------------------------------------------------------------- shapes

  /** Ships today: `Option`-wrapped merge, three `Ref` ops per message. */
  @Benchmark
  def mergeOptionTwoRefs(bh: Blackhole): Unit =
    val ticks =
      Stream.awakeEvery[IO](tickInterval).as(Option.empty[NatsMessage])
    val ordered = source
      .map(Option(_))
      .mergeHaltL(ticks)
      .evalMapFilter {
        case Some(m) => onDataTwoRefs(m)
        case None    => livenessCheckTwoRefs.as(None)
      }
    bh.consume(measure(resetTwoRefs, ordered))

  /** Sentinel tick instead of the `Option` wrap; merge and `Ref`s unchanged. */
  @Benchmark
  def mergeSentinelTwoRefs(bh: Blackhole): Unit =
    val ticks = Stream.awakeEvery[IO](tickInterval).as(livenessTick)
    val ordered = source.mergeHaltL(ticks).evalMapFilter { m =>
      if m eq livenessTick then livenessCheckTwoRefs.as(None)
      else onDataTwoRefs(m)
    }
    bh.consume(measure(resetTwoRefs, ordered))

  /** Sentinel tick plus the single-`Ref`, single-`modify` handler. */
  @Benchmark
  def mergeSentinelSingleRef(bh: Blackhole): Unit =
    val ticks = Stream.awakeEvery[IO](tickInterval).as(livenessTick)
    val ordered = source.mergeHaltL(ticks).evalMapFilter { m =>
      if m eq livenessTick then livenessCheckSingleRef.as(None)
      else onDataSingleRef(m)
    }
    bh.consume(measure(resetSingleRef, ordered))

  /** No merge: the delivery stream stays direct and liveness runs in a
    * background fiber. Measures the ceiling — and gives up the mutual exclusion
    * the single sequential pull provides for free.
    */
  @Benchmark
  def concurrentlySingleRef(bh: Blackhole): Unit =
    val ordered = source
      .evalMapFilter(onDataSingleRef)
      .concurrently(
        Stream.awakeEvery[IO](tickInterval).evalMap(_ => livenessCheckSingleRef)
      )
    bh.consume(measure(resetSingleRef, ordered))

  /** Floor: no liveness at all. The gap to `concurrentlySingleRef` is the
    * irreducible cost of keeping a liveness fiber alive per subscription.
    */
  @Benchmark
  def directNoLiveness(bh: Blackhole): Unit =
    bh.consume(measure(resetSingleRef, source.evalMapFilter(onDataSingleRef)))

  // ------------------------------------------------------------ controls

  /** Control: what the runtime allocates while a fiber is simply parked, and
    * nothing else runs.
    *
    * Every shape above that keeps a tick fiber alive allocates at a nearly
    * constant ~460 MB/s no matter how many chunks it processes, which is the
    * signature of garbage that is proportional to WALL TIME rather than to
    * work. `gc.alloc.rate.norm` is bytes/op = alloc rate / ops per second, so
    * any such background churn is silently billed to whichever shape is
    * slowest. This benchmark prices it: divide its bytes/op by 25 to get the
    * per-millisecond churn on this machine, multiply by another shape's ms/op,
    * and subtract before reading its bytes/op as "what the loop allocates".
    *
    * The sleep is 25 ms so the op lands in the same range as the slower shapes
    * and JMH's per-op overhead stays negligible.
    */
  @Benchmark
  def parkedFloor(bh: Blackhole): Unit =
    bh.consume(IO.sleep(25.millis).unsafeRunSync())

  /** Control: the direct loop plus a background fiber that never ticks and
    * carries no timer.
    *
    * `concurrentlySingleRef` costs a flat ~20 ms per op over `directNoLiveness`
    * that does not move with `chunkSize`, i.e. it is charged once per
    * materialized stream, not per message. That matters in production because
    * Object Store `get` and KV `history`/`keys` materialize a fresh ordered
    * consumer per call. This isolates whether that fixed cost comes from
    * starting/cancelling the background fiber at all or from the pending
    * `awakeEvery` timer it is blocked on.
    */
  @Benchmark
  def concurrentlyIdleFiber(bh: Blackhole): Unit =
    val ordered = source
      .evalMapFilter(onDataSingleRef)
      .concurrently(Stream.eval(IO.never[Unit]))
    bh.consume(measure(resetSingleRef, ordered))

  // -------------------------------------------------------------- runner

  private def resetTwoRefs: IO[Unit] =
    state.set(initialState) *> lastSeenRef.set(0.nanos)

  private def resetSingleRef: IO[Unit] =
    namedState.set(initialNamedState) *> IO(lastSeen.set(0L))

  /** Runs one op and asserts it emitted every message: the sequence tracking is
    * stateful, so a shape that drops or duplicates a delivery would otherwise
    * just look faster.
    */
  private def measure(reset: IO[Unit], s: Stream[IO, NatsMessage]): Long =
    val emitted = (reset *> s.compile.count).unsafeRunSync()
    if emitted != msgCount.toLong then
      throw new AssertionError(s"expected $msgCount messages, got $emitted")
    emitted
