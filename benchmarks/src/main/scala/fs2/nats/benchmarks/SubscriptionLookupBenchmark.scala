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
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import fs2.Chunk
import fs2.nats.protocol.Headers
import fs2.nats.subscriptions.NatsMessage
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.LongMap

/** Isolates the per-received-message subscription lookup and the surrounding
  * effect-graph shape in `SubscriptionManager.deliverMessage`.
  *
  * Two lookup structures, each holding the same subscriptions:
  *   - `Map[Long, _]` (before): the primitive sid is boxed at the call site and
  *     hashed generically. Sids below 128 hit `java.lang.Long`'s box cache;
  *     above it every lookup allocates.
  *   - `LongMap[_]` (after): primitive key, a `get` that is final on a sealed
  *     class, and a PATRICIA walk over `Bin` nodes.
  *
  * `*_mixed` rotates over maps of several different runtime classes (Map1..
  * Map4, HashMap), which is what the single production call site actually sees
  * as a client's subscription set grows and shrinks — the boxing is only part
  * of the cost, the megamorphic `get` is the rest. The fixed-size variants are
  * the optimistic monomorphic case a naive benchmark would report.
  *
  * The three `*Delivery` shapes attribute the two halves of the change
  * separately: `current` is the old graph (`Ref.get` -> `flatMap` -> `Ref.get`
  * -> `flatMap`), `mapVolatile` collapses only the state read into a plain
  * volatile read in the existing continuation while keeping `Map[Long, _]`, and
  * `proposed` is what ships (`LongMap` plus that volatile read). As with
  * `SendCoordinationBenchmark`, run the `run*` variants with `-prof gc` and
  * read `gc.alloc.rate.norm` (bytes/op) — `unsafeRunSync` dominates wall time
  * and carries a fixed fiber+latch floor, so compare deltas between the three
  * shapes, not absolute numbers.
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*SubscriptionLookupBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class SubscriptionLookupBenchmark:

  /** Mirror of `SubscriptionManager.SubState`. */
  private final class St(val active: Boolean, val remaining: Int)

  /** Mirror of today's `InternalSubscription`: state behind a `Ref`. */
  private final class SubRef(
      val sid: Long,
      val queue: Queue[IO, NatsMessage],
      val stateRef: Ref[IO, St]
  )

  /** Shipped shape: state in an `AtomicReference` field on the subscription,
    * read with a single volatile read and no effect node.
    */
  private final class SubVol(
      val sid: Long,
      val queue: Queue[IO, NatsMessage]
  ):
    private val stateRef = new AtomicReference[St](new St(true, -1))
    def state: St = stateRef.get()

  private val msg: NatsMessage =
    NatsMessage(
      "events.orders.created",
      None,
      Headers.empty,
      Chunk.array(Array.fill[Byte](16)('x'.toByte)),
      100000L
    )

  // Sids a long-lived client actually sees: the allocator never reuses ids, so
  // they climb past java.lang.Long's -128..127 box cache.
  private val highBase = 100000L
  private val lowBase = 1L

  private var map1Low: Map[Long, SubVol] = _
  private var map1High: Map[Long, SubVol] = _
  private var map8: Map[Long, SubVol] = _
  private var map64: Map[Long, SubVol] = _
  private var mixedMaps: Array[Map[Long, SubVol]] = _

  private var long1: LongMap[SubVol] = _
  private var long8: LongMap[SubVol] = _
  private var long64: LongMap[SubVol] = _
  private var mixedLongs: Array[LongMap[SubVol]] = _

  private var sids8: Array[Long] = _
  private var sids64: Array[Long] = _
  private var mixedSids: Array[Long] = _

  private var i8 = 0
  private var i64 = 0
  private var iMixed = 0

  // Shared full queue: `tryOffer` fails immediately, so the delivery-shape
  // benchmarks measure lookup + state read, not queue growth.
  private var fullQueue: Queue[IO, NatsMessage] = _
  private var subRef: SubRef = _
  private var subVol: SubVol = _
  private var subsRef: Ref[IO, Map[Long, SubRef]] = _
  private var mapRef: Ref[IO, Map[Long, SubVol]] = _
  private var longRef: Ref[IO, LongMap[SubVol]] = _

  private def mkSubs(base: Long, n: Int): Array[SubVol] =
    Array.tabulate(n)(k => new SubVol(base + k, fullQueue))

  @Setup
  def setup(): Unit =
    fullQueue = Queue.bounded[IO, NatsMessage](1).unsafeRunSync()
    fullQueue.offer(msg).unsafeRunSync()

    def maps(base: Long, n: Int): (Map[Long, SubVol], LongMap[SubVol]) =
      val subs = mkSubs(base, n)
      val m = subs.map(s => s.sid -> s).toMap
      val lm =
        subs.foldLeft(LongMap.empty[SubVol])((acc, s) => acc.updated(s.sid, s))
      (m, lm)

    map1Low = maps(lowBase, 1)._1
    map1High = maps(highBase, 1)._1
    val (m8, lm8) = maps(highBase, 8)
    val (m64, lm64) = maps(highBase, 64)
    map8 = m8
    map64 = m64
    long1 = maps(highBase, 1)._2
    long8 = lm8
    long64 = lm64

    sids8 = Array.tabulate(8)(k => highBase + k)
    sids64 = Array.tabulate(64)(k => highBase + k)

    // Map1, Map2, Map3, Map4 and HashMap all reaching the same call site.
    val sizes = Array(1, 2, 3, 4, 8, 16)
    mixedMaps = sizes.map(n => maps(highBase, n)._1)
    mixedLongs = sizes.map(n => maps(highBase, n)._2)
    mixedSids = sizes.map(n => highBase + n - 1)

    subVol = new SubVol(highBase, fullQueue)
    subRef = new SubRef(
      highBase,
      fullQueue,
      Ref.of[IO, St](new St(true, -1)).unsafeRunSync()
    )
    subsRef =
      Ref.of[IO, Map[Long, SubRef]](Map(highBase -> subRef)).unsafeRunSync()
    mapRef =
      Ref.of[IO, Map[Long, SubVol]](Map(highBase -> subVol)).unsafeRunSync()
    longRef = Ref
      .of[IO, LongMap[SubVol]](LongMap.empty[SubVol].updated(highBase, subVol))
      .unsafeRunSync()

  // ---------------------------------------------------------------- lookups

  @Benchmark
  def mapGet_1_lowSid(bh: Blackhole): Unit =
    map1Low.get(lowBase) match
      case Some(s) => bh.consume(s)
      case None    => bh.consume(0)

  @Benchmark
  def mapGet_1_highSid(bh: Blackhole): Unit =
    map1High.get(highBase) match
      case Some(s) => bh.consume(s)
      case None    => bh.consume(0)

  @Benchmark
  def longMapGet_1(bh: Blackhole): Unit =
    long1.get(highBase) match
      case Some(s) => bh.consume(s)
      case None    => bh.consume(0)

  @Benchmark
  def mapGet_8(bh: Blackhole): Unit =
    i8 = (i8 + 1) & 7
    map8.get(sids8(i8)) match
      case Some(s) => bh.consume(s)
      case None    => bh.consume(0)

  @Benchmark
  def longMapGet_8(bh: Blackhole): Unit =
    i8 = (i8 + 1) & 7
    long8.get(sids8(i8)) match
      case Some(s) => bh.consume(s)
      case None    => bh.consume(0)

  @Benchmark
  def mapGet_64(bh: Blackhole): Unit =
    i64 = (i64 + 1) & 63
    map64.get(sids64(i64)) match
      case Some(s) => bh.consume(s)
      case None    => bh.consume(0)

  @Benchmark
  def longMapGet_64(bh: Blackhole): Unit =
    i64 = (i64 + 1) & 63
    long64.get(sids64(i64)) match
      case Some(s) => bh.consume(s)
      case None    => bh.consume(0)

  @Benchmark
  def mapGet_mixed(bh: Blackhole): Unit =
    iMixed = (iMixed + 1) % mixedMaps.length
    mixedMaps(iMixed).get(mixedSids(iMixed)) match
      case Some(s) => bh.consume(s)
      case None    => bh.consume(0)

  @Benchmark
  def longMapGet_mixed(bh: Blackhole): Unit =
    iMixed = (iMixed + 1) % mixedLongs.length
    mixedLongs(iMixed).get(mixedSids(iMixed)) match
      case Some(s) => bh.consume(s)
      case None    => bh.consume(0)

  // -------------------------------------------------------- delivery shapes

  private val falseIO: IO[Boolean] = IO.pure(false)

  /** Before: `Ref.get` -> `flatMap` -> `Ref.get` -> `flatMap`. The inner effect
    * is built at run time inside the outer continuation.
    */
  private def currentDelivery(m: NatsMessage): IO[Boolean] =
    subsRef.get.flatMap { subs =>
      subs.get(m.sid) match
        case None      => falseIO
        case Some(sub) =>
          sub.stateRef.get.flatMap { st =>
            if !st.active then falseIO
            else if st.remaining < 0 then sub.queue.tryOffer(m)
            else falseIO
          }
    }

  /** What ships: `LongMap` lookup, then a single volatile read taken directly
    * in the existing continuation — no second effect node at all. Wrapping the
    * read in `IO.defer` would be observationally identical (the continuation is
    * re-entered on every run) but would cost a Delay plus a FlatMap per
    * message.
    */
  private def proposedDelivery(m: NatsMessage): IO[Boolean] =
    longRef.get.flatMap { subs =>
      subs.get(m.sid) match
        case None      => falseIO
        case Some(sub) =>
          val st = sub.state
          if !st.active then falseIO
          else if st.remaining < 0 then sub.queue.tryOffer(m)
          else falseIO
    }

  /** Halfway: the inner `Ref` collapses into one volatile read, but the lookup
    * structure is still `Map[Long, _]`. Attributes how much of the delta
    * belongs to the state change versus the map swap.
    */
  private def mapVolatileDelivery(m: NatsMessage): IO[Boolean] =
    mapRef.get.flatMap { subs =>
      subs.get(m.sid) match
        case None      => falseIO
        case Some(sub) =>
          val st = sub.state
          if !st.active then falseIO
          else if st.remaining < 0 then sub.queue.tryOffer(m)
          else falseIO
    }

  @Benchmark
  def buildMapVolatileDelivery(bh: Blackhole): Unit =
    bh.consume(mapVolatileDelivery(msg))

  @Benchmark
  def runMapVolatileDelivery(bh: Blackhole): Unit =
    bh.consume(mapVolatileDelivery(msg).unsafeRunSync())

  @Benchmark
  def buildCurrentDelivery(bh: Blackhole): Unit =
    bh.consume(currentDelivery(msg))

  @Benchmark
  def buildProposedDelivery(bh: Blackhole): Unit =
    bh.consume(proposedDelivery(msg))

  @Benchmark
  def runCurrentDelivery(bh: Blackhole): Unit =
    bh.consume(currentDelivery(msg).unsafeRunSync())

  @Benchmark
  def runProposedDelivery(bh: Blackhole): Unit =
    bh.consume(proposedDelivery(msg).unsafeRunSync())
