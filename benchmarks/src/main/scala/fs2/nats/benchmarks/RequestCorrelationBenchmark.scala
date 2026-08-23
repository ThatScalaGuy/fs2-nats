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

import fs2.nats.util.Tokens
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec

/** Isolates the per-request correlation bookkeeping in `Requestor.request` plus
  * the reply-side removal in `Requestor.handleReply`.
  *
  * One request touches the shared correlation state four times: a token draw
  * from `Ref[F, Long]`, an insert into `Ref[F, Map[String, Deferred]]`, the
  * drain fiber's `modify`-remove when the reply lands, and the guaranteed
  * `update`-remove. Every one of those is a `SyncRef` spin: read the
  * `AtomicReference`, rebuild the immutable value, `compareAndSet`, retry the
  * whole thing on failure. The `*Ref` methods reproduce that spin exactly (see
  * `cats.effect.kernel.SyncRef`); the `*Chm` methods are the same four steps
  * over a `ConcurrentHashMap` plus an `AtomicLong`.
  *
  * `inFlight` is the steady-state size of the correlation map — the pending
  * window a pipelined publisher keeps open (`publishAsyncMaxPending` defaults
  * to 256). It is what decides how much an immutable-map insert copies and how
  * much a lost CAS throws away.
  *
  * Run the raw shapes at several thread counts; the whole point of the change
  * is the shape of the curve, not the single-threaded number:
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc -t 1 .*RequestCorrelationBenchmark.*RoundTrip"
  *   sbt "benchmarks/Jmh/run -prof gc -t 8 .*RequestCorrelationBenchmark.*RoundTrip"
  * }}}
  */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class RequestCorrelationBenchmark:

  /** Steady-state number of in-flight requests held in the correlation map. */
  @Param(Array("1", "32", "256"))
  var inFlight: Int = _

  private val inboxPrefix = "_INBOX.pQrStUvWxYz0123456789"
  private val tokenStart = inboxPrefix.length + 1

  // Stand-in for the pending value. Its identity is all the map ever does with
  // it, and it is common to both shapes, so it stays out of the measurement.
  private val value = new AnyRef

  // ---- today's shape: everything behind SyncRef ----
  private var pendingRef: AtomicReference[Map[String, AnyRef]] = _
  // `Ref[F, Long]` is a `SyncRef` over an `AtomicReference` whose type
  // parameter is erased, so the counter lives in the heap as a boxed
  // `java.lang.Long` and every write allocates a fresh box. Modelled with
  // `AnyRef` so the boxing is reproduced rather than accidentally optimized
  // away (and so `compareAndSet`, which compares by reference, behaves as it
  // does in `SyncRef`).
  private var counterRef: AtomicReference[AnyRef] = _

  // ---- proposed shape ----
  private var pendingChm: ConcurrentHashMap[String, AnyRef] = _
  private var counterAtomic: AtomicLong = _

  @Setup
  def setup(): Unit =
    // Preload with token-shaped keys drawn from a counter, exactly as the
    // live map is filled.
    val preload =
      (0 until inFlight).map(i => Tokens.base62(1000000L + i) -> value).toMap
    pendingRef = new AtomicReference(preload)
    counterRef = new AtomicReference[AnyRef](java.lang.Long.valueOf(0L))
    pendingChm = new ConcurrentHashMap[String, AnyRef](preload.size * 2 + 16)
    preload.foreach { case (k, v) => pendingChm.put(k, v) }
    counterAtomic = new AtomicLong(0L)

  // --------------------------------------------------------- SyncRef spins

  /** `SyncRef.getAndUpdate`: CAS loop, and the `Long` is boxed on every write. */
  @tailrec
  private def refGetAndInc(ar: AtomicReference[AnyRef]): Long =
    val a = ar.get
    val v = a.asInstanceOf[java.lang.Long].longValue
    if !ar.compareAndSet(a, java.lang.Long.valueOf(v + 1L)) then
      refGetAndInc(ar)
    else v

  /** `SyncRef.update`: CAS loop, `f` re-run (and the map re-copied) per retry. */
  @tailrec
  private def refUpdate(
      ar: AtomicReference[Map[String, AnyRef]],
      f: Map[String, AnyRef] => Map[String, AnyRef]
  ): Unit =
    val a = ar.get
    if !ar.compareAndSet(a, f(a)) then refUpdate(ar, f)

  /** `SyncRef.modify`: the drain fiber's remove. Allocates the tuple and the
    * `Option`, and CASes even when the token is absent.
    */
  @tailrec
  private def refModifyRemove(
      ar: AtomicReference[Map[String, AnyRef]],
      token: String
  ): Option[AnyRef] =
    val c = ar.get
    val (u, b) = c.get(token) match
      case Some(d) => (c - token, Some(d))
      case None    => (c, None)
    if !ar.compareAndSet(c, u) then refModifyRemove(ar, token)
    else b

  // ------------------------------------------------------ raw round trips

  /** Today: token draw, insert, reply-side modify-remove, guaranteed remove. */
  @Benchmark
  def refRoundTrip(bh: Blackhole): Unit =
    val n = refGetAndInc(counterRef)
    val token = Tokens.base62(n)
    val replySubject = s"$inboxPrefix.$token"
    refUpdate(pendingRef, _ + (token -> value))
    // Reply side re-derives the token from the delivered subject.
    val echoed = replySubject.substring(tokenStart)
    bh.consume(refModifyRemove(pendingRef, echoed))
    refUpdate(pendingRef, _ - token)

  /** Proposed: `AtomicLong` plus `ConcurrentHashMap`, same four steps. */
  @Benchmark
  def chmRoundTrip(bh: Blackhole): Unit =
    val n = counterAtomic.getAndIncrement()
    val token = Tokens.base62(n)
    val replySubject = s"$inboxPrefix.$token"
    pendingChm.put(token, value)
    val echoed = replySubject.substring(tokenStart)
    bh.consume(pendingChm.remove(echoed))
    pendingChm.remove(token): Unit
