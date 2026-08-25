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

import fs2.nats.jetstream.PublishOptions
import fs2.nats.jetstream.protocol.JsHeaders
import fs2.nats.protocol.Headers
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit

/** Mirror of `JetStreamImpl.mergePublishHeaders`, which is a `private def` on
  * an object-private class and so is unreachable from here — the same reason
  * [[OrderedLoopBenchmark]] mirrors `OrderedState`. This measures the *shape*
  * of the merge, not the shipped method.
  *
  * `opts` is the branch mix that matters. `default` is every plain `publish`,
  * every KV `put`/`putAsync`/`delete`/`purge` and every Object Store chunk
  * write: those call sites take the defaulted argument, which the compiler
  * lowers to a `getstatic` of the shared `PublishOptions.default`, so the
  * instance really is the shared one. `msgId` is a de-duplicated publish and
  * `full` is the worst case, all five header options set — that one must not
  * regress.
  *
  * Read `gc.alloc.rate.norm`: the five `::` cells, the wrapped array behind the
  * `List` and the `flatten` builder are what the guard removes, and they are
  * charged once per publish. Throughput on this machine is inside the error
  * bars either way.
  *
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*PublishMergeBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class PublishMergeBenchmark:

  @Param(Array("default", "msgId", "full"))
  var opts: String = "default"

  private var publishOpts: PublishOptions = PublishOptions.default

  @Setup
  def setup(): Unit =
    publishOpts = opts match
      case "msgId" => PublishOptions(msgId = Some("9f3c1a2b4d5e6f70"))
      case "full"  =>
        PublishOptions(
          msgId = Some("9f3c1a2b4d5e6f70"),
          expectedStream = Some("ORDERS"),
          expectedLastSeq = Some(4711L),
          expectedLastSubjectSeq = Some(42L),
          expectedLastMsgId = Some("9f3c1a2b4d5e6f6f")
        )
      // Take the shared instance, not a fresh `PublishOptions()`: the defaulted
      // argument at the call sites is exactly this reference.
      case _ => PublishOptions.default

  /** What ships today. */
  @Benchmark
  def listFold(): Headers = fold(Headers.empty, publishOpts)

  /** What #54.1 ships: an arity-checked pattern match, so a caller who only
    * overrides `timeout` (not a header) also skips the fold, and so that adding
    * a sixth header option to `PublishOptions` fails to compile here instead of
    * silently dropping a header.
    */
  @Benchmark
  def guarded(): Headers =
    publishOpts match
      case PublishOptions(None, None, None, None, None, _) => Headers.empty
      case o => fold(Headers.empty, o)

  /** The `eq PublishOptions.default` variant the issue proposes, kept as a
    * control: it is the same cost as `guarded` but misses
    * `PublishOptions(timeout = ...)`.
    */
  @Benchmark
  def eqFastPath(): Headers =
    if publishOpts eq PublishOptions.default then Headers.empty
    else fold(Headers.empty, publishOpts)

  private def fold(headers: Headers, o: PublishOptions): Headers =
    List(
      o.msgId.map(JsHeaders.MsgId -> _),
      o.expectedStream.map(JsHeaders.ExpectedStream -> _),
      o.expectedLastSeq.map(v => JsHeaders.ExpectedLastSeq -> v.toString),
      o.expectedLastSubjectSeq.map(v =>
        JsHeaders.ExpectedLastSubjectSeq -> v.toString
      ),
      o.expectedLastMsgId.map(JsHeaders.ExpectedLastMsgId -> _)
    ).flatten.foldLeft(headers) { case (h, (k, v)) => h.set(k, v) }
