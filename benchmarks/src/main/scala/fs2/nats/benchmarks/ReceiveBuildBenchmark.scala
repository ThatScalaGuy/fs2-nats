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

import fs2.Chunk
import fs2.nats.protocol.{Headers, NatsFrame}
import fs2.nats.subscriptions.NatsMessage
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit

/** Isolates the per-received-message object count on the MSG/HMSG data path
  * (P1.3).
  *
  * Old path: the parser emitted a `NatsFrame.MsgFrame` (which crossed the frame
  * stream and so genuinely escaped), then the subscription router re-wrapped it
  * into the user-facing `NatsMessage` — two objects per message. New path: the
  * parser builds the `NatsMessage` directly via `NatsMessage.parserBuilder` — one
  * object. The `Blackhole.consume(frame)` in the `old*` methods reproduces that
  * escape so the JIT cannot scalar-replace the intermediate frame and hide the
  * difference (in the real pipeline the frame is emitted downstream).
  *
  * Run with `-prof gc` and compare `gc.alloc.rate.norm` (bytes/op): the delta is
  * the eliminated frame object. The subject/replyTo/payload are shared by
  * reference in both, so only the wrapper object is saved.
  *
  * Run:
  * {{{
  *   sbt "benchmarks/Jmh/run -prof gc .*ReceiveBuildBenchmark.*"
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class ReceiveBuildBenchmark:

  private val subject = "events.orders.created"
  private val replyTo = Some("_INBOX.9f3c1a2b4d5e6f70")
  private val payload = Chunk.array(Array.fill[Byte](16)('x'.toByte))
  private val sid = 7L
  private val headers = Headers("X-Trace" -> "00-1234abcd")

  /** Old: MsgFrame (escapes) + the NatsMessage the router built from it. */
  @Benchmark
  def msg_oldTwoObjects(bh: Blackhole): Unit =
    val f = NatsFrame.MsgFrame(subject, sid, None, payload)
    bh.consume(f)
    bh.consume(
      NatsMessage(f.subject, f.replyTo, Headers.empty, f.payload, f.sid)
    )

  /** New: the parser builds the NatsMessage directly. */
  @Benchmark
  def msg_newOneObject(bh: Blackhole): Unit =
    bh.consume(NatsMessage.parserBuilder.msg(subject, sid, None, payload))

  /** Old: HMsgFrame (escapes) + the NatsMessage the router built from it. */
  @Benchmark
  def hmsg_oldTwoObjects(bh: Blackhole): Unit =
    val f = NatsFrame.HMsgFrame(
      subject,
      sid,
      replyTo,
      headers,
      Some(100),
      Some("Idle"),
      payload
    )
    bh.consume(f)
    bh.consume(
      NatsMessage(
        f.subject,
        f.replyTo,
        f.headers,
        f.payload,
        f.sid,
        f.statusCode,
        f.statusDescription
      )
    )

  /** New: the parser builds the NatsMessage directly. */
  @Benchmark
  def hmsg_newOneObject(bh: Blackhole): Unit =
    bh.consume(
      NatsMessage.parserBuilder
        .hmsg(subject, sid, replyTo, headers, Some(100), Some("Idle"), payload)
    )
