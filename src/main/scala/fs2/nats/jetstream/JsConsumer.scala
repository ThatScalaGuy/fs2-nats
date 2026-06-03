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

package fs2.nats.jetstream

import cats.effect.Resource
import fs2.{Chunk, Stream}
import fs2.nats.jetstream.protocol.ConsumerInfo

import scala.concurrent.duration.*

/** Options for a continuous `consume` pull loop. */
final case class ConsumeOptions(
    maxMessages: Int = 500,
    maxBytes: Option[Int] = None,
    expires: FiniteDuration = 30.seconds,
    idleHeartbeat: FiniteDuration = 5.seconds
)
object ConsumeOptions:
  val default: ConsumeOptions = ConsumeOptions()

/** A handle to a JetStream consumer: the object-centric entry point for pull
  * consumption.
  */
trait JsConsumer[F[_]]:

  /** The stream this consumer is bound to. */
  def stream: String

  /** The consumer name (server-assigned for ephemeral consumers). */
  def name: String

  /** Fetch the current consumer information. */
  def info: F[ConsumerInfo]

  /** Issue one pull request for up to `batch` messages, returning when the
    * batch is filled or `maxWait` elapses. A request timeout (408) yields a
    * possibly-empty `Chunk` rather than an error; hard failures (consumer
    * deleted, push-based) fail the effect.
    */
  def fetch(batch: Int, maxWait: FiniteDuration): F[Chunk[JsMessage[F]]]

  /** Like `fetch` but with `no_wait`: only currently-buffered messages are
    * returned (404 when none, yielding an empty `Chunk`).
    */
  def fetchNoWait(batch: Int): F[Chunk[JsMessage[F]]]

  /** Continuously consume messages. The `Resource` owns the reply inbox
    * subscription and the background pull loop.
    */
  def consume(
      opts: ConsumeOptions = ConsumeOptions.default
  ): Resource[F, Stream[F, JsMessage[F]]]
