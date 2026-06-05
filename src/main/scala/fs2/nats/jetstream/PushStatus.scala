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

/** Classification of a push-delivery message based on its status line. */
private[nats] enum PushSignal:
  /** A normal data message. */
  case Data

  /** A 100 idle heartbeat: ignore (keep-alive). */
  case Heartbeat

  /** A 100 flow-control request: echo an empty message back to `reply`. */
  case FlowControl(reply: String)

  /** The consumer is invalid (e.g. deleted); fail the stream. */
  case Fail(code: Int, description: String)

private[nats] object PushStatus:

  /** Classify a push delivery. A 100 with a non-empty reply is flow control; a
    * 100 without a reply is an idle heartbeat; any other status fails the
    * stream (push consumers do not receive pull statuses like 404/408).
    */
  def classify(
      status: Option[Int],
      replyTo: Option[String],
      description: Option[String]
  ): PushSignal =
    status match
      case None      => PushSignal.Data
      case Some(100) =>
        replyTo.filter(_.nonEmpty) match
          case Some(reply) => PushSignal.FlowControl(reply)
          case None        => PushSignal.Heartbeat
      case Some(code) =>
        PushSignal.Fail(code, description.getOrElse("consumer error"))
