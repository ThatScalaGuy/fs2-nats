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

/** Classification of a pull-delivery message based on its status line. */
private[jetstream] enum PullSignal:
  /** A normal data message. */
  case Data

  /** A 100 idle-heartbeat / flow-control keep-alive (not counted). */
  case Heartbeat

  /** The current pull request is finished normally (404/408/409 batch or
    * max-bytes). Re-issue to continue.
    */
  case Complete

  /** The request (or consumer) is invalid; fail the operation. */
  case Fail(code: Int, description: String)

private[jetstream] object PullStatus:

  /** Classify a delivered message by its status code + description per the
    * JetStream pull-status table (§4.4 of the design).
    */
  def classify(status: Option[Int], description: Option[String]): PullSignal =
    status match
      case None       => PullSignal.Data
      case Some(100)  => PullSignal.Heartbeat
      case Some(code) =>
        val d = description.getOrElse("").toLowerCase
        code match
          case 404                                   => PullSignal.Complete
          case 408                                   => PullSignal.Complete
          case 409 if d.contains("consumer deleted") =>
            // Terminal: the consumer is gone, re-pulling cannot recover.
            PullSignal.Fail(code, description.getOrElse("consumer deleted"))
          case 409 if d.contains("push") =>
            PullSignal.Fail(
              code,
              description.getOrElse("consumer is push based")
            )
          case 409 if d.contains("exceeded") =>
            PullSignal.Fail(
              code,
              description.getOrElse("request exceeded limit")
            )
          case 409 =>
            // Transient/normal: "Batch Completed", "Message Size Exceeds
            // MaxBytes", and cluster events ("Leadership Change", "Server
            // Shutdown") — a durable consumer survives these, so finish this
            // request and let the loop re-pull (resuming after reconnect).
            PullSignal.Complete
          case 400 =>
            PullSignal.Fail(code, description.getOrElse("bad request"))
          case other =>
            PullSignal.Fail(other, description.getOrElse("unexpected status"))
