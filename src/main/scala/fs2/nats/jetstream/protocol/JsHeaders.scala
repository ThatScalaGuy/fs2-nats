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

package fs2.nats.jetstream.protocol

/** Exact JetStream header names used for publish options and control-message
  * inspection.
  */
object JsHeaders:

  // ---- Publish options (dedup + optimistic concurrency) ----
  val MsgId: String = "Nats-Msg-Id"
  val ExpectedStream: String = "Nats-Expected-Stream"
  val ExpectedLastSeq: String = "Nats-Expected-Last-Sequence"
  val ExpectedLastSubjectSeq: String = "Nats-Expected-Last-Subject-Sequence"
  val ExpectedLastMsgId: String = "Nats-Expected-Last-Msg-Id"

  // ---- Control message / heartbeat headers ----
  val PendingMessages: String = "Nats-Pending-Messages"
  val PendingBytes: String = "Nats-Pending-Bytes"
  val LastConsumer: String = "Nats-Last-Consumer"
  val LastStream: String = "Nats-Last-Stream"
  val ConsumerStalled: String = "Nats-Consumer-Stalled"
