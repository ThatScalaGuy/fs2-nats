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

package fs2.nats.kv.protocol

/** KV-specific header names and marker values.
  *
  * `KV-Operation` marks delete/purge entries (absent means a normal PUT);
  * `Nats-Rollup: sub` is sent with a purge to collapse a key's history.
  */
object KvHeaders:

  val Operation: String = "KV-Operation"
  val OperationDelete: String = "DEL"
  val OperationPurge: String = "PURGE"

  val Rollup: String = "Nats-Rollup"
  val RollupSubject: String = "sub"

  // ---- Direct-get reply headers (server-stamped on the raw message) ----
  val ExpectedLastSubjectSequence: String =
    "Nats-Expected-Last-Subject-Sequence"
  val Sequence: String = "Nats-Sequence"
  val TimeStamp: String = "Nats-Time-Stamp"
  val Subject: String = "Nats-Subject"
