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

import fs2.nats.jetstream.protocol.ConsumerConfig

/** Selection of the 2.9+ `CONSUMER.CREATE` subject for a consumer config. */
private[jetstream] object ConsumerCreate:

  /** Build the create subject:
    *   - named (durable or explicit name) with exactly one `filterSubject` and
    *     no `filterSubjects` => the `.<filter>` variant;
    *   - named otherwise => the `.<consumer>` variant;
    *   - neither durable nor name => the ephemeral variant.
    */
  def subject(
      subjects: ApiSubjects,
      stream: String,
      cfg: ConsumerConfig
  ): String =
    val consumerName = cfg.durable.orElse(cfg.name)
    val filterToken =
      if cfg.filterSubjects.isEmpty then cfg.filterSubject else None
    consumerName match
      case Some(n) =>
        filterToken match
          case Some(f) => subjects.consumerCreateFiltered(stream, n, f)
          case None    => subjects.consumerCreate(stream, n)
      case None =>
        subjects.consumerCreateEphemeral(stream)
