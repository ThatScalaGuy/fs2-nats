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

package fs2.nats.protocol

import fs2.Chunk

/** Builds the object the parser emits for MSG/HMSG *data* frames.
  *
  * Injected into [[ProtocolParser]] so the data path constructs the final
  * delivered type directly. With [[MsgBuilder.natsFrame]] it reproduces the
  * historical [[NatsFrame.MsgFrame]] / [[NatsFrame.HMsgFrame]] output (used by
  * `parseStream` and the parser's own tests). The client injects a builder that
  * constructs `subscriptions.NatsMessage` directly, eliminating the separate
  * `MsgFrame` object that a later routing stage used to re-wrap.
  *
  * `A` is covariant so a `MsgBuilder[NatsMessage]` is usable wherever a
  * `MsgBuilder[Frame]` is expected.
  *
  * @tparam A
  *   The type built for a data frame; must be a subtype of [[Frame]].
  */
trait MsgBuilder[+A <: Frame]:

  /** Build the object for a `MSG` frame (no headers). */
  def msg(
      subject: String,
      sid: Long,
      replyTo: Option[String],
      payload: Chunk[Byte]
  ): A

  /** Build the object for an `HMSG` frame (with parsed headers / status). */
  def hmsg(
      subject: String,
      sid: Long,
      replyTo: Option[String],
      headers: Headers,
      statusCode: Option[Int],
      statusDescription: Option[String],
      payload: Chunk[Byte]
  ): A

object MsgBuilder:

  /** Historical behavior: build the `NatsFrame` data variants. Backs the
    * `NatsFrame`-typed `parseStream` overloads.
    */
  val natsFrame: MsgBuilder[NatsFrame] = new MsgBuilder[NatsFrame]:
    def msg(
        subject: String,
        sid: Long,
        replyTo: Option[String],
        payload: Chunk[Byte]
    ): NatsFrame =
      NatsFrame.MsgFrame(subject, sid, replyTo, payload)

    def hmsg(
        subject: String,
        sid: Long,
        replyTo: Option[String],
        headers: Headers,
        statusCode: Option[Int],
        statusDescription: Option[String],
        payload: Chunk[Byte]
    ): NatsFrame =
      NatsFrame.HMsgFrame(
        subject,
        sid,
        replyTo,
        headers,
        statusCode,
        statusDescription,
        payload
      )
