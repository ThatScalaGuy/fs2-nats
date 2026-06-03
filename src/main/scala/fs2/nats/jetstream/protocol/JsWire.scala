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

import fs2.Chunk
import io.circe.{ACursor, Decoder, Json}

import java.time.Instant
import java.util.Base64
import scala.concurrent.duration.*

/** Shared wire-encoding helpers for JetStream JSON, centralizing the codec
  * foot-guns called out in the design: durations are encoded as int64
  * nanoseconds, timestamps as RFC3339 strings, and binary blobs as base64.
  */
private[jetstream] object JsWire:

  /** Encode a duration as int64 nanoseconds (the JetStream wire format). */
  def durationToNanos(d: FiniteDuration): Json = Json.fromLong(d.toNanos)

  /** Decode int64 nanoseconds into a `FiniteDuration`. */
  def nanosToDuration(nanos: Long): FiniteDuration = nanos.nanos

  /** Encode an `Instant` as an RFC3339 string. */
  def instantToJson(i: Instant): Json = Json.fromString(i.toString)

  /** Encode binary data as a base64 string. */
  def bytesToBase64(bytes: Chunk[Byte]): Json =
    Json.fromString(Base64.getEncoder.encodeToString(bytes.toArray))

  /** Decode an optional duration field given in nanoseconds. A `0` (or absent)
    * value is treated as `None` (JetStream's "unset/unlimited" convention).
    */
  def optDurationNanos(c: ACursor): Decoder.Result[Option[FiniteDuration]] =
    c.as[Option[Long]].map(_.filter(_ > 0).map(nanosToDuration))

  /** Decoder for an RFC3339 timestamp string into an `Instant`. */
  given Decoder[Instant] = Decoder.decodeString.emap { s =>
    try Right(Instant.parse(s))
    catch case _: Throwable => Left(s"Invalid RFC3339 timestamp: $s")
  }

  /** Decoder for a base64 string into a `Chunk[Byte]`. */
  given Decoder[Chunk[Byte]] = Decoder.decodeString.emap { s =>
    try Right(Chunk.array(Base64.getDecoder.decode(s)))
    catch case _: Throwable => Left(s"Invalid base64 data")
  }

  /** Build a JSON object from optional fields, omitting `None` entries (the
    * `omitempty` convention used throughout the JetStream API).
    */
  def obj(fields: Option[(String, Json)]*): Json =
    Json.obj(fields.flatten*)
