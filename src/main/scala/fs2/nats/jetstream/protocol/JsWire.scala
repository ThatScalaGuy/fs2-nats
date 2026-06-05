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

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import fs2.Chunk

import scala.concurrent.duration.*

/** Shared jsoniter wire-encoding helpers for JetStream JSON, centralizing the
  * codec foot-guns called out in the design: durations are encoded as int64
  * nanoseconds, binary blobs as base64. Timestamps use jsoniter's built-in
  * `Instant` codec (ISO-8601), which matches the RFC3339 the server emits.
  */
private[jetstream] object JsWire:

  /** Shared config: map camelCase case-class fields to their snake_case wire
    * names. Individual fields override this with `@named` where irregular.
    * `inline` so the jsoniter `make` macro can read it as a constant
    * expression.
    */
  inline def snake: CodecMakerConfig =
    CodecMakerConfig.withFieldNameMapper(JsonCodecMaker.enforce_snake_case)

  /** Durations on the JetStream wire are int64 nanoseconds. */
  given JsonValueCodec[FiniteDuration] with
    def decodeValue(in: JsonReader, default: FiniteDuration): FiniteDuration =
      in.readLong().nanos
    def encodeValue(x: FiniteDuration, out: JsonWriter): Unit =
      out.writeVal(x.toNanos)
    def nullValue: FiniteDuration = null

  /** Binary blobs are standard (padded) base64 strings. */
  given JsonValueCodec[Chunk[Byte]] with
    def decodeValue(in: JsonReader, default: Chunk[Byte]): Chunk[Byte] =
      Chunk.array(in.readBase64AsBytes(null))
    def encodeValue(x: Chunk[Byte], out: JsonWriter): Unit =
      out.writeBase64Val(x.toArray, doPadding = true)
    def nullValue: Chunk[Byte] = null

/** Low-level read helpers shared by the hand-written codecs. */
private[protocol] object JsRead:

  /** Read a JSON array of strings into a `List`. */
  def stringList(in: JsonReader): List[String] =
    if in.isNextToken('[') then
      if in.isNextToken(']') then Nil
      else
        in.rollbackToken()
        val b = List.newBuilder[String]
        var cont = true
        while cont do
          b += in.readString(null)
          cont = in.isNextToken(',')
        if !in.isCurrentToken(']') then in.arrayEndOrCommaError()
        b.result()
    else in.readNullOrTokenError(Nil, '[')

  /** Read a JSON array of objects via `codec` into a `List`. */
  def objectList[A](in: JsonReader, codec: JsonValueCodec[A]): List[A] =
    if in.isNextToken('[') then
      if in.isNextToken(']') then Nil
      else
        in.rollbackToken()
        val b = List.newBuilder[A]
        var cont = true
        while cont do
          b += codec.decodeValue(in, codec.nullValue)
          cont = in.isNextToken(',')
        if !in.isCurrentToken(']') then in.arrayEndOrCommaError()
        b.result()
    else in.readNullOrTokenError(Nil, '[')

  /** Read a JSON array of int64 nanoseconds into a `List[FiniteDuration]`. */
  def durationListNanos(in: JsonReader): List[FiniteDuration] =
    if in.isNextToken('[') then
      if in.isNextToken(']') then Nil
      else
        in.rollbackToken()
        val b = List.newBuilder[FiniteDuration]
        var cont = true
        while cont do
          b += in.readLong().nanos
          cont = in.isNextToken(',')
        if !in.isCurrentToken(']') then in.arrayEndOrCommaError()
        b.result()
    else in.readNullOrTokenError(Nil, '[')

  /** Decode an int64-nanoseconds duration, treating `0` as `None` (JetStream's
    * "unset/unlimited" convention).
    */
  def optDurationNanos(in: JsonReader): Option[FiniteDuration] =
    val v = in.readLong()
    if v > 0 then Some(v.nanos) else None
