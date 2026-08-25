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

package fs2.nats.micro

import com.github.plokhotnyuk.jsoniter_scala.core.*
import fs2.Chunk

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

/** Encodes/decodes a request or response body. Instances are created via the
  * factories on the companion; the class is sealed so methods can be added
  * compatibly.
  */
sealed abstract class Payload[A]:
  def encode(a: A): Chunk[Byte]
  def decode(bytes: Chunk[Byte]): Either[String, A]

  /** Optional schema description, published to `$SRV.INFO` endpoint metadata
    * (`request_schema` / `response_schema`). `None` by default.
    */
  def schema: Option[String]

  /** Transform this payload codec; the decode direction may fail. */
  final def imap[B](f: A => Either[String, B])(g: B => A): Payload[B] =
    val self = this
    new Payload[B]:
      def encode(b: B): Chunk[Byte] = self.encode(g(b))
      def decode(bytes: Chunk[Byte]): Either[String, B] =
        self.decode(bytes).flatMap(f)
      def schema: Option[String] = self.schema

object Payload:

  def from[A](
      enc: A => Chunk[Byte],
      dec: Chunk[Byte] => Either[String, A]
  ): Payload[A] =
    new Payload[A]:
      def encode(a: A): Chunk[Byte] = enc(a)
      def decode(bytes: Chunk[Byte]): Either[String, A] = dec(bytes)
      def schema: Option[String] = None

  /** Encodes to an empty chunk; decoding ignores the bytes entirely. */
  val empty: Payload[Unit] =
    from(_ => Chunk.empty, _ => Right(()))

  /** Identity codec over raw bytes. */
  val bytes: Payload[Chunk[Byte]] =
    from(identity, Right(_))

  /** UTF-8 text. */
  val string: Payload[String] =
    from(
      s => Chunk.array(s.getBytes(StandardCharsets.UTF_8)),
      b => Right(new String(b.toArray, StandardCharsets.UTF_8))
    )

  /** JSON via a jsoniter codec. Decode failures surface as `Left` with the
    * reader's message.
    */
  def json[A](using codec: JsonValueCodec[A]): Payload[A] =
    // No hex dump in parse errors: the message ends up in the single-line
    // Nats-Service-Error reply header.
    val readerConfig = ReaderConfig.withAppendHexDumpToParseException(false)
    from(
      a => Chunk.array(writeToArray(a)),
      b =>
        try Right(readFromArray[A](b.toArray, readerConfig))
        catch case NonFatal(e) => Left(e.getMessage)
    )

  /** Attach a schema description to an existing payload codec. */
  def withSchema[A](p: Payload[A], schemaText: String): Payload[A] =
    new Payload[A]:
      def encode(a: A): Chunk[Byte] = p.encode(a)
      def decode(bytes: Chunk[Byte]): Either[String, A] = p.decode(bytes)
      def schema: Option[String] = Some(schemaText)
