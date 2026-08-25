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

/** Encodes/decodes a single subject token (one `*` capture, or the `.`-joined
  * tail of a `>` capture). Instances are created via the factories on the
  * companion; the class is sealed so methods can be added compatibly.
  */
sealed abstract class TokenCodec[A]:
  def encode(a: A): String
  def decode(s: String): Either[String, A]

  /** Transform this codec with a total mapping in both directions. */
  final def imap[B](f: A => B)(g: B => A): TokenCodec[B] =
    TokenCodec.from(b => encode(g(b)), s => decode(s).map(f))

object TokenCodec:

  def from[A](
      enc: A => String,
      dec: String => Either[String, A]
  ): TokenCodec[A] =
    new TokenCodec[A]:
      def encode(a: A): String = enc(a)
      def decode(s: String): Either[String, A] = dec(s)

  given string: TokenCodec[String] =
    from(identity, Right(_))

  given int: TokenCodec[Int] =
    from(_.toString, s => s.toIntOption.toRight(s"not an Int: '$s'"))

  given long: TokenCodec[Long] =
    from(_.toString, s => s.toLongOption.toRight(s"not a Long: '$s'"))

  given uuid: TokenCodec[java.util.UUID] =
    from(
      _.toString,
      s =>
        try Right(java.util.UUID.fromString(s))
        catch case _: IllegalArgumentException => Left(s"not a UUID: '$s'")
    )
