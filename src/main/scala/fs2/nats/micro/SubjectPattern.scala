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

import scala.annotation.implicitNotFound
import scala.annotation.tailrec
import scala.quoted.*

/** A subject pattern whose captures decode to/encode from `P`.
  *
  * Create instances with the [[pattern]] macro, which validates the literal at
  * compile time and computes the capture arity.
  */
sealed abstract class SubjectPattern[P]:
  /** The original wildcard literal, e.g. `"orders.get.*"`. */
  def render: String

  /** Client side: substitute `p` into the wildcard slots. */
  private[micro] def fill(p: P): Either[String, String]

  /** Server side: recover `p` from a concrete matched subject. */
  private[micro] def extract(subject: String): Either[String, P]

object SubjectPattern:

  /** Internal: called by code expanded from the [[pattern]] macro. Public only
    * because macro-spliced trees expand in user compilation units; do not call
    * directly.
    */
  def unsafeLiteral(rendered: String): SubjectPattern[Unit] =
    new SubjectPattern[Unit]:
      def render: String = rendered
      private[micro] def fill(p: Unit): Either[String, String] = Right(rendered)
      private[micro] def extract(subject: String): Either[String, Unit] =
        Right(())

  /** Pattern with `N` unbound captures. Only operation is [[bind]]. */
  sealed abstract class Unbound[N <: Int]:
    def bind[T](using b: Binder[T] { type Arity = N }): SubjectPattern[T]

  object Unbound:
    /** Internal: called by code expanded from the [[pattern]] macro. Public
      * only because macro-spliced trees expand in user compilation units; do
      * not call directly.
      */
    def unsafe[N <: Int](rendered: String): Unbound[N] =
      new Unbound[N]:
        def bind[T](using b: Binder[T] { type Arity = N }): SubjectPattern[T] =
          bound(rendered, b)

  private def bound[T](rendered: String, binder: Binder[T]): SubjectPattern[T] =
    new SubjectPattern[T]:
      private val tokens = rendered.split('.').toList

      def render: String = rendered

      private[micro] def fill(p: T): Either[String, String] =
        val values = binder.write(p).iterator
        val out = new StringBuilder
        var error: Option[String] = None
        var first = true
        tokens.foreach { tok =>
          if error.isEmpty then
            val piece =
              if tok == "*" then
                val v = values.next()
                if v.isEmpty then
                  error = Some("encoded token is empty")
                  v
                else if v.exists(c =>
                    c == '.' || c == '*' || c == '>' || c.isWhitespace
                  )
                then
                  error =
                    Some(s"encoded token '$v' contains '.', ' ', '*' or '>'")
                  v
                else v
              else if tok == ">" then
                val v = values.next()
                if v.isEmpty then error = Some("encoded '>' tail is empty")
                v
              else tok
            if !first then out.append('.')
            out.append(piece)
            first = false
        }
        error.toLeft(out.result())

      private[micro] def extract(subject: String): Either[String, T] =
        @tailrec
        def loop(
            pat: List[String],
            subj: List[String],
            acc: List[String]
        ): Either[String, List[String]] =
          (pat, subj) match
            case (">" :: Nil, rest) =>
              if rest.isEmpty then Left(s"no tokens left for '>' in '$subject'")
              else Right((rest.mkString(".") :: acc).reverse)
            case ("*" :: pt, s :: st) => loop(pt, st, s :: acc)
            // Literal tokens are trusted: NATS already matched them.
            case (_ :: pt, _ :: st) => loop(pt, st, acc)
            case (Nil, Nil)         => Right(acc.reverse)
            case _                  =>
              Left(s"subject '$subject' does not match pattern '$rendered'")
        loop(tokens, subject.split('.').toList, Nil).flatMap(binder.read)

/** How a params type maps to N wildcard tokens. Arity is a type member so
  * `bind`'s arity check is a plain implicit search.
  */
@implicitNotFound(
  "Cannot bind ${T} to this subject pattern: no Binder[${T}] whose Arity " +
    "matches the pattern's capture count (its number of '*'/'>' tokens).\n" +
    "  - 1 capture:    any A with a given TokenCodec[A]\n" +
    "  - 2-4 captures: a Tuple2..Tuple4 with a TokenCodec per component\n" +
    "If ${T} is a tuple, its size must equal the capture count."
)
sealed abstract class Binder[T]:
  type Arity <: Int
  private[micro] def write(t: T): List[String]
  private[micro] def read(tokens: List[String]): Either[String, T]

object Binder:

  given single[A](using c: TokenCodec[A]): (Binder[A] { type Arity = 1 }) =
    new Binder[A]:
      type Arity = 1
      private[micro] def write(t: A): List[String] = List(c.encode(t))
      private[micro] def read(tokens: List[String]): Either[String, A] =
        tokens match
          case t :: Nil => c.decode(t)
          case other    => Left(s"expected 1 capture, got ${other.length}")

  given tuple2[A, B](using
      ca: TokenCodec[A],
      cb: TokenCodec[B]
  ): (Binder[(A, B)] { type Arity = 2 }) =
    new Binder[(A, B)]:
      type Arity = 2
      private[micro] def write(t: (A, B)): List[String] =
        List(ca.encode(t._1), cb.encode(t._2))
      private[micro] def read(tokens: List[String]): Either[String, (A, B)] =
        tokens match
          case a :: b :: Nil =>
            for x <- ca.decode(a); y <- cb.decode(b) yield (x, y)
          case other => Left(s"expected 2 captures, got ${other.length}")

  given tuple3[A, B, C](using
      ca: TokenCodec[A],
      cb: TokenCodec[B],
      cc: TokenCodec[C]
  ): (Binder[(A, B, C)] { type Arity = 3 }) =
    new Binder[(A, B, C)]:
      type Arity = 3
      private[micro] def write(t: (A, B, C)): List[String] =
        List(ca.encode(t._1), cb.encode(t._2), cc.encode(t._3))
      private[micro] def read(
          tokens: List[String]
      ): Either[String, (A, B, C)] =
        tokens match
          case a :: b :: c :: Nil =>
            for
              x <- ca.decode(a)
              y <- cb.decode(b)
              z <- cc.decode(c)
            yield (x, y, z)
          case other => Left(s"expected 3 captures, got ${other.length}")

  given tuple4[A, B, C, D](using
      ca: TokenCodec[A],
      cb: TokenCodec[B],
      cc: TokenCodec[C],
      cd: TokenCodec[D]
  ): (Binder[(A, B, C, D)] { type Arity = 4 }) =
    new Binder[(A, B, C, D)]:
      type Arity = 4
      private[micro] def write(t: (A, B, C, D)): List[String] =
        List(
          ca.encode(t._1),
          cb.encode(t._2),
          cc.encode(t._3),
          cd.encode(t._4)
        )
      private[micro] def read(
          tokens: List[String]
      ): Either[String, (A, B, C, D)] =
        tokens match
          case a :: b :: c :: d :: Nil =>
            for
              x <- ca.decode(a)
              y <- cb.decode(b)
              z <- cc.decode(c)
              w <- cd.decode(d)
            yield (x, y, z, w)
          case other => Left(s"expected 4 captures, got ${other.length}")

/** Validate a NATS subject-pattern literal at compile time and compute its
  * capture arity. Returns `SubjectPattern[Unit]` for zero captures, or
  * `SubjectPattern.Unbound[N]` (to be `.bind`-ed) for 1-4 captures.
  */
transparent inline def pattern[S <: String & Singleton]: Any =
  ${ SubjectPatternMacros.patternImpl[S] }

private[micro] object SubjectPatternMacros:

  def patternImpl[S <: String & Singleton: Type](using Quotes): Expr[Any] =
    import quotes.reflect.*
    val subject = TypeRepr.of[S].dealias match
      case ConstantType(StringConstant(s)) => s
      case other                           =>
        report.errorAndAbort(
          s"pattern requires a string literal, got ${other.show}"
        )
    if subject.isEmpty then
      report.errorAndAbort("subject pattern must not be empty")
    val tokens = subject.split("\\.", -1)
    tokens.zipWithIndex.foreach { case (tok, i) =>
      if tok.isEmpty then
        report.errorAndAbort(
          s"""empty token at position ${i + 1} in "$subject""""
        )
      else if tok.exists(_.isWhitespace) then
        report.errorAndAbort(s"""token "$tok" contains whitespace""")
      else if tok.length > 1 && (tok.contains('*') || tok.contains('>')) then
        report.errorAndAbort(
          s"""wildcard must be a whole token, found "$tok""""
        )
      else if tok == ">" && i != tokens.length - 1 then
        report.errorAndAbort(
          s"""">" is only allowed as the last token, found at position ${i + 1}"""
        )
    }
    val n = tokens.count(t => t == "*" || t == ">")
    val s = Expr(subject)
    n match
      case 0 => '{ SubjectPattern.unsafeLiteral($s) }
      case 1 => '{ SubjectPattern.Unbound.unsafe[1]($s) }
      case 2 => '{ SubjectPattern.Unbound.unsafe[2]($s) }
      case 3 => '{ SubjectPattern.Unbound.unsafe[3]($s) }
      case 4 => '{ SubjectPattern.Unbound.unsafe[4]($s) }
      case _ =>
        report.errorAndAbort(
          s"pattern has $n captures; at most 4 are supported"
        )
