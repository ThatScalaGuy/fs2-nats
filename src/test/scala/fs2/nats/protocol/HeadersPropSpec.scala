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

import java.nio.charset.StandardCharsets
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** A/B equivalence gate for the rewrites of `Headers`, in both directions: the
  * index-scan rewrite of `Headers.parse` / `Headers.parseWithStatus` and the
  * write-into-a-sized-array rewrite of `Headers.toBytes`.
  *
  * Random header blocks are round-tripped through both the live `Headers` and
  * the frozen [[ReferenceHeaders]] and must agree byte for byte (and, on the
  * parse side, on the whole `Either`, error text included).
  * [[ProtocolParserPropSpec]] cannot serve as this gate: its reference calls
  * the *live* `Headers.parseWithStatus`, so both of its sides move together
  * whenever `Headers` is rewritten.
  */
class HeadersPropSpec extends ScalaCheckSuite:

  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters.withMinSuccessfulTests(500)

  // Every character that means something to the parser, plus both non-ASCII
  // whitespace classes (U+00A0 is `isWhitespace`-false but non-`trim`-able,
  // U+2003 is `isWhitespace`-true and also non-`trim`-able) and a multi-byte
  // letter.
  private val genChar: Gen[Char] = Gen.oneOf(
    '\r', '\n', ':', ' ', '\t', '\u0001', '\u00a0', '\u2003', 'A', 'b', '1',
    '-', 'é'
  )

  private val genBlock: Gen[String] =
    for
      prefix <- Gen.oneOf(
        "NATS/1.0",
        "NATS/1.0 100",
        "NATS/1.0 ",
        "nats/1.0",
        ""
      )
      n <- Gen.choose(0, 48)
      cs <- Gen.listOfN(n, genChar)
    yield prefix + cs.mkString

  property("parseWithStatus matches the frozen split-based reference") {
    forAll(genBlock)(s =>
      Headers.parseWithStatus(s) == ReferenceHeaders.parseWithStatus(s)
    )
  }

  property("parse matches the frozen split-based reference") {
    forAll(genBlock)(s => Headers.parse(s) == ReferenceHeaders.parse(s))
  }

  property("parse agrees with parseWithStatus on the headers it returns") {
    forAll(genBlock)(s =>
      Headers.parse(s) == Headers.parseWithStatus(s).map(_._3)
    )
  }

  // Deliberately hostile: embedded ':' and CRLF, a C0 control, both non-ASCII
  // whitespace classes, a multi-byte letter, an astral pair and — the case
  // `getBytes` handles specially — lone surrogates, which UTF-8-encode to a
  // single '?' rather than to U+FFFD.
  private val genHeaderChar: Gen[Char] = Gen.oneOf(
    'A', 'b', '1', '-', ':', ' ', '\t', '\r', '\n', '\u0001', '\u00a0',
    '\u2003', 'é', '\u4f60', '\ud83d', '\ude00', '\ud800', '\udc00'
  )

  private val genEntries: Gen[Vector[(String, String)]] =
    for
      n <- Gen.choose(0, 4)
      es <- Gen.listOfN(
        n,
        for
          kn <- Gen.choose(0, 8)
          vn <- Gen.choose(0, 8)
          k <- Gen.listOfN(kn, genHeaderChar)
          v <- Gen.listOfN(vn, genHeaderChar)
        yield (k.mkString, v.mkString)
      )
    yield es.toVector

  // Only blocks the wire format can actually represent. A key holding ':', or
  // either side holding CRLF or leading/trailing spaces, serializes fine but
  // is not recoverable, so those shapes belong to the oracle property above
  // and not to the round-trip one below. Pieces rather than chars, because the
  // astral character has to stay a matched surrogate pair: an unpaired half is
  // encoded as '?' and cannot round-trip either.
  private val genRoundTripPiece: Gen[String] =
    Gen.oneOf("A", "b", "1", "-", "é", "\u4f60", "\ud83d\ude00")

  private val genRoundTripEntries: Gen[Vector[(String, String)]] =
    for
      n <- Gen.choose(1, 4)
      es <- Gen.listOfN(
        n,
        for
          kn <- Gen.choose(1, 8)
          vn <- Gen.choose(1, 8)
          k <- Gen.listOfN(kn, genRoundTripPiece)
          v <- Gen.listOfN(vn, genRoundTripPiece)
        yield (k.mkString, v.mkString)
      )
    yield es.toVector

  property("toBytes matches the frozen StringBuilder reference") {
    forAll(genEntries)(es =>
      Headers(es).toBytes.toList == ReferenceHeaders.toBytes(es).toList
    )
  }

  property("byteLength agrees with toBytes.size") {
    forAll(genEntries)(es => Headers(es).byteLength == Headers(es).toBytes.size)
  }

  property("parse recovers what toBytes wrote") {
    forAll(genRoundTripEntries)(es =>
      Headers.parse(
        new String(Headers(es).toBytes.toArray, StandardCharsets.UTF_8)
      ) == Right(Headers(es))
    )
  }
