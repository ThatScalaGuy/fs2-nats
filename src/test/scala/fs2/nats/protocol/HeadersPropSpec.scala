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

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** A/B equivalence gate for the index-scan rewrite of `Headers.parse` /
  * `Headers.parseWithStatus`.
  *
  * Random header blocks are parsed by both the live `Headers` and the frozen
  * [[ReferenceHeaders]] (the `split("\r\n", -1)` implementation) and must agree
  * on the whole `Either`, error text included. [[ProtocolParserPropSpec]]
  * cannot serve as this gate: its reference calls the *live*
  * `Headers.parseWithStatus`, so both of its sides move together whenever
  * `Headers` is rewritten.
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
