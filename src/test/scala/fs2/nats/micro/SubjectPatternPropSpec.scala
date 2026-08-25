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

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** `extract(fill(p)) == Right(p)` for token-safe values at every supported
  * arity, plus the `>` tail form. Token-safe means non-empty alphanumeric
  * (numeric types are token-safe by construction: their `toString` holds no
  * `.`, `*`, `>` or whitespace).
  */
class SubjectPatternPropSpec extends ScalaCheckSuite:

  private val genToken: Gen[String] =
    for
      n <- Gen.choose(1, 8)
      cs <- Gen.listOfN(n, Gen.alphaNumChar)
    yield cs.mkString

  private val genInt: Gen[Int] = Gen.choose(Int.MinValue, Int.MaxValue)
  private val genLong: Gen[Long] = Gen.choose(Long.MinValue, Long.MaxValue)

  private val p1 = pattern["orders.get.*"].bind[String]
  private val p2 = pattern["a.*.b.*"].bind[(String, Int)]
  private val p3 = pattern["a.*.*.*"].bind[(String, Int, Long)]
  private val p4 = pattern["*.x.*.*.*"].bind[(String, Int, Long, String)]
  private val pTail = pattern["files.>"].bind[String]

  property("arity-1 round trip") {
    forAll(genToken)(s => p1.fill(s).flatMap(p1.extract) == Right(s))
  }

  property("arity-2 round trip") {
    forAll(genToken, genInt) { (s, i) =>
      val p = (s, i)
      p2.fill(p).flatMap(p2.extract) == Right(p)
    }
  }

  property("arity-3 round trip") {
    forAll(genToken, genInt, genLong) { (s, i, l) =>
      val p = (s, i, l)
      p3.fill(p).flatMap(p3.extract) == Right(p)
    }
  }

  property("arity-4 round trip") {
    forAll(genToken, genInt, genLong, genToken) { (s, i, l, t) =>
      val p = (s, i, l, t)
      p4.fill(p).flatMap(p4.extract) == Right(p)
    }
  }

  private val genTail: Gen[String] =
    for
      n <- Gen.choose(1, 4)
      ts <- Gen.listOfN(n, genToken)
    yield ts.mkString(".")

  property("'>' round trips a dot-joined tail") {
    forAll(genTail)(t => pTail.fill(t).flatMap(pTail.extract) == Right(t))
  }
