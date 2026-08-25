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

class SubjectPatternSpec extends munit.FunSuite:

  test("zero captures yields SubjectPattern[Unit]") {
    val p: SubjectPattern[Unit] = pattern["orders.add"]
    assertEquals(p.render, "orders.add")
    assertEquals(p.fill(()), Right("orders.add"))
    assertEquals(p.extract("orders.add"), Right(()))
  }

  test("one capture binds a single TokenCodec type") {
    val p: SubjectPattern[Int] = pattern["orders.get.*"].bind[Int]
    assertEquals(p.render, "orders.get.*")
    assertEquals(p.fill(42), Right("orders.get.42"))
    assertEquals(p.extract("orders.get.42"), Right(42))
  }

  test("two captures bind a tuple") {
    val p: SubjectPattern[(String, Long)] =
      pattern["a.*.b.*"].bind[(String, Long)]
    assertEquals(p.fill(("x", 7L)), Right("a.x.b.7"))
    assertEquals(p.extract("a.x.b.7"), Right(("x", 7L)))
  }

  test("four captures bind a tuple4") {
    val p: SubjectPattern[(Int, Int, Int, Int)] =
      pattern["a.*.*.*.*"].bind[(Int, Int, Int, Int)]
    assertEquals(p.fill((1, 2, 3, 4)), Right("a.1.2.3.4"))
    assertEquals(p.extract("a.1.2.3.4"), Right((1, 2, 3, 4)))
  }

  test("'>' captures the dot-joined tail") {
    val p: SubjectPattern[String] = pattern["files.>"].bind[String]
    assertEquals(p.fill("x.y.z"), Right("files.x.y.z"))
    assertEquals(p.extract("files.x.y.z"), Right("x.y.z"))
  }

  test("fill rejects an encoded '*' token containing a dot") {
    val p = pattern["orders.get.*"].bind[String]
    assert(p.fill("a.b").isLeft)
    assert(p.fill("").isLeft)
    assert(p.fill("a b").isLeft)
    assert(p.fill("a*").isLeft)
  }

  test("extract decode failure is a Left") {
    val p = pattern["orders.get.*"].bind[Int]
    assert(p.extract("orders.get.notanint").isLeft)
  }

  test("invalid literals are compile errors") {
    assert(
      clue(compileErrors("""fs2.nats.micro.pattern["a..b"]""")).contains(
        "empty token at position 2"
      )
    )
    assert(
      clue(compileErrors("""fs2.nats.micro.pattern["a. b"]""")).contains(
        "contains whitespace"
      )
    )
    assert(
      clue(compileErrors("""fs2.nats.micro.pattern["a.>.b"]""")).contains(
        "only allowed as the last token"
      )
    )
    assert(
      clue(compileErrors("""fs2.nats.micro.pattern["a*b"]""")).contains(
        "wildcard must be a whole token"
      )
    )
    assert(
      clue(compileErrors("""fs2.nats.micro.pattern[""]""")).contains(
        "must not be empty"
      )
    )
  }

  test("arity mismatch on bind is a compile error") {
    val errs = compileErrors(
      """fs2.nats.micro.pattern["a.*.*"].bind[(String, String, String)]"""
    )
    assert(clue(errs).nonEmpty)
  }

  test("unbound pattern is not a SubjectPattern") {
    val errs = compileErrors(
      """val x: fs2.nats.micro.SubjectPattern[?] = fs2.nats.micro.pattern["a.*"]"""
    )
    assert(clue(errs).nonEmpty)
  }
