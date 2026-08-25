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

class CodecsSpec extends munit.FunSuite:

  // ---- TokenCodec -------------------------------------------------------

  test("TokenCodec.string round trips") {
    val enc = TokenCodec.string.encode("abc-123")
    assertEquals(TokenCodec.string.decode(enc), Right("abc-123"))
  }

  test("TokenCodec.int round trips") {
    assertEquals(TokenCodec.int.decode(TokenCodec.int.encode(-42)), Right(-42))
  }

  test("TokenCodec.long round trips") {
    val enc = TokenCodec.long.encode(Long.MinValue)
    assertEquals(TokenCodec.long.decode(enc), Right(Long.MinValue))
  }

  test("TokenCodec.uuid round trips") {
    val id = java.util.UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
    assertEquals(TokenCodec.uuid.decode(TokenCodec.uuid.encode(id)), Right(id))
  }

  test("TokenCodec decode failures are Left") {
    assert(TokenCodec.int.decode("abc").isLeft)
    assert(TokenCodec.long.decode("12x").isLeft)
    assert(TokenCodec.uuid.decode("not-a-uuid").isLeft)
  }

  private case class UserName(value: String)

  test("TokenCodec.imap round trips a wrapper type") {
    val codec = TokenCodec.string.imap(UserName(_))(_.value)
    assertEquals(codec.encode(UserName("sven")), "sven")
    assertEquals(codec.decode("sven"), Right(UserName("sven")))
  }

  // ---- Payload ----------------------------------------------------------

  test("Payload.empty encodes to Chunk.empty and decodes anything") {
    assertEquals(Payload.empty.encode(()), Chunk.empty[Byte])
    assertEquals(
      Payload.empty.decode(Chunk.array(Array[Byte](1, 2, 3))),
      Right(())
    )
    assertEquals(Payload.empty.decode(Chunk.empty), Right(()))
  }

  test("Payload.bytes is the identity") {
    val c = Chunk.array(Array[Byte](1, 2, 3))
    assertEquals(Payload.bytes.encode(c), c)
    assertEquals(Payload.bytes.decode(c), Right(c))
  }

  test("Payload.string round trips UTF-8 text") {
    val s = "héllo wörld ✓ 你好"
    assertEquals(Payload.string.decode(Payload.string.encode(s)), Right(s))
  }

  private final case class Boxed(n: Int)

  // Hand-written: jsoniter-scala-macros is compile-internal only, so
  // `JsonCodecMaker` is not on the test classpath.
  private given JsonValueCodec[Boxed] = new JsonValueCodec[Boxed]:
    def nullValue: Boxed = null

    def encodeValue(b: Boxed, out: JsonWriter): Unit =
      out.writeObjectStart()
      out.writeKey("n"); out.writeVal(b.n)
      out.writeObjectEnd()

    def decodeValue(in: JsonReader, default: Boxed): Boxed =
      if in.isNextToken('{') then
        var n = 0
        if !in.isNextToken('}') then
          in.rollbackToken()
          var cont = true
          while cont do
            val l = in.readKeyAsCharBuf()
            if in.isCharBufEqualsTo(l, "n") then n = in.readInt()
            else in.skip()
            cont = in.isNextToken(',')
          if !in.isCurrentToken('}') then in.objectEndOrCommaError()
        Boxed(n)
      else in.readNullOrTokenError(default, '{')

  test("Payload.json round trips via a jsoniter codec") {
    val p = Payload.json[Boxed]
    assertEquals(p.decode(p.encode(Boxed(42))), Right(Boxed(42)))
  }

  test("Payload.json decode failure is Left") {
    val bytes = Chunk.array("not json".getBytes(StandardCharsets.UTF_8))
    assert(Payload.json[Boxed].decode(bytes).isLeft)
  }

  test("Payload.withSchema sets schema and preserves the round trip") {
    val p = Payload.withSchema(Payload.string, "text/plain")
    assertEquals(Payload.string.schema, None)
    assertEquals(p.schema, Some("text/plain"))
    assertEquals(p.decode(p.encode("hi")), Right("hi"))
  }

  test("Payload.imap decode failure propagates") {
    val p = Payload.string
      .imap(s => s.toIntOption.toRight(s"not an Int: '$s'"))(_.toString)
    assertEquals(p.decode(p.encode(42)), Right(42))
    assertEquals(
      p.decode(Payload.string.encode("abc")),
      Left("not an Int: 'abc'")
    )
  }

  // ---- ServiceErr -------------------------------------------------------

  private sealed trait Failure
  private case object Missing extends Failure
  private final case class Other(code: Int, description: String) extends Failure

  private val failureErr: ServiceErr[Failure] =
    ServiceErr.from[Failure](
      {
        case Missing     => (404, "missing")
        case Other(c, d) => (c, d)
      },
      (code, desc) => if code == 404 then Missing else Other(code, desc)
    )

  test("ServiceErr.from encode/decode round trips") {
    assertEquals(failureErr.encode(Missing), (404, "missing"))
    assertEquals(failureErr.decode(404, "missing"), Missing: Failure)
    val (c, d) = failureErr.encode(Other(500, "boom"))
    assertEquals(failureErr.decode(c, d), Other(500, "boom"): Failure)
  }

  test("ServiceErr.plain passes code and description through") {
    assertEquals(ServiceErr.plain.encode((418, "teapot")), (418, "teapot"))
    assertEquals(ServiceErr.plain.decode(418, "teapot"), (418, "teapot"))
  }
