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

package fs2.nats.auth

import fs2.nats.client.NatsCredentials
import fs2.nats.errors.NatsError
import munit.FunSuite

class CredsSpec extends FunSuite:

  // A real NATS user seed (same fixture as NKeySpec). The JWT below is a
  // JWT-shaped placeholder (header.payload.sig in base64url) — the parser is
  // purely structural and never inspects the JWT contents.
  private val Seed =
    "SUAO7NFVXBKDTBLB74TVJUV4H2BGV6YJYAIAQGFROYC3FWWLBGRHG6YHPQ"
  private val Jwt =
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ" +
      ".eyJqdGkiOiJBQkMiLCJpYXQiOjE3MDAwMDAwMDAsInN1YiI6IlUxMjMifQ" +
      ".c2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJl"

  /** Build a `.creds` file in the canonical nsc layout with the given line
    * ending.
    */
  private def credsFile(jwt: String, seed: String, eol: String): String =
    s"-----BEGIN NATS USER JWT-----$eol" +
      s"$jwt$eol" +
      s"------END NATS USER JWT------$eol" +
      eol +
      s"************************* IMPORTANT *************************$eol" +
      s"NKEY Seed printed below can be used to sign and prove identity.$eol" +
      s"NKEYs are sensitive and should be treated as secrets.$eol" +
      eol +
      s"-----BEGIN USER NKEY SEED-----$eol" +
      s"$seed$eol" +
      s"------END USER NKEY SEED------$eol" +
      eol +
      s"*************************************************************$eol"

  test("parse extracts JWT and seed from a canonical creds file (LF)") {
    assertEquals(
      Creds.parse(credsFile(Jwt, Seed, "\n")),
      Right(NatsCredentials.NKey(Seed, Some(Jwt)))
    )
  }

  test("parse handles CRLF line endings") {
    assertEquals(
      Creds.parse(credsFile(Jwt, Seed, "\r\n")),
      Right(NatsCredentials.NKey(Seed, Some(Jwt)))
    )
  }

  test("parse tolerates extra blank lines and surrounding whitespace") {
    val content =
      "\n\n" + credsFile("  " + Jwt + "  ", "  " + Seed + "  ", "\n") + "\n"
    assertEquals(
      Creds.parse(content),
      Right(NatsCredentials.NKey(Seed, Some(Jwt)))
    )
  }

  test("parse selects the first two blocks when extra blocks are present") {
    val extra =
      credsFile(Jwt, Seed, "\n") +
        "-----BEGIN SOMETHING ELSE-----\nignored\n------END SOMETHING ELSE------\n"
    assertEquals(
      Creds.parse(extra),
      Right(NatsCredentials.NKey(Seed, Some(Jwt)))
    )
  }

  test("parse yields NKey credentials carrying the JWT (operator path)") {
    Creds.parse(credsFile(Jwt, Seed, "\n")) match
      case Right(NatsCredentials.NKey(_, jwt)) =>
        assert(jwt.isDefined, "expected the JWT to be carried")
      case other => fail(s"expected NKey credentials, got $other")
  }

  test("parse rejects a file with only a JWT block (no seed)") {
    val onlyJwt =
      s"-----BEGIN NATS USER JWT-----\n$Jwt\n------END NATS USER JWT------\n"
    assertParseFails(onlyJwt, "seed")
  }

  test("parse rejects a file with only a seed block (no JWT)") {
    val onlySeed =
      s"-----BEGIN USER NKEY SEED-----\n$Seed\n------END USER NKEY SEED------\n"
    assertParseFails(onlySeed, "seed")
  }

  test("parse rejects input with no credential blocks") {
    assertParseFails("not a creds file at all", "no credential blocks")
  }

  test("parse rejects empty input") {
    assertParseFails("", "no credential blocks")
  }

  /** Assert the parse failed with an AuthorizationError whose message contains
    * `fragment` and never leaks the seed.
    */
  private def assertParseFails(content: String, fragment: String): Unit =
    Creds.parse(content) match
      case Left(e: NatsError.AuthorizationError) =>
        assert(
          e.getMessage.toLowerCase.contains(fragment.toLowerCase),
          s"message '${e.getMessage}' did not contain '$fragment'"
        )
        assert(!e.getMessage.contains(Seed), "error message leaked the seed")
      case other => fail(s"expected AuthorizationError, got $other")
