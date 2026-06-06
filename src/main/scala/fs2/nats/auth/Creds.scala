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

import cats.effect.kernel.Async
import fs2.io.file.{Files, Path}
import fs2.nats.client.NatsCredentials
import fs2.nats.errors.NatsError

/** NATS decentralized-JWT credential (`.creds`) parsing.
  *
  * A `.creds` file is the credential format produced by `nsc` (and used by NGS
  * / Synadia and self-hosted operator deployments). It bundles a user JWT and
  * an NKey seed in a single file, each wrapped in PEM-like delimiter lines:
  *
  * {{{
  * -----BEGIN NATS USER JWT-----
  * eyJ0eXAiOiJKV1Qi...
  * ------END NATS USER JWT------
  *
  * -----BEGIN USER NKEY SEED-----
  * SUAGC3D...
  * ------END USER NKEY SEED------
  * }}}
  *
  * The first delimited block is the JWT, the second is the seed. The result is
  * a [[fs2.nats.client.NatsCredentials.NKey]] carrying both, which makes the
  * client send `jwt` + `sig` on CONNECT (the operator/decentralized auth path).
  *
  * This parser is purely structural: it does not cryptographically validate the
  * seed — that happens in [[NKey]] when the server nonce is signed at connect
  * time. As with [[NKey]], no credential material is included in error
  * messages.
  */
object Creds:

  // A credential block is the payload captured between two lines of three or
  // more dashes (with arbitrary header text). Mirrors the nats.go reference
  // regex. (?s) lets `.` span newlines; (?m) makes the anchors line-based.
  private val BlockPattern =
    """(?ms)(?:(?:[-]{3,}[^\n]*[-]{3,}\r?\n)(.+?)(?:\r?\n\s*[-]{3,}[^\n]*[-]{3,}\r?\n))""".r

  /** Parse the contents of a `.creds` file. The first delimited block is taken
    * as the user JWT and the second as the NKey seed.
    */
  def parse(content: String): Either[Throwable, NatsCredentials] =
    BlockPattern.findAllMatchIn(content).map(_.group(1).trim).toList match
      case jwt :: seed :: _ if jwt.nonEmpty && seed.nonEmpty =>
        Right(NatsCredentials.NKey(seed, Some(jwt)))
      case _ :: Nil =>
        Left(err("missing NKey seed block in credentials"))
      case Nil =>
        Left(err("no credential blocks found"))
      case _ =>
        Left(err("malformed credentials: empty JWT or seed block"))

  /** Read and parse a `.creds` file from disk. */
  def fromFile[F[_]: Files: Async](path: Path): F[NatsCredentials] =
    Async[F].flatMap(Files[F].readUtf8(path).compile.string)(content =>
      Async[F].fromEither(parse(content))
    )

  private def err(message: String): NatsError =
    NatsError.AuthorizationError(message)
