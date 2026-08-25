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

import cats.effect.Async
import cats.syntax.all.*
import fs2.nats.client.NatsClient
import fs2.nats.errors.NatsError
import fs2.nats.micro.protocol.MicroHeaders

import scala.concurrent.duration.*

/** Typed client for [[Rpc]] endpoints. Service errors come back as `Left(E)`.
  * Raised in `F` are only transport failures (`Timeout`, `NoResponders`,
  * `PayloadDecodeError`) and `IllegalArgumentException` when `params` encode to
  * an invalid subject token (programmer error).
  */
sealed abstract class Micro[F[_]]:

  def call[P, I, E, O](rpc: Rpc[P, I, E, O])(
      params: P,
      in: I,
      timeout: FiniteDuration = 5.seconds
  ): F[Either[E, O]]

  /** Sugar for endpoints without a request payload. */
  def call[P, E, O](rpc: Rpc[P, Unit, E, O])(params: P): F[Either[E, O]]

object Micro:

  def apply[F[_]](client: NatsClient[F])(using F: Async[F]): Micro[F] =
    new Micro[F]:

      def call[P, I, E, O](rpc: Rpc[P, I, E, O])(
          params: P,
          in: I,
          timeout: FiniteDuration
      ): F[Either[E, O]] =
        // defer: fill/encode run user codecs that may throw; failures must
        // surface through the returned F, not at call-construction time.
        F.defer {
          rpc.subject.fill(params) match
            case Left(e) =>
              F.raiseError(
                new IllegalArgumentException(
                  s"invalid subject params for '${rpc.subject.render}': $e"
                )
              )
            case Right(subject) =>
              client
                .request(subject, rpc.in.encode(in), timeout = timeout)
                .flatMap { reply =>
                  reply.headers.get(MicroHeaders.ErrorCode) match
                    case Some(codeStr) =>
                      val code = codeStr.toIntOption.getOrElse(500)
                      val desc =
                        reply.headers.get(MicroHeaders.Error).getOrElse("")
                      F.pure(Left(rpc.err.decode(code, desc)))
                    case None =>
                      rpc.out.decode(reply.payload) match
                        case Right(o) => F.pure(Right(o))
                        case Left(e)  =>
                          F.raiseError(
                            NatsError.PayloadDecodeError(subject, e)
                          )
                }
        }

      def call[P, E, O](rpc: Rpc[P, Unit, E, O])(params: P): F[Either[E, O]] =
        call[P, Unit, E, O](rpc)(params, (), 5.seconds)
