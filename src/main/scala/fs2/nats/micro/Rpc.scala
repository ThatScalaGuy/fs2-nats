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

import fs2.nats.protocol.Headers

/** A value describing one endpoint: subject pattern with typed captures `P`,
  * request payload `I`, typed error `E`, response payload `O`.
  *
  * The same value is interpreted by [[NatsService]] (server side) and [[Micro]]
  * (client side).
  */
final class Rpc[P, I, E, O] private (
    val name: String,
    val subject: SubjectPattern[P],
    val in: Payload[I],
    val err: ServiceErr[E],
    val out: Payload[O],
    val queueGroup: Option[String],
    val metadata: Map[String, String]
):

  private def copy(
      queueGroup: Option[String] = queueGroup,
      metadata: Map[String, String] = metadata
  ): Rpc[P, I, E, O] =
    new Rpc(name, subject, in, err, out, queueGroup, metadata)

  /** Override the service-level queue group for this endpoint. */
  def withQueueGroup(q: String): Rpc[P, I, E, O] =
    copy(queueGroup = Some(q))

  /** Endpoint metadata published in `$SRV.INFO`. */
  def withMetadata(m: Map[String, String]): Rpc[P, I, E, O] =
    copy(metadata = m)

  /** Attach server logic. */
  def handle[F[_]](f: (P, I) => F[Either[E, O]]): MicroHandler[F] =
    new MicroHandler.Impl[F, P, I, E, O](this, (p, _, i) => f(p, i))

  /** Variant with access to request headers. */
  def handleWithHeaders[F[_]](
      f: (P, Headers, I) => F[Either[E, O]]
  ): MicroHandler[F] =
    new MicroHandler.Impl[F, P, I, E, O](this, f)

object Rpc:

  def apply[P, I, E, O](
      name: String,
      subject: SubjectPattern[P],
      in: Payload[I],
      err: ServiceErr[E],
      out: Payload[O]
  ): Rpc[P, I, E, O] =
    new Rpc(name, subject, in, err, out, None, Map.empty)

/** An [[Rpc]] with logic attached; opaque to users, consumed by
  * [[NatsService]].
  */
sealed abstract class MicroHandler[F[_]]

object MicroHandler:

  private[micro] final class Impl[F[_], P, I, E, O](
      val rpc: Rpc[P, I, E, O],
      val run: (P, Headers, I) => F[Either[E, O]]
  ) extends MicroHandler[F]
