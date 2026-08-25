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

import cats.effect.{Async, Resource}
import fs2.nats.client.NatsClient

/** A running NATS micro service instance (ADR-32): endpoint subscriptions,
  * `$SRV` discovery control plane and per-endpoint statistics. Scoped to the
  * `Resource` returned by [[NatsService.apply]]; release unsubscribes and
  * cancels in-flight handlers.
  */
sealed trait NatsService[F[_]]:
  /** Unique instance id (22-char base62). */
  def id: String
  def info: F[ServiceInfo]
  def stats: F[ServiceStats]

  /** Zero all counters and re-stamp `started`. */
  def reset: F[Unit]

object NatsService:

  /** Start a service. Validation failures (bad name/version, duplicate or empty
    * endpoints) raise `IllegalArgumentException` during acquisition.
    */
  def apply[F[_]: Async](
      client: NatsClient[F],
      config: ServiceConfig,
      handlers: List[MicroHandler[F]]
  ): Resource[F, NatsService[F]] =
    ServiceImpl.resource(client, config, handlers)

private[micro] final class NatsServiceImpl[F[_]](runtime: ServiceRuntime[F])
    extends NatsService[F]:
  def id: String = runtime.id
  def info: F[ServiceInfo] = runtime.info
  def stats: F[ServiceStats] = runtime.stats
  def reset: F[Unit] = runtime.reset
