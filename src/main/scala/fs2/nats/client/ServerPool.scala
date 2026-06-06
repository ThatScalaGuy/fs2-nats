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

package fs2.nats.client

import scala.util.Random

/** Immutable pool of known NATS servers with a round-robin rotation cursor.
  *
  * `servers` holds the configured seed addresses (always present, order
  * preserved). `discovered` holds peers learned at runtime from the server's
  * INFO `connect_urls`; it is fully replaced on each merge so the pool tracks
  * the cluster as nodes join and leave. The rotation walks
  * `servers ++ discovered`.
  *
  * Pure data; the effectful state lives in a `Ref` in `ConnectionManager`.
  *
  * @param servers
  *   The configured seed servers
  * @param discovered
  *   Peers discovered via `connect_urls`
  * @param cursor
  *   Index (into [[candidates]]) of the server to try next
  */
final case class ServerPool(
    servers: Vector[ServerAddress],
    discovered: Vector[ServerAddress],
    cursor: Int
)

object ServerPool:

  /** Build a pool from the configured seed list.
    *
    * @param seeds
    *   The seed servers (the first is the primary)
    * @param randomize
    *   When true, shuffle the seeds but keep the first one pinned in place
    * @param rng
    *   Source of randomness for the shuffle
    */
  def fromSeeds(
      seeds: List[ServerAddress],
      randomize: Boolean,
      rng: Random
  ): ServerPool =
    val ordered =
      if randomize then
        seeds.headOption.toVector ++ rng.shuffle(seeds.drop(1)).toVector
      else seeds.toVector
    ServerPool(ordered, Vector.empty, 0)

  extension (p: ServerPool)
    /** The de-duplicated rotation list: seeds followed by discovered peers. */
    def candidates: Vector[ServerAddress] = (p.servers ++ p.discovered).distinct

    /** The next server to try, advancing the cursor round-robin (wraps).
      * Returns `(updatedPool, (server, startsNewSweep))` to compose directly
      * with `Ref.modify`. `startsNewSweep` is true when the returned server is
      * the first candidate (index 0) — used to gate reconnect backoff so a full
      * pool sweep happens back-to-back and the delay applies only between
      * sweeps.
      */
    def next: (ServerPool, (ServerAddress, Boolean)) =
      val c = p.candidates
      val idx = p.cursor % c.size
      (p.copy(cursor = (p.cursor + 1) % c.size), (c(idx), idx == 0))

    /** Merge gossiped `connect_urls` into the pool.
      *
      *   - `connect_urls` is the complete advertised set, so `discovered` is
      *     replaced (not appended) — peers that drop out of the cluster are
      *     pruned.
      *   - Seeds always survive and are never duplicated into `discovered`.
      *   - The currently-connected server is retained even if momentarily
      *     unadvertised.
      *   - An empty advertised list (single-server / non-cluster) is a no-op so
      *     a transient INFO never wipes legitimately discovered peers.
      *
      * Resets the cursor so the next reconnect sweep starts fresh.
      *
      * @param advertised
      *   Parsed `connect_urls` addresses
      * @param connected
      *   The address currently connected to
      */
    def merge(
        advertised: List[ServerAddress],
        connected: ServerAddress
    ): ServerPool =
      if advertised.isEmpty then p
      else
        val seedSet = p.servers.toSet
        val keepConnected =
          if seedSet(connected) then Vector.empty else Vector(connected)
        val newDiscovered =
          (advertised.toVector ++ keepConnected).distinct.filterNot(seedSet)
        p.copy(discovered = newDiscovered, cursor = 0)
