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

import com.comcast.ip4s.{Host, Port}
import munit.FunSuite
import scala.util.Random

class ServerPoolSpec extends FunSuite:

  private def sa(host: String, port: Int): ServerAddress =
    ServerAddress(Host.fromString(host).get, Port.fromInt(port).get)

  // --- ServerAddress.fromHostPort ---

  test("fromHostPort parses host:port") {
    assertEquals(
      ServerAddress.fromHostPort("nats.example:4222"),
      Some(sa("nats.example", 4222))
    )
  }

  test("fromHostPort defaults a bare host to 4222") {
    assertEquals(
      ServerAddress.fromHostPort("localhost"),
      Some(sa("localhost", 4222))
    )
  }

  test("fromHostPort parses bracketed IPv6") {
    assertEquals(
      ServerAddress.fromHostPort("[::1]:4222"),
      Some(sa("::1", 4222))
    )
  }

  test("fromHostPort rejects a non-numeric port") {
    assertEquals(ServerAddress.fromHostPort("host:nope"), None)
  }

  test("fromHostPort rejects empty input") {
    assertEquals(ServerAddress.fromHostPort(""), None)
  }

  // --- fromSeeds ---

  test("fromSeeds preserves order when not randomizing") {
    val seeds = List(sa("a", 4222), sa("b", 4222), sa("c", 4222))
    val pool = ServerPool.fromSeeds(seeds, randomize = false, new Random(0))
    assertEquals(pool.servers, seeds.toVector)
    assertEquals(pool.discovered, Vector.empty)
    assertEquals(pool.cursor, 0)
  }

  test("fromSeeds pins the first seed and keeps every seed when randomizing") {
    val seeds = (1 to 8).map(i => sa(s"n$i", 4222)).toList
    val pool = ServerPool.fromSeeds(seeds, randomize = true, new Random(42))
    assertEquals(pool.servers.head, seeds.head)
    assertEquals(pool.servers.toSet, seeds.toSet)
    assertEquals(pool.servers.size, seeds.size)
  }

  test("fromSeeds shuffle is deterministic for a given seed") {
    val seeds = (1 to 8).map(i => sa(s"n$i", 4222)).toList
    val a = ServerPool.fromSeeds(seeds, randomize = true, new Random(7))
    val b = ServerPool.fromSeeds(seeds, randomize = true, new Random(7))
    assertEquals(a.servers, b.servers)
  }

  // --- next ---

  test("next round-robins over candidates, wraps, and flags new sweeps") {
    val seeds = List(sa("a", 4222), sa("b", 4222), sa("c", 4222))
    val p0 = ServerPool.fromSeeds(seeds, randomize = false, new Random(0))
    val (p1, (a1, s1)) = p0.next
    val (p2, (a2, s2)) = p1.next
    val (p3, (a3, s3)) = p2.next
    val (_, (a4, s4)) = p3.next
    assertEquals(List(a1, a2, a3), seeds)
    assertEquals(a4, seeds.head) // wrapped
    // startsNewSweep is true only on the first candidate of each sweep
    assertEquals(List(s1, s2, s3, s4), List(true, false, false, true))
  }

  // --- merge ---

  test("merge adds discovered peers and excludes seeds") {
    val p0 = ServerPool.fromSeeds(
      List(sa("s1", 4222)),
      randomize = false,
      new Random(0)
    )
    val p1 = p0.merge(
      List(sa("s1", 4222), sa("s2", 4222), sa("s3", 4222)),
      sa("s1", 4222)
    )
    assertEquals(p1.discovered, Vector(sa("s2", 4222), sa("s3", 4222)))
    assertEquals(
      p1.candidates,
      Vector(sa("s1", 4222), sa("s2", 4222), sa("s3", 4222))
    )
  }

  test("merge replaces discovered so departed peers are pruned") {
    val p0 = ServerPool.fromSeeds(
      List(sa("s1", 4222)),
      randomize = false,
      new Random(0)
    )
    val p1 = p0.merge(List(sa("s2", 4222), sa("s3", 4222)), sa("s1", 4222))
    val p2 = p1.merge(List(sa("s2", 4222)), sa("s1", 4222)) // s3 gone
    assertEquals(p2.discovered, Vector(sa("s2", 4222)))
    assertEquals(p2.candidates, Vector(sa("s1", 4222), sa("s2", 4222)))
  }

  test("merge always keeps seeds even when unadvertised") {
    val p0 = ServerPool.fromSeeds(
      List(sa("s1", 4222)),
      randomize = false,
      new Random(0)
    )
    val p1 = p0.merge(List(sa("s2", 4222)), sa("s1", 4222))
    assert(p1.candidates.contains(sa("s1", 4222)))
  }

  test("merge with an empty advertised list is a no-op (never prunes)") {
    val p0 = ServerPool.fromSeeds(
      List(sa("s1", 4222)),
      randomize = false,
      new Random(0)
    )
    val p1 = p0.merge(List(sa("s2", 4222)), sa("s1", 4222))
    val p2 = p1.merge(Nil, sa("s1", 4222))
    assertEquals(p2.discovered, p1.discovered)
  }

  test(
    "merge retains the currently-connected discovered node even if unadvertised"
  ) {
    val p0 = ServerPool.fromSeeds(
      List(sa("s1", 4222)),
      randomize = false,
      new Random(0)
    )
    val p1 = p0.merge(List(sa("s2", 4222), sa("s3", 4222)), sa("s1", 4222))
    // Connected to s2 (a discovered node); next INFO advertises only s3.
    val p2 = p1.merge(List(sa("s3", 4222)), sa("s2", 4222))
    assert(p2.candidates.contains(sa("s2", 4222)))
    assert(p2.candidates.contains(sa("s3", 4222)))
  }
