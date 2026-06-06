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

import fs2.nats.errors.NatsError
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters
import org.bouncycastle.crypto.signers.Ed25519Signer
import munit.FunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

class NKeySpec extends FunSuite:

  // A real, matched NATS user key pair, generated independently of this code
  // with `nsc generate nkey --user`. These are test fixtures, not live
  // credentials. The pair lets the known-answer test exercise base32 decode,
  // CRC-16 validation and Ed25519 key derivation in one shot.
  private val Seed =
    "SUAO7NFVXBKDTBLB74TVJUV4H2BGV6YJYAIAQGFROYC3FWWLBGRHG6YHPQ"
  private val PublicKey =
    "UAYWWLF5J52RINLYC3BIFGJMY2IIMCXW3PG4VZSIPAIF67WULKORA7XT"

  test("publicKeyFromSeed derives the matching public NKey") {
    assertEquals(NKey.publicKeyFromSeed(Seed), Right(PublicKey))
  }

  test("signNonce matches a cross-implementation known-answer vector") {
    // Expected signature computed independently with Python's `cryptography`
    // (RFC 8032 Ed25519) over the same seed + nonce; Ed25519 signatures are
    // deterministic, so any correct implementation must reproduce this exactly.
    val nonce = "PXoWU7zWkEwIHCWQ-test-nonce"
    val expected =
      "pc2Qmhac4s_kTR0GVBqLr6BXAVWvfTCvNh-hya29P3nhWsHLtvHQZSrQiBGKyIfG9poaEw1WvLVNX4KIAJUeDA"
    assertEquals(NKey.signNonce(Seed, nonce), Right(expected))
  }

  test("signNonce produces a signature verifiable by the derived public key") {
    val nonce = "a-server-nonce-1234567890"
    val sig = NKey.signNonce(Seed, nonce).fold(throw _, identity)
    assert(verify(PublicKey, nonce, sig), "signature did not verify")
  }

  test("signNonce rejects a seed with a corrupted checksum") {
    // Flip one character in the payload region so the stored CRC no longer matches.
    val corrupted = Seed.updated(8, if Seed.charAt(8) == 'Y' then 'Z' else 'Y')
    assertSigningFails(corrupted)
  }

  test("signNonce rejects a public key passed where a seed is expected") {
    assertSigningFails(PublicKey)
  }

  test("signNonce rejects a non-base32 seed") {
    assertSigningFails(Seed.updated(2, '1'))
  }

  test("signNonce rejects an empty seed") {
    assertSigningFails("")
  }

  private def assertSigningFails(seed: String): Unit =
    NKey.signNonce(seed, "nonce") match
      case Left(_: NatsError.AuthorizationError) => ()
      case other => fail(s"expected AuthorizationError, got $other")

  /** Verify a base64url (no-padding) signature against an `nkey` public key by
    * decoding it back to the raw 32-byte Ed25519 key and using Bouncy Castle.
    */
  private def verify(nkey: String, nonce: String, sigB64Url: String): Boolean =
    val raw =
      base32Decode(nkey).slice(1, 33) // drop prefix byte, take 32-byte key
    val verifier = new Ed25519Signer
    verifier.init(false, new Ed25519PublicKeyParameters(raw, 0))
    val bytes = nonce.getBytes(UTF_8)
    verifier.update(bytes, 0, bytes.length)
    verifier.verifySignature(Base64.getUrlDecoder.decode(sigB64Url))

  private def base32Decode(s: String): Array[Byte] =
    val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
    val out = scala.collection.mutable.ArrayBuilder.make[Byte]
    var value = 0
    var bits = 0
    var i = 0
    while i < s.length do
      value = (value << 5) | alphabet.indexOf(s.charAt(i).toInt)
      bits += 5
      if bits >= 8 then
        bits -= 8
        out += ((value >> bits) & 0xff).toByte
      i += 1
    out.result()
