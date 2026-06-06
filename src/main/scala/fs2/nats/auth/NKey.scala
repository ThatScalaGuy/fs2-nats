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
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters
import org.bouncycastle.crypto.signers.Ed25519Signer

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import scala.util.control.NonFatal

/** NATS NKey (Ed25519) signing.
  *
  * Implements the seed-decoding and nonce-signing parts of the NATS NKey scheme
  * so the client can answer the server's authentication challenge. A NATS seed
  * (an `S...` string) carries the Ed25519 private key from which both the
  * signature and the public key are derived.
  *
  * All cryptographic dependencies (Bouncy Castle) are confined to this object
  * so the provider can be swapped without touching the rest of the client. None
  * of these functions log or otherwise expose the seed or raw key bytes.
  */
object NKey:

  // PREFIX_BYTE_SEED = 18 << 3; encoded in the high 5 bits of a seed's first byte.
  private val PrefixByteSeed = 18 << 3

  // RFC 4648 base32 alphabet (no padding), as used by NATS NKeys.
  private val Base32Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"

  /** Sign `nonce` with the Ed25519 key derived from `seed` and return the
    * signature as a base64url string without padding (the CONNECT `sig` field).
    * The raw bytes of the nonce string are signed as-is.
    */
  def signNonce(seed: String, nonce: String): Either[Throwable, String] =
    decodeSeed(seed).flatMap { case (_, raw) =>
      catchNonFatal {
        val priv = new Ed25519PrivateKeyParameters(raw, 0)
        val signer = new Ed25519Signer
        signer.init(true, priv)
        val bytes = nonce.getBytes(UTF_8)
        signer.update(bytes, 0, bytes.length)
        Base64.getUrlEncoder.withoutPadding
          .encodeToString(signer.generateSignature())
      }
    }

  /** Derive the public NKey string (e.g. a `U...` user key) from `seed`. */
  def publicKeyFromSeed(seed: String): Either[Throwable, String] =
    decodeSeed(seed).flatMap { case (prefix, raw) =>
      catchNonFatal {
        val priv = new Ed25519PrivateKeyParameters(raw, 0)
        encodeNKey(prefix, priv.generatePublicKey().getEncoded())
      }
    }

  /** Decode a seed into its key-type prefix byte and the raw 32-byte Ed25519
    * seed, validating the base32 encoding, the seed prefix and the CRC-16
    * checksum.
    */
  private def decodeSeed(seed: String): Either[Throwable, (Byte, Array[Byte])] =
    base32Decode(seed).flatMap { decoded =>
      if decoded.length != 36 then
        Left(authError("invalid NKey seed: unexpected length"))
      else
        val payload = decoded.slice(0, decoded.length - 2)
        val storedCrc =
          (decoded(decoded.length - 2) & 0xff) |
            ((decoded(decoded.length - 1) & 0xff) << 8)
        if crc16(payload) != storedCrc then
          Left(authError("invalid NKey seed: checksum mismatch"))
        else
          val b0 = payload(0) & 0xff
          val b1 = payload(1) & 0xff
          if (b0 & 0xf8) != PrefixByteSeed then
            Left(authError("invalid NKey seed: not a seed"))
          else
            val prefix = (((b0 & 7) << 5) | ((b1 & 0xf8) >> 3)).toByte
            Right((prefix, payload.slice(2, 34)))
    }

  /** Encode a key-type prefix byte and a 32-byte public key into an NKey string
    * (base32 of prefix ++ key ++ CRC-16).
    */
  private def encodeNKey(prefix: Byte, key: Array[Byte]): String =
    val body = Array(prefix) ++ key
    val crc = crc16(body)
    base32Encode(
      body ++ Array((crc & 0xff).toByte, ((crc >> 8) & 0xff).toByte)
    )

  private def base32Decode(s: String): Either[Throwable, Array[Byte]] =
    s.find(c => Base32Alphabet.indexOf(c.toInt) < 0) match
      case Some(c) =>
        Left(authError(s"invalid NKey seed: non-base32 character '$c'"))
      case None =>
        val out = scala.collection.mutable.ArrayBuilder.make[Byte]
        var value = 0
        var bits = 0
        var i = 0
        while i < s.length do
          value = (value << 5) | Base32Alphabet.indexOf(s.charAt(i).toInt)
          bits += 5
          if bits >= 8 then
            bits -= 8
            out += ((value >> bits) & 0xff).toByte
          i += 1
        Right(out.result())

  private def base32Encode(bytes: Array[Byte]): String =
    val sb = new StringBuilder
    var value = 0
    var bits = 0
    var i = 0
    while i < bytes.length do
      value = (value << 8) | (bytes(i) & 0xff)
      bits += 8
      while bits >= 5 do
        bits -= 5
        sb.append(Base32Alphabet.charAt((value >> bits) & 0x1f))
      i += 1
    if bits > 0 then
      sb.append(Base32Alphabet.charAt((value << (5 - bits)) & 0x1f))
    sb.toString

  // CRC-16/XMODEM: polynomial 0x1021, initial value 0x0000, no reflection.
  private def crc16(data: Array[Byte]): Int =
    var crc = 0
    var i = 0
    while i < data.length do
      crc ^= (data(i) & 0xff) << 8
      var j = 0
      while j < 8 do
        crc =
          if (crc & 0x8000) != 0 then (crc << 1) ^ 0x1021
          else crc << 1
        crc &= 0xffff
        j += 1
      i += 1
    crc

  private def authError(message: String): NatsError =
    NatsError.AuthorizationError(message)

  private def catchNonFatal[A](thunk: => A): Either[Throwable, A] =
    try Right(thunk)
    catch
      case NonFatal(e) =>
        // Deliberately omit the exception message to avoid leaking key material.
        Left(authError(s"NKey signing failed (${e.getClass.getSimpleName})"))
