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

package fs2.nats.kv

/** Bucket/key validation and the key <-> subject mapping for the KV layer.
  *
  * A KV bucket is a JetStream stream named `KV_<bucket>` whose subjects are
  * `$KV.<bucket>.>`; a key `a.b` maps to the subject `$KV.<bucket>.a.b`. Keys
  * follow the NATS KV character set (`[-/_=.a-zA-Z0-9]+`); watch patterns
  * additionally allow the `*` and `>` wildcards.
  */
object KvNames:

  /** The JetStream stream name backing a bucket. */
  def streamName(bucket: String): String = "KV_" + bucket

  /** The bucket name encoded in a `KV_`-prefixed stream name, if any. */
  def bucketFromStream(stream: String): Option[String] =
    if stream.startsWith("KV_") then Some(stream.substring(3)) else None

  /** The subject a key is published to. */
  def keySubject(bucket: String, key: String): String =
    s"$$KV.$bucket.$key"

  /** The wildcard subject covering every key in a bucket. */
  def allKeysSubject(bucket: String): String = s"$$KV.$bucket.>"

  /** The subject covering a watch pattern (keys may contain `*`/`>`). */
  def patternSubject(bucket: String, pattern: String): String =
    s"$$KV.$bucket.$pattern"

  /** Recover the key from a delivered `$KV.<bucket>.<key>` subject. */
  def keyFromSubject(bucket: String, subject: String): String =
    val prefix = s"$$KV.$bucket."
    if subject.startsWith(prefix) then subject.substring(prefix.length)
    else subject

  /** Validate a bucket name (`[A-Za-z0-9_-]+`). */
  def validateBucket(bucket: String): Either[String, String] =
    if bucket.isEmpty then Left("Bucket name cannot be empty")
    else if isValidBucket(bucket) then Right(bucket)
    else Left(s"Invalid bucket name '$bucket': must match [A-Za-z0-9_-]+")

  /** Validate a key for put/get/delete (no wildcards). */
  def validateKey(key: String): Either[String, String] =
    validate(key, allowWildcards = false)

  /** Validate a watch pattern (allows the `*` and `>` wildcards). */
  def validateKeyPattern(pattern: String): Either[String, String] =
    validate(pattern, allowWildcards = true)

  private def validate(
      key: String,
      allowWildcards: Boolean
  ): Either[String, String] =
    if key.isEmpty then Left("Key cannot be empty")
    else if key.charAt(0) == '.' || key.charAt(key.length - 1) == '.' then
      Left(s"Invalid key '$key': cannot start or end with '.'")
    else if !validChars(key, allowWildcards) then
      Left(s"Invalid key '$key': illegal characters")
    else Right(key)

  private def isValidBucket(s: String): Boolean =
    var i = 0
    var ok = true
    while ok && i < s.length do
      val c = s.charAt(i)
      ok = isAlphaNum(c) || c == '_' || c == '-'
      i += 1
    ok

  private def validChars(s: String, allowWildcards: Boolean): Boolean =
    var i = 0
    var ok = true
    while ok && i < s.length do
      val c = s.charAt(i)
      ok = isAlphaNum(c) || c == '-' || c == '/' || c == '_' ||
        c == '=' || c == '.' ||
        (allowWildcards && (c == '*' || c == '>'))
      i += 1
    ok

  private def isAlphaNum(c: Char): Boolean =
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
