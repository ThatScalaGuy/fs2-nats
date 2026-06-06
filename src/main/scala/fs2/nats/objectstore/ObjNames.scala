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

package fs2.nats.objectstore

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

/** Bucket/object validation and the name <-> subject mapping for the Object
  * Store layer.
  *
  * An Object Store bucket is a JetStream stream named `OBJ_<bucket>` with two
  * subject spaces: chunks at `$O.<bucket>.C.<object-nuid>` and per-object meta
  * at `$O.<bucket>.M.<name-encoded>`, where `name-encoded` is the URL-safe
  * base64 (with padding) of the UTF-8 object name — so any object name is safe
  * as a single NATS subject token.
  */
object ObjNames:

  /** Maximum raw object-name length in bytes (keeps the base64 subject token
    * within practical NATS limits).
    */
  val MaxNameBytes: Int = 190

  /** The JetStream stream name backing a bucket. */
  def streamName(bucket: String): String = "OBJ_" + bucket

  /** The bucket name encoded in an `OBJ_`-prefixed stream name, if any. */
  def bucketFromStream(stream: String): Option[String] =
    if stream.startsWith("OBJ_") then Some(stream.substring(4)) else None

  /** The wildcard subject covering every chunk in a bucket. */
  def chunkStreamSubject(bucket: String): String = s"$$O.$bucket.C.>"

  /** The wildcard subject covering every meta entry in a bucket. */
  def metaStreamSubject(bucket: String): String = s"$$O.$bucket.M.>"

  /** The subject the chunks of one object (identified by `nuid`) are published
    * to.
    */
  def chunkSubject(bucket: String, nuid: String): String =
    s"$$O.$bucket.C.$nuid"

  /** The subject an object's meta is published to. */
  def metaSubject(bucket: String, name: String): String =
    s"$$O.$bucket.M.${encodeName(name)}"

  /** URL-safe base64 (with padding) of the UTF-8 object name. */
  def encodeName(name: String): String =
    Base64.getUrlEncoder.encodeToString(name.getBytes(UTF_8))

  /** Inverse of [[encodeName]]. */
  def decodeName(token: String): String =
    new String(Base64.getUrlDecoder.decode(token), UTF_8)

  /** Recover the object name from a delivered `$O.<bucket>.M.<encoded>`
    * subject.
    */
  def nameFromMetaSubject(bucket: String, subject: String): String =
    val prefix = s"$$O.$bucket.M."
    if subject.startsWith(prefix) then
      decodeName(subject.substring(prefix.length))
    else subject

  /** Validate a bucket name (`[A-Za-z0-9_-]+`). */
  def validateBucket(bucket: String): Either[String, String] =
    if bucket.isEmpty then Left("Bucket name cannot be empty")
    else if isValidBucket(bucket) then Right(bucket)
    else Left(s"Invalid bucket name '$bucket': must match [A-Za-z0-9_-]+")

  /** Validate an object name (non-empty, within the length bound). */
  def validateObjectName(name: String): Either[String, String] =
    if name.isEmpty then Left("Object name cannot be empty")
    else if name.getBytes(UTF_8).length > MaxNameBytes then
      Left(s"Object name too long (max $MaxNameBytes bytes)")
    else Right(name)

  private def isValidBucket(s: String): Boolean =
    var i = 0
    var ok = true
    while ok && i < s.length do
      val c = s.charAt(i)
      ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || c == '_' || c == '-'
      i += 1
    ok
