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

package fs2.nats.jetstream

/** Builds `$JS.API.*` subjects from a single configured prefix. With a domain
  * the prefix becomes `$JS.<domain>.API.`; otherwise it is the configured
  * `apiPrefix` (default `$JS.API.`).
  */
final class ApiSubjects(val prefix: String):

  // ---- Account ----
  def accountInfo: String = s"${prefix}INFO"

  // ---- Streams ----
  def streamCreate(name: String): String = s"${prefix}STREAM.CREATE.$name"
  def streamUpdate(name: String): String = s"${prefix}STREAM.UPDATE.$name"
  def streamInfo(name: String): String = s"${prefix}STREAM.INFO.$name"
  def streamDelete(name: String): String = s"${prefix}STREAM.DELETE.$name"
  def streamNames: String = s"${prefix}STREAM.NAMES"
  def streamList: String = s"${prefix}STREAM.LIST"
  def streamPurge(name: String): String = s"${prefix}STREAM.PURGE.$name"
  def msgGet(name: String): String = s"${prefix}STREAM.MSG.GET.$name"
  def msgDelete(name: String): String = s"${prefix}STREAM.MSG.DELETE.$name"

  // ---- Consumers ----
  def consumerCreate(stream: String, consumer: String): String =
    s"${prefix}CONSUMER.CREATE.$stream.$consumer"
  def consumerCreateFiltered(
      stream: String,
      consumer: String,
      filter: String
  ): String =
    s"${prefix}CONSUMER.CREATE.$stream.$consumer.$filter"
  def consumerCreateEphemeral(stream: String): String =
    s"${prefix}CONSUMER.CREATE.$stream"
  def consumerInfo(stream: String, consumer: String): String =
    s"${prefix}CONSUMER.INFO.$stream.$consumer"
  def consumerDelete(stream: String, consumer: String): String =
    s"${prefix}CONSUMER.DELETE.$stream.$consumer"
  def consumerNames(stream: String): String =
    s"${prefix}CONSUMER.NAMES.$stream"
  def consumerList(stream: String): String =
    s"${prefix}CONSUMER.LIST.$stream"
  def msgNext(stream: String, consumer: String): String =
    s"${prefix}CONSUMER.MSG.NEXT.$stream.$consumer"

object ApiSubjects:

  /** Construct subjects for the given API prefix and optional domain. */
  def apply(apiPrefix: String, domain: Option[String]): ApiSubjects =
    val prefix = domain match
      case Some(d) if d.nonEmpty => s"$$JS.$d.API."
      case _                     => apiPrefix
    new ApiSubjects(prefix)
