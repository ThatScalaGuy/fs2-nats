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

package fs2.nats.jetstream.protocol

import io.circe.{Decoder, Encoder}

/** Helper to build a string-valued circe codec pair from an explicit wire
  * mapping, raising a decode error on unknown values.
  */
private[protocol] object EnumCodec:
  def encoder[A](toWire: A => String): Encoder[A] =
    Encoder.encodeString.contramap(toWire)

  def decoder[A](
      name: String,
      fromWire: PartialFunction[String, A]
  ): Decoder[A] =
    Decoder.decodeString.emap { s =>
      fromWire.lift(s).toRight(s"Unknown $name value: $s")
    }

/** Stream retention policy. */
enum RetentionPolicy:
  case Limits, Interest, WorkQueue
object RetentionPolicy:
  private val toWire: RetentionPolicy => String =
    case Limits    => "limits"
    case Interest  => "interest"
    case WorkQueue => "workqueue"
  given Encoder[RetentionPolicy] = EnumCodec.encoder(toWire)
  given Decoder[RetentionPolicy] = EnumCodec.decoder(
    "RetentionPolicy",
    {
      case "limits"    => Limits
      case "interest"  => Interest
      case "workqueue" => WorkQueue
    }
  )

/** Stream storage backend. */
enum StorageType:
  case File, Memory
object StorageType:
  private val toWire: StorageType => String =
    case File   => "file"
    case Memory => "memory"
  given Encoder[StorageType] = EnumCodec.encoder(toWire)
  given Decoder[StorageType] = EnumCodec.decoder(
    "StorageType",
    {
      case "file"   => File
      case "memory" => Memory
    }
  )

/** Behavior when a limit-retention stream is full. */
enum DiscardPolicy:
  case Old, New
object DiscardPolicy:
  private val toWire: DiscardPolicy => String =
    case Old => "old"
    case New => "new"
  given Encoder[DiscardPolicy] = EnumCodec.encoder(toWire)
  given Decoder[DiscardPolicy] = EnumCodec.decoder(
    "DiscardPolicy",
    {
      case "old" => Old
      case "new" => New
    }
  )

/** Stream storage compression. */
enum StoreCompression:
  case None, S2
object StoreCompression:
  private val toWire: StoreCompression => String =
    case None => "none"
    case S2   => "s2"
  given Encoder[StoreCompression] = EnumCodec.encoder(toWire)
  given Decoder[StoreCompression] = EnumCodec.decoder(
    "StoreCompression",
    {
      case "none" => None
      case "s2"   => S2
    }
  )

/** Where a consumer begins delivering from. */
enum DeliverPolicy:
  case All, Last, New, ByStartSequence, ByStartTime, LastPerSubject
object DeliverPolicy:
  private val toWire: DeliverPolicy => String =
    case All             => "all"
    case Last            => "last"
    case New             => "new"
    case ByStartSequence => "by_start_sequence"
    case ByStartTime     => "by_start_time"
    case LastPerSubject  => "last_per_subject"
  given Encoder[DeliverPolicy] = EnumCodec.encoder(toWire)
  given Decoder[DeliverPolicy] = EnumCodec.decoder(
    "DeliverPolicy",
    {
      case "all"               => All
      case "last"              => Last
      case "new"               => New
      case "by_start_sequence" => ByStartSequence
      case "by_start_time"     => ByStartTime
      case "last_per_subject"  => LastPerSubject
    }
  )

/** Consumer acknowledgement policy. */
enum AckPolicy:
  case None, All, Explicit
object AckPolicy:
  private val toWire: AckPolicy => String =
    case None     => "none"
    case All      => "all"
    case Explicit => "explicit"
  given Encoder[AckPolicy] = EnumCodec.encoder(toWire)
  given Decoder[AckPolicy] = EnumCodec.decoder(
    "AckPolicy",
    {
      case "none"     => None
      case "all"      => All
      case "explicit" => Explicit
    }
  )

/** Consumer replay pacing. */
enum ReplayPolicy:
  case Instant, Original
object ReplayPolicy:
  private val toWire: ReplayPolicy => String =
    case Instant  => "instant"
    case Original => "original"
  given Encoder[ReplayPolicy] = EnumCodec.encoder(toWire)
  given Decoder[ReplayPolicy] = EnumCodec.decoder(
    "ReplayPolicy",
    {
      case "instant"  => Instant
      case "original" => Original
    }
  )
