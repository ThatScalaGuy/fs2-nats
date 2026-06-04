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

import com.github.plokhotnyuk.jsoniter_scala.core.*

/** Helper to build a string-valued jsoniter codec from an explicit wire
  * mapping, raising a decode error on unknown values.
  */
private[protocol] object EnumCodec:
  def apply[A](
      name: String,
      toWire: A => String,
      fromWire: PartialFunction[String, A]
  ): JsonValueCodec[A] =
    new JsonValueCodec[A]:
      private val lifted = fromWire.lift
      def decodeValue(in: JsonReader, default: A): A =
        val s = in.readString(null)
        lifted(s) match
          case Some(a) => a
          case None    => in.decodeError(s"Unknown $name value: $s")
      def encodeValue(x: A, out: JsonWriter): Unit =
        out.writeVal(toWire(x))
      def nullValue: A = null.asInstanceOf[A]

/** Stream retention policy. */
enum RetentionPolicy:
  case Limits, Interest, WorkQueue
object RetentionPolicy:
  private val toWire: RetentionPolicy => String =
    case Limits    => "limits"
    case Interest  => "interest"
    case WorkQueue => "workqueue"
  given JsonValueCodec[RetentionPolicy] = EnumCodec(
    "RetentionPolicy",
    toWire,
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
  given JsonValueCodec[StorageType] = EnumCodec(
    "StorageType",
    toWire,
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
  given JsonValueCodec[DiscardPolicy] = EnumCodec(
    "DiscardPolicy",
    toWire,
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
  given JsonValueCodec[StoreCompression] = EnumCodec(
    "StoreCompression",
    toWire,
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
  given JsonValueCodec[DeliverPolicy] = EnumCodec(
    "DeliverPolicy",
    toWire,
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
  given JsonValueCodec[AckPolicy] = EnumCodec(
    "AckPolicy",
    toWire,
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
  given JsonValueCodec[ReplayPolicy] = EnumCodec(
    "ReplayPolicy",
    toWire,
    {
      case "instant"  => Instant
      case "original" => Original
    }
  )
