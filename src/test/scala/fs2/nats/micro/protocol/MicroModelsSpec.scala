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

package fs2.nats.micro.protocol

import com.github.plokhotnyuk.jsoniter_scala.core.{
  readFromString,
  writeToString
}

import java.time.Instant

/** Golden tests for the ADR-32 discovery wire format: exact JSON strings, so
  * any codec-config drift (field naming, omission rules, the `type` tag,
  * `Instant` encoding) fails loudly.
  */
class MicroModelsSpec extends munit.FunSuite:

  private val ping =
    PingResponse(
      name = "calc",
      id = "abc123",
      version = "1.2.3",
      metadata = Map("region" -> "eu")
    )

  private val info =
    InfoResponse(
      name = "calc",
      id = "abc123",
      version = "1.2.3",
      description = Some("does maths"),
      endpoints = List(
        WireEndpointInfo(
          "add",
          "calc.add.*",
          "q",
          Map("request_schema" -> "two ints", "response_schema" -> "an int")
        )
      )
    )

  private val stats =
    StatsResponse(
      name = "calc",
      id = "abc123",
      version = "1.2.3",
      started = Instant.parse("2026-01-02T03:04:05Z"),
      endpoints = List(
        WireEndpointStats(
          name = "add",
          subject = "calc.add.*",
          queueGroup = "q",
          numRequests = 3L,
          numErrors = 1L,
          lastError = Some("500 boom"),
          processingTime = 3000L,
          averageProcessingTime = 1000L
        ),
        WireEndpointStats(
          name = "sub",
          subject = "calc.sub.*",
          queueGroup = "q",
          numRequests = 0L,
          numErrors = 0L,
          lastError = None,
          processingTime = 0L,
          averageProcessingTime = 0L
        )
      )
    )

  test("PING response golden JSON with metadata present") {
    assertEquals(
      writeToString(ping),
      """{"type":"io.nats.micro.v1.ping_response","name":"calc","id":"abc123","version":"1.2.3","metadata":{"region":"eu"}}"""
    )
  }

  test("INFO response golden JSON with description and endpoint schemas") {
    // Service-level metadata is empty and therefore omitted entirely.
    assertEquals(
      writeToString(info),
      """{"type":"io.nats.micro.v1.info_response","name":"calc","id":"abc123","version":"1.2.3","description":"does maths","endpoints":[{"name":"add","subject":"calc.add.*","queue_group":"q","metadata":{"request_schema":"two ints","response_schema":"an int"}}]}"""
    )
  }

  test("STATS response golden JSON, lastError present and absent") {
    // `last_error` appears on the first endpoint only; `average_processing_time`
    // is a plain int64 field, not recomputed by the codec.
    assertEquals(
      writeToString(stats),
      """{"type":"io.nats.micro.v1.stats_response","name":"calc","id":"abc123","version":"1.2.3","started":"2026-01-02T03:04:05Z","endpoints":[{"name":"add","subject":"calc.add.*","queue_group":"q","num_requests":3,"num_errors":1,"last_error":"500 boom","processing_time":3000,"average_processing_time":1000},{"name":"sub","subject":"calc.sub.*","queue_group":"q","num_requests":0,"num_errors":0,"processing_time":0,"average_processing_time":0}]}"""
    )
  }

  test("PING response round-trips") {
    assertEquals(readFromString[PingResponse](writeToString(ping)), ping)
  }

  test("INFO response round-trips") {
    assertEquals(readFromString[InfoResponse](writeToString(info)), info)
  }

  test("STATS response round-trips") {
    assertEquals(readFromString[StatsResponse](writeToString(stats)), stats)
  }
