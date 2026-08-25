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

package fs2.nats.micro

import fs2.nats.protocol.Headers

/** A successful response together with the headers to put on the reply.
  *
  * Produced by handlers attached with `Rpc#handleWithHeaders` and returned to
  * callers of `Micro#callWithHeaders`. `Reply(o)` means "no response headers",
  * which is what `Rpc#handle` and `Micro#call` use throughout.
  *
  * Only success replies carry custom headers: an error reply is the ADR-32
  * empty body plus `Nats-Service-Error-Code` / `Nats-Service-Error`, so a
  * handler's `Left(e)` has nowhere to put them.
  *
  * @param value
  *   The response payload, encoded with the endpoint's `out` codec
  * @param headers
  *   Headers to publish with the reply (empty by default)
  */
final case class Reply[+O](value: O, headers: Headers = Headers.empty)
