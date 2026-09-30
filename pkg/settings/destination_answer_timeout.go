/*
 * Copyright © 2026 STRATO GmbH
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

package settings

import "github.com/clyso/chorus/pkg/switches"

// How long a read served from the replication destination waits for it to
// begin answering, in milliseconds, connecting to it included. Reading the
// answer itself is not bounded by it, so no object is too large for it.
// Zero waits as long as any other read does.
//
// A destination slower than this is not the faster storage the read went to it
// for, and at runtime this is the lever for one that turns out to be slow.
var DestinationAnswerTimeout = switches.NewInt("destination-answer-timeout", 5000, 0, 3600000, nil)
