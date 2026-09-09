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

// How many reads in a row the replication destination may fail to answer in
// time before reads are kept off it, see DestinationAnswerTimeout. Zero turns
// the pausing off.
var DestinationTimeoutLimit = switches.NewInt("destination-timeout-limit", 3, 0, 1000, nil)

// How long reads stay on the source storage once the destination reached
// DestinationTimeoutLimit, in milliseconds. Zero turns the pausing off.
var DestinationPause = switches.NewInt("destination-pause", 300000, 0, 86400000, nil)
