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

// Whether this process takes part in reading migrated objects from the
// replication destination: the proxy serves such reads from there, and the
// worker records what it copies for the proxy to read. Off unless a migration
// asks for it.
//
// Off again at runtime is the lever for a destination that turns out to answer
// wrongly: reads go back to the source at once, and the records the worker
// keeps meanwhile let it be turned on again without listing anything twice.
var ReadFromDestination = switches.NewBool("read-from-destination", false, nil)
