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

// The settings of chorus that an operator can change while it runs, over the
// maint api. A change lasts until the next restart, which starts from the
// config again, and that is what makes one safe to change on a running
// migration.
//
// What a switch is made of is pkg/switches. Where one is set from at startup
// is neither here nor there: that is a config key of whatever part of chorus
// the setting belongs to, and its Start sets it.

package settings

import "github.com/clyso/chorus/pkg/switches"

// All returns every switch, so that the api needs no list of its own.
func All() []switches.Switch {
	return []switches.Switch{ListingSpeed, HeadBucketCache}
}
