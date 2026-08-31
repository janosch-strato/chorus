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

package tasks

import (
	"fmt"
	"sync/atomic"

	"github.com/clyso/chorus/pkg/dom"
)

// How fast a bucket listing produces object copy tasks. Listing is orders of
// magnitude faster than copying, so at ListingFull millions of tasks pile up
// in redis long before the copies need them. At ListingAuto the listing stops
// while enough work is queued and continues when the queue has drained.
//
// Stopping a listing completely is not one of these: pause its queue instead,
// which leaves the tasks where they are and resumes them on unpause.
const (
	ListingFull = "full"
	ListingAuto = "auto"
)

// listingAuto is process wide: one chorus instance runs one migration. It is
// set from the config at startup and can be changed at runtime, which lasts
// until the next restart.
var listingAuto atomic.Bool

// SetListingSpeed sets the listing speed of this process by its configured name.
func SetListingSpeed(speed string) error {
	switch speed {
	case "", ListingFull:
		listingAuto.Store(false)
	case ListingAuto:
		listingAuto.Store(true)
	default:
		return fmt.Errorf("%w: unknown listing speed %q, want %s or %s", dom.ErrInvalidArg, speed, ListingFull, ListingAuto)
	}
	return nil
}

// GetListingSpeed returns the listing speed of this process.
func GetListingSpeed() string {
	if listingAuto.Load() {
		return ListingAuto
	}
	return ListingFull
}
