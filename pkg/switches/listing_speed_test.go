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

package switches

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
)

func Test_ListingSpeed(t *testing.T) {
	r := require.New(t)
	defer func() { r.NoError(SetListingSpeed(ListingFull)) }()

	r.Equal(ListingFull, ListingSpeed(), "full speed unless configured otherwise")

	r.NoError(SetListingSpeed(ListingAuto))
	r.Equal(ListingAuto, ListingSpeed())

	// an unset config value keeps the default rather than failing a startup
	r.NoError(SetListingSpeed(""))
	r.Equal(ListingFull, ListingSpeed())

	r.NoError(SetListingSpeed(ListingAuto))
	r.ErrorIs(SetListingSpeed("paused"), dom.ErrInvalidArg)
	r.Equal(ListingAuto, ListingSpeed(), "a rejected speed changes nothing")
}
