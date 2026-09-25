// Copyright 2026 STRATO GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package validate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
)

func TestIdentifier(t *testing.T) {
	r := require.New(t)
	r.NoError(Identifier("user", "alice"))
	r.Error(Identifier("user", "ali:ce"), "a colon breaks the replication queue name parsing")
}

func TestReplicationStatusID_RejectsColon(t *testing.T) {
	r := require.New(t)
	valid := entity.NewReplicationStatusID("user", "from", "from-bucket", "to", "to-bucket")
	r.NoError(ReplicationStatusID(valid))

	withColon := valid
	withColon.User = "u:ser"
	r.Error(ReplicationStatusID(withColon))

	withColon = valid
	withColon.FromStorage = "fr:om"
	r.Error(ReplicationStatusID(withColon))

	withColon = valid
	withColon.ToStorage = "t:o"
	r.Error(ReplicationStatusID(withColon))
}
