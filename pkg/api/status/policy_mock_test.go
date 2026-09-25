/*
 * Copyright © 2026 Strato GmbH
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

package status

import (
	"context"
	"time"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
)

var _ policy.Service = (*policyServiceMock)(nil)

type policyServiceMock struct {
	replications map[entity.ReplicationStatusID]entity.ReplicationStatusExtended
	err          error
}

func (m *policyServiceMock) ListReplicationPolicyInfo(_ context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.replications, nil
}

// --- unused methods: panic if called ---

func (m *policyServiceMock) GetRoutingPolicy(context.Context, entity.BucketRoutingPolicyID) (string, error) {
	panic("not implemented")
}

func (m *policyServiceMock) GetUserRoutingPolicy(context.Context, string) (string, error) {
	panic("not implemented")
}

func (m *policyServiceMock) AddUserRoutingPolicy(context.Context, string, string) error {
	panic("not implemented")
}

func (m *policyServiceMock) AddBucketRoutingPolicy(context.Context, entity.BucketRoutingPolicyID, string, bool) error {
	panic("not implemented")
}

func (m *policyServiceMock) SetDowntimeReplicationSwitch(context.Context, entity.ReplicationStatusID, *entity.ReplicationSwitchDowntimeOpts) error {
	panic("not implemented")
}

func (m *policyServiceMock) UpdateDowntimeSwitchStatus(context.Context, entity.ReplicationStatusID, entity.ReplicationSwitchStatus, string, *time.Time, *time.Time) error {
	panic("not implemented")
}

func (m *policyServiceMock) AddZeroDowntimeReplicationSwitch(context.Context, entity.ReplicationStatusID, *entity.ReplicationSwitchZeroDowntimeOpts) error {
	panic("not implemented")
}

func (m *policyServiceMock) CompleteZeroDowntimeReplicationSwitch(context.Context, entity.ReplicationStatusID) error {
	panic("not implemented")
}

func (m *policyServiceMock) DeleteReplicationSwitch(context.Context, entity.ReplicationStatusID) error {
	panic("not implemented")
}

func (m *policyServiceMock) GetReplicationSwitchInfo(context.Context, entity.ReplicationStatusID) (entity.ReplicationSwitchInfo, error) {
	panic("not implemented")
}

func (m *policyServiceMock) ListReplicationSwitchInfo(context.Context) ([]entity.ReplicationSwitchInfo, error) {
	panic("not implemented")
}

func (m *policyServiceMock) GetInProgressZeroDowntimeSwitchInfo(context.Context, entity.ReplicationSwitchInfoID) (entity.ZeroDowntimeSwitchInProgressInfo, error) {
	panic("not implemented")
}

func (m *policyServiceMock) GetBucketReplicationPolicies(context.Context, entity.BucketReplicationPolicyID) (*entity.StorageReplicationPolicies, error) {
	panic("not implemented")
}

func (m *policyServiceMock) GetUserReplicationPolicies(context.Context, string) (*entity.StorageReplicationPolicies, error) {
	panic("not implemented")
}

func (m *policyServiceMock) AddUserReplicationPolicy(context.Context, string, entity.UserReplicationPolicy) error {
	panic("not implemented")
}

func (m *policyServiceMock) DeleteUserReplication(context.Context, string, entity.UserReplicationPolicy) error {
	panic("not implemented")
}

func (m *policyServiceMock) AddBucketReplicationPolicy(context.Context, entity.ReplicationStatusID, *string) (entity.ReplicationPolicyDestination, error) {
	panic("not implemented")
}

func (m *policyServiceMock) GetReplicationPolicyInfo(context.Context, entity.ReplicationStatusID) (entity.ReplicationStatus, error) {
	panic("not implemented")
}

func (m *policyServiceMock) GetReplicationPolicyInfoExtended(context.Context, entity.ReplicationStatusID) (entity.ReplicationStatusExtended, error) {
	panic("not implemented")
}

func (m *policyServiceMock) IsReplicationPolicyExists(context.Context, entity.ReplicationStatusID) (bool, error) {
	panic("not implemented")
}

func (m *policyServiceMock) ListingDone(context.Context, entity.ReplicationStatusID) error {
	panic("not implemented")
}

func (m *policyServiceMock) PauseReplication(context.Context, entity.ReplicationStatusID) error {
	panic("not implemented")
}

func (m *policyServiceMock) ResumeReplication(context.Context, entity.ReplicationStatusID) error {
	panic("not implemented")
}

func (m *policyServiceMock) DropReplicationRecords(context.Context, entity.ReplicationStatusID) error {
	panic("not implemented")
}

func (m *policyServiceMock) LiveSyncStarted(context.Context, entity.ReplicationStatusID) error {
	panic("not implemented")
}

func (m *policyServiceMock) DeleteBucketReplicationsByUser(context.Context, string, string, string) ([]entity.ReplicationStatusID, error) {
	panic("not implemented")
}

func (m *policyServiceMock) Config() *s3.StorageConfig {
	panic("not implemented")
}
