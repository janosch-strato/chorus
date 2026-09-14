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

package meta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
)

const deleteIntentKey = "delete-intent"

// DeleteIntent is a delete a proxy has begun and not yet recorded, and says
// enough for another run of it to finish the job.
type DeleteIntent struct {
	// Request keeps two clients deleting the same object from taking each
	// other's intent away by finishing first.
	Request  string                `json:"request"`
	Owner    string                `json:"owner"`
	User     string                `json:"user"`
	Storage  string                `json:"storage"`
	Object   dom.Object            `json:"object"`
	Versions map[Destination]int64 `json:"versions"`
}

// DeleteIntents is the log of deletes that are under way.
//
// A delete carried out and then not written down is lost for good: the
// destination keeps the object and nothing is looking for it. So it is written
// down first and taken off once the replication is queued, and whatever is
// left is what was interrupted. A proxy starting up finishes those by deleting
// again rather than working out whether the storage got there, which cannot be
// found out anyway.
type DeleteIntents struct {
	client redis.UniversalClient
}

func NewDeleteIntents(client redis.UniversalClient) *DeleteIntents {
	return &DeleteIntents{client: client}
}

func (i DeleteIntent) field() string {
	return i.Request + ":" + i.Object.Key()
}

// Written down before anything is asked of the storage.
//
// All or nothing: an intent that cannot be encoded stops the batch before it
// is sent, and MULTI/EXEC has Redis store either every intent or none.
func (d *DeleteIntents) Record(ctx context.Context, intents []DeleteIntent) error {
	if len(intents) == 0 {
		return nil
	}
	_, err := d.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for _, intent := range intents {
			value, marshalErr := json.Marshal(intent)
			if marshalErr != nil {
				return fmt.Errorf("delete intent: unable to encode %+v: %w", intent, marshalErr)
			}
			p.HSet(ctx, deleteIntentKey, intent.field(), string(value))
		}
		return nil
	})
	return err
}

func (d *DeleteIntents) Done(ctx context.Context, intents []DeleteIntent) error {
	if len(intents) == 0 {
		return nil
	}
	fields := make([]string, 0, len(intents))
	for _, intent := range intents {
		fields = append(fields, intent.field())
	}
	return d.client.HDel(ctx, deleteIntentKey, fields...).Err()
}

// Anyone else's may be a delete running right now, not this proxy's to finish.
func (d *DeleteIntents) OwnedBy(ctx context.Context, owner string) ([]DeleteIntent, error) {
	values, err := d.client.HVals(ctx, deleteIntentKey).Result()
	if err != nil {
		return nil, err
	}
	mine := make([]DeleteIntent, 0, len(values))
	for _, value := range values {
		var intent DeleteIntent
		if unmarshalErr := json.Unmarshal([]byte(value), &intent); unmarshalErr != nil {
			return nil, fmt.Errorf("delete intent: unable to decode %q: %w", value, unmarshalErr)
		}
		if intent.Owner == owner {
			mine = append(mine, intent)
		}
	}
	return mine, nil
}
