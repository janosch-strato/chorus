/*
 * Copyright © 2024 Clyso GmbH
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

package api

import (
	"testing"

	"github.com/clyso/chorus/pkg/entity"
)

func Test_toListed(t *testing.T) {
	tests := []struct {
		name string
		in   entity.QueueStats
		want int64
	}{
		{
			name: "empty queue",
			in:   entity.QueueStats{},
			want: 0,
		},
		{
			name: "no retries",
			in:   entity.QueueStats{Pending: 3, Done: 7},
			want: 10,
		},
		{
			name: "with retries: rescheduled subtracted from listed",
			// 10 objects: 7 done, 3 pending; 2 of the 7 failed once before succeeding
			// ProcessedTotal = 7 (success) + 2 (retry failures) = 9 → Done=9
			// FailedTotal = 2 → Rescheduled=2
			in:   entity.QueueStats{Pending: 3, Done: 9, Rescheduled: 2},
			want: 10, // 3 + 9 - 2
		},
		{
			name: "with permanently failed: not counted in listed",
			// 7 done, 2 permanently archived, 1 pending → 10 total, but 2 are unrecoverable
			// toListed should reflect only what can reach done: pending + unique_success = 1 + 7 = 8
			// ProcessedTotal = 7 (success) + 2 (archive) = 9 → Done=9
			// FailedTotal = 2, all archived → Rescheduled=0, Failed=2
			in:   entity.QueueStats{Pending: 1, Done: 9, Rescheduled: 0, Failed: 2},
			want: 8, // pending(1) + toDone(7) = 1 + (9-0-2)
		},
		{
			name: "mixed: retries and archived",
			// 7 unique successes (6 clean + 1 after retry), 1 archived after retry, 2 pending
			// ProcessedTotal = 6 + 1(success) + 1(retry failure) + 1(archive) = 9
			// FailedTotal = 1 (retry failure) + 1 (archive) = 2; Archived=1
			// Rescheduled = FailedTotal - Archived = 1; Failed = 1
			in:   entity.QueueStats{Pending: 2, Done: 9, Rescheduled: 1, Failed: 1},
			want: 9, // pending(2) + toDone(7) = 2 + (9-1-1)
		},
		{
			name: "all done, none failed: toListed equals toDone",
			// 10 objects, all succeed after some retries; nothing pending
			// ProcessedTotal = 10 + 3 (retry failures) = 13; FailedTotal = 3; Archived=0
			// Rescheduled = 3; Failed = 0
			in:   entity.QueueStats{Pending: 0, Done: 13, Rescheduled: 3, Failed: 0},
			want: 10, // toDone = 13-3-0 = 10
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toListed(tt.in); got != tt.want {
				t.Errorf("toListed() = %d, want %d", got, tt.want)
			}
		})
	}
}

func Test_toDone(t *testing.T) {
	tests := []struct {
		name string
		in   entity.QueueStats
		want int64
	}{
		{
			name: "empty queue",
			in:   entity.QueueStats{},
			want: 0,
		},
		{
			name: "no retries",
			in:   entity.QueueStats{Pending: 3, Done: 7},
			want: 7,
		},
		{
			name: "with retries: retry failures subtracted",
			// 7 unique objects completed; 2 failed once before succeeding
			// ProcessedTotal = 7 + 2 = 9; FailedTotal = 2; Rescheduled=2, Failed=0
			in:   entity.QueueStats{Pending: 3, Done: 9, Rescheduled: 2, Failed: 0},
			want: 7, // 9 - 2 - 0
		},
		{
			name: "with permanently failed: not counted as done",
			// 7 done cleanly, 2 permanently archived (not retryable)
			// ProcessedTotal = 7 + 2 = 9; FailedTotal = 2, all archived → Rescheduled=0, Failed=2
			in:   entity.QueueStats{Pending: 1, Done: 9, Rescheduled: 0, Failed: 2},
			want: 7, // 9 - 0 - 2
		},
		{
			name: "mixed: retries and archived",
			// 6 done cleanly, 1 done after 1 retry, 1 archived after 1 retry, 2 pending
			// ProcessedTotal = 6 + 1(success) + 1(retry failure) + 1(archive) = 9
			// FailedTotal = 2; Archived = 1 → Rescheduled=1, Failed=1
			in:   entity.QueueStats{Pending: 2, Done: 9, Rescheduled: 1, Failed: 1},
			want: 7, // 9 - 1 - 1
		},
		{
			name: "all pending, nothing done yet",
			in:   entity.QueueStats{Pending: 10},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toDone(tt.in); got != tt.want {
				t.Errorf("toDone() = %d, want %d", got, tt.want)
			}
		})
	}
}
