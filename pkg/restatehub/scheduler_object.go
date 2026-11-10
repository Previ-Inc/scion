// Copyright 2026 Google LLC
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

package restatehub

import (
	"fmt"
	"net/http"
	"time"

	restate "github.com/restatedev/sdk-go"
)

// Granular state keys inside SchedulerObject[schedule_id].
const (
	schedulerKeySpec     = "spec"
	schedulerKeyNextSeq  = "next_seq"
	schedulerKeyOccPrefix = "occ:"
)

// ScheduleSpec defines a durable recurring or one-shot timer schedule.
type ScheduleSpec struct {
	ScheduleID string        `json:"scheduleId"`
	ProjectID  string        `json:"projectId,omitempty"`
	CronExpr   string        `json:"cronExpr,omitempty"`
	Delay      time.Duration `json:"delay,omitempty"`
	Target     string        `json:"target"`
	Payload    []byte        `json:"payload,omitempty"`
	Enabled    bool          `json:"enabled"`
	Generation uint64        `json:"generation"`
	CreatedAt  time.Time     `json:"createdAt"`
}

// SchedulerObject is the Restate Virtual Object managing durable timers and scheduled events
// using restate.Sleep and delayed sends with generation fencing (Invariant 3).
type SchedulerObject struct{}

// NewSchedulerObject constructs a SchedulerObject.
func NewSchedulerObject() *SchedulerObject {
	return &SchedulerObject{}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (s *SchedulerObject) ServiceName() string {
	return ServiceSchedulerObject
}

// SaveSchedule creates or updates a schedule under "spec".
func (s *SchedulerObject) SaveSchedule(ctx restate.ObjectContext, spec ScheduleSpec) (ScheduleSpec, error) {
	if spec.ScheduleID == "" {
		spec.ScheduleID = restate.Key(ctx)
	}
	now := time.Now().UTC()
	if spec.CreatedAt.IsZero() {
		spec.CreatedAt = now
	}
	restate.Set(ctx, schedulerKeySpec, spec)
	return spec, nil
}

// GetSchedule reads schedule spec.
func (s *SchedulerObject) GetSchedule(ctx restate.ObjectSharedContext, _ restate.Void) (ScheduleSpec, error) {
	spec, err := restate.Get[ScheduleSpec](ctx, schedulerKeySpec)
	if err != nil {
		return ScheduleSpec{}, err
	}
	if spec.ScheduleID == "" {
		return ScheduleSpec{}, restate.ToTerminalError(
			fmt.Errorf("schedule %s not found", restate.Key(ctx)),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return spec, nil
}
