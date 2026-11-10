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

package cmd

import (
	"context"
	"database/sql"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/restatehub"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
)

var utcTimestampRepairBudget = 30 * time.Minute

const utcTimestampSnapshotLabel = "pre-utc-timestamp-normalize"
const utcTimestampStaleTmpAge = 35 * time.Minute

type timestampRepairDB interface {
	DB() *sql.DB
	Dialect() string
}

type utcTimestampRepairResult struct {
	Completed    bool
	Unparseable  int
	SnapshotPath string
	ProbeFailed  bool
}

func repairUnreadableTimestamps(_ context.Context, _ any) utcTimestampRepairResult {
	return utcTimestampRepairResult{}
}

func markUTCTimestampRepairComplete(ctx context.Context, s store.Store, res utcTimestampRepairResult) {
	if !res.Completed {
		return
	}
	now := time.Now().UTC()
	op, err := s.GetMaintenanceOperation(ctx, restatehub.UTCTimestampNormalizeKey)
	if err != nil {
		return
	}
	op.Status = store.MaintenanceStatusCompleted
	op.CompletedAt = &now
	_ = s.UpdateMaintenanceOperation(ctx, op)
}
