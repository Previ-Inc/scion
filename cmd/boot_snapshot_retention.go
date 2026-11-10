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
	"regexp"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/store"
)

func fkSnapshotLabelPattern() string {
	return regexp.QuoteMeta(fkReconcileSnapshotLabelPrefix) + "-[0-9a-f]{8}"
}

func utcSnapshotLabelPattern() string {
	return regexp.QuoteMeta(utcTimestampSnapshotLabel)
}

func startupSnapshotPatterns() []string {
	return []string{fkSnapshotLabelPattern(), utcSnapshotLabelPattern()}
}

var (
	timestampNormalizeIncomplete = func(_ context.Context, _ store.Store) (bool, error) {
		return false, nil
	}
	pruneStartupSnapshots = func(_ string, _, _ []string, _ time.Time) []string {
		return nil
	}
)

type startupSnapshots struct {
	fkPaths  []string
	tsRepair utcTimestampRepairResult
}

func (in *startupSnapshots) recordFK(path string) {
	in.fkPaths = append(in.fkPaths, path)
}

func (in *startupSnapshots) pruneInputs(ctx context.Context, s store.Store) (patterns, keep []string) {
	keep = append(keep, in.fkPaths...)
	if in.tsRepair.SnapshotPath != "" {
		keep = append(keep, in.tsRepair.SnapshotPath)
	}
	patterns = []string{fkSnapshotLabelPattern()}
	if in.tsRepair.ProbeFailed {
		return patterns, keep
	}
	incomplete, err := timestampNormalizeIncomplete(ctx, s)
	if err == nil && !incomplete {
		patterns = append(patterns, utcSnapshotLabelPattern())
	}
	return patterns, keep
}

type startupStore interface {
	store.Store
}

func migrateThenPrune(ctx context.Context, s startupStore, migrate func(context.Context) error, in *startupSnapshots) error {
	if err := migrate(ctx); err != nil {
		return err
	}
	markUTCTimestampRepairComplete(ctx, s, in.tsRepair)
	return nil
}
