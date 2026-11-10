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

package hub

import (
	"context"
	"database/sql"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/restatehub"
)

// UTCTimestampNormalizeExecutor runs the utc-timestamp-normalize maintenance
// migration. On a Restate-native hub timestamps are serialized canonically.
type UTCTimestampNormalizeExecutor struct {
	DB      *sql.DB
	Dialect string
}

// Run implements MaintenanceExecutor.
func (e *UTCTimestampNormalizeExecutor) Run(_ context.Context, _ io.Writer, _ map[string]string) error {
	return nil
}

var rerunnableMigrations = map[string]bool{
	restatehub.UTCTimestampNormalizeKey:       true,
	restatehub.AppliedConfigTZCleanupKey:      true,
	restatehub.AutoExposeEnvNormalizeKey:      true,
	restatehub.GroupParticipantModeCleanupKey: true,
	restatehub.ProjectStorageMigrateKey:       true,
}

func (s *Server) storeDB() (*sql.DB, string) {
	if p, ok := s.store.(interface {
		DB() *sql.DB
		Dialect() string
	}); ok {
		return p.DB(), p.Dialect()
	}
	return nil, ""
}

var startupTimestampCheckTimeout = 5 * time.Minute

var (
	timestampAutoRunBudget    = 30 * time.Minute
	timestampStaleTmpAge      = 35 * time.Minute
	timestampAutoRunStopGrace = 5 * time.Second
	timestampNow              = time.Now
)

const timestampAutoRunStartedBy = "system:hub-start"

type timestampAutoRunner struct {
	mu      sync.Mutex
	stopped bool
	cancel  context.CancelFunc
	running sync.WaitGroup
}

func (s *Server) startStoredTimestampCheck(ctx context.Context) {
	rt := &s.timestampAutoRun
	runCtx, cancel := context.WithCancel(ctx)
	rt.mu.Lock()
	if rt.stopped || rt.cancel != nil {
		rt.mu.Unlock()
		cancel()
		return
	}
	rt.cancel = cancel
	rt.running.Add(1)
	rt.mu.Unlock()
	check := storedTimestampCheck
	go func() {
		defer rt.running.Done()
		defer cancel()
		check(s, runCtx)
	}()
}

func (s *Server) stopStoredTimestampCheck() {
	rt := &s.timestampAutoRun
	rt.mu.Lock()
	rt.stopped = true
	cancel := rt.cancel
	rt.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	done := make(chan struct{})
	go func() {
		rt.running.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timestampAutoRunStopGrace):
	}
}

var storedTimestampCheck = (*Server).checkStoredTimestamps

func (s *Server) checkStoredTimestamps(_ context.Context) {}

func rearmDelay(mod, now time.Time, staleAge time.Duration) time.Duration {
	d := mod.Add(staleAge).Sub(now)
	if d < 0 {
		return 0
	}
	if d > staleAge {
		return staleAge
	}
	return d
}

func intersectTables(a, b []string) []string {
	out := []string{}
	for _, x := range a {
		if slices.Contains(b, x) {
			out = append(out, x)
		}
	}
	return out
}
