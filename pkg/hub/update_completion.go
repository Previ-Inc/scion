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
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GoogleCloudPlatform/scion/pkg/plugin"
)

const (
	defaultUpdateTimeout = 10 * time.Minute
	defaultPollInterval  = 20 * time.Second
)

// pendingUpdateEntry tracks a single pending update with its poll goroutine
// and timeout timer.
type pendingUpdateEntry struct {
	updateID         string
	preUpdateVersion string
	cancel           context.CancelFunc
	timer            *time.Timer
}

type integrationUpdateRecord struct {
	ID          uuid.UUID
	Integration string
	State       string
	Detail      string
	NewVersion  string
	RequestedBy string
	CreateTime  time.Time
	UpdateTime  time.Time
}

type pendingUpdateTracker struct {
	mu      sync.Mutex
	pending map[string]*pendingUpdateEntry
	updates map[uuid.UUID]*integrationUpdateRecord
}

func newPendingUpdateTracker() *pendingUpdateTracker {
	return &pendingUpdateTracker{
		pending: make(map[string]*pendingUpdateEntry),
		updates: make(map[uuid.UUID]*integrationUpdateRecord),
	}
}

// startUpdateTracking starts poll-based completion detection for an HA update.
// A goroutine polls BrokerInfo every ~20s. A timeout timer marks the update
// failed after defaultUpdateTimeout if the version hasn't changed.
func (s *Server) startUpdateTracking(integrationName, updateID, preUpdateVersion string) {
	if s.updateTracker == nil {
		return
	}

	s.updateTracker.mu.Lock()
	defer s.updateTracker.mu.Unlock()

	// Cancel any existing tracking for this integration.
	if existing, ok := s.updateTracker.pending[integrationName]; ok {
		existing.cancel()
		existing.timer.Stop()
	}

	ctx, cancel := context.WithCancel(context.Background())

	timer := time.AfterFunc(defaultUpdateTimeout, func() {
		s.handleUpdateTimeout(integrationName, updateID)
	})

	s.updateTracker.pending[integrationName] = &pendingUpdateEntry{
		updateID:         updateID,
		preUpdateVersion: preUpdateVersion,
		cancel:           cancel,
		timer:            timer,
	}

	go s.pollUpdateCompletion(ctx, integrationName, updateID, preUpdateVersion)
}

// pollUpdateCompletion periodically checks BrokerInfo for a version change.
func (s *Server) pollUpdateCompletion(ctx context.Context, integrationName, updateID, preUpdateVersion string) {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkUpdateCompletion(integrationName, updateID, preUpdateVersion)
		}
	}
}

// triggerImmediatePoll is called on reconnect as a fast-path hint.
// It triggers an immediate completion check outside the regular poll interval.
func (s *Server) triggerImmediatePoll(integrationName string) {
	if s.updateTracker == nil {
		return
	}

	s.updateTracker.mu.Lock()
	entry, ok := s.updateTracker.pending[integrationName]
	if !ok {
		s.updateTracker.mu.Unlock()
		return
	}
	updateID := entry.updateID
	preUpdateVersion := entry.preUpdateVersion
	s.updateTracker.mu.Unlock()

	go s.checkUpdateCompletion(integrationName, updateID, preUpdateVersion)
}

// checkUpdateCompletion checks if the integration version has changed,
// indicating the update completed successfully.
func (s *Server) checkUpdateCompletion(integrationName, updateID, preUpdateVersion string) {
	if s.updateTracker == nil {
		return
	}

	s.mu.RLock()
	mgr := s.pluginManager
	s.mu.RUnlock()

	if mgr == nil {
		return
	}

	newVersion, _, _, err := mgr.BrokerInfo(integrationName)
	if err != nil {
		return
	}

	uid, err := uuid.Parse(updateID)
	if err != nil {
		return
	}
	if preUpdateVersion == "" || newVersion == preUpdateVersion {
		return
	}

	s.updateTracker.mu.Lock()
	rec, ok := s.updateTracker.updates[uid]
	if !ok || rec.State == "completed" || rec.State == "failed" {
		s.updateTracker.mu.Unlock()
		return
	}
	rec.State = "completed"
	rec.NewVersion = newVersion
	rec.Detail = ""
	rec.UpdateTime = time.Now().UTC()
	s.updateTracker.mu.Unlock()
	slog.Info("Update completed — version changed",
		"integration", integrationName, "old_version", preUpdateVersion,
		"new_version", newVersion)

	// Clean up tracker entry only if updateID matches.
	s.updateTracker.mu.Lock()
	if e, ok := s.updateTracker.pending[integrationName]; ok && e.updateID == updateID {
		e.cancel()
		e.timer.Stop()
		delete(s.updateTracker.pending, integrationName)
	}
	s.updateTracker.mu.Unlock()
}

// handleUpdateTimeout marks an update as failed due to timeout.
func (s *Server) handleUpdateTimeout(integrationName, updateID string) {
	if s.updateTracker == nil {
		return
	}

	s.updateTracker.mu.Lock()
	if e, ok := s.updateTracker.pending[integrationName]; ok && e.updateID == updateID {
		e.cancel()
		delete(s.updateTracker.pending, integrationName)
	}
	uid, err := uuid.Parse(updateID)
	if err == nil {
		if rec, ok := s.updateTracker.updates[uid]; ok && rec.State != "completed" && rec.State != "failed" {
			rec.State = "failed"
			rec.Detail = "Update timed out — version unchanged after restart"
			rec.UpdateTime = time.Now().UTC()
		}
	}
	s.updateTracker.mu.Unlock()
}

// registerReconnectCallbacks sets up reconnect callbacks on all HA integration
// adapters. Callbacks serve as fast-path hints to trigger an immediate
// completion check — they are not the sole detection mechanism.
func (s *Server) registerReconnectCallbacks(mgr IntegrationManager) {
	for _, key := range mgr.ListPlugins() {
		name := pluginNameFromKey(key)
		if name == "" {
			continue
		}
		if mgr.GetDeploymentMode("broker", name) != plugin.DeploymentModeHA {
			continue
		}
		adapter := mgr.GetGRPCBrokerAdapter(name)
		if adapter == nil {
			continue
		}
		integrationName := name
		adapter.OnReconnect(func() {
			s.triggerImmediatePoll(integrationName)
		})
	}
}

// sweepOrphanedUpdates recovers non-terminal integration_updates rows that
// were pending when the hub last shut down. Recent rows get their tracking
// re-armed; old rows are marked failed.
func (s *Server) sweepOrphanedUpdates() {
	if s.updateTracker == nil {
		return
	}
	now := time.Now()
	s.updateTracker.mu.Lock()
	var toRearm []*integrationUpdateRecord
	for _, row := range s.updateTracker.updates {
		if row.State == "completed" || row.State == "failed" {
			continue
		}
		age := now.Sub(row.CreateTime)
		if age >= defaultUpdateTimeout {
			row.State = "failed"
			row.Detail = "Update orphaned — hub restarted after timeout expired"
			row.UpdateTime = now.UTC()
			continue
		}
		cp := *row
		toRearm = append(toRearm, &cp)
	}
	s.updateTracker.mu.Unlock()

	for _, row := range toRearm {
		preUpdateVersion := ""
		if strings.HasPrefix(row.Detail, "pre_update_version=") {
			preUpdateVersion = strings.TrimPrefix(row.Detail, "pre_update_version=")
		}
		remaining := defaultUpdateTimeout - now.Sub(row.CreateTime)
		s.rearmUpdateTracking(row.Integration, row.ID.String(), preUpdateVersion, remaining)
	}
}

// rearmUpdateTracking re-arms tracking for an update with a custom timeout.
func (s *Server) rearmUpdateTracking(integrationName, updateID, preUpdateVersion string, timeout time.Duration) {
	if s.updateTracker == nil {
		return
	}

	s.updateTracker.mu.Lock()
	defer s.updateTracker.mu.Unlock()

	if existing, ok := s.updateTracker.pending[integrationName]; ok {
		existing.cancel()
		existing.timer.Stop()
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	timer := time.AfterFunc(timeout, func() {
		s.handleUpdateTimeout(integrationName, updateID)
	})

	s.updateTracker.pending[integrationName] = &pendingUpdateEntry{
		updateID:         updateID,
		preUpdateVersion: preUpdateVersion,
		cancel:           cancelFn,
		timer:            timer,
	}

	go s.pollUpdateCompletion(ctx, integrationName, updateID, preUpdateVersion)
}
