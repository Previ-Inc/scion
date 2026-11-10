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
	"context"
	"encoding/json"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/api"
	"github.com/GoogleCloudPlatform/scion/pkg/conduit/registry"
	"github.com/GoogleCloudPlatform/scion/pkg/config/opsettings"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
	"github.com/google/uuid"
)

// --- BrokerSecretStore & BrokerSettingStore ---

func (s *RestateStore) CreateBrokerSecret(_ context.Context, sec *store.BrokerSecret) error {
	if sec == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.brokerSecrets[sec.BrokerID]; exists {
		return store.ErrAlreadyExists
	}
	cp := *sec
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}
	s.brokerSecrets[sec.BrokerID] = &cp
	return nil
}

func (s *RestateStore) GetBrokerSecret(_ context.Context, brokerID string) (*store.BrokerSecret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sec, ok := s.brokerSecrets[brokerID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *sec
	return &cp, nil
}

func (s *RestateStore) GetActiveSecrets(_ context.Context, brokerID string) ([]*store.BrokerSecret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sec, ok := s.brokerSecrets[brokerID]
	if !ok {
		return nil, nil
	}
	cp := *sec
	return []*store.BrokerSecret{&cp}, nil
}

func (s *RestateStore) UpdateBrokerSecret(_ context.Context, sec *store.BrokerSecret) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.brokerSecrets[sec.BrokerID]; !ok {
		return store.ErrNotFound
	}
	cp := *sec
	s.brokerSecrets[sec.BrokerID] = &cp
	return nil
}

func (s *RestateStore) DeleteBrokerSecret(_ context.Context, brokerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.brokerSecrets[brokerID]; !ok {
		return store.ErrNotFound
	}
	delete(s.brokerSecrets, brokerID)
	return nil
}

func (s *RestateStore) CreateJoinToken(_ context.Context, token *store.BrokerJoinToken) error {
	if token == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.brokerJoinTokens[token.BrokerID]; exists {
		return store.ErrAlreadyExists
	}
	cp := *token
	s.brokerJoinTokens[token.BrokerID] = &cp
	return nil
}

func (s *RestateStore) UpsertJoinToken(_ context.Context, token *store.BrokerJoinToken) (bool, error) {
	if token == nil {
		return false, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, replaced := s.brokerJoinTokens[token.BrokerID]
	cp := *token
	s.brokerJoinTokens[token.BrokerID] = &cp
	return replaced, nil
}

func (s *RestateStore) ConsumeJoinToken(_ context.Context, tokenHash, brokerID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.brokerJoinTokens[brokerID]
	if !ok || t.TokenHash != tokenHash || !t.ExpiresAt.After(now) {
		return store.ErrNotFound
	}
	delete(s.brokerJoinTokens, brokerID)
	return nil
}

func (s *RestateStore) DeleteExpiredJoinToken(_ context.Context, tokenHash string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for bid, t := range s.brokerJoinTokens {
		if t.TokenHash == tokenHash && !t.ExpiresAt.After(now) {
			delete(s.brokerJoinTokens, bid)
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *RestateStore) GetJoinToken(_ context.Context, tokenHash string) (*store.BrokerJoinToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, t := range s.brokerJoinTokens {
		if t.TokenHash == tokenHash {
			cp := *t
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetJoinTokenByBrokerID(_ context.Context, brokerID string) (*store.BrokerJoinToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.brokerJoinTokens[brokerID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (s *RestateStore) DeleteJoinToken(_ context.Context, brokerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.brokerJoinTokens[brokerID]; !ok {
		return store.ErrNotFound
	}
	delete(s.brokerJoinTokens, brokerID)
	return nil
}

func (s *RestateStore) CleanExpiredJoinTokens(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	n := 0
	for bid, t := range s.brokerJoinTokens {
		if !t.ExpiresAt.After(now) {
			delete(s.brokerJoinTokens, bid)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeleteOrphanBrokerCredentials(_ context.Context) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	secRemoved, tokRemoved := 0, 0
	for bid := range s.brokerSecrets {
		if _, ok := s.brokers[bid]; !ok {
			delete(s.brokerSecrets, bid)
			secRemoved++
		}
	}
	for bid := range s.brokerJoinTokens {
		if _, ok := s.brokers[bid]; !ok {
			delete(s.brokerJoinTokens, bid)
			tokRemoved++
		}
	}
	return secRemoved, tokRemoved, nil
}

func (s *RestateStore) GetBrokerSettings(_ context.Context, brokerID string) (*store.BrokerSettingsRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.brokerSettings[brokerID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rec
	return &cp, nil
}

func (s *RestateStore) PutBrokerSettings(_ context.Context, brokerID string, settings store.BrokerSettings, expectedRevision int64, updatedBy string) (*store.BrokerSettingsRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, exists := s.brokerSettings[brokerID]
	if expectedRevision == 0 && exists {
		return nil, store.ErrRevisionConflict
	}
	if expectedRevision > 0 && (!exists || cur.Revision != expectedRevision) {
		return nil, store.ErrRevisionConflict
	}
	rev := int64(1)
	if exists {
		rev = cur.Revision + 1
	}
	rec := &store.BrokerSettingsRecord{
		BrokerID:  brokerID,
		Settings:  settings,
		Revision:  rev,
		Updated:   time.Now().UTC(),
		UpdatedBy: updatedBy,
	}
	s.brokerSettings[brokerID] = rec
	cp := *rec
	return &cp, nil
}

func (s *RestateStore) DeleteBrokerSettings(_ context.Context, brokerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.brokerSettings, brokerID)
	return nil
}

// --- NotificationStore ---

func (s *RestateStore) CreateNotificationSubscription(_ context.Context, sub *store.NotificationSubscription) error {
	if sub == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub.ID == "" {
		sub.ID = uuid.NewString()
	}
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now().UTC()
	}
	cp := *sub
	s.notifSubscriptions[sub.ID] = &cp
	return nil
}

func (s *RestateStore) GetNotificationSubscription(_ context.Context, id string) (*store.NotificationSubscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.notifSubscriptions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *sub
	return &cp, nil
}

func (s *RestateStore) GetNotificationSubscriptions(_ context.Context, agentID string) ([]store.NotificationSubscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.NotificationSubscription
	for _, sub := range s.notifSubscriptions {
		if sub.AgentID == agentID {
			out = append(out, *sub)
		}
	}
	return out, nil
}

func (s *RestateStore) GetNotificationSubscriptionsByProject(_ context.Context, projectID string) ([]store.NotificationSubscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.NotificationSubscription
	for _, sub := range s.notifSubscriptions {
		if sub.ProjectID == projectID {
			out = append(out, *sub)
		}
	}
	return out, nil
}

func (s *RestateStore) GetNotificationSubscriptionsByProjectScope(_ context.Context, projectID string) ([]store.NotificationSubscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.NotificationSubscription
	for _, sub := range s.notifSubscriptions {
		if sub.ProjectID == projectID && sub.Scope == "project" {
			out = append(out, *sub)
		}
	}
	return out, nil
}

func (s *RestateStore) GetSubscriptionsForSubscriber(_ context.Context, subscriberType, subscriberID string) ([]store.NotificationSubscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.NotificationSubscription
	for _, sub := range s.notifSubscriptions {
		if sub.SubscriberType == subscriberType && sub.SubscriberID == subscriberID {
			out = append(out, *sub)
		}
	}
	return out, nil
}

func (s *RestateStore) UpdateNotificationSubscriptionTriggers(_ context.Context, id string, triggerActivities []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.notifSubscriptions[id]
	if !ok {
		return store.ErrNotFound
	}
	sub.TriggerActivities = slices.Clone(triggerActivities)
	return nil
}

func (s *RestateStore) DeleteNotificationSubscription(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.notifSubscriptions[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.notifSubscriptions, id)
	return nil
}

func (s *RestateStore) DeleteNotificationSubscriptionsForAgent(_ context.Context, agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, sub := range s.notifSubscriptions {
		if sub.AgentID == agentID {
			delete(s.notifSubscriptions, id)
		}
	}
	return nil
}

func (s *RestateStore) CreateNotification(_ context.Context, notif *store.Notification) error {
	if notif == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if notif.ID == "" {
		notif.ID = uuid.NewString()
	}
	if notif.CreatedAt.IsZero() {
		notif.CreatedAt = time.Now().UTC()
	}
	cp := *notif
	s.notifications[notif.ID] = &cp
	return nil
}

func (s *RestateStore) GetNotifications(_ context.Context, subscriberType, subscriberID string, onlyUnacknowledged bool) ([]store.Notification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.Notification
	for _, n := range s.notifications {
		if n.SubscriberType == subscriberType && n.SubscriberID == subscriberID && (!onlyUnacknowledged || !n.Acknowledged) {
			out = append(out, *n)
		}
	}
	return out, nil
}

func (s *RestateStore) GetNotificationsByAgent(_ context.Context, agentID, subscriberType, subscriberID string, onlyUnacknowledged bool) ([]store.Notification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.Notification
	for _, n := range s.notifications {
		if n.AgentID == agentID && n.SubscriberType == subscriberType && n.SubscriberID == subscriberID && (!onlyUnacknowledged || !n.Acknowledged) {
			out = append(out, *n)
		}
	}
	return out, nil
}

func (s *RestateStore) GetNotification(_ context.Context, id string) (*store.Notification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.notifications[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *n
	return &cp, nil
}

func (s *RestateStore) AcknowledgeNotification(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifications[id]
	if !ok {
		return store.ErrNotFound
	}
	n.Acknowledged = true
	return nil
}

func (s *RestateStore) AcknowledgeAllNotifications(_ context.Context, subscriberType, subscriberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.notifications {
		if n.SubscriberType == subscriberType && n.SubscriberID == subscriberID {
			n.Acknowledged = true
		}
	}
	return nil
}

func (s *RestateStore) MarkNotificationDispatched(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifications[id]
	if !ok {
		return store.ErrNotFound
	}
	n.Dispatched = true
	return nil
}

func (s *RestateStore) GetLastNotificationStatus(_ context.Context, subscriptionID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var latest *store.Notification
	for _, n := range s.notifications {
		if n.SubscriptionID == subscriptionID && (latest == nil || n.CreatedAt.After(latest.CreatedAt)) {
			latest = n
		}
	}
	if latest == nil {
		return "", nil
	}
	return latest.Status, nil
}

func (s *RestateStore) ClaimNotificationForDispatch(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifications[id]
	if !ok || n.Dispatched {
		return false, nil
	}
	n.Dispatched = true
	return true, nil
}

func (s *RestateStore) UnmarkNotificationDispatched(_ context.Context, id string, notBefore time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifications[id]
	if !ok || n.DispatchOutcome != "" {
		return false, nil
	}
	n.Dispatched = false
	if !notBefore.IsZero() {
		n.NextAttemptAt = &notBefore
	}
	return true, nil
}

func (s *RestateStore) RecordNotificationDispatchAttempt(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifications[id]
	if !ok {
		return store.ErrNotFound
	}
	n.DispatchAttempts++
	return nil
}

func (s *RestateStore) SetNotificationDispatchOutcome(_ context.Context, id, outcome, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifications[id]
	if !ok {
		return false, store.ErrNotFound
	}
	n.Dispatched = true
	n.DispatchOutcome = outcome
	n.DispatchReason = reason
	return true, nil
}

func (s *RestateStore) ExpireStaleAgentNotifications(_ context.Context, _ time.Time) (int, error) {
	return 0, nil
}

func (s *RestateStore) GetUndispatchedAgentNotifications(_ context.Context, _ string) ([]store.Notification, error) {
	return nil, nil
}

func (s *RestateStore) PurgeOrphanedNotifications(_ context.Context) (int, error) {
	return 0, nil
}

func (s *RestateStore) CreateSubscriptionTemplate(_ context.Context, tmpl *store.SubscriptionTemplate) error {
	if tmpl == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if tmpl.ID == "" {
		tmpl.ID = uuid.NewString()
	}
	cp := *tmpl
	s.subscriptionTemplates[tmpl.ID] = &cp
	return nil
}

func (s *RestateStore) GetSubscriptionTemplate(_ context.Context, id string) (*store.SubscriptionTemplate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.subscriptionTemplates[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (s *RestateStore) ListSubscriptionTemplates(_ context.Context, projectID string) ([]store.SubscriptionTemplate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.SubscriptionTemplate
	for _, t := range s.subscriptionTemplates {
		if (projectID == "" && t.ProjectID == "") || (projectID != "" && (t.ProjectID == "" || t.ProjectID == projectID)) {
			out = append(out, *t)
		}
	}
	return out, nil
}

func (s *RestateStore) DeleteSubscriptionTemplate(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptionTemplates[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.subscriptionTemplates, id)
	return nil
}

// --- ScheduledEventStore & ScheduleStore ---

func (s *RestateStore) CreateScheduledEvent(_ context.Context, ev *store.ScheduledEvent) error {
	if ev == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ev.ID == "" {
		ev.ID = uuid.NewString()
	}
	if ev.Status == "" {
		ev.Status = "pending"
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}
	cp := *ev
	s.scheduledEvents[ev.ID] = &cp
	return nil
}

func (s *RestateStore) GetScheduledEvent(_ context.Context, id string) (*store.ScheduledEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ev, ok := s.scheduledEvents[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *ev
	return &cp, nil
}

func (s *RestateStore) ListPendingScheduledEvents(_ context.Context) ([]store.ScheduledEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.ScheduledEvent
	for _, ev := range s.scheduledEvents {
		if ev.Status == "pending" {
			out = append(out, *ev)
		}
	}
	return out, nil
}

func (s *RestateStore) UpdateScheduledEventStatus(_ context.Context, id, status string, firedAt *time.Time, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.scheduledEvents[id]
	if !ok {
		return store.ErrNotFound
	}
	ev.Status = status
	ev.FiredAt = firedAt
	ev.Error = errMsg
	return nil
}

func (s *RestateStore) CancelScheduledEvent(ctx context.Context, id string) error {
	return s.CancelScheduledEventWithReason(ctx, id, "")
}

func (s *RestateStore) CancelScheduledEventWithReason(_ context.Context, id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.scheduledEvents[id]
	if !ok || ev.Status != "pending" {
		return store.ErrNotFound
	}
	ev.Status = "cancelled"
	if reason != "" {
		ev.Error = reason
	}
	return nil
}

func (s *RestateStore) ListScheduledEvents(_ context.Context, filter store.ScheduledEventFilter, _ store.ListOptions) (*store.ListResult[store.ScheduledEvent], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.ScheduledEvent
	for _, ev := range s.scheduledEvents {
		if filter.ProjectID != "" && ev.ProjectID != filter.ProjectID {
			continue
		}
		if filter.Status != "" && ev.Status != filter.Status {
			continue
		}
		items = append(items, *ev)
	}
	return &store.ListResult[store.ScheduledEvent]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) PurgeOldScheduledEvents(_ context.Context, _ time.Time) (int, error) {
	return 0, nil
}

func (s *RestateStore) CreateSchedule(_ context.Context, sc *store.Schedule) error {
	if sc == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if sc.ID == "" {
		sc.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	sc.CreatedAt = now
	sc.UpdatedAt = now
	cp := *sc
	s.schedules[sc.ID] = &cp
	return nil
}

func (s *RestateStore) GetSchedule(_ context.Context, id string) (*store.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sc, ok := s.schedules[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *sc
	return &cp, nil
}

func (s *RestateStore) ListSchedules(_ context.Context, filter store.ScheduleFilter, _ store.ListOptions) (*store.ListResult[store.Schedule], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.Schedule
	for _, sc := range s.schedules {
		if filter.ProjectID != "" && sc.ProjectID != filter.ProjectID {
			continue
		}
		if filter.Status != "" && sc.Status != filter.Status {
			continue
		}
		items = append(items, *sc)
	}
	return &store.ListResult[store.Schedule]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) ListActiveZonePrefixedSchedules(_ context.Context, _ int, _ []string) ([]store.Schedule, error) {
	return nil, nil
}

func (s *RestateStore) UpdateSchedule(_ context.Context, sc *store.Schedule, _ store.ScheduleFieldMask, _ int, _ bool, attribution *store.InitiatorAttribution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.schedules[sc.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *sc
	if attribution != nil {
		cp.InitiatorAttribution = *attribution
	}
	cp.UpdatedAt = time.Now().UTC()
	s.schedules[sc.ID] = &cp
	return nil
}

func (s *RestateStore) UpdateScheduleStatus(_ context.Context, id, status, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.schedules[id]
	if !ok {
		return store.ErrNotFound
	}
	sc.Status = status
	sc.StatusReason = reason
	sc.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *RestateStore) UpdateScheduleAfterRun(_ context.Context, id string, ranAt, nextRunAt time.Time, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.schedules[id]
	if !ok {
		return store.ErrNotFound
	}
	sc.LastRunAt = &ranAt
	sc.NextRunAt = &nextRunAt
	sc.LastRunError = errMsg
	if errMsg != "" {
		sc.LastRunStatus = "error"
		sc.ErrorCount++
	} else {
		sc.LastRunStatus = "success"
	}
	sc.RunCount++
	return nil
}

func (s *RestateStore) DeleteSchedule(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.schedules[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.schedules, id)
	return nil
}

func (s *RestateStore) DeleteSchedulesByProject(_ context.Context, projectID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, sc := range s.schedules {
		if sc.ProjectID == projectID {
			delete(s.schedules, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ListDueSchedules(_ context.Context, now time.Time) ([]store.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.Schedule
	for _, sc := range s.schedules {
		if sc.Status == "active" && sc.NextRunAt != nil && !sc.NextRunAt.After(now) {
			out = append(out, *sc)
		}
	}
	return out, nil
}

// --- GCPServiceAccountStore & GitHubInstallationStore ---

func (s *RestateStore) CreateGCPServiceAccount(_ context.Context, sa *store.GCPServiceAccount) error {
	if sa == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if sa.ID == "" {
		sa.ID = uuid.NewString()
	}
	if sa.CreatedAt.IsZero() {
		sa.CreatedAt = time.Now().UTC()
	}
	cp := *sa
	s.gcpServiceAccounts[sa.ID] = &cp
	return nil
}

func (s *RestateStore) GetGCPServiceAccount(_ context.Context, id string) (*store.GCPServiceAccount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sa, ok := s.gcpServiceAccounts[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *sa
	return &cp, nil
}

func (s *RestateStore) UpdateGCPServiceAccount(_ context.Context, sa *store.GCPServiceAccount) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.gcpServiceAccounts[sa.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *sa
	s.gcpServiceAccounts[sa.ID] = &cp
	return nil
}

func (s *RestateStore) DeleteGCPServiceAccount(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.gcpServiceAccounts[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.gcpServiceAccounts, id)
	return nil
}

func (s *RestateStore) ListGCPServiceAccounts(_ context.Context, filter store.GCPServiceAccountFilter) ([]store.GCPServiceAccount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.GCPServiceAccount
	for _, sa := range s.gcpServiceAccounts {
		if filter.Scope != "" {
			if filter.IncludeHubScoped && sa.Scope == "hub" {
				// matched hub scope
			} else if sa.Scope != filter.Scope || (filter.ScopeID != "" && sa.ScopeID != filter.ScopeID) {
				continue
			}
		}
		if filter.Email != "" && sa.Email != filter.Email {
			continue
		}
		if filter.EmailFold != "" && !strings.EqualFold(sa.Email, filter.EmailFold) {
			continue
		}
		out = append(out, *sa)
	}
	return out, nil
}

func (s *RestateStore) CountGCPServiceAccounts(ctx context.Context, filter store.GCPServiceAccountFilter) (int, error) {
	list, err := s.ListGCPServiceAccounts(ctx, filter)
	return len(list), err
}

func (s *RestateStore) CreateGitHubInstallation(_ context.Context, inst *store.GitHubInstallation) error {
	if inst == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *inst
	s.githubInstallations[inst.InstallationID] = &cp
	return nil
}

func (s *RestateStore) GetGitHubInstallation(_ context.Context, installationID int64) (*store.GitHubInstallation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inst, ok := s.githubInstallations[installationID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *inst
	return &cp, nil
}

func (s *RestateStore) UpdateGitHubInstallation(_ context.Context, inst *store.GitHubInstallation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.githubInstallations[inst.InstallationID]; !ok {
		return store.ErrNotFound
	}
	cp := *inst
	s.githubInstallations[inst.InstallationID] = &cp
	return nil
}

func (s *RestateStore) DeleteGitHubInstallation(_ context.Context, installationID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.githubInstallations[installationID]; !ok {
		return store.ErrNotFound
	}
	delete(s.githubInstallations, installationID)
	return nil
}

func (s *RestateStore) ListGitHubInstallations(_ context.Context, filter store.GitHubInstallationFilter) ([]store.GitHubInstallation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.GitHubInstallation
	for _, inst := range s.githubInstallations {
		if filter.Status != "" && inst.Status != filter.Status {
			continue
		}
		out = append(out, *inst)
	}
	return out, nil
}

func (s *RestateStore) GetInstallationForRepository(_ context.Context, repoFullName string) (*store.GitHubInstallation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, inst := range s.githubInstallations {
		if inst.Status == "active" && slices.Contains(inst.Repositories, repoFullName) {
			cp := *inst
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

// --- MessageStore, ConversationStore, ThreadGroupStore ---

func (s *RestateStore) CreateMessage(ctx context.Context, msg *store.Message) error {
	if msg == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	if msg.ID == "" {
		msg.ID = uuid.NewString()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}
	cp := *msg
	s.messages[msg.ID] = &cp
	s.mu.Unlock()

	if s.ingressActive() && cp.ConversationID != "" {
		_, _ = InvokeObject[ConversationMessage, uint64](ctx, s.host, ServiceConversationObject, cp.ConversationID, "AppendMessage", ConversationMessage{
			MessageID: cp.ID,
			SenderID:  cp.SenderID,
			Content:   cp.Msg,
			Kind:      cp.Type,
			Timestamp: cp.CreatedAt,
		})
	}
	return nil
}

func (s *RestateStore) GetMessage(_ context.Context, id string) (*store.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.messages[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (s *RestateStore) GetMessagesByIDs(_ context.Context, ids []string) (map[string]*store.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*store.Message, len(ids))
	for _, id := range ids {
		if m, ok := s.messages[id]; ok {
			cp := *m
			out[id] = &cp
		}
	}
	return out, nil
}

func (s *RestateStore) ListMessages(_ context.Context, filter store.MessageFilter, opts store.ListOptions) (*store.ListResult[store.Message], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.Message
	for _, m := range s.messages {
		if filter.ProjectID != "" && m.ProjectID != filter.ProjectID {
			continue
		}
		if filter.ConversationID != "" && m.ConversationID != filter.ConversationID {
			continue
		}
		if filter.ThreadID != "" && m.ThreadID != filter.ThreadID {
			continue
		}
		if filter.RecipientID != "" && m.RecipientID != filter.RecipientID {
			continue
		}
		items = append(items, *m)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	total := len(items)
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return &store.ListResult[store.Message]{Items: items, TotalCount: total}, nil
}

func (s *RestateStore) LatestMessagesByThreadIDs(_ context.Context, threadIDs []string, _ store.LatestMessageOptions) (map[string]*store.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*store.Message)
	for _, m := range s.messages {
		if slices.Contains(threadIDs, m.ThreadID) {
			if cur, ok := out[m.ThreadID]; !ok || m.CreatedAt.After(cur.CreatedAt) {
				cp := *m
				out[m.ThreadID] = &cp
			}
		}
	}
	return out, nil
}

func (s *RestateStore) LatestMessagesByConversationIDs(_ context.Context, conversationIDs []string, _ store.LatestMessageOptions) (map[string]*store.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*store.Message)
	for _, m := range s.messages {
		if slices.Contains(conversationIDs, m.ConversationID) {
			if cur, ok := out[m.ConversationID]; !ok || m.CreatedAt.After(cur.CreatedAt) {
				cp := *m
				out[m.ConversationID] = &cp
			}
		}
	}
	return out, nil
}

func (s *RestateStore) MarkMessageRead(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.messages[id]
	if !ok {
		return store.ErrNotFound
	}
	m.Read = true
	return nil
}

func (s *RestateStore) MarkAllMessagesRead(_ context.Context, recipientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.messages {
		if m.RecipientID == recipientID {
			m.Read = true
		}
	}
	return nil
}

func (s *RestateStore) PurgeOldMessages(_ context.Context, _, _ time.Time) (int, error) {
	return 0, nil
}

func (s *RestateStore) PurgeFailedMessages(_ context.Context, _ time.Time) (int, error) {
	return 0, nil
}

func (s *RestateStore) SetMessageConversationID(_ context.Context, messageID, conversationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.messages[messageID]
	if !ok {
		return store.ErrNotFound
	}
	m.ConversationID = conversationID
	return nil
}

func (s *RestateStore) CountUnbackfilledMessages(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (s *RestateStore) CountUnreachableUnbackfilledMessages(_ context.Context) (int, error) {
	return 0, nil
}

func (s *RestateStore) CreateConversation(_ context.Context, conv *store.Conversation) error {
	if conv == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if conv.ID == "" {
		conv.ID = uuid.NewString()
	}
	if _, exists := s.conversations[conv.ID]; exists {
		return store.ErrAlreadyExists
	}
	now := time.Now().UTC()
	conv.CreatedAt = now
	conv.LastActivityAt = now
	cp := *conv
	s.conversations[conv.ID] = &cp
	return nil
}

func (s *RestateStore) GetConversation(_ context.Context, id string) (*store.Conversation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.conversations[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (s *RestateStore) UpdateConversation(_ context.Context, conv *store.Conversation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.conversations[conv.ID]
	if !ok {
		return store.ErrNotFound
	}
	cp := *conv
	cp.CreatedAt = cur.CreatedAt
	cp.LastActivityAt = time.Now().UTC()
	s.conversations[conv.ID] = &cp
	return nil
}

func (s *RestateStore) DeleteConversation(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.conversations[id]
	if !ok {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	c.DeletedAt = &now
	return nil
}

func (s *RestateStore) ListConversations(_ context.Context, filter store.ConversationFilter, _ store.ListOptions) (*store.ListResult[store.Conversation], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.Conversation
	for _, c := range s.conversations {
		if c.DeletedAt != nil {
			continue
		}
		if filter.ProjectID != "" && (c.ProjectID == nil || *c.ProjectID != filter.ProjectID) {
			continue
		}
		items = append(items, *c)
	}
	return &store.ListResult[store.Conversation]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) GetConversationByExternalRef(_ context.Context, surface, externalRef string) (*store.Conversation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.conversations {
		if c.DeletedAt == nil && c.Surface == surface && c.ExternalRef == externalRef {
			cp := *c
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetConversationsByExternalRefs(_ context.Context, surface string, externalRefs []string) (map[string]*store.Conversation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*store.Conversation)
	for _, c := range s.conversations {
		if c.DeletedAt == nil && c.Surface == surface && slices.Contains(externalRefs, c.ExternalRef) {
			cp := *c
			out[c.ExternalRef] = &cp
		}
	}
	return out, nil
}

func (s *RestateStore) UpsertConversationByExternalRef(_ context.Context, conv *store.Conversation) (*store.Conversation, error) {
	if conv == nil {
		return nil, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conversations {
		if c.DeletedAt == nil && c.Surface == conv.Surface && c.ExternalRef == conv.ExternalRef {
			c.LastActivityAt = time.Now().UTC()
			cp := *c
			return &cp, nil
		}
	}
	cp := *conv
	if cp.ID == "" {
		cp.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	cp.CreatedAt = now
	cp.LastActivityAt = now
	s.conversations[cp.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) AddParticipant(_ context.Context, p *store.ConversationParticipant) error {
	if p == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.participants[p.ConversationID]
	for i := range list {
		if list[i].PrincipalKind == p.PrincipalKind && list[i].PrincipalID == p.PrincipalID {
			list[i].LeftAt = nil
			s.participants[p.ConversationID] = list
			return nil
		}
	}
	if p.ID == "" {
		p.ID = uuid.NewString()
	}
	if p.JoinedAt.IsZero() {
		p.JoinedAt = time.Now().UTC()
	}
	s.participants[p.ConversationID] = append(list, *p)
	return nil
}

func (s *RestateStore) EnsureParticipant(ctx context.Context, p *store.ConversationParticipant) error {
	return s.AddParticipant(ctx, p)
}

func (s *RestateStore) RemoveParticipant(_ context.Context, conversationID, principalKind, principalID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.participants[conversationID]
	now := time.Now().UTC()
	for i := range list {
		if list[i].PrincipalKind == principalKind && list[i].PrincipalID == principalID {
			list[i].LeftAt = &now
			s.participants[conversationID] = list
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *RestateStore) ListParticipants(_ context.Context, conversationID string) ([]store.ConversationParticipant, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.ConversationParticipant
	for _, p := range s.participants[conversationID] {
		if p.LeftAt == nil {
			out = append(out, p)
		}
	}
	return out, nil
}

func (s *RestateStore) GetConversationsForPrincipal(_ context.Context, principalKind, principalID string) ([]store.Conversation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.Conversation
	for cid, list := range s.participants {
		for _, p := range list {
			if p.LeftAt == nil && p.PrincipalKind == principalKind && p.PrincipalID == principalID {
				if c, ok := s.conversations[cid]; ok && c.DeletedAt == nil {
					out = append(out, *c)
				}
				break
			}
		}
	}
	return out, nil
}

func (s *RestateStore) AddAddressee(_ context.Context, a *store.MessageAddressee) error {
	if a == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if a.ID == "" {
		a.ID = uuid.NewString()
	}
	s.addressees[a.MessageID] = append(s.addressees[a.MessageID], *a)
	return nil
}

func (s *RestateStore) ListAddressees(_ context.Context, messageID string) ([]store.MessageAddressee, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.addressees[messageID]), nil
}

func (s *RestateStore) UpdateDeliveryState(_ context.Context, id, state string, failureReason *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for mid, list := range s.addressees {
		for i := range list {
			if list[i].ID == id {
				list[i].DeliveryState = state
				list[i].FailureReason = failureReason
				s.addressees[mid] = list
				return nil
			}
		}
	}
	return store.ErrNotFound
}

func (s *RestateStore) CreateThreadGroup(_ context.Context, g *store.ThreadGroup) error {
	if g == nil || g.ProjectID == "" || g.Name == "" {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if g.ID == "" {
		g.ID = uuid.NewString()
	}
	g.NameKey = store.ThreadGroupNameKey(g.Name)
	now := time.Now().UTC()
	g.CreatedAt = now
	g.UpdatedAt = now
	cp := *g
	s.threadGroups[g.ID] = &cp
	return nil
}

func (s *RestateStore) GetThreadGroup(_ context.Context, id string) (*store.ThreadGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.threadGroups[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *g
	return &cp, nil
}

func (s *RestateStore) ListThreadGroups(_ context.Context, projectID string) ([]store.ThreadGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]store.ThreadGroup, 0)
	for _, g := range s.threadGroups {
		if g.ProjectID == projectID {
			out = append(out, *g)
		}
	}
	return out, nil
}

func (s *RestateStore) CountThreadGroups(_ context.Context, projectID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, g := range s.threadGroups {
		if g.ProjectID == projectID {
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) GetThreadGroupIDForConversation(_ context.Context, conversationID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.threadGroupMembers[conversationID], nil
}

func (s *RestateStore) RenameThreadGroup(_ context.Context, id, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.threadGroups[id]
	if !ok {
		return store.ErrNotFound
	}
	g.Name = name
	g.NameKey = store.ThreadGroupNameKey(name)
	g.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *RestateStore) DeleteThreadGroup(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.threadGroups[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.threadGroups, id)
	for cid, gid := range s.threadGroupMembers {
		if gid == id {
			delete(s.threadGroupMembers, cid)
		}
	}
	return nil
}

func (s *RestateStore) SetThreadGroupMember(_ context.Context, groupID, conversationID, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.threadGroups[groupID]; !ok {
		return store.ErrNotFound
	}
	s.threadGroupMembers[conversationID] = groupID
	return nil
}

func (s *RestateStore) AddThreadGroupMemberIfUnfiled(_ context.Context, groupID, conversationID, _ string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.threadGroups[groupID]; !ok {
		return false, store.ErrNotFound
	}
	if _, filed := s.threadGroupMembers[conversationID]; filed {
		return false, nil
	}
	s.threadGroupMembers[conversationID] = groupID
	return true, nil
}

func (s *RestateStore) RemoveThreadGroupMember(_ context.Context, groupID, conversationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.threadGroupMembers[conversationID] == groupID {
		delete(s.threadGroupMembers, conversationID)
	}
	return nil
}

func (s *RestateStore) RemoveThreadGroupMembersForConversation(_ context.Context, conversationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.threadGroupMembers, conversationID)
	return nil
}

func (s *RestateStore) DeleteThreadGroupsForProject(_ context.Context, projectID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, g := range s.threadGroups {
		if g.ProjectID == projectID {
			delete(s.threadGroups, id)
		}
	}
	return nil
}

// --- MaintenanceStore, HubSettingStore, AgentSessionMetricsStore, HubInstanceStore ---

func (s *RestateStore) ListMaintenanceOperations(_ context.Context) ([]store.MaintenanceOperation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.MaintenanceOperation
	for _, op := range s.maintenanceOps {
		out = append(out, *op)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *RestateStore) GetMaintenanceOperation(_ context.Context, key string) (*store.MaintenanceOperation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.maintenanceOps[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *op
	return &cp, nil
}

func (s *RestateStore) UpdateMaintenanceOperation(_ context.Context, op *store.MaintenanceOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.maintenanceOps[op.Key]; !ok {
		return store.ErrNotFound
	}
	cp := *op
	s.maintenanceOps[op.Key] = &cp
	return nil
}

func (s *RestateStore) CreateMaintenanceRun(_ context.Context, run *store.MaintenanceOperationRun) error {
	if run == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if run.ID == "" {
		run.ID = uuid.NewString()
	}
	cp := *run
	s.maintenanceRuns[run.ID] = &cp
	return nil
}

func (s *RestateStore) UpdateMaintenanceRun(_ context.Context, run *store.MaintenanceOperationRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.maintenanceRuns[run.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *run
	s.maintenanceRuns[run.ID] = &cp
	return nil
}

func (s *RestateStore) GetMaintenanceRun(_ context.Context, id string) (*store.MaintenanceOperationRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.maintenanceRuns[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *r
	return &cp, nil
}

func (s *RestateStore) ListMaintenanceRuns(_ context.Context, operationKey string, limit int) ([]store.MaintenanceOperationRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.MaintenanceOperationRun
	for _, r := range s.maintenanceRuns {
		if operationKey == "" || r.OperationKey == operationKey {
			out = append(out, *r)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *RestateStore) AbortRunningMaintenanceOps(_ context.Context) (int64, int64, error) {
	return 0, 0, nil
}

func (s *RestateStore) GetHubSetting(_ context.Context, section string) (*store.HubSetting, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hs, ok := s.hubSettings[section]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *hs
	return &cp, nil
}

func (s *RestateStore) ListHubSettings(_ context.Context) ([]store.HubSetting, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.HubSetting
	for _, hs := range s.hubSettings {
		out = append(out, *hs)
	}
	return out, nil
}

func (s *RestateStore) bumpSettingsRevisionLocked() int64 {
	revRow, ok := s.hubSettings[store.HubSettingsRevisionSection]
	if !ok {
		revRow = &store.HubSetting{
			Section:   store.HubSettingsRevisionSection,
			Value:     json.RawMessage(`{}`),
			Revision:  0,
			UpdatedAt: time.Now().UTC(),
		}
		s.hubSettings[store.HubSettingsRevisionSection] = revRow
	}
	revRow.Revision++
	revRow.UpdatedAt = time.Now().UTC()
	return revRow.Revision
}

func (s *RestateStore) UpsertHubSetting(_ context.Context, section string, value json.RawMessage, updatedBy string, expectedRevision int64, origin string) (*store.HubSetting, error) {
	if section == store.HubSettingsRevisionSection {
		return nil, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, exists := s.hubSettings[section]
	if expectedRevision == 0 && exists {
		return nil, store.ErrRevisionConflict
	}
	if expectedRevision > 0 && (!exists || cur.Revision != expectedRevision) {
		return nil, store.ErrRevisionConflict
	}
	rev := int64(1)
	if exists {
		rev = cur.Revision + 1
	}
	var settingsRev int64
	if opsettings.SectionByName(section) != nil {
		settingsRev = s.bumpSettingsRevisionLocked()
	}
	hs := &store.HubSetting{
		Section:          section,
		Value:            slices.Clone(value),
		Revision:         rev,
		SettingsRevision: settingsRev,
		Origin:           origin,
		UpdatedBy:        updatedBy,
		UpdatedAt:        time.Now().UTC(),
	}
	s.hubSettings[section] = hs
	cp := *hs
	return &cp, nil
}

func (s *RestateStore) DeleteHubSetting(ctx context.Context, section string) error {
	_, err := s.DeleteHubSettingRevision(ctx, section)
	return err
}

func (s *RestateStore) DeleteHubSettingRevision(_ context.Context, section string) (int64, error) {
	if section == store.HubSettingsRevisionSection {
		return 0, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.hubSettings[section]; !ok {
		return 0, store.ErrNotFound
	}
	delete(s.hubSettings, section)
	var settingsRev int64
	if opsettings.SectionByName(section) != nil {
		settingsRev = s.bumpSettingsRevisionLocked()
	}
	return settingsRev, nil
}

func (s *RestateStore) BackfillOrigin(_ context.Context) error {
	return nil
}

func (s *RestateStore) CreateAgentSessionMetrics(_ context.Context, m *store.AgentSessionMetrics) error {
	if m == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.ID == "" {
		m.ID = uuid.NewString()
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	cp := *m
	s.agentSessionMetrics[m.ID] = &cp
	return nil
}

func (s *RestateStore) GetAgentSessionMetrics(_ context.Context, id string) (*store.AgentSessionMetrics, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.agentSessionMetrics[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (s *RestateStore) ListAgentSessionMetricsByAgent(_ context.Context, agentID string) ([]*store.AgentSessionMetrics, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.AgentSessionMetrics
	for _, m := range s.agentSessionMetrics {
		if m.AgentID == agentID {
			cp := *m
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) ListAgentSessionMetricsByProject(_ context.Context, projectID string) ([]*store.AgentSessionMetrics, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.AgentSessionMetrics
	for _, m := range s.agentSessionMetrics {
		if m.ProjectID == projectID {
			cp := *m
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) AggregateByAgent(_ context.Context, _ string) (*store.AgentSessionMetricsAggregates, error) {
	return &store.AgentSessionMetricsAggregates{}, nil
}

func (s *RestateStore) AggregateByProject(_ context.Context, _ string) (*store.AgentSessionMetricsAggregates, error) {
	return &store.AgentSessionMetricsAggregates{}, nil
}

func (s *RestateStore) CountDistinctAgentsByProject(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (s *RestateStore) UpsertHubInstance(_ context.Context, in store.HubInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	cur, exists := s.hubInstances[in.ID]
	if !exists {
		in.StartedAt = now
	} else {
		in.StartedAt = cur.StartedAt
	}
	in.LastSeen = now
	in.StoppedAt = nil
	in.Checks = maps.Clone(in.Checks)
	s.hubInstances[in.ID] = in
	return nil
}

func (s *RestateStore) TouchHubInstance(_ context.Context, id string, _ *api.HubInstanceDBStats) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.hubInstances[id]
	if !ok {
		return false, nil
	}
	cur.LastSeen = time.Now().UTC()
	s.hubInstances[id] = cur
	return true, nil
}

func (s *RestateStore) ListHubInstances(_ context.Context, window time.Duration) ([]store.HubInstance, time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	cutoff := now.Add(-window)
	var rows []store.HubInstance
	for _, inst := range s.hubInstances {
		last := inst.LastSeen
		if inst.StoppedAt != nil {
			last = *inst.StoppedAt
		}
		if !last.Before(cutoff) {
			rows = append(rows, inst)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows, now, nil
}

func (s *RestateStore) MarkHubInstanceStopped(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.hubInstances[id]
	if !ok {
		return nil
	}
	now := time.Now().UTC()
	cur.LastSeen = now
	cur.StoppedAt = &now
	s.hubInstances[id] = cur
	return nil
}

func (s *RestateStore) PruneHubInstances(_ context.Context, retention time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().UTC().Add(-retention)
	n := 0
	for id, inst := range s.hubInstances {
		last := inst.LastSeen
		if inst.StoppedAt != nil {
			last = *inst.StoppedAt
		}
		if last.Before(cutoff) {
			delete(s.hubInstances, id)
			n++
		}
	}
	return n, nil
}

// --- Conduit registry.Store implementation ---

func (s *RestateStore) RegisterRelay(_ context.Context, r registry.RelayInstance) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, exists := s.conduitRelays[r.InstanceID]
	if exists {
		r.Generation = cur.Generation + 1
	} else if r.Generation <= 0 {
		r.Generation = 1
	}
	r.Draining = false
	s.conduitRelays[r.InstanceID] = r
	return r.Generation, nil
}

func (s *RestateStore) HeartbeatRelay(_ context.Context, instanceID string, gen int64, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.conduitRelays[instanceID]
	if !ok || r.Generation != gen {
		return registry.ErrRelaySuperseded
	}
	r.LastSeen = now
	s.conduitRelays[instanceID] = r
	return nil
}

func (s *RestateStore) SetRelayDraining(_ context.Context, instanceID string, gen int64, draining bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.conduitRelays[instanceID]
	if !ok || r.Generation != gen {
		return registry.ErrRelaySuperseded
	}
	r.Draining = draining
	s.conduitRelays[instanceID] = r
	return nil
}

func (s *RestateStore) DeleteSessionsOfOlderGenerations(_ context.Context, instanceID string, currentGen int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for sid, sess := range s.conduitSessions {
		if sess.RelayInstanceID == instanceID && sess.RelayGeneration < currentGen {
			delete(s.conduitSessions, sid)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) InsertSessionWithNextEpoch(_ context.Context, sess registry.SessionRecord) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.conduitRelays[sess.RelayInstanceID]
	if !ok || r.Generation != sess.RelayGeneration {
		return 0, registry.ErrRelaySuperseded
	}
	epKey := sess.PrincipalKind + ":" + sess.PrincipalID
	epoch := s.conduitEpochs[epKey] + 1
	s.conduitEpochs[epKey] = epoch
	sess.ConnectionEpoch = epoch
	s.conduitSessions[sess.SessionID] = sess
	return epoch, nil
}

func (s *RestateStore) TouchSession(_ context.Context, sessionID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.conduitSessions[sessionID]
	if !ok {
		return registry.ErrSessionNotFound
	}
	sess.LastSeen = now
	s.conduitSessions[sessionID] = sess
	return nil
}

func (s *RestateStore) SetSessionDraining(_ context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.conduitSessions[sessionID]
	if !ok {
		return registry.ErrSessionNotFound
	}
	sess.Draining = true
	s.conduitSessions[sessionID] = sess
	return nil
}

func (s *RestateStore) SetRelaySessionsDraining(_ context.Context, instanceID string, gen int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.conduitRelays[instanceID]
	if !ok || r.Generation != gen {
		return 0, registry.ErrRelaySuperseded
	}
	n := 0
	for sid, sess := range s.conduitSessions {
		if sess.RelayInstanceID == instanceID && sess.RelayGeneration == gen {
			sess.Draining = true
			s.conduitSessions[sid] = sess
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeleteSessionCAS(_ context.Context, sessionID, relayInstanceID string, relayGen int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.conduitSessions[sessionID]
	if !ok || sess.RelayInstanceID != relayInstanceID || sess.RelayGeneration != relayGen {
		return false, nil
	}
	delete(s.conduitSessions, sessionID)
	return true, nil
}

func (s *RestateStore) ListPrincipalSessions(_ context.Context, principalKind, principalID string) (registry.PrincipalSessions, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	epKey := principalKind + ":" + principalID
	out := registry.PrincipalSessions{
		PrincipalKind: principalKind,
		PrincipalID:   principalID,
		CurrentEpoch:  s.conduitEpochs[epKey],
	}
	for _, sess := range s.conduitSessions {
		if sess.PrincipalKind == principalKind && sess.PrincipalID == principalID {
			var rPtr *registry.RelayInstance
			if r, ok := s.conduitRelays[sess.RelayInstanceID]; ok {
				cp := r
				rPtr = &cp
			}
			out.Sessions = append(out.Sessions, registry.SessionView{Session: sess, Relay: rPtr})
		}
	}
	return out, nil
}

func (s *RestateStore) ListPrincipalSessionsBySession(ctx context.Context, sessionID string) (registry.PrincipalSessions, bool, error) {
	s.mu.RLock()
	sess, ok := s.conduitSessions[sessionID]
	s.mu.RUnlock()
	if !ok {
		return registry.PrincipalSessions{}, false, nil
	}
	ps, err := s.ListPrincipalSessions(ctx, sess.PrincipalKind, sess.PrincipalID)
	return ps, err == nil, err
}

func (s *RestateStore) DeleteSessionsOfStaleRelays(_ context.Context, staleBefore time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for sid, sess := range s.conduitSessions {
		if r, ok := s.conduitRelays[sess.RelayInstanceID]; !ok || r.LastSeen.Before(staleBefore) {
			delete(s.conduitSessions, sid)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeleteStaleSessions(_ context.Context, staleBefore time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for sid, sess := range s.conduitSessions {
		if sess.LastSeen.Before(staleBefore) {
			delete(s.conduitSessions, sid)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeleteIdleRelays(_ context.Context, staleBefore time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for rid, r := range s.conduitRelays {
		if r.LastSeen.Before(staleBefore) {
			delete(s.conduitRelays, rid)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeletePrincipalEpoch(_ context.Context, principalKind, principalID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conduitEpochs, principalKind+":"+principalID)
	return nil
}

func (s *RestateStore) ListRelays(_ context.Context) ([]registry.RelayInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []registry.RelayInstance
	for _, r := range s.conduitRelays {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out, nil
}

func (s *RestateStore) CountRelaySessions(_ context.Context, instanceID string, generation int64) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, sess := range s.conduitSessions {
		if sess.RelayInstanceID == instanceID && sess.RelayGeneration == generation {
			n++
		}
	}
	return n, nil
}
