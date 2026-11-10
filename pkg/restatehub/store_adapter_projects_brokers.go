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
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/api"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
	"github.com/google/uuid"
)

func cloneProject(p *store.Project) *store.Project {
	if p == nil {
		return nil
	}
	cp := *p
	if p.Labels != nil {
		cp.Labels = maps.Clone(p.Labels)
	}
	if p.Annotations != nil {
		cp.Annotations = maps.Clone(p.Annotations)
	}
	if p.SharedDirs != nil {
		cp.SharedDirs = slices.Clone(p.SharedDirs)
	}
	return &cp
}

// --- ProjectStore ---

func (s *RestateStore) CreateProject(ctx context.Context, p *store.Project) error {
	if p == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	if p.ID == "" {
		p.ID = uuid.NewString()
	}
	if p.Slug == "" {
		p.Slug = api.Slugify(p.Name)
	}
	for _, existing := range s.projects {
		if existing.ID == p.ID || strings.EqualFold(existing.Slug, p.Slug) {
			s.mu.Unlock()
			return store.ErrAlreadyExists
		}
	}
	now := time.Now().UTC()
	if p.Created.IsZero() {
		p.Created = now
	}
	p.Updated = now
	cp := cloneProject(p)
	s.projects[p.ID] = cp
	s.mu.Unlock()

	if s.ingressActive() {
		_, _ = InvokeObject[ProjectMeta, ProjectMeta](ctx, s.host, ServiceProjectObject, cp.ID, "CreateOrUpdateMeta", ProjectMeta{
			ID:              cp.ID,
			Slug:            cp.Slug,
			Name:            cp.Name,
			GitRemote:       cp.GitRemote,
			DefaultBrokerID: cp.DefaultRuntimeBrokerID,
			OwnerID:         cp.OwnerID,
			Labels:          cp.Labels,
			Annotations:     cp.Annotations,
			Created:         cp.Created,
			Updated:         cp.Updated,
		})
	}
	return nil
}

func (s *RestateStore) populateProjectComputedLocked(p *store.Project) {
	if p == nil {
		return
	}
	agentCount := 0
	for _, a := range s.agents {
		if a.ProjectID == p.ID && a.DeletedAt.IsZero() {
			agentCount++
		}
	}
	p.AgentCount = agentCount
	p.ActiveBrokerCount = len(s.projectProviders[p.ID])
}

func (s *RestateStore) GetProject(_ context.Context, id string) (*store.Project, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.projects[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := cloneProject(p)
	s.populateProjectComputedLocked(cp)
	return cp, nil
}

func (s *RestateStore) GetProjectBySlug(_ context.Context, slug string) (*store.Project, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, p := range s.projects {
		if p.Slug == slug {
			cp := cloneProject(p)
			s.populateProjectComputedLocked(cp)
			return cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetProjectBySlugCaseInsensitive(_ context.Context, slug string) (*store.Project, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, p := range s.projects {
		if strings.EqualFold(p.Slug, slug) {
			cp := cloneProject(p)
			s.populateProjectComputedLocked(cp)
			return cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetProjectsByGitRemote(_ context.Context, gitRemote string) ([]*store.Project, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.Project
	for _, p := range s.projects {
		if p.GitRemote == gitRemote {
			cp := cloneProject(p)
			s.populateProjectComputedLocked(cp)
			out = append(out, cp)
		}
	}
	return out, nil
}

func (s *RestateStore) NextAvailableSlug(_ context.Context, baseSlug string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	taken := func(cand string) bool {
		for _, p := range s.projects {
			if strings.EqualFold(p.Slug, cand) {
				return true
			}
		}
		return false
	}
	if !taken(baseSlug) {
		return baseSlug, nil
	}
	for i := 1; i < 10000; i++ {
		cand := fmt.Sprintf("%s-%d", baseSlug, i)
		if !taken(cand) {
			return cand, nil
		}
	}
	return baseSlug + "-" + uuid.NewString()[:8], nil
}

func (s *RestateStore) UpdateProject(ctx context.Context, p *store.Project) error {
	if p == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	cur, ok := s.projects[p.ID]
	if !ok {
		s.mu.Unlock()
		return store.ErrNotFound
	}
	cp := cloneProject(p)
	cp.OwnerID = cur.OwnerID
	cp.Updated = time.Now().UTC()
	s.projects[p.ID] = cp
	p.OwnerID = cur.OwnerID
	p.Updated = cp.Updated
	s.mu.Unlock()

	if s.ingressActive() {
		_, _ = InvokeObject[ProjectMeta, ProjectMeta](ctx, s.host, ServiceProjectObject, cp.ID, "CreateOrUpdateMeta", ProjectMeta{
			ID:              cp.ID,
			Slug:            cp.Slug,
			Name:            cp.Name,
			GitRemote:       cp.GitRemote,
			DefaultBrokerID: cp.DefaultRuntimeBrokerID,
			OwnerID:         cp.OwnerID,
			Labels:          cp.Labels,
			Annotations:     cp.Annotations,
			Created:         cp.Created,
			Updated:         cp.Updated,
		})
	}
	return nil
}

func (s *RestateStore) SetProjectOwnerID(_ context.Context, projectID, ownerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.projects[projectID]
	if !ok {
		return store.ErrNotFound
	}
	p.OwnerID = ownerID
	p.Updated = time.Now().UTC()
	return nil
}

func (s *RestateStore) DeleteProject(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.projects[id]; !ok {
		return store.ErrNotFound
	}
	for aid, a := range s.agents {
		if a.ProjectID == id {
			delete(s.agents, aid)
		}
	}
	delete(s.agentKeys, id)
	delete(s.projectProviders, id)
	delete(s.projects, id)
	return nil
}

func (s *RestateStore) matchesProjectFilter(p *store.Project, filter store.ProjectFilter) bool {
	if filter.OwnerID != "" && len(filter.MemberOrOwnerIDs) == 0 && p.OwnerID != filter.OwnerID {
		return false
	}
	if len(filter.MemberOrOwnerIDs) > 0 {
		if p.OwnerID != filter.OwnerID && !slices.Contains(filter.MemberOrOwnerIDs, p.ID) {
			return false
		}
	}
	if len(filter.MemberProjectIDs) > 0 && !slices.Contains(filter.MemberProjectIDs, p.ID) {
		return false
	}
	if filter.ExcludeOwnerID != "" && p.OwnerID == filter.ExcludeOwnerID {
		return false
	}
	if filter.GitRemote != "" && p.GitRemote != filter.GitRemote {
		return false
	}
	if filter.Name != "" && !strings.EqualFold(p.Name, filter.Name) {
		return false
	}
	if filter.Slug != "" && !strings.EqualFold(p.Slug, filter.Slug) {
		return false
	}
	if filter.Search != "" {
		q := strings.ToLower(filter.Search)
		if !strings.Contains(strings.ToLower(p.Name), q) && !strings.Contains(strings.ToLower(p.Slug), q) {
			return false
		}
	}
	if filter.AuthorizedProjectIDs != nil && !slices.Contains(filter.AuthorizedProjectIDs, p.ID) {
		return false
	}
	if len(filter.ExcludedProjectIDs) > 0 && slices.Contains(filter.ExcludedProjectIDs, p.ID) {
		return false
	}
	return true
}

func (s *RestateStore) ListProjects(_ context.Context, filter store.ProjectFilter, opts store.ListOptions) (*store.ListResult[store.Project], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.Project
	for _, p := range s.projects {
		if s.matchesProjectFilter(p, filter) {
			cp := cloneProject(p)
			s.populateProjectComputedLocked(cp)
			items = append(items, *cp)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	total := len(items)
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return &store.ListResult[store.Project]{Items: items, TotalCount: total}, nil
}

func (s *RestateStore) ListProjectSummaries(ctx context.Context, filter store.ProjectFilter, opts store.ListOptions) (*store.ListResult[store.Project], error) {
	return s.ListProjects(ctx, filter, opts)
}

func (s *RestateStore) LockProjectForMembership(_ context.Context, projectID string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.projects[projectID]; !ok {
		return store.ErrNotFound
	}
	return nil
}

func (s *RestateStore) LockProjectAgents(_ context.Context, _ string) error {
	return nil
}

func (s *RestateStore) UpdateProjectMessagingPolicy(_ context.Context, projectID, inbound string, expectedRevision int64) (*store.Project, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.projects[projectID]
	if !ok {
		return nil, store.ErrNotFound
	}
	if p.CrossProjectInboundRevision != expectedRevision {
		return nil, store.ErrRevisionConflict
	}
	p.CrossProjectInbound = inbound
	p.CrossProjectInboundRevision++
	p.Updated = time.Now().UTC()
	return cloneProject(p), nil
}

// --- RuntimeBrokerStore ---

func (s *RestateStore) CreateRuntimeBroker(ctx context.Context, b *store.RuntimeBroker) error {
	if b == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	if b.ID == "" {
		b.ID = uuid.NewString()
	}
	if _, exists := s.brokers[b.ID]; exists {
		s.mu.Unlock()
		return store.ErrAlreadyExists
	}
	now := time.Now().UTC()
	if b.Created.IsZero() {
		b.Created = now
	}
	b.Updated = now
	cp := *b
	s.brokers[b.ID] = &cp
	s.mu.Unlock()

	if s.ingressActive() {
		_, _ = InvokeObject[BrokerRegistration, BrokerRegistration](ctx, s.host, ServiceBrokerObject, cp.ID, "Register", BrokerRegistration{
			BrokerID:     cp.ID,
			Name:         cp.Name,
			Endpoint:     cp.Endpoint,
			Status:       cp.Status,
			Version:      cp.Version,
			Labels:       cp.Labels,
			RegisteredAt: cp.Created,
		})
	}
	return nil
}

func (s *RestateStore) CreateRuntimeBrokerClearingCredentials(ctx context.Context, b *store.RuntimeBroker) error {
	if b != nil && b.ID != "" {
		s.mu.Lock()
		delete(s.brokerSecrets, b.ID)
		delete(s.brokerJoinTokens, b.ID)
		delete(s.brokers, b.ID)
		s.mu.Unlock()
	}
	return s.CreateRuntimeBroker(ctx, b)
}

func (s *RestateStore) GetRuntimeBroker(_ context.Context, id string) (*store.RuntimeBroker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.brokers[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *b
	return &cp, nil
}

func (s *RestateStore) GetRuntimeBrokerByName(_ context.Context, name string) (*store.RuntimeBroker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, b := range s.brokers {
		if strings.EqualFold(b.Name, name) {
			cp := *b
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetLegacyRuntimeBrokerByName(_ context.Context, name string) (*store.RuntimeBroker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, b := range s.brokers {
		if strings.EqualFold(b.Name, name) && b.RuntimeTarget == nil {
			cp := *b
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) SetRuntimeBrokerTarget(_ context.Context, brokerID string, desc api.RuntimeTargetDescriptor) (*store.RuntimeBroker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[brokerID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cpDesc := desc
	b.RuntimeTarget = &cpDesc
	cp := *b
	return &cp, nil
}

func (s *RestateStore) SetRuntimeBrokerPresentation(_ context.Context, brokerID, targetID string, p *api.RuntimeTargetPresentation, hash string, epoch, seq int64, observedAt time.Time) (store.PresentationOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[brokerID]
	if !ok {
		return "", store.ErrNotFound
	}
	if b.RuntimeTarget == nil {
		b.RuntimeTarget = &api.RuntimeTargetDescriptor{ID: targetID}
	}
	b.RuntimeTargetPresentation = p
	b.RuntimeTargetPresentationHash = hash
	b.PresentationEpoch = &epoch
	b.PresentationSeq = &seq
	b.PresentationFencedAt = &observedAt
	b.RuntimeTargetPresentationObservedAt = &observedAt
	return store.PresentationWritten, nil
}

func (s *RestateStore) UpdateRuntimeBroker(_ context.Context, b *store.RuntimeBroker) error {
	if b == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.brokers[b.ID]
	if !ok {
		return store.ErrNotFound
	}
	cp := *b
	cp.CreatedBy = cur.CreatedBy
	cp.Updated = time.Now().UTC()
	s.brokers[b.ID] = &cp
	return nil
}

func (s *RestateStore) RefreshRuntimeBrokerState(_ context.Context, id string, refresh func(broker *store.RuntimeBroker) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[id]
	if !ok {
		return store.ErrNotFound
	}
	cp := *b
	if refresh(&cp) {
		b.Capabilities = cp.Capabilities
		b.WorkspaceStorage = cp.WorkspaceStorage
		b.DefaultProfile = cp.DefaultProfile
		b.Health = cp.Health
		b.Profiles = cp.Profiles
		b.Version = cp.Version
		b.Updated = time.Now().UTC()
	}
	return nil
}

func (s *RestateStore) SetRuntimeBrokerCreatedByIfEmpty(_ context.Context, id, createdBy string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[id]
	if !ok || b.CreatedBy != "" {
		return false, nil
	}
	b.CreatedBy = createdBy
	return true, nil
}

func (s *RestateStore) SetRuntimeBrokerGCPHostIfUnset(_ context.Context, id, email, project, source string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[id]
	if !ok || b.GCPHostServiceAccountEmail != "" || b.GCPHostProjectID != "" {
		return false, nil
	}
	b.GCPHostServiceAccountEmail = email
	b.GCPHostProjectID = project
	b.GCPHostSource = source
	return true, nil
}

func (s *RestateStore) SetRuntimeBrokerGCPHostDetected(_ context.Context, id, email, project string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[id]
	if !ok || b.GCPHostSource == store.GCPHostSourceAdmin {
		return false, nil
	}
	b.GCPHostServiceAccountEmail = email
	b.GCPHostProjectID = project
	b.GCPHostSource = store.GCPHostSourceDetected
	return true, nil
}

func (s *RestateStore) SetRuntimeBrokerGCPHost(_ context.Context, id, email, project string) (store.GCPHostSetting, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[id]
	if !ok {
		return store.GCPHostSetting{}, store.ErrNotFound
	}
	prev := store.GCPHostSetting{
		Email:   b.GCPHostServiceAccountEmail,
		Project: b.GCPHostProjectID,
		Source:  b.GCPHostSource,
	}
	b.GCPHostServiceAccountEmail = email
	b.GCPHostProjectID = project
	b.GCPHostSource = store.GCPHostSourceAdmin
	return prev, nil
}

func (s *RestateStore) SetRuntimeBrokerGCPHostIfPrior(_ context.Context, id, priorEmail, priorProject, email, project string) (store.GCPHostSetting, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[id]
	if !ok {
		return store.GCPHostSetting{}, false, store.ErrNotFound
	}
	prev := store.GCPHostSetting{
		Email:   b.GCPHostServiceAccountEmail,
		Project: b.GCPHostProjectID,
		Source:  b.GCPHostSource,
	}
	if !strings.EqualFold(b.GCPHostServiceAccountEmail, priorEmail) || b.GCPHostProjectID != priorProject {
		return prev, false, nil
	}
	b.GCPHostServiceAccountEmail = email
	b.GCPHostProjectID = project
	b.GCPHostSource = store.GCPHostSourceAdmin
	return prev, true, nil
}

func (s *RestateStore) SetRuntimeBrokerGCPHostSource(_ context.Context, id, source string) (store.GCPHostSetting, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[id]
	if !ok {
		return store.GCPHostSetting{}, store.ErrNotFound
	}
	prev := store.GCPHostSetting{
		Email:   b.GCPHostServiceAccountEmail,
		Project: b.GCPHostProjectID,
		Source:  b.GCPHostSource,
	}
	b.GCPHostSource = source
	return prev, nil
}

func (s *RestateStore) RecordRuntimeBrokerHostObservation(_ context.Context, id string, obs store.HostIdentityObservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[id]
	if !ok {
		return store.ErrNotFound
	}
	b.HostObservation = &obs
	return nil
}

func (s *RestateStore) DeleteRuntimeBroker(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.brokers[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.brokers, id)
	delete(s.brokerSecrets, id)
	delete(s.brokerJoinTokens, id)
	delete(s.brokerSettings, id)
	return nil
}

func (s *RestateStore) ListRuntimeBrokers(_ context.Context, filter store.RuntimeBrokerFilter, opts store.ListOptions) (*store.ListResult[store.RuntimeBroker], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.RuntimeBroker
	for _, b := range s.brokers {
		if filter.Status != "" && b.Status != filter.Status {
			continue
		}
		if filter.Name != "" && !strings.EqualFold(b.Name, filter.Name) {
			continue
		}
		if filter.AutoProvide != nil && b.AutoProvide != *filter.AutoProvide {
			continue
		}
		items = append(items, *b)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	total := len(items)
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return &store.ListResult[store.RuntimeBroker]{Items: items, TotalCount: total}, nil
}

func (s *RestateStore) FindEmbeddedBroker(_ context.Context) (*store.RuntimeBroker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var match *store.RuntimeBroker
	for _, b := range s.brokers {
		if b.RuntimeTarget == nil && b.CreatedBy == "" && b.Labels["scion.io/broker-role"] == "embedded" {
			if match != nil {
				return nil, nil
			}
			cp := *b
			match = &cp
		}
	}
	return match, nil
}

func (s *RestateStore) UpdateRuntimeBrokerHeartbeat(_ context.Context, id, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[id]
	if !ok {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	b.LastHeartbeat = now
	if status != "" {
		b.Status = status
	}
	b.Updated = now
	return nil
}

func (s *RestateStore) ClaimRuntimeBrokerConnection(_ context.Context, brokerID, hubInstanceID, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[brokerID]
	if !ok {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	b.ConnectedHubID = &hubInstanceID
	b.ConnectedSessionID = &sessionID
	b.ConnectedAt = &now
	b.Status = store.BrokerStatusOnline
	b.LastHeartbeat = now
	return nil
}

func (s *RestateStore) ReleaseRuntimeBrokerConnection(_ context.Context, brokerID, hubInstanceID, sessionID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[brokerID]
	if !ok || b.ConnectedHubID == nil || *b.ConnectedHubID != hubInstanceID || b.ConnectedSessionID == nil || *b.ConnectedSessionID != sessionID {
		return false, nil
	}
	b.ConnectedHubID = nil
	b.ConnectedSessionID = nil
	b.ConnectedAt = nil
	return true, nil
}

func (s *RestateStore) ReleaseAndMarkBrokerOffline(_ context.Context, brokerID, hubInstanceID, sessionID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[brokerID]
	if !ok || b.ConnectedHubID == nil || *b.ConnectedHubID != hubInstanceID || b.ConnectedSessionID == nil || *b.ConnectedSessionID != sessionID {
		return false, nil
	}
	b.ConnectedHubID = nil
	b.ConnectedSessionID = nil
	b.ConnectedAt = nil
	b.Status = store.BrokerStatusOffline
	return true, nil
}

func (s *RestateStore) ReapStaleBrokerAffinity(_ context.Context, staleBefore time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.brokers {
		if b.ConnectedHubID != nil && b.LastHeartbeat.Before(staleBefore) {
			b.ConnectedHubID = nil
			b.ConnectedSessionID = nil
			b.ConnectedAt = nil
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) MarkBrokerOffline(_ context.Context, brokerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.brokers[brokerID]
	if !ok {
		return store.ErrNotFound
	}
	b.Status = store.BrokerStatusOffline
	return nil
}

func (s *RestateStore) MarkStaleBrokersOffline(_ context.Context, threshold time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for id, b := range s.brokers {
		if b.Status != store.BrokerStatusOffline && b.LastHeartbeat.Before(threshold) {
			b.Status = store.BrokerStatusOffline
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// --- BrokerDispatchStore ---

func (s *RestateStore) InsertBrokerDispatch(_ context.Context, d *store.BrokerDispatch) error {
	if d == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.ID == "" {
		d.ID = uuid.NewString()
	}
	if d.State == "" {
		d.State = "pending"
	}
	now := time.Now().UTC()
	d.CreatedAt = now
	d.UpdatedAt = now
	cp := *d
	s.brokerDispatches[d.ID] = &cp
	return nil
}

func (s *RestateStore) ClaimBrokerDispatch(_ context.Context, id, _ string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.brokerDispatches[id]
	if !ok || d.State != "pending" {
		return false, nil
	}
	d.State = "in_progress"
	d.UpdatedAt = time.Now().UTC()
	return true, nil
}

func (s *RestateStore) CompleteBrokerDispatch(_ context.Context, id, result string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.brokerDispatches[id]
	if !ok {
		return store.ErrNotFound
	}
	d.State = "done"
	d.Result = result
	d.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *RestateStore) FailBrokerDispatch(_ context.Context, id, errMsg, result string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.brokerDispatches[id]
	if !ok {
		return store.ErrNotFound
	}
	d.State = "failed"
	d.Error = errMsg
	d.Result = result
	d.Attempts++
	d.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *RestateStore) GetBrokerDispatch(_ context.Context, id string) (*store.BrokerDispatch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.brokerDispatches[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *d
	return &cp, nil
}

func (s *RestateStore) ListPendingDispatch(_ context.Context, brokerID string) ([]store.BrokerDispatch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.BrokerDispatch
	for _, d := range s.brokerDispatches {
		if d.BrokerID == brokerID && d.State == "pending" {
			out = append(out, *d)
		}
	}
	return out, nil
}

func (s *RestateStore) HasOutstandingBrokerDispatch(_ context.Context, agentID, op string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.brokerDispatches {
		if d.AgentID == agentID && d.Op == op && (d.State == "pending" || d.State == "in_progress") {
			return true, nil
		}
	}
	return false, nil
}

func (s *RestateStore) HasCompletedBrokerDispatchSince(_ context.Context, agentID, op string, since time.Time) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.brokerDispatches {
		if d.AgentID == agentID && d.Op == op && d.State == "done" && !d.UpdatedAt.Before(since) {
			return true, nil
		}
	}
	return false, nil
}

func (s *RestateStore) MarkMessageDispatched(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.messages[id]
	if !ok || m.DispatchState != "pending" {
		return false, nil
	}
	m.DispatchState = "dispatched"
	return true, nil
}

func (s *RestateStore) MarkMessageFailed(_ context.Context, id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.messages[id]
	if !ok {
		return store.ErrNotFound
	}
	m.DispatchState = "failed"
	m.DispatchFailureReason = &reason
	return nil
}

func (s *RestateStore) ListPendingMessages(_ context.Context, _ string) ([]store.Message, error) {
	return nil, nil
}

func (s *RestateStore) ReapStuckDispatch(_ context.Context, _ time.Time, _ int) (int, int, error) {
	return 0, 0, nil
}

func (s *RestateStore) CountBrokerDispatchHealth(_ context.Context, _, _ time.Time) (int, int, error) {
	return 0, 0, nil
}

func (s *RestateStore) CountStuckPendingMessages(_ context.Context, _ time.Time) (int, error) {
	return 0, nil
}

func (s *RestateStore) ListStuckPendingMessageIDs(_ context.Context, _ time.Time, _ int) ([]string, error) {
	return nil, nil
}

func (s *RestateStore) ExpireStuckPendingMessages(_ context.Context, _ time.Time, _ string) (int, error) {
	return 0, nil
}

func (s *RestateStore) FailPendingMessagesWithMissingRecipient(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (s *RestateStore) ExpireAndListStuckPendingMessages(_ context.Context, _ time.Time, _ string) ([]store.ExpiredPendingMessage, error) {
	return nil, nil
}

func (s *RestateStore) BackfillNonAgentDispatchState(_ context.Context, _ string) (int, error) {
	return 0, nil
}

// --- ProjectProviderStore & ProjectSyncStateStore & ProjectPreStartHookStore ---

func (s *RestateStore) AddProjectProvider(_ context.Context, provider *store.ProjectProvider) error {
	if provider == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.projectProviders[provider.ProjectID]
	for i := range list {
		if list[i].BrokerID == provider.BrokerID {
			list[i] = *provider
			s.projectProviders[provider.ProjectID] = list
			return nil
		}
	}
	s.projectProviders[provider.ProjectID] = append(list, *provider)
	return nil
}

func (s *RestateStore) RemoveProjectProvider(_ context.Context, projectID, brokerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.projectProviders[projectID]
	for i := range list {
		if list[i].BrokerID == brokerID {
			s.projectProviders[projectID] = slices.Delete(list, i, i+1)
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *RestateStore) GetProjectProvider(_ context.Context, projectID, brokerID string) (*store.ProjectProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, p := range s.projectProviders[projectID] {
		if p.BrokerID == brokerID {
			cp := p
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetProjectProviders(_ context.Context, projectID string) ([]store.ProjectProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.projectProviders[projectID]), nil
}

func (s *RestateStore) GetBrokerProjects(_ context.Context, brokerID string) ([]store.ProjectProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.ProjectProvider
	for _, list := range s.projectProviders {
		for _, p := range list {
			if p.BrokerID == brokerID {
				out = append(out, p)
			}
		}
	}
	return out, nil
}

func (s *RestateStore) UpdateProviderStatus(_ context.Context, projectID, brokerID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.projectProviders[projectID]
	for i := range list {
		if list[i].BrokerID == brokerID {
			list[i].Status = status
			list[i].LastSeen = time.Now().UTC()
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *RestateStore) DeleteProjectProvidersByProject(_ context.Context, projectID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.projectProviders[projectID])
	delete(s.projectProviders, projectID)
	return n, nil
}

func (s *RestateStore) UpsertProjectSyncState(_ context.Context, st *store.ProjectSyncState) error {
	if st == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *st
	s.projectSyncStates[st.ProjectID+":"+st.BrokerID] = &cp
	return nil
}

func (s *RestateStore) GetProjectSyncState(_ context.Context, projectID, brokerID string) (*store.ProjectSyncState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.projectSyncStates[projectID+":"+brokerID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *st
	return &cp, nil
}

func (s *RestateStore) ListProjectSyncStates(_ context.Context, projectID string) ([]store.ProjectSyncState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.ProjectSyncState
	for _, st := range s.projectSyncStates {
		if st.ProjectID == projectID {
			out = append(out, *st)
		}
	}
	return out, nil
}

func (s *RestateStore) DeleteProjectSyncState(_ context.Context, projectID, brokerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := projectID + ":" + brokerID
	if _, ok := s.projectSyncStates[key]; !ok {
		return store.ErrNotFound
	}
	delete(s.projectSyncStates, key)
	return nil
}

func (s *RestateStore) DeleteProjectSyncStatesByProject(_ context.Context, projectID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, st := range s.projectSyncStates {
		if st.ProjectID == projectID {
			delete(s.projectSyncStates, k)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) GetActiveProjectPreStartHook(_ context.Context, projectID string) (*store.ProjectPreStartHook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, h := range s.projectPreStartHooks {
		if h.Scope == store.PreStartHookScopeProject && h.ProjectID == projectID && h.Status == store.ProjectPreStartHookStatusActive {
			cp := *h
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetProjectPreStartHook(_ context.Context, hookID, projectID string) (*store.ProjectPreStartHook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.projectPreStartHooks[hookID]
	if !ok || h.ProjectID != projectID {
		return nil, store.ErrNotFound
	}
	cp := *h
	return &cp, nil
}

func (s *RestateStore) ListProjectPreStartHooks(_ context.Context, projectID string) ([]*store.ProjectPreStartHook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.ProjectPreStartHook
	for _, h := range s.projectPreStartHooks {
		if h.Scope == store.PreStartHookScopeProject && h.ProjectID == projectID {
			cp := *h
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) CreateProjectPreStartHook(_ context.Context, hook *store.ProjectPreStartHook) (*store.ProjectPreStartHook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if hook.ID == "" {
		hook.ID = uuid.NewString()
	}
	hook.Scope = store.PreStartHookScopeProject
	hook.Status = store.ProjectPreStartHookStatusActive
	now := time.Now().UTC()
	hook.Created = now
	hook.Updated = now
	for _, h := range s.projectPreStartHooks {
		if h.Scope == store.PreStartHookScopeProject && h.ProjectID == hook.ProjectID {
			h.Status = store.ProjectPreStartHookStatusArchived
		}
	}
	cp := *hook
	s.projectPreStartHooks[hook.ID] = &cp
	return &cp, nil
}

func (s *RestateStore) UpdateProjectPreStartHook(_ context.Context, hook *store.ProjectPreStartHook) (*store.ProjectPreStartHook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.projectPreStartHooks[hook.ID]
	if !ok {
		return nil, store.ErrNotFound
	}
	h.Name = hook.Name
	h.Description = hook.Description
	h.Script = hook.Script
	h.Updated = time.Now().UTC()
	cp := *h
	return &cp, nil
}

func (s *RestateStore) ActivateProjectPreStartHook(_ context.Context, hookID, projectID string) (*store.ProjectPreStartHook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	target, ok := s.projectPreStartHooks[hookID]
	if !ok || target.ProjectID != projectID {
		return nil, store.ErrNotFound
	}
	for _, h := range s.projectPreStartHooks {
		if h.Scope == store.PreStartHookScopeProject && h.ProjectID == projectID {
			h.Status = store.ProjectPreStartHookStatusArchived
		}
	}
	target.Status = store.ProjectPreStartHookStatusActive
	cp := *target
	return &cp, nil
}

func (s *RestateStore) DeleteProjectPreStartHook(_ context.Context, hookID, projectID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.projectPreStartHooks[hookID]
	if !ok || h.ProjectID != projectID {
		return store.ErrNotFound
	}
	delete(s.projectPreStartHooks, hookID)
	return nil
}

func (s *RestateStore) GetActiveHubPreStartHook(_ context.Context) (*store.ProjectPreStartHook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, h := range s.projectPreStartHooks {
		if h.Scope == store.PreStartHookScopeHub && h.Status == store.ProjectPreStartHookStatusActive {
			cp := *h
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetHubPreStartHook(_ context.Context, hookID string) (*store.ProjectPreStartHook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.projectPreStartHooks[hookID]
	if !ok || h.Scope != store.PreStartHookScopeHub {
		return nil, store.ErrNotFound
	}
	cp := *h
	return &cp, nil
}

func (s *RestateStore) ListHubPreStartHooks(_ context.Context) ([]*store.ProjectPreStartHook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.ProjectPreStartHook
	for _, h := range s.projectPreStartHooks {
		if h.Scope == store.PreStartHookScopeHub {
			cp := *h
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) CreateHubPreStartHook(_ context.Context, hook *store.ProjectPreStartHook) (*store.ProjectPreStartHook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if hook.ID == "" {
		hook.ID = uuid.NewString()
	}
	hook.Scope = store.PreStartHookScopeHub
	hook.ProjectID = ""
	hook.Status = store.ProjectPreStartHookStatusActive
	now := time.Now().UTC()
	hook.Created = now
	hook.Updated = now
	for _, h := range s.projectPreStartHooks {
		if h.Scope == store.PreStartHookScopeHub {
			h.Status = store.ProjectPreStartHookStatusArchived
		}
	}
	cp := *hook
	s.projectPreStartHooks[hook.ID] = &cp
	return &cp, nil
}

func (s *RestateStore) UpdateHubPreStartHook(ctx context.Context, hook *store.ProjectPreStartHook) (*store.ProjectPreStartHook, error) {
	return s.UpdateProjectPreStartHook(ctx, hook)
}

func (s *RestateStore) ActivateHubPreStartHook(_ context.Context, hookID string) (*store.ProjectPreStartHook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	target, ok := s.projectPreStartHooks[hookID]
	if !ok || target.Scope != store.PreStartHookScopeHub {
		return nil, store.ErrNotFound
	}
	for _, h := range s.projectPreStartHooks {
		if h.Scope == store.PreStartHookScopeHub {
			h.Status = store.ProjectPreStartHookStatusArchived
		}
	}
	target.Status = store.ProjectPreStartHookStatusActive
	cp := *target
	return &cp, nil
}

func (s *RestateStore) DeleteHubPreStartHook(_ context.Context, hookID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.projectPreStartHooks[hookID]
	if !ok || h.Scope != store.PreStartHookScopeHub {
		return store.ErrNotFound
	}
	delete(s.projectPreStartHooks, hookID)
	return nil
}

func (s *RestateStore) DeletePreStartHooksByProject(_ context.Context, projectID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, h := range s.projectPreStartHooks {
		if h.ProjectID == projectID {
			delete(s.projectPreStartHooks, id)
			n++
		}
	}
	return n, nil
}
