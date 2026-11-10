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
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/api"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
	"github.com/google/uuid"
)

// --- TemplateStore & HarnessConfigStore ---

func (s *RestateStore) CreateTemplate(_ context.Context, t *store.Template) error {
	if t == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.ID == "" {
		t.ID = uuid.NewString()
	}
	if t.Slug == "" {
		t.Slug = api.Slugify(t.Name)
	}
	now := time.Now().UTC()
	if t.Created.IsZero() {
		t.Created = now
	}
	t.Updated = now
	cp := *t
	s.templates[t.ID] = &cp
	return nil
}

func (s *RestateStore) GetTemplate(_ context.Context, id string) (*store.Template, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.templates[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (s *RestateStore) GetTemplateBySlug(_ context.Context, slug, scope, projectID string) (*store.Template, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, t := range s.templates {
		if strings.EqualFold(t.Slug, slug) && (scope == "" || t.Scope == scope) && (projectID == "" || t.ProjectID == projectID || t.ScopeID == projectID) {
			cp := *t
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) UpdateTemplate(_ context.Context, t *store.Template) error {
	if t == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.templates[t.ID]
	if !ok {
		return store.ErrNotFound
	}
	cur.Name = t.Name
	cur.Slug = t.Slug
	cur.DisplayName = t.DisplayName
	cur.Description = t.Description
	cur.BaseTemplate = t.BaseTemplate
	cur.Updated = time.Now().UTC()
	return nil
}

func (s *RestateStore) UpdateTemplateContent(_ context.Context, t *store.Template, expected store.TemplateContentPrecondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.templates[t.ID]
	if !ok {
		return store.ErrNotFound
	}
	if expected.ContentHash != "" && cur.ContentHash != expected.ContentHash {
		return store.ErrTemplateConflict
	}
	cur.Files = slices.Clone(t.Files)
	cur.ContentHash = t.ContentHash
	cur.Harness = t.Harness
	cur.DefaultHarnessConfig = t.DefaultHarnessConfig
	cur.AgentConfig = t.AgentConfig
	cur.Layout = t.Layout
	cur.StoragePath = t.StoragePath
	cur.StorageBucket = t.StorageBucket
	cur.StorageURI = t.StorageURI
	cur.Status = t.Status
	cur.SourceURL = t.SourceURL
	cur.UpdatedBy = t.UpdatedBy
	cur.Updated = time.Now().UTC()
	return nil
}

func (s *RestateStore) SetTemplateAgentConfig(_ context.Context, id string, cfg *api.ScionConfig, _ store.TemplateContentPrecondition) (bool, error) {
	if cfg == nil {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.templates[id]
	if !ok || cur.AgentConfig != nil {
		return false, nil
	}
	cur.AgentConfig = cfg
	return true, nil
}

func (s *RestateStore) DeleteTemplate(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.templates[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.templates, id)
	return nil
}

func (s *RestateStore) DeleteTemplatesByScope(_ context.Context, scope, scopeID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, t := range s.templates {
		if t.Scope == scope && t.ScopeID == scopeID {
			delete(s.templates, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ListTemplates(_ context.Context, filter store.TemplateFilter, opts store.ListOptions) (*store.ListResult[store.Template], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.Template
	for _, t := range s.templates {
		if filter.Name != "" && !strings.EqualFold(t.Name, filter.Name) {
			continue
		}
		if filter.Scope != "" && t.Scope != filter.Scope {
			continue
		}
		if filter.ScopeID != "" && t.ScopeID != filter.ScopeID {
			continue
		}
		if filter.Harness != "" && t.Harness != filter.Harness {
			continue
		}
		if filter.Status != "" && t.Status != filter.Status {
			continue
		}
		items = append(items, *t)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	total := len(items)
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return &store.ListResult[store.Template]{Items: items, TotalCount: total}, nil
}

func (s *RestateStore) CreateHarnessConfig(_ context.Context, hc *store.HarnessConfig) error {
	if hc == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if hc.ID == "" {
		hc.ID = uuid.NewString()
	}
	if hc.Slug == "" {
		hc.Slug = api.Slugify(hc.Name)
	}
	now := time.Now().UTC()
	if hc.Created.IsZero() {
		hc.Created = now
	}
	hc.Updated = now
	cp := *hc
	s.harnessConfigs[hc.ID] = &cp
	return nil
}

func (s *RestateStore) GetHarnessConfig(_ context.Context, id string) (*store.HarnessConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hc, ok := s.harnessConfigs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *hc
	return &cp, nil
}

func (s *RestateStore) GetHarnessConfigBySlug(_ context.Context, slug, scope, scopeID string) (*store.HarnessConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, hc := range s.harnessConfigs {
		if strings.EqualFold(hc.Slug, slug) && (scope == "" || hc.Scope == scope) && (scopeID == "" || hc.ScopeID == scopeID) {
			cp := *hc
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) UpdateHarnessConfig(_ context.Context, hc *store.HarnessConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.harnessConfigs[hc.ID]
	if !ok {
		return store.ErrNotFound
	}
	cur.Name = hc.Name
	cur.Slug = hc.Slug
	cur.DisplayName = hc.DisplayName
	cur.Description = hc.Description
	cur.Updated = time.Now().UTC()
	return nil
}

func (s *RestateStore) UpdateHarnessConfigContent(_ context.Context, hc *store.HarnessConfig, expected store.HarnessConfigContentPrecondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.harnessConfigs[hc.ID]
	if !ok {
		return store.ErrNotFound
	}
	if expected.ContentHash != "" && cur.ContentHash != expected.ContentHash {
		return store.ErrHarnessConfigConflict
	}
	cur.Files = slices.Clone(hc.Files)
	cur.ContentHash = hc.ContentHash
	cur.Harness = hc.Harness
	cur.Config = hc.Config
	cur.Status = hc.Status
	cur.Layout = hc.Layout
	cur.StorageURI = hc.StorageURI
	cur.StorageBucket = hc.StorageBucket
	cur.StoragePath = hc.StoragePath
	cur.Updated = time.Now().UTC()
	return nil
}

func (s *RestateStore) UpdateHarnessConfigBuiltImage(_ context.Context, id, image string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.harnessConfigs[id]
	if !ok {
		return store.ErrNotFound
	}
	cur.BuiltImage = image
	return nil
}

func (s *RestateStore) DeleteHarnessConfig(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.harnessConfigs[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.harnessConfigs, id)
	return nil
}

func (s *RestateStore) DeleteHarnessConfigsByScope(_ context.Context, scope, scopeID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, hc := range s.harnessConfigs {
		if hc.Scope == scope && hc.ScopeID == scopeID {
			delete(s.harnessConfigs, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ListHarnessConfigs(_ context.Context, filter store.HarnessConfigFilter, opts store.ListOptions) (*store.ListResult[store.HarnessConfig], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.HarnessConfig
	for _, hc := range s.harnessConfigs {
		if filter.Name != "" && !strings.EqualFold(hc.Name, filter.Name) {
			continue
		}
		if filter.Scope != "" && hc.Scope != filter.Scope {
			continue
		}
		if filter.ScopeID != "" && hc.ScopeID != filter.ScopeID {
			continue
		}
		if filter.Harness != "" && hc.Harness != filter.Harness {
			continue
		}
		items = append(items, *hc)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	total := len(items)
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return &store.ListResult[store.HarnessConfig]{Items: items, TotalCount: total}, nil
}

func (s *RestateStore) UpdateHarnessConfigImageStatus(_ context.Context, id, status string, checkedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.harnessConfigs[id]
	if !ok {
		return store.ErrNotFound
	}
	cur.ImageStatus = status
	cur.ImageStatusCheckedAt = &checkedAt
	return nil
}

// --- EnvVarStore & SecretStore & SecretEncryptionStore ---

func envKey(scope, scopeID, key string) string {
	return scope + ":" + scopeID + ":" + key
}

func (s *RestateStore) CreateEnvVar(_ context.Context, ev *store.EnvVar) error {
	if ev == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := envKey(ev.Scope, ev.ScopeID, ev.Key)
	if _, exists := s.envVars[k]; exists {
		return store.ErrAlreadyExists
	}
	if ev.ID == "" {
		ev.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	ev.Created = now
	ev.Updated = now
	cp := *ev
	s.envVars[k] = &cp
	return nil
}

func (s *RestateStore) GetEnvVar(_ context.Context, key, scope, scopeID string) (*store.EnvVar, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ev, ok := s.envVars[envKey(scope, scopeID, key)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *ev
	return &cp, nil
}

func (s *RestateStore) UpdateEnvVar(_ context.Context, ev *store.EnvVar) error {
	if ev == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := envKey(ev.Scope, ev.ScopeID, ev.Key)
	cur, ok := s.envVars[k]
	if !ok {
		return store.ErrNotFound
	}
	cp := *ev
	cp.ID = cur.ID
	cp.Created = cur.Created
	cp.Updated = time.Now().UTC()
	s.envVars[k] = &cp
	return nil
}

func (s *RestateStore) UpsertEnvVar(_ context.Context, ev *store.EnvVar) (bool, error) {
	if ev == nil {
		return false, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := envKey(ev.Scope, ev.ScopeID, ev.Key)
	cur, exists := s.envVars[k]
	now := time.Now().UTC()
	cp := *ev
	if !exists {
		if cp.ID == "" {
			cp.ID = uuid.NewString()
		}
		cp.Created = now
	} else {
		cp.ID = cur.ID
		cp.Created = cur.Created
	}
	cp.Updated = now
	s.envVars[k] = &cp
	*ev = cp
	return !exists, nil
}

func (s *RestateStore) DeleteEnvVar(_ context.Context, key, scope, scopeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := envKey(scope, scopeID, key)
	if _, ok := s.envVars[k]; !ok {
		return store.ErrNotFound
	}
	delete(s.envVars, k)
	return nil
}

func (s *RestateStore) DeleteEnvVarsByScope(_ context.Context, scope, scopeID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, ev := range s.envVars {
		if ev.Scope == scope && ev.ScopeID == scopeID {
			delete(s.envVars, k)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ListEnvVars(_ context.Context, filter store.EnvVarFilter) ([]store.EnvVar, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.EnvVar
	for _, ev := range s.envVars {
		if filter.Scope != "" && ev.Scope != filter.Scope {
			continue
		}
		if filter.ScopeID != "" && ev.ScopeID != filter.ScopeID {
			continue
		}
		if filter.Key != "" && ev.Key != filter.Key {
			continue
		}
		out = append(out, *ev)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *RestateStore) ListProgenyEnvVars(_ context.Context, ancestorIDs []string) ([]store.EnvVar, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.EnvVar
	for _, ev := range s.envVars {
		if ev.Scope == store.ScopeUser && ev.AllowProgeny && ev.InjectionMode == "always" && slices.Contains(ancestorIDs, ev.CreatedBy) {
			out = append(out, *ev)
		}
	}
	return out, nil
}

func (s *RestateStore) CreateSecret(ctx context.Context, sec *store.Secret) error {
	if sec == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	k := envKey(sec.Scope, sec.ScopeID, sec.Key)
	if _, exists := s.secrets[k]; exists {
		s.mu.Unlock()
		return store.ErrAlreadyExists
	}
	if sec.ID == "" {
		sec.ID = uuid.NewString()
	}
	if sec.Version <= 0 {
		sec.Version = 1
	}
	now := time.Now().UTC()
	sec.Created = now
	sec.Updated = now
	cp := *sec
	s.secrets[k] = &cp
	s.mu.Unlock()

	if s.ingressActive() && cp.Scope == store.ScopeProject && strings.HasPrefix(cp.EncryptedValue, "enc:v2:") {
		_, _ = InvokeObject[SetProjectSecretRequest, ProjectSecretRecord](ctx, s.host, ServiceProjectObject, cp.ScopeID, "SetSecret", SetProjectSecretRequest{
			Name:        cp.Key,
			Type:        cp.SecretType,
			Target:      cp.Target,
			Ciphertext:  cp.EncryptedValue,
			Ref:         cp.SecretRef,
			Description: cp.Description,
			UpdatedBy:   cp.UpdatedBy,
		})
	}
	return nil
}

func (s *RestateStore) GetSecret(_ context.Context, key, scope, scopeID string) (*store.Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sec, ok := s.secrets[envKey(scope, scopeID, key)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *sec
	return &cp, nil
}

func (s *RestateStore) UpdateSecret(_ context.Context, sec *store.Secret) error {
	if sec == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := envKey(sec.Scope, sec.ScopeID, sec.Key)
	cur, ok := s.secrets[k]
	if !ok {
		return store.ErrNotFound
	}
	cp := *sec
	cp.ID = cur.ID
	cp.Created = cur.Created
	cp.Version = cur.Version + 1
	cp.Updated = time.Now().UTC()
	s.secrets[k] = &cp
	*sec = cp
	return nil
}

func (s *RestateStore) UpdateSecretRefIfMatches(_ context.Context, key, scope, scopeID, expectedRef string, expectedVersion int, newRef string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.secrets[envKey(scope, scopeID, key)]
	if !ok || cur.SecretRef != expectedRef || cur.Version != expectedVersion {
		return false, nil
	}
	cur.SecretRef = newRef
	cur.Version++
	cur.Updated = time.Now().UTC()
	return true, nil
}

func (s *RestateStore) UpdateSecretValueIfVersion(_ context.Context, key, scope, scopeID string, expectedVersion int, newEncryptedValue string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.secrets[envKey(scope, scopeID, key)]
	if !ok || cur.Version != expectedVersion {
		return false, nil
	}
	cur.EncryptedValue = newEncryptedValue
	cur.Version++
	cur.Updated = time.Now().UTC()
	return true, nil
}

func (s *RestateStore) UpsertSecret(ctx context.Context, sec *store.Secret) (bool, error) {
	if sec == nil {
		return false, store.ErrInvalidInput
	}
	s.mu.Lock()
	k := envKey(sec.Scope, sec.ScopeID, sec.Key)
	cur, exists := s.secrets[k]
	now := time.Now().UTC()
	cp := *sec
	if !exists {
		if cp.ID == "" {
			cp.ID = uuid.NewString()
		}
		cp.Version = 1
		cp.Created = now
	} else {
		cp.ID = cur.ID
		cp.Created = cur.Created
		cp.Version = cur.Version + 1
	}
	cp.Updated = now
	s.secrets[k] = &cp
	*sec = cp
	s.mu.Unlock()

	if s.ingressActive() && cp.Scope == store.ScopeProject && strings.HasPrefix(cp.EncryptedValue, "enc:v2:") {
		_, _ = InvokeObject[SetProjectSecretRequest, ProjectSecretRecord](ctx, s.host, ServiceProjectObject, cp.ScopeID, "SetSecret", SetProjectSecretRequest{
			Name:        cp.Key,
			Type:        cp.SecretType,
			Target:      cp.Target,
			Ciphertext:  cp.EncryptedValue,
			Ref:         cp.SecretRef,
			Description: cp.Description,
			UpdatedBy:   cp.UpdatedBy,
		})
	}
	return !exists, nil
}

func (s *RestateStore) DeleteSecret(_ context.Context, key, scope, scopeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := envKey(scope, scopeID, key)
	if _, ok := s.secrets[k]; !ok {
		return store.ErrNotFound
	}
	delete(s.secrets, k)
	return nil
}

func (s *RestateStore) DeleteSecretsByScope(_ context.Context, scope, scopeID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, sec := range s.secrets {
		if sec.Scope == scope && sec.ScopeID == scopeID {
			delete(s.secrets, k)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ListSecrets(_ context.Context, filter store.SecretFilter) ([]store.Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.Secret
	for _, sec := range s.secrets {
		if filter.Scope != "" && sec.Scope != filter.Scope {
			continue
		}
		if filter.ScopeID != "" && sec.ScopeID != filter.ScopeID {
			continue
		}
		if filter.Key != "" && sec.Key != filter.Key {
			continue
		}
		cp := *sec
		cp.EncryptedValue = ""
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *RestateStore) GetSecretValue(_ context.Context, key, scope, scopeID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sec, ok := s.secrets[envKey(scope, scopeID, key)]
	if !ok {
		return "", store.ErrNotFound
	}
	return sec.EncryptedValue, nil
}

func (s *RestateStore) UpdateSecretMeta(_ context.Context, key, scope, scopeID string, meta *store.SecretMetaUpdate) (*store.Secret, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, ok := s.secrets[envKey(scope, scopeID, key)]
	if !ok {
		return nil, store.ErrNotFound
	}
	if meta != nil {
		if meta.Description != nil {
			sec.Description = *meta.Description
		}
		if meta.InjectionMode != "" {
			sec.InjectionMode = meta.InjectionMode
		}
		if meta.SecretType != "" {
			sec.SecretType = meta.SecretType
		}
		if meta.Target != "" {
			sec.Target = meta.Target
		}
		if meta.AllowProgeny != nil {
			sec.AllowProgeny = *meta.AllowProgeny
		}
	}
	sec.Version++
	sec.Updated = time.Now().UTC()
	cp := *sec
	cp.EncryptedValue = ""
	return &cp, nil
}

func (s *RestateStore) ListProgenySecrets(_ context.Context, ancestorIDs []string) ([]store.Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.Secret
	for _, sec := range s.secrets {
		if sec.Scope == store.ScopeUser && sec.AllowProgeny && slices.Contains(ancestorIDs, sec.CreatedBy) {
			cp := *sec
			cp.EncryptedValue = ""
			out = append(out, cp)
		}
	}
	return out, nil
}

func (s *RestateStore) ScanSecretPayloads(_ context.Context, afterID string, limit int) ([]store.SecretPayload, error) {
	if limit <= 0 || limit > store.MaxSecretPayloadScan {
		return nil, store.ErrInvalidInput
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.SecretPayload
	for _, sec := range s.secrets {
		if sec.ID > afterID {
			out = append(out, store.SecretPayload{
				ID:             sec.ID,
				Key:            sec.Key,
				Scope:          sec.Scope,
				ScopeID:        sec.ScopeID,
				Version:        sec.Version,
				EncryptedValue: sec.EncryptedValue,
				SecretRef:      sec.SecretRef,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *RestateStore) ReplaceSecretCiphertext(_ context.Context, id string, expectedVersion int, expectedCiphertext, replacement string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sec := range s.secrets {
		if sec.ID == id {
			if sec.Version != expectedVersion || sec.EncryptedValue != expectedCiphertext {
				return false, nil
			}
			sec.EncryptedValue = replacement
			return true, nil
		}
	}
	return false, nil
}

func (s *RestateStore) AttributeUserSecretToOwner(_ context.Context, id, expectedCreatedBy string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sec := range s.secrets {
		if sec.ID == id {
			if sec.Scope != store.ScopeUser || sec.ScopeID == "" || sec.CreatedBy != expectedCreatedBy {
				return false, nil
			}
			sec.CreatedBy = sec.ScopeID
			return true, nil
		}
	}
	return false, nil
}

func (s *RestateStore) GetEncryptionPolicy(_ context.Context) (*store.EncryptionPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.encryptionPolicy == nil {
		return nil, store.ErrNotFound
	}
	cp := *s.encryptionPolicy
	return &cp, nil
}

func (s *RestateStore) UpdateEncryptionPolicy(_ context.Context, expectedGeneration int64, next store.EncryptionPolicy) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var curGen int64
	if s.encryptionPolicy != nil {
		curGen = s.encryptionPolicy.Generation
	}
	if curGen != expectedGeneration {
		return false, nil
	}
	cp := next
	s.encryptionPolicy = &cp
	return true, nil
}

func (s *RestateStore) CreateEncryptionMigration(_ context.Context, m *store.EncryptionMigration) error {
	if m == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.encryptionMigrations[m.ID]; exists {
		return store.ErrAlreadyExists
	}
	cp := *m
	s.encryptionMigrations[m.ID] = &cp
	return nil
}

func (s *RestateStore) GetEncryptionMigration(_ context.Context, id string) (*store.EncryptionMigration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.encryptionMigrations[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (s *RestateStore) UpdateEncryptionMigration(_ context.Context, m *store.EncryptionMigration, expectedState store.EncryptionMigrationState) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.encryptionMigrations[m.ID]
	if !ok || cur.State != expectedState {
		return false, nil
	}
	cp := *m
	s.encryptionMigrations[m.ID] = &cp
	return true, nil
}

// --- SkillStore, SkillRegistryStore, SkillInjectionStore, LifecycleHookStore ---

func (s *RestateStore) CreateSkill(_ context.Context, skill *store.Skill) error {
	if skill == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if skill.ID == "" {
		skill.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	skill.Created = now
	skill.Updated = now
	cp := *skill
	s.skills[skill.ID] = &cp
	return nil
}

func (s *RestateStore) GetSkill(_ context.Context, id string) (*store.Skill, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sk, ok := s.skills[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *sk
	return &cp, nil
}

func (s *RestateStore) GetSkillBySlug(_ context.Context, slug, scope, scopeID string) (*store.Skill, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sk := range s.skills {
		if strings.EqualFold(sk.Slug, slug) && (scope == "" || sk.Scope == scope) && (scopeID == "" || sk.ScopeID == scopeID) {
			cp := *sk
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) UpdateSkill(_ context.Context, skill *store.Skill) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skills[skill.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *skill
	cp.Updated = time.Now().UTC()
	s.skills[skill.ID] = &cp
	return nil
}

func (s *RestateStore) DeleteSkill(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skills[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.skills, id)
	return nil
}

func (s *RestateStore) ListSkills(_ context.Context, filter store.SkillFilter, opts store.ListOptions) (*store.ListResult[store.Skill], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.Skill
	for _, sk := range s.skills {
		if filter.Scope != "" && sk.Scope != filter.Scope {
			continue
		}
		if filter.ScopeID != "" && sk.ScopeID != filter.ScopeID {
			continue
		}
		items = append(items, *sk)
	}
	return &store.ListResult[store.Skill]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) CreateSkillVersion(_ context.Context, v *store.SkillVersion) error {
	if v == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if v.ID == "" {
		v.ID = uuid.NewString()
	}
	v.Created = time.Now().UTC()
	cp := *v
	s.skillVersions[v.ID] = &cp
	return nil
}

func (s *RestateStore) GetSkillVersion(_ context.Context, id string) (*store.SkillVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.skillVersions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *v
	return &cp, nil
}

func (s *RestateStore) GetSkillVersionByNumber(_ context.Context, skillID, version string) (*store.SkillVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.skillVersions {
		if v.SkillID == skillID && v.Version == version {
			cp := *v
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) ListSkillVersions(_ context.Context, skillID string, _ store.ListOptions) (*store.ListResult[store.SkillVersion], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.SkillVersion
	for _, v := range s.skillVersions {
		if v.SkillID == skillID {
			items = append(items, *v)
		}
	}
	return &store.ListResult[store.SkillVersion]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) UpdateSkillVersion(_ context.Context, v *store.SkillVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skillVersions[v.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *v
	s.skillVersions[v.ID] = &cp
	return nil
}

func (s *RestateStore) DeleteSkillVersion(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skillVersions[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.skillVersions, id)
	return nil
}

func (s *RestateStore) ResolveSkillVersion(_ context.Context, skillID, constraint string) (*store.SkillVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.skillVersions {
		if v.SkillID == skillID && (constraint == "" || constraint == "latest" || v.Version == constraint) {
			cp := *v
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) IncrementSkillVersionDownloadCount(_ context.Context, versionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.skillVersions[versionID]; ok {
		v.DownloadCount++
	}
	return nil
}

func (s *RestateStore) CreateSkillRegistry(_ context.Context, reg *store.SkillRegistry) error {
	if reg == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if reg.ID == "" {
		reg.ID = uuid.NewString()
	}
	cp := *reg
	s.skillRegistries[reg.ID] = &cp
	return nil
}

func (s *RestateStore) GetSkillRegistry(_ context.Context, id string) (*store.SkillRegistry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.skillRegistries[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *r
	return &cp, nil
}

func (s *RestateStore) GetSkillRegistryByName(_ context.Context, name string) (*store.SkillRegistry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.skillRegistries {
		if strings.EqualFold(r.Name, name) {
			cp := *r
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) UpdateSkillRegistry(_ context.Context, reg *store.SkillRegistry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skillRegistries[reg.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *reg
	s.skillRegistries[reg.ID] = &cp
	return nil
}

func (s *RestateStore) DeleteSkillRegistry(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skillRegistries[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.skillRegistries, id)
	delete(s.skillPinnedHashes, id)
	return nil
}

func (s *RestateStore) ListSkillRegistries(_ context.Context, _ store.ListOptions) (*store.ListResult[store.SkillRegistry], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.SkillRegistry
	for _, r := range s.skillRegistries {
		items = append(items, *r)
	}
	return &store.ListResult[store.SkillRegistry]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) PinSkillHash(_ context.Context, registryID, uri, hash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.skillPinnedHashes[registryID] == nil {
		s.skillPinnedHashes[registryID] = make(map[string]string)
	}
	s.skillPinnedHashes[registryID][uri] = hash
	return nil
}

func (s *RestateStore) UnpinSkillHash(_ context.Context, registryID, uri string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m := s.skillPinnedHashes[registryID]; m != nil {
		delete(m, uri)
	}
	return nil
}

func (s *RestateStore) GetPinnedHash(_ context.Context, registryID, uri string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m := s.skillPinnedHashes[registryID]; m != nil {
		if h, ok := m[uri]; ok {
			return h, nil
		}
	}
	return "", store.ErrNotFound
}

func (s *RestateStore) ListPinnedHashes(_ context.Context, registryID string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.skillPinnedHashes[registryID]), nil
}

func (s *RestateStore) ListSkillInjections(_ context.Context, scope, scopeID string) ([]store.SkillInjection, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.SkillInjection
	for _, si := range s.skillInjections {
		if si.Scope == scope && si.ScopeID == scopeID {
			out = append(out, *si)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SortOrder < out[j].SortOrder })
	return out, nil
}

func (s *RestateStore) AddSkillInjection(_ context.Context, si *store.SkillInjection) error {
	if si == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if si.ID == "" {
		si.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	si.CreatedAt = now
	cp := *si
	s.skillInjections[si.ID] = &cp
	return nil
}

func (s *RestateStore) UpdateSkillInjection(_ context.Context, si *store.SkillInjection) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skillInjections[si.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *si
	cp.CreatedAt = time.Now().UTC()
	s.skillInjections[si.ID] = &cp
	return nil
}

func (s *RestateStore) RemoveSkillInjection(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skillInjections[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.skillInjections, id)
	return nil
}

func (s *RestateStore) SetSkillInjections(_ context.Context, scope, scopeID string, entries []store.SkillInjection, createdBy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, si := range s.skillInjections {
		if si.Scope == scope && si.ScopeID == scopeID {
			delete(s.skillInjections, id)
		}
	}
	now := time.Now().UTC()
	for i := range entries {
		cp := entries[i]
		if cp.ID == "" {
			cp.ID = uuid.NewString()
		}
		cp.Scope = scope
		cp.ScopeID = scopeID
		if cp.CreatedBy == "" {
			cp.CreatedBy = createdBy
		}
		cp.CreatedAt = now
		s.skillInjections[cp.ID] = &cp
	}
	return nil
}

func (s *RestateStore) DeleteSkillInjectionsByScope(_ context.Context, scope, scopeID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, si := range s.skillInjections {
		if si.Scope == scope && si.ScopeID == scopeID {
			delete(s.skillInjections, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ListProgenySkillInjections(_ context.Context, ancestorIDs []string) ([]store.SkillInjection, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.SkillInjection
	for _, si := range s.skillInjections {
		if si.Scope == store.ScopeUser && si.AllowProgeny && slices.Contains(ancestorIDs, si.CreatedBy) {
			out = append(out, *si)
		}
	}
	return out, nil
}

func (s *RestateStore) CreateLifecycleHook(_ context.Context, hook *store.LifecycleHook) error {
	if hook == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if hook.ID == "" {
		hook.ID = uuid.NewString()
	}
	if _, exists := s.lifecycleHooks[hook.ID]; exists {
		return store.ErrAlreadyExists
	}
	now := time.Now().UTC()
	hook.Created = now
	hook.Updated = now
	hook.StateVersion = 1
	cp := *hook
	s.lifecycleHooks[hook.ID] = &cp
	return nil
}

func (s *RestateStore) GetLifecycleHook(_ context.Context, id string) (*store.LifecycleHook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.lifecycleHooks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *h
	return &cp, nil
}

func (s *RestateStore) UpdateLifecycleHook(_ context.Context, hook *store.LifecycleHook) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.lifecycleHooks[hook.ID]
	if !ok {
		return store.ErrNotFound
	}
	if hook.StateVersion > 0 && cur.StateVersion != hook.StateVersion {
		return store.ErrVersionConflict
	}
	cp := *hook
	cp.StateVersion = cur.StateVersion + 1
	cp.Updated = time.Now().UTC()
	s.lifecycleHooks[hook.ID] = &cp
	hook.StateVersion = cp.StateVersion
	return nil
}

func (s *RestateStore) DeleteLifecycleHook(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.lifecycleHooks[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.lifecycleHooks, id)
	return nil
}

func (s *RestateStore) ListLifecycleHooks(_ context.Context, filter store.LifecycleHookFilter, _ store.ListOptions) (*store.ListResult[store.LifecycleHook], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.LifecycleHook
	for _, h := range s.lifecycleHooks {
		if filter.ScopeType != "" && h.ScopeType != filter.ScopeType {
			continue
		}
		if filter.ScopeID != "" && h.ScopeID != filter.ScopeID {
			continue
		}
		if filter.Trigger != "" && h.Trigger != filter.Trigger {
			continue
		}
		if filter.Enabled != nil && h.Enabled != *filter.Enabled {
			continue
		}
		items = append(items, *h)
	}
	return &store.ListResult[store.LifecycleHook]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) CompareAndSetHookPhase(_ context.Context, agentID, newPhase string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hookPhases[agentID] == newPhase {
		return false, nil
	}
	s.hookPhases[agentID] = newPhase
	return true, nil
}

func (s *RestateStore) DeleteHookPhase(_ context.Context, agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.hookPhases, agentID)
	return nil
}

func (s *RestateStore) DeleteLifecycleHooksByScope(_ context.Context, scopeType, scopeID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, h := range s.lifecycleHooks {
		if h.ScopeType == scopeType && h.ScopeID == scopeID {
			delete(s.lifecycleHooks, id)
			n++
		}
	}
	return n, nil
}
