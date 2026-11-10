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
	"encoding/json"
	"errors"
	"slices"

	"github.com/google/uuid"

	"github.com/GoogleCloudPlatform/scion/pkg/agent"
	"github.com/GoogleCloudPlatform/scion/pkg/preflight"
	"github.com/GoogleCloudPlatform/scion/pkg/restatehub"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
)

type preflightReader struct {
	q           *restatehub.RestateStore
	colocatedID string
}

func newPreflightReader(q *restatehub.RestateStore, colocatedID string) *preflightReader {
	return &preflightReader{q: q, colocatedID: colocatedID}
}

var _ preflight.ReadOnlyQueries = (*preflightReader)(nil)

func (r *preflightReader) Broker(ctx context.Context, id string) (found, flat bool, err error) {
	if _, err := uuid.Parse(id); err != nil {
		return false, false, nil
	}
	if r.q == nil {
		return false, false, nil
	}
	b, err := r.q.GetRuntimeBroker(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if b.RuntimeTarget != nil && b.RuntimeTarget.ID != "" {
		flat = true
	}
	return true, flat, nil
}

func (r *preflightReader) HubSettings(ctx context.Context, sections []string) ([]preflight.HubSettingRow, error) {
	if r.q == nil {
		return nil, nil
	}
	all, err := r.q.ListHubSettings(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]preflight.HubSettingRow, 0)
	for _, s := range all {
		if !slices.Contains(sections, s.Section) {
			continue
		}
		ub := s.UpdatedBy
		row := preflight.HubSettingRow{
			Section:   s.Section,
			Origin:    s.Origin,
			Value:     json.RawMessage(s.Value),
			UpdatedBy: &ub,
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (r *preflightReader) Agents(ctx context.Context) ([]agent.ProfileRefAgentRow, int, error) {
	if r.q == nil {
		return nil, 0, nil
	}
	res, err := r.q.ListAgents(ctx, store.AgentFilter{}, store.ListOptions{Limit: 10000})
	if err != nil {
		return nil, 0, err
	}
	rows := make([]agent.ProfileRefAgentRow, 0, len(res.Items))
	for _, a := range res.Items {
		profile := ""
		if a.AppliedConfig != nil {
			profile = a.AppliedConfig.Profile
		}
		rows = append(rows, agent.ProfileRefAgentRow{
			Slug:            a.Slug,
			ProjectID:       a.ProjectID,
			RuntimeBrokerID: a.RuntimeBrokerID,
			AppliedProfile:  profile,
		})
	}
	return rows, 0, nil
}

func (r *preflightReader) Projects(ctx context.Context) ([]agent.ProfileRefProject, error) {
	if r.q == nil {
		return nil, nil
	}
	res, err := r.q.ListProjects(ctx, store.ProjectFilter{}, store.ListOptions{Limit: 10000})
	if err != nil {
		return nil, err
	}
	rows := make([]agent.ProfileRefProject, 0, len(res.Items))
	for _, p := range res.Items {
		rows = append(rows, agent.ProfileRefProject{
			ID:              p.ID,
			Slug:            p.Slug,
			ActiveProfile:   p.Annotations[store.AnnotationProjectActiveProfile],
			SharedWorkspace: p.IsSharedWorkspace(),
		})
	}
	return rows, nil
}

func (r *preflightReader) Providers(ctx context.Context, brokerID string) ([]agent.ProfileRefProvider, error) {
	if _, err := uuid.Parse(brokerID); err != nil || r.q == nil {
		return nil, nil
	}
	cs, err := r.q.GetBrokerProjects(ctx, brokerID)
	if err != nil {
		return nil, err
	}
	rows := make([]agent.ProfileRefProvider, 0, len(cs))
	for _, c := range cs {
		rows = append(rows, agent.ProfileRefProvider{ProjectID: c.ProjectID, LocalPath: c.LocalPath})
	}
	return rows, nil
}
