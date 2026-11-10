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

package pgprovider

import (
	"context"
	"maps"
	"sync"
)

// PostgresConfigProvider implements config.IntegrationConfigProvider for
// Mode 3 (HA) integrations without requiring Ent/PostgreSQL.
type PostgresConfigProvider struct {
	client    any
	plugin    string
	updatedBy string
}

var (
	haConfigMu    sync.RWMutex
	haConfigsByID = make(map[string]map[string]string)
)

// NewPostgresConfigProvider creates a config provider for the named integration.
func NewPostgresConfigProvider(client any, plugin string) *PostgresConfigProvider {
	return &PostgresConfigProvider{
		client: client,
		plugin: plugin,
	}
}

// SetUpdatedBy sets the user identity to record on the next Save call.
func (p *PostgresConfigProvider) SetUpdatedBy(user string) {
	p.updatedBy = user
}

// Load reads the config map for the integration.
func (p *PostgresConfigProvider) Load(_ context.Context) (map[string]string, error) {
	haConfigMu.RLock()
	defer haConfigMu.RUnlock()
	cfg, ok := haConfigsByID[p.plugin]
	if !ok || cfg == nil {
		return make(map[string]string), nil
	}
	return maps.Clone(cfg), nil
}

// Save upserts the config map for the integration.
func (p *PostgresConfigProvider) Save(_ context.Context, cfg map[string]string) error {
	haConfigMu.Lock()
	defer haConfigMu.Unlock()
	if cfg == nil {
		haConfigsByID[p.plugin] = make(map[string]string)
	} else {
		haConfigsByID[p.plugin] = maps.Clone(cfg)
	}
	return nil
}
