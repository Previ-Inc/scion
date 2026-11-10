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

// Granular state keys inside CatalogObject[scope_id].
const (
	catalogKeyTemplatePrefix = "tpl:"
	catalogKeyHConfigPrefix  = "hcfg:"
	catalogKeySkillPrefix    = "skill:"
	catalogKeyRegPrefix      = "skill_reg:"
	catalogKeyInjPrefix      = "skill_inj:"
	catalogKeyHookPrefix     = "hook:"
)

// CatalogItemRecord stores template, harness config, skill, or hook metadata under scope_id ("global" or "project:<id>").
type CatalogItemRecord struct {
	ID          string         `json:"id"`
	Kind        string         `json:"kind"` // "template", "harness_config", "skill", "hook"
	Name        string         `json:"name"`
	Hash        string         `json:"hash,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	CreatedBy   string         `json:"createdBy,omitempty"`
	CreatedAt   time.Time      `json:"createdAt"`
	UpdatedAt   time.Time      `json:"updatedAt"`
}

// CatalogObject is the Restate Virtual Object managing templates, harness configs, skills, and lifecycle hooks metadata.
type CatalogObject struct{}

// NewCatalogObject constructs a CatalogObject.
func NewCatalogObject() *CatalogObject {
	return &CatalogObject{}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (c *CatalogObject) ServiceName() string {
	return ServiceCatalogObject
}

// SaveItem stores a catalog item metadata record.
func (c *CatalogObject) SaveItem(ctx restate.ObjectContext, item CatalogItemRecord) (CatalogItemRecord, error) {
	if item.ID == "" || item.Kind == "" || item.Name == "" {
		return CatalogItemRecord{}, restate.ToTerminalError(
			fmt.Errorf("id, kind, and name are required for catalog item"),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
	prefix := catalogKeyTemplatePrefix
	switch item.Kind {
	case "harness_config":
		prefix = catalogKeyHConfigPrefix
	case "skill":
		prefix = catalogKeySkillPrefix
	case "hook":
		prefix = catalogKeyHookPrefix
	}
	now := time.Now().UTC()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	restate.Set(ctx, prefix+item.ID, item)
	return item, nil
}

// GetItem retrieves a catalog item by ID and kind.
func (c *CatalogObject) GetItem(ctx restate.ObjectSharedContext, req struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}) (CatalogItemRecord, error) {
	prefix := catalogKeyTemplatePrefix
	switch req.Kind {
	case "harness_config":
		prefix = catalogKeyHConfigPrefix
	case "skill":
		prefix = catalogKeySkillPrefix
	case "hook":
		prefix = catalogKeyHookPrefix
	}
	item, err := restate.Get[CatalogItemRecord](ctx, prefix+req.ID)
	if err != nil {
		return CatalogItemRecord{}, err
	}
	if item.ID == "" {
		return CatalogItemRecord{}, restate.ToTerminalError(
			fmt.Errorf("catalog item %s (%s) not found", req.ID, req.Kind),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return item, nil
}

// DeleteItem removes a catalog item.
func (c *CatalogObject) DeleteItem(ctx restate.ObjectContext, req struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}) (restate.Void, error) {
	prefix := catalogKeyTemplatePrefix
	switch req.Kind {
	case "harness_config":
		prefix = catalogKeyHConfigPrefix
	case "skill":
		prefix = catalogKeySkillPrefix
	case "hook":
		prefix = catalogKeyHookPrefix
	}
	restate.Clear(ctx, prefix+req.ID)
	return restate.Void{}, nil
}
