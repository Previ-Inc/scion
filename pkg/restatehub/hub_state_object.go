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

// Granular state keys inside HubStateObject["system"].
const (
	hubStateKeySettingPrefix = "setting:"
	hubStateKeyRevision      = "revision"
	hubStateKeyGCPGAPrefix   = "gcp_sa:"
	hubStateKeyGHInstall     = "gh_install:"
	hubStateKeyMetricsPrefix = "metrics:"
)

// HubSettingRecord stores a two-tier configuration section under "setting:<section>".
type HubSettingRecord struct {
	Section   string         `json:"section"`
	Values    map[string]any `json:"values"`
	Revision  int64          `json:"revision"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

// HubStateObject is the Restate Virtual Object managing global Hub state, settings,
// and encrypted service credentials (Invariant 4).
type HubStateObject struct {
	cipher *ValueCipher
}

// NewHubStateObject constructs a HubStateObject.
func NewHubStateObject(cipher *ValueCipher) *HubStateObject {
	return &HubStateObject{cipher: cipher}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (h *HubStateObject) ServiceName() string {
	return ServiceHubStateObject
}

// SetSetting saves a configuration section under "setting:<section>".
func (h *HubStateObject) SetSetting(ctx restate.ObjectContext, setting HubSettingRecord) (HubSettingRecord, error) {
	if setting.Section == "" {
		return HubSettingRecord{}, restate.ToTerminalError(
			fmt.Errorf("setting section name is required"),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
	rev, _ := restate.Get[int64](ctx, hubStateKeyRevision)
	rev++
	setting.Revision = rev
	setting.UpdatedAt = time.Now().UTC()
	restate.Set(ctx, hubStateKeyRevision, rev)
	restate.Set(ctx, hubStateKeySettingPrefix+setting.Section, setting)
	return setting, nil
}

// GetSetting reads a configuration section.
func (h *HubStateObject) GetSetting(ctx restate.ObjectSharedContext, section string) (HubSettingRecord, error) {
	rec, err := restate.Get[HubSettingRecord](ctx, hubStateKeySettingPrefix+section)
	if err != nil {
		return HubSettingRecord{}, err
	}
	if rec.Section == "" {
		return HubSettingRecord{}, restate.ToTerminalError(
			fmt.Errorf("setting section %q not found", section),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return rec, nil
}
