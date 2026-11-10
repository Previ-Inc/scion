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

package testinfra

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/config"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
)

const MarkerID = "test-infra-marker"
const MarkMutationType = "test_infra_mark"
const UnmarkMutationType = "test_infra_unmark"
const WebSessionKeyLen = 64

var ErrMarkerAbsent = errors.New("test infrastructure marker absent")
var ErrMarkerExists = errors.New("test infrastructure marker already present")

var (
	ErrWebSessionKeyMissing     = errors.New("test infrastructure marker has no web session key; mark the database again")
	ErrWebSessionKeyInvalid     = errors.New("test infrastructure marker web session key is malformed")
	ErrWebSessionKeyHubMismatch = errors.New("test infrastructure marker names a different hub id")
	ErrWebSessionKeyClass       = errors.New(`test infrastructure marker class is not "test"`)
)

var randRead = rand.Read

func newWebSessionKey() ([]byte, error) {
	key := make([]byte, WebSessionKeyLen)
	if _, err := randRead(key); err != nil {
		return nil, fmt.Errorf("generate web session key: %w", err)
	}
	if bytes.Equal(key, make([]byte, WebSessionKeyLen)) {
		return nil, errors.New("generate web session key: random source returned zeros")
	}
	return key, nil
}

type Marker struct {
	HubID         string
	Class         string
	CreatedAt     time.Time
	CreatedByHost string
	Attested      json.RawMessage
}

type storedMarker struct {
	Marker
	webSessionKey []byte
}

var (
	markerMu  sync.Mutex
	curMarker *storedMarker
)

func ReadWebSessionKey(_ context.Context, client any, hubID string) ([]byte, error) {
	if client == nil {
		return nil, errors.New("test infrastructure marker: no database client")
	}
	markerMu.Lock()
	defer markerMu.Unlock()
	if curMarker == nil {
		return nil, ErrMarkerAbsent
	}
	switch {
	case hubID == "" || curMarker.HubID != hubID:
		return nil, ErrWebSessionKeyHubMismatch
	case !config.IsTestHubClass(curMarker.Class):
		return nil, ErrWebSessionKeyClass
	case len(curMarker.webSessionKey) == 0:
		return nil, ErrWebSessionKeyMissing
	case len(curMarker.webSessionKey) != WebSessionKeyLen || bytes.Equal(curMarker.webSessionKey, make([]byte, WebSessionKeyLen)):
		return nil, ErrWebSessionKeyInvalid
	}
	key := make([]byte, WebSessionKeyLen)
	copy(key, curMarker.webSessionKey)
	return key, nil
}

func Read(_ context.Context, client any) (*Marker, error) {
	if client == nil {
		return nil, errors.New("test infrastructure marker: no database client")
	}
	markerMu.Lock()
	defer markerMu.Unlock()
	if curMarker == nil {
		return nil, ErrMarkerAbsent
	}
	cp := curMarker.Marker
	return &cp, nil
}

type MarkInput struct {
	HubID    string
	Operator string
	Host     string
	Attested json.RawMessage
	Now      time.Time
}

func Mark(ctx context.Context, client any, in MarkInput) (*Marker, error) {
	if client == nil {
		return nil, errors.New("test infrastructure marker: no database client")
	}
	hubID := strings.TrimSpace(in.HubID)
	if hubID == "" || hubID != in.HubID {
		return nil, errors.New("test infrastructure marker: hub id must be non-empty and carry no surrounding whitespace")
	}
	if len(in.Attested) > 0 {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(in.Attested, &obj); err != nil || obj == nil {
			return nil, errors.New("test infrastructure marker: attested must be a JSON object")
		}
	}
	operator := strings.TrimSpace(in.Operator)
	if operator == "" {
		operator = "unknown"
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	webSessionKey, err := newWebSessionKey()
	if err != nil {
		return nil, err
	}

	markerMu.Lock()
	if curMarker != nil {
		markerMu.Unlock()
		return nil, ErrMarkerExists
	}
	m := Marker{
		HubID:         hubID,
		Class:         config.HubClassTest,
		CreatedAt:     now,
		CreatedByHost: in.Host,
		Attested:      in.Attested,
	}
	curMarker = &storedMarker{Marker: m, webSessionKey: webSessionKey}
	markerMu.Unlock()

	if as, ok := client.(store.MutationAuditStore); ok {
		_ = as.CreateMutationAudit(ctx, &store.MutationAuditRecord{
			Timestamp:          now,
			MutationType:       MarkMutationType,
			ActorPrincipalKind: "operator",
			ActorPrincipalID:   operator,
			TargetType:         "test_infra_marker",
			TargetID:           MarkerID,
			AfterSummary: fmt.Sprintf("test infrastructure marker written: hub_id=%s class=%s host=%s operator=%s",
				hubID, config.HubClassTest, in.Host, operator),
		})
	}
	return &m, nil
}

type UnmarkInput struct {
	Operator string
	Host     string
	Now      time.Time
}

func Unmark(ctx context.Context, client any, in UnmarkInput) (*Marker, error) {
	if client == nil {
		return nil, errors.New("test infrastructure marker: no database client")
	}
	operator := strings.TrimSpace(in.Operator)
	if operator == "" {
		operator = "unknown"
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	markerMu.Lock()
	if curMarker == nil {
		markerMu.Unlock()
		return nil, ErrMarkerAbsent
	}
	m := curMarker.Marker
	curMarker = nil
	markerMu.Unlock()

	if as, ok := client.(store.MutationAuditStore); ok {
		_ = as.CreateMutationAudit(ctx, &store.MutationAuditRecord{
			Timestamp:          now,
			MutationType:       UnmarkMutationType,
			ActorPrincipalKind: "operator",
			ActorPrincipalID:   operator,
			TargetType:         "test_infra_marker",
			TargetID:           MarkerID,
			BeforeSummary: fmt.Sprintf("test infrastructure marker deleted: hub_id=%s class=%s host=%s operator=%s",
				m.HubID, m.Class, in.Host, operator),
		})
	}
	return &m, nil
}
