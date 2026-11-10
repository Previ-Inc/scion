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

package relaydb

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/conduit/registry"
	"github.com/GoogleCloudPlatform/scion/pkg/conduit/relayrole"
	"github.com/GoogleCloudPlatform/scion/pkg/config/opsettings"
	"github.com/GoogleCloudPlatform/scion/pkg/experiments"
	"github.com/GoogleCloudPlatform/scion/pkg/restatehub"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
)

type Options struct {
	Driver          string
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	EventSubjects   []string
	Logger          *slog.Logger
}

type DB struct {
	Registry registry.Store
	Store    registry.Store
	Settings *SettingsReader
	events   *Listener
	close    func()
}

func (db *DB) Events() relayrole.Events {
	if db == nil || db.events == nil {
		return nil
	}
	return db.events
}

func (db *DB) StartEvents() {
	if db != nil && db.events != nil {
		db.events.Start()
	}
}

func (db *DB) Close() {
	if db != nil && db.close != nil {
		db.close()
	}
}

func Open(_ context.Context, o Options) (*DB, error) {
	if o.DSN == "" {
		return nil, errors.New("relaydb: empty database URL")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	rs := restatehub.NewRestateStore(nil, nil)
	db := &DB{
		Registry: rs,
		Store:    rs,
		Settings: &SettingsReader{store: rs},
		close:    func() { _ = rs.Close() },
	}
	if strings.EqualFold(o.Driver, "postgres") && len(o.EventSubjects) > 0 {
		l, err := NewListener(o.DSN, o.EventSubjects, o.Logger.With("subsystem", "relay.events"))
		if err != nil {
			db.close()
			return nil, err
		}
		db.events = l
		closeClient := db.close
		db.close = func() { l.Close(); closeClient() }
	}
	return db, nil
}

const experimentsSection = "experiments"
const conduitExperiment = "hub.conduit"

type SettingsReader struct {
	store store.HubSettingStore
	reg   *experiments.Registry
}

func (r *SettingsReader) ConduitEnabled(ctx context.Context) (bool, error) {
	var overrides map[string]bool
	malformed := false
	if r != nil && r.store != nil {
		row, err := r.store.GetHubSetting(ctx, experimentsSection)
		if err == nil && row != nil {
			doc, bad := opsettings.ParseExperimentsDoc(row.Value)
			overrides, malformed = doc.Overrides, bad
		}
	}
	reg := r.reg
	if reg == nil {
		reg = experiments.Default()
	}
	return reg.Resolve(conduitExperiment, overrides, malformed), nil
}
