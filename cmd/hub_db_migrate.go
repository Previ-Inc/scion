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

	"github.com/GoogleCloudPlatform/scion/pkg/restatehub"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
)

func migrateHubStore(ctx context.Context, s store.Store, _ bool) error {
	return s.Migrate(ctx)
}

func migrateHubSchema(_ context.Context, _ any, _, _ bool) error {
	return nil
}

func openCmdStore(ctx context.Context) (*restatehub.RestateStore, error) {
	s := restatehub.NewRestateStore(nil, nil)
	if err := s.Migrate(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}
