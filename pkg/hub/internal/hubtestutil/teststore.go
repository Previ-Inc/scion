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

package hubtestutil

import (
	"context"
	"testing"

	"github.com/GoogleCloudPlatform/scion/pkg/restatehub"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
)

// OpenMigratedTestStore opens a fresh RestateStore, runs Migrate on it, and
// closes it in t.Cleanup.
func OpenMigratedTestStore(t testing.TB) (store.Store, error) {
	t.Helper()
	s := restatehub.NewRestateStore(nil, nil)
	if err := s.Migrate(context.Background()); err != nil {
		_ = s.Close()
		return nil, err
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, nil
}
