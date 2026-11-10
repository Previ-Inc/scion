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
	"fmt"
	"io"
	"strings"

	"github.com/GoogleCloudPlatform/scion/pkg/config"
	"github.com/GoogleCloudPlatform/scion/pkg/messaging"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
	"github.com/GoogleCloudPlatform/scion/pkg/restatehub"
	"github.com/spf13/cobra"
)

var (
	dmMigrationExecute   bool
	dmMigrationBatchSize int
	dmMigrationDB        string
)

var serverDMMigrationCmd = &cobra.Command{
	Use:   "migrate-dm-keys",
	Short: "Migrate DM conversation keys to kind-encoded format",
	Long: `Scan direct conversations and migrate old-format external_ref keys
(dm:<uuidA>:<uuidB>) to the kind-encoded format
(dm:<kind>:<uuid>:<kind>:<uuid>).

By default runs in DRY-RUN mode — scans and reports what would change;
it removes or rewrites no rows but still applies the schema migration.
Pass --execute to apply changes.

The migration is idempotent: conversations with kind-encoded keys are
scanned but not modified (they may gain missing participants), so
re-running is safe.

Examples:
  # Preview what would change (dry-run):
  scion server migrate-dm-keys

  # Apply changes:
  scion server migrate-dm-keys --execute`,
	RunE: runServerDMMigration,
}

func init() {
	serverCmd.AddCommand(serverDMMigrationCmd)
	serverDMMigrationCmd.Flags().BoolVar(&dmMigrationExecute, "execute", false, "Apply changes (default: dry-run)")
	serverDMMigrationCmd.Flags().IntVar(&dmMigrationBatchSize, "batch-size", 0, "Conversations per batch (0 = default 100)")
	serverDMMigrationCmd.Flags().StringVar(&dmMigrationDB, "db", "", "Database DSN (overrides config/env)")
}

// dmMigrationConfigFromFlags builds the DMMigrationConfig from command flags.
// Extracted so the dry-run default is assertable without a database.
func dmMigrationConfigFromFlags() messaging.DMMigrationConfig {
	return messaging.DMMigrationConfig{
		DryRun:    !dmMigrationExecute,
		BatchSize: dmMigrationBatchSize,
	}
}

func runServerDMMigration(cmd *cobra.Command, _ []string) error {
	pinProcessUTC()

	ctx := cmd.Context()
	out := cmd.OutOrStdout()

	s, err := openDMMigrationStore(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	cfg := dmMigrationConfigFromFlags()
	result, err := runDMMigrationWithStore(ctx, s, cfg)
	if err != nil {
		return fmt.Errorf("dm key migration: %w", err)
	}

	printDMMigrationReport(out, result)

	if len(result.Errors) > 0 {
		return fmt.Errorf("dm key migration completed with %d error(s)", len(result.Errors))
	}
	return nil
}

// runDMMigrationWithStore is the testable core: given a store and config,
// run the DM key migration and return the result.
func runDMMigrationWithStore(ctx context.Context, s store.Store, cfg messaging.DMMigrationConfig) (*messaging.DMMigrationResult, error) {
	svc := messaging.NewDMMigrationService(s)
	return svc.Run(ctx, cfg)
}

// openDMMigrationStore resolves the database DSN and returns a CompositeStore.
// Precedence: --db flag > config file (via LoadGlobalConfig).
// Mirrors openBackfillStore in server_backfill.go.
func openDMMigrationStore(ctx context.Context) (*restatehub.RestateStore, error) {
	cfg, err := config.LoadGlobalConfig(serverConfigPath)
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}
	if dmMigrationDB != "" {
		cfg.Database.URL = dmMigrationDB
		if strings.HasPrefix(dmMigrationDB, "postgres://") || strings.HasPrefix(dmMigrationDB, "postgresql://") {
			cfg.Database.Driver = "postgres"
		} else {
			cfg.Database.Driver = "sqlite"
		}
	}
	if cfg.Database.URL == "" {
		return nil, fmt.Errorf("no database configured: set --db, SCION_SERVER_DATABASE_URL env, or database.url in server config")
	}
	switch cfg.Database.Driver {
	case "sqlite", "postgres", "restate", "":
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", cfg.Database.Driver)
	}
	s := restatehub.NewRestateStore(nil, nil)
	if err := migrateMaintenanceDB(ctx, s, dmMigrationExecute, cfg.Database.AllowShadowedSchema); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// printDMMigrationReport writes a human-readable summary to out, matching
// the style of printBackfillReport.
func printDMMigrationReport(out io.Writer, r *messaging.DMMigrationResult) {
	mode := "dry-run"
	if dmMigrationExecute {
		mode = "execute"
	}

	_, _ = fmt.Fprintln(out, "DM Key Migration Report")
	_, _ = fmt.Fprintln(out, "=======================")
	_, _ = fmt.Fprintf(out, "Mode:                  %s\n", mode)
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintf(out, "Conversations scanned: %d\n", r.TotalScanned)
	_, _ = fmt.Fprintf(out, "  Participants added:  %d\n", r.ParticipantsAdded)
	_, _ = fmt.Fprintf(out, "  Old-format re-keyed: %d\n", r.OldFormatRekeyed)
	_, _ = fmt.Fprintf(out, "  Empty-ref skipped:   %d  (B14: left keyless)\n", r.EmptyRefSkipped)
	_, _ = fmt.Fprintf(out, "  Unparseable:         %d\n", r.Unparseable)
	_, _ = fmt.Fprintf(out, "  Ambiguous:           %d\n", r.Ambiguous)
	_, _ = fmt.Fprintf(out, "Errors:                %d\n", len(r.Errors))

	if len(r.Errors) > 0 {
		_, _ = fmt.Fprintln(out)
		_, _ = fmt.Fprintln(out, "Errors:")
		// Bound the error output to avoid flooding on large runs.
		limit := 20
		for i, e := range r.Errors {
			if i >= limit {
				_, _ = fmt.Fprintf(out, "  ... and %d more\n", len(r.Errors)-limit)
				break
			}
			_, _ = fmt.Fprintf(out, "  - %s\n", e)
		}
	}
}
