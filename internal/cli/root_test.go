package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/01builders/cometbft-archive/internal/archive"
	"github.com/01builders/cometbft-archive/internal/blocksyncarchive"
	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/store"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/gogoproto/proto"
)

const cliTestChainID = "archive-test-chain"

type staticHeightReader struct {
	height int64
}

func (r staticHeightReader) Height() int64 {
	return r.height
}

func execute(t *testing.T, args ...string) string {
	t.Helper()
	cmd, err := NewRootCommand()
	if err != nil {
		t.Fatalf("NewRootCommand: %v", err)
	}
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute %v: %v\n%s", args, err, out.String())
	}
	return out.String()
}

func executeErr(t *testing.T, args ...string) string {
	t.Helper()
	cmd, err := NewRootCommand()
	if err != nil {
		t.Fatalf("NewRootCommand: %v", err)
	}
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err = cmd.Execute()
	if err == nil {
		t.Fatalf("execute %v succeeded, want error\n%s", args, out.String())
	}
	return out.String() + err.Error()
}

func TestHelpIncludesRequiredCommands(t *testing.T) {
	out := execute(t, "--help")
	for _, want := range []string{migrateCommandName, verifyCommandName, inspectCommandName, hydrateCommandName, archiveReadyCommandName, pruneHotCommandName, soakCommandName, serveCommandName} {
		if !strings.Contains(out, want) {
			t.Fatalf("help output missing %q:\n%s", want, out)
		}
	}
}

func TestLiveIngestStartHeightUsesArchiveWhenHotStoreIsEmpty(t *testing.T) {
	start := liveIngestStartHeight(staticHeightReader{height: 0}, archive.InspectSummary{LastHeight: 42})
	if start != 43 {
		t.Fatalf("start height %d, want 43", start)
	}
	if start := liveIngestStartHeight(staticHeightReader{height: 40}, archive.InspectSummary{LastHeight: 42}); start != 0 {
		t.Fatalf("non-empty hot store start height %d, want default 0", start)
	}
	if start := liveIngestStartHeight(staticHeightReader{height: 0}, archive.InspectSummary{}); start != 0 {
		t.Fatalf("empty archive start height %d, want default 0", start)
	}
}

func TestCLIEndToEnd(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	createCLIBlockStoreFixture(t, dbDir, 4)
	objectRoot := filepath.Join(t.TempDir(), "objects")
	storeURL := "file://" + objectRoot

	migrateOut := execute(t, migrateCommandName,
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", storeURL,
		"--chain-id", cliTestChainID,
		"--start-height", "1",
		"--end-height", "4",
		"--segment-blocks", "2",
	)
	if !strings.Contains(migrateOut, `"segments": 2`) {
		t.Fatalf("unexpected migrate output:\n%s", migrateOut)
	}
	verifyOut := execute(t, verifyCommandName, "--store", storeURL, "--chain-id", cliTestChainID)
	if !strings.Contains(verifyOut, `"blocks_checked": 4`) {
		t.Fatalf("unexpected verify output:\n%s", verifyOut)
	}
	inspectOut := execute(t, "inspect", "--store", storeURL, "--chain-id", cliTestChainID)
	if !strings.Contains(inspectOut, `"segments": 2`) {
		t.Fatalf("unexpected inspect output:\n%s", inspectOut)
	}
	cacheDir := filepath.Join(t.TempDir(), "cache")
	hydrateOut := execute(t, hydrateCommandName,
		"--store", storeURL,
		"--chain-id", cliTestChainID,
		"--cache-dir", cacheDir,
		"--start-height", "2",
		"--end-height", "3",
	)
	if !strings.Contains(hydrateOut, `"blocks_written": 2`) {
		t.Fatalf("unexpected hydrate output:\n%s", hydrateOut)
	}
	liveStoreURL := "file://" + filepath.Join(t.TempDir(), "live-objects")
	archiveReadyOut := execute(t, archiveReadyCommandName,
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", liveStoreURL,
		"--chain-id", cliTestChainID,
		"--ready-height", "3",
		"--segment-blocks", "2",
	)
	if !strings.Contains(archiveReadyOut, `"blocks_archived": 3`) || !strings.Contains(archiveReadyOut, `"last_height": 3`) {
		t.Fatalf("unexpected archive-ready output:\n%s", archiveReadyOut)
	}
	pruneOut := execute(t, pruneHotCommandName,
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", liveStoreURL,
		"--chain-id", cliTestChainID,
		"--retain-blocks", "1",
		"--evidence-max-age-blocks", "1",
		"--evidence-max-age-duration", "1ns",
	)
	if !strings.Contains(pruneOut, `"pruned": 3`) || !strings.Contains(pruneOut, `"base_after": 4`) {
		t.Fatalf("unexpected prune-hot output:\n%s", pruneOut)
	}
	serveOut := execute(t, "serve", "--store", storeURL, "--chain-id", cliTestChainID)
	if !strings.Contains(serveOut, `"status": "dry-run"`) || !strings.Contains(serveOut, `"blocksync_peer": true`) {
		t.Fatalf("unexpected serve output:\n%s", serveOut)
	}
}

func TestCLIWriterCommandsHonorManifestKeyOverride(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	createCLIBlockStoreFixture(t, dbDir, 3)
	objectRoot := filepath.Join(t.TempDir(), "objects")
	storeURL := "file://" + objectRoot
	migrationKey := "custom/migration.json"
	migrateOut := execute(t, migrateCommandName,
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", storeURL,
		"--chain-id", cliTestChainID,
		"--start-height", "1",
		"--end-height", "2",
		"--manifest-key", migrationKey,
	)
	if !strings.Contains(migrateOut, `"manifest_key": "custom/migration.json"`) {
		t.Fatalf("unexpected migrate output:\n%s", migrateOut)
	}
	if _, err := os.Stat(filepath.Join(objectRoot, filepath.FromSlash(migrationKey))); err != nil {
		t.Fatalf("custom migration manifest missing: %v", err)
	}
	liveKey := "custom/live.json"
	archiveReadyOut := execute(t, archiveReadyCommandName,
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", storeURL,
		"--chain-id", cliTestChainID,
		"--ready-height", "3",
		"--manifest-key", liveKey,
	)
	if !strings.Contains(archiveReadyOut, `"manifest_key": "custom/live.json"`) {
		t.Fatalf("unexpected archive-ready output:\n%s", archiveReadyOut)
	}
	if _, err := os.Stat(filepath.Join(objectRoot, filepath.FromSlash(liveKey))); err != nil {
		t.Fatalf("custom live manifest missing: %v", err)
	}
}

func TestServeDryRunDoesNotRequireExistingManifest(t *testing.T) {
	storeURL := "file://" + filepath.Join(t.TempDir(), "objects")
	out := execute(t, "serve", "--store", storeURL, "--chain-id", cliTestChainID)
	if !strings.Contains(out, `"archive_range": ""`) || !strings.Contains(out, `"blocksync_peer": true`) {
		t.Fatalf("unexpected serve dry-run output:\n%s", out)
	}
}

func TestServeDryRunDoesNotOpenObjectStore(t *testing.T) {
	storeURL := "s3://archive-bucket/root?region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A1&path_style=true"
	out := execute(t, "serve", "--store", storeURL, "--chain-id", cliTestChainID)
	if !strings.Contains(out, `"status": "dry-run"`) || !strings.Contains(out, `"archive_range": ""`) {
		t.Fatalf("unexpected serve dry-run output:\n%s", out)
	}
}

func TestServeDryRunValidatesObjectStoreURL(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store string
		want  string
	}{
		{
			name:  "unsupported",
			store: "gs://archive-bucket/root",
			want:  "unsupported object store URL",
		},
		{
			name:  "bad-s3-query",
			store: "s3://archive-bucket/root?path_style=maybe",
			want:  "invalid path_style value",
		},
		{
			name:  "empty-file-root",
			store: "file://",
			want:  "file object store URL requires a root path",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := executeErr(t, "serve", "--store", tc.store, "--chain-id", cliTestChainID)
			if !strings.Contains(out, tc.want) {
				t.Fatalf("error output missing %q:\n%s", tc.want, out)
			}
		})
	}
}

func TestReadOnlyArchiveCommandsDoNotCreateMissingLocalObjectStoreRoot(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{
			name: "verify",
			args: []string{verifyCommandName, "--chain-id", cliTestChainID},
		},
		{
			name: "inspect",
			args: []string{inspectCommandName, "--chain-id", cliTestChainID},
		},
		{
			name: "hydrate",
			args: []string{hydrateCommandName, "--chain-id", cliTestChainID, "--cache-dir", filepath.Join(t.TempDir(), "cache")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objectRoot := filepath.Join(t.TempDir(), "missing-objects")
			args := append([]string{tc.name, "--store", "file://" + objectRoot}, tc.args[1:]...)
			out := executeErr(t, args...)
			if !strings.Contains(out, archive.ErrObjectNotFound.Error()) {
				t.Fatalf("error output missing object-not-found error:\n%s", out)
			}
			if _, err := os.Stat(objectRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("object store root stat err=%v, want not exist", err)
			}
		})
	}
}

func TestCommandsValidateManifestKeyBeforeOpeningObjectStore(t *testing.T) {
	for _, name := range []string{verifyCommandName, inspectCommandName, hydrateCommandName, pruneHotCommandName} {
		t.Run(name, func(t *testing.T) {
			objectRoot := filepath.Join(t.TempDir(), "objects")
			args := []string{name, "--store", "file://" + objectRoot, "--manifest-key", "../escape"}
			if name == hydrateCommandName {
				args = append(args, "--cache-dir", filepath.Join(t.TempDir(), "cache"))
			}
			if name == pruneHotCommandName {
				dbDir := filepath.Join(t.TempDir(), "db")
				createCLIBlockStoreFixture(t, dbDir, 1)
				args = append(args, "--db-dir", dbDir)
			}
			out := executeErr(t, args...)
			if !strings.Contains(out, "invalid object key") {
				t.Fatalf("error output missing manifest-key validation:\n%s", out)
			}
			if _, err := os.Stat(objectRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("object store root stat err=%v, want not exist", err)
			}
		})
	}
}

func TestCommandsValidateArchivePrefixBeforeOpeningObjectStore(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dbDir string
		args  []string
	}{
		{
			name:  migrateCommandName,
			dbDir: t.TempDir(),
		},
		{
			name:  archiveReadyCommandName,
			dbDir: t.TempDir(),
			args:  []string{"--ready-height", "1"},
		},
		{
			name:  serveCommandName,
			dbDir: t.TempDir(),
			args:  []string{"--dry-run=false"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name != serveCommandName {
				createCLIBlockStoreFixture(t, tc.dbDir, 1)
			}
			objectRoot := filepath.Join(t.TempDir(), "objects")
			args := make([]string, 0, 5+len(tc.args)+4)
			args = append(args, tc.name, "--chain-id", cliTestChainID, "--db-dir", tc.dbDir)
			args = append(args, tc.args...)
			args = append(args, "--store", "file://"+objectRoot, "--prefix", "../escape")
			out := executeErr(t, args...)
			if !strings.Contains(out, "invalid object key") {
				t.Fatalf("error output missing archive prefix validation:\n%s", out)
			}
			if _, err := os.Stat(objectRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("object store root stat err=%v, want not exist", err)
			}
		})
	}
}

func TestOfflineCommandsValidateRuntimeConfig(t *testing.T) {
	storeURL := "file://" + filepath.Join(t.TempDir(), "objects")
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "migrate-segment-blocks",
			args: []string{migrateCommandName, "--store", storeURL, "--chain-id", cliTestChainID, "--db-dir", "unused", "--segment-blocks", "0"},
			want: "segment blocks must be positive",
		},
		{
			name: "migrate-segment-blocks-max",
			args: []string{migrateCommandName, "--store", storeURL, "--chain-id", cliTestChainID, "--db-dir", "unused", "--segment-blocks", fmt.Sprint(archive.MaxSegmentBlocks + 1)},
			want: fmt.Sprintf("segment blocks cannot exceed %d", archive.MaxSegmentBlocks),
		},
		{
			name: "verify-sample-every",
			args: []string{verifyCommandName, "--store", storeURL, "--chain-id", cliTestChainID, "--sample-every", "-1"},
			want: "sample every cannot be negative",
		},
		{
			name: "hydrate-cache-limit",
			args: []string{hydrateCommandName, "--store", storeURL, "--chain-id", cliTestChainID, "--cache-dir", t.TempDir(), "--max-cache-bytes", "-1"},
			want: "max cache bytes cannot be negative",
		},
		{
			name: "archive-ready-segment-blocks",
			args: []string{archiveReadyCommandName, "--store", storeURL, "--chain-id", cliTestChainID, "--db-dir", "unused", "--ready-height", "1", "--segment-blocks", "0"},
			want: "segment blocks must be positive",
		},
		{
			name: "archive-ready-segment-blocks-max",
			args: []string{archiveReadyCommandName, "--store", storeURL, "--chain-id", cliTestChainID, "--db-dir", "unused", "--ready-height", "1", "--segment-blocks", fmt.Sprint(archive.MaxSegmentBlocks + 1)},
			want: fmt.Sprintf("segment blocks cannot exceed %d", archive.MaxSegmentBlocks),
		},
		{
			name: "prune-retain-blocks",
			args: []string{pruneHotCommandName, "--store", storeURL, "--chain-id", cliTestChainID, "--db-dir", "unused", "--retain-blocks", "0"},
			want: "retain blocks must be positive",
		},
		{
			name: "prune-evidence-blocks",
			args: []string{pruneHotCommandName, "--store", storeURL, "--chain-id", cliTestChainID, "--db-dir", "unused", "--evidence-max-age-blocks", "-1"},
			want: "evidence max age blocks cannot be negative",
		},
		{
			name: "prune-evidence-duration",
			args: []string{pruneHotCommandName, "--store", storeURL, "--chain-id", cliTestChainID, "--db-dir", "unused", "--evidence-max-age-duration", "-1s"},
			want: "evidence max age duration cannot be negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := executeErr(t, tc.args...)
			if !strings.Contains(out, tc.want) {
				t.Fatalf("error output missing %q:\n%s", tc.want, out)
			}
		})
	}
}

func TestServeDryRunValidatesLiveValidationConfig(t *testing.T) {
	storeURL := "file://" + filepath.Join(t.TempDir(), "objects")
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "bad-mode",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "bad"},
			want: `unsupported validation mode "bad"`,
		},
		{
			name: "storage-only-with-checkpoint",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--checkpoint", "1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			want: "storage-only validation does not accept checkpoint or validator-set options",
		},
		{
			name: "missing-checkpoint",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "checkpoint"},
			want: "checkpoint validation requires at least one checkpoint",
		},
		{
			name: "zero-checkpoint-height",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "checkpoint", "--checkpoint", "0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			want: "has invalid height 0",
		},
		{
			name: "short-checkpoint-hash",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "checkpoint", "--checkpoint", "1:abcd"},
			want: "hash must be 64 hex characters",
		},
		{
			name: "non-hex-checkpoint-hash",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "checkpoint", "--checkpoint", "1:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
			want: "hash is not valid hex",
		},
		{
			name: "checkpoint-with-validator-set",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "checkpoint", "--checkpoint", "1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--validator-set-rpc", "http://127.0.0.1:26657"},
			want: "checkpoint validation does not accept validator-set options",
		},
		{
			name: "malformed-validator-set",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "validator-set", "--validator-set", "bad"},
			want: `validator set "bad" must be in height:path form`,
		},
		{
			name: "missing-validator-set-file",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "validator-set", "--validator-set", "1:" + filepath.Join(t.TempDir(), "missing.pb")},
			want: "read validator set",
		},
		{
			name: "validator-set-with-checkpoint",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "validator-set", "--checkpoint", "1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--validator-set-rpc", "http://127.0.0.1:26657"},
			want: "validator-set validation does not accept checkpoint options",
		},
		{
			name: "missing-validator-source",
			args: []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID, "--validation", "validator-set"},
			want: "validator-set validation requires at least one validator set or validator set RPC",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := executeErr(t, tc.args...)
			if !strings.Contains(out, tc.want) {
				t.Fatalf("error output missing %q:\n%s", tc.want, out)
			}
		})
	}
}

func TestServeDryRunValidatesPEXConfig(t *testing.T) {
	storeURL := "file://" + filepath.Join(t.TempDir(), "objects")
	out := executeErr(t, "serve", "--store", storeURL, "--chain-id", cliTestChainID, "--pex")
	if !strings.Contains(out, "PEX requires at least one seed or an address book file") {
		t.Fatalf("error output missing PEX config error:\n%s", out)
	}
}

func TestServeDryRunValidatesP2PAddressConfig(t *testing.T) {
	storeURL := "file://" + filepath.Join(t.TempDir(), "objects")
	base := []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "listen-address",
			args: []string{"--p2p-listen=tcp://127.0.0.1"},
			want: "p2p listen address",
		},
		{
			name: "persistent-peer-form",
			args: []string{"--persistent-peers=127.0.0.1:26656"},
			want: "must be in id@host:port form",
		},
		{
			name: "persistent-peer-id",
			args: []string{"--persistent-peers=abcd@127.0.0.1:26656"},
			want: "invalid hex length",
		},
		{
			name: "seed-address",
			args: []string{"--pex", "--seeds=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.1"},
			want: "seed",
		},
		{
			name: "private-peer-id",
			args: []string{"--private-peer-ids=not-hex"},
			want: "private peer ID",
		},
		{
			name: "duplicate-persistent-peer-id",
			args: []string{"--persistent-peers=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.1:26656,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.2:26656"},
			want: "duplicate peer ID",
		},
		{
			name: "duplicate-persistent-peer-id-case-insensitive",
			args: []string{"--persistent-peers=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.1:26656,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA@127.0.0.2:26656"},
			want: "duplicate peer ID",
		},
		{
			name: "duplicate-seed-id",
			args: []string{"--pex", "--seeds=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.1:26656,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.2:26656"},
			want: "duplicate peer ID",
		},
		{
			name: "persistent-peer-also-seed",
			args: []string{"--pex", "--persistent-peers=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.1:26656", "--seeds=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.2:26656"},
			want: "configured as both persistent peer and seed",
		},
		{
			name: "duplicate-private-peer-id",
			args: []string{"--private-peer-ids=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			want: "duplicate private peer ID",
		},
		{
			name: "duplicate-private-peer-id-case-insensitive",
			args: []string{"--private-peer-ids=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			want: "duplicate private peer ID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append(append([]string{}, base...), tc.args...)
			out := executeErr(t, args...)
			if !strings.Contains(out, tc.want) {
				t.Fatalf("error output missing %q:\n%s", tc.want, out)
			}
		})
	}
}

func TestServeDryRunValidatesMetricsListenConfig(t *testing.T) {
	storeURL := "file://" + filepath.Join(t.TempDir(), "objects")
	out := executeErr(t, "serve", "--store", storeURL, "--chain-id", cliTestChainID, "--metrics-listen=127.0.0.1")
	if !strings.Contains(out, "metrics listen address") {
		t.Fatalf("error output missing metrics listen config error:\n%s", out)
	}
}

func TestServeDryRunValidatesRuntimeConfig(t *testing.T) {
	storeURL := "file://" + filepath.Join(t.TempDir(), "objects")
	base := []string{"serve", "--store", storeURL, "--chain-id", cliTestChainID}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "request-limit",
			args: []string{"--request-limit=0"},
			want: "request limit must be positive",
		},
		{
			name: "request-limit-max",
			args: []string{"--request-limit=" + fmt.Sprint(blocksyncarchive.MaxRequestLimit+1)},
			want: fmt.Sprintf("request limit cannot exceed %d", blocksyncarchive.MaxRequestLimit),
		},
		{
			name: "cold-workers",
			args: []string{"--cold-workers=-1"},
			want: "cold workers cannot be negative",
		},
		{
			name: "cold-workers-max",
			args: []string{"--cold-workers=" + fmt.Sprint(blocksyncarchive.MaxRequestLimit+1)},
			want: fmt.Sprintf("cold workers cannot exceed %d", blocksyncarchive.MaxRequestLimit),
		},
		{
			name: "cold-manifest-cache-ttl",
			args: []string{"--cold-manifest-cache-ttl=-1s"},
			want: "cold manifest cache TTL cannot be negative",
		},
		{
			name: "request-timeout",
			args: []string{"--request-timeout=0s"},
			want: "request timeout must be positive",
		},
		{
			name: "status-interval",
			args: []string{"--status-request-interval=-1s"},
			want: "status request interval cannot be negative",
		},
		{
			name: "safety-window",
			args: []string{"--safety-window=-1"},
			want: "safety window cannot be negative",
		},
		{
			name: "archive-interval",
			args: []string{"--archive-interval=-1s"},
			want: "archive interval cannot be negative",
		},
		{
			name: "prune-interval",
			args: []string{"--prune-interval=-1s"},
			want: "prune interval cannot be negative",
		},
		{
			name: "retain-blocks",
			args: []string{"--prune-interval=1s", "--retain-blocks=0"},
			want: "retain blocks must be positive when live pruning is enabled",
		},
		{
			name: "evidence-blocks",
			args: []string{"--evidence-max-age-blocks=-1"},
			want: "evidence max age blocks cannot be negative",
		},
		{
			name: "evidence-duration",
			args: []string{"--evidence-max-age-duration=-1s"},
			want: "evidence max age duration cannot be negative",
		},
		{
			name: "segment-blocks",
			args: []string{"--segment-blocks=0"},
			want: "segment blocks must be positive",
		},
		{
			name: "segment-blocks-max",
			args: []string{"--segment-blocks=" + fmt.Sprint(archive.MaxSegmentBlocks+1)},
			want: fmt.Sprintf("segment blocks cannot exceed %d", archive.MaxSegmentBlocks),
		},
		{
			name: "validator-timeout",
			args: []string{"--validator-set-timeout=0s"},
			want: "validator set timeout must be positive",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append(append([]string{}, base...), tc.args...)
			out := executeErr(t, args...)
			if !strings.Contains(out, tc.want) {
				t.Fatalf("error output missing %q:\n%s", tc.want, out)
			}
		})
	}
}

func TestServeLiveRequiresDBDirBeforeOpeningObjectStore(t *testing.T) {
	objectRoot := filepath.Join(t.TempDir(), "objects")
	out := executeErr(t, "serve",
		"--dry-run=false",
		"--store", "file://"+objectRoot,
		"--chain-id", cliTestChainID,
	)
	if !strings.Contains(out, "db-dir is required when dry-run=false") {
		t.Fatalf("error output missing db-dir validation:\n%s", out)
	}
	if _, err := os.Stat(objectRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("object store root stat err=%v, want not exist", err)
	}
}

func TestServeLiveRequiresNodeKeyFileBeforeOpeningObjectStore(t *testing.T) {
	objectRoot := filepath.Join(t.TempDir(), "objects")
	out := executeErr(t, "serve",
		"--dry-run=false",
		"--store", "file://"+objectRoot,
		"--chain-id", cliTestChainID,
		"--db-dir", t.TempDir(),
	)
	if !strings.Contains(out, "node-key-file is required when dry-run=false") {
		t.Fatalf("error output missing node-key-file validation:\n%s", out)
	}
	if _, err := os.Stat(objectRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("object store root stat err=%v, want not exist", err)
	}
}

func TestServeRequiresChainIDWithManifestKeyBeforeOpeningObjectStore(t *testing.T) {
	for _, tc := range []struct {
		name    string
		chainID string
		want    string
	}{
		{
			name: "missing",
			want: "chain-id is required for serve",
		},
		{
			name:    "unsafe",
			chainID: "../escape",
			want:    "unsafe chain ID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objectRoot := filepath.Join(t.TempDir(), "objects")
			args := []string{
				"serve",
				"--store", "file://" + objectRoot,
				"--manifest-key", "custom/manifest.json",
			}
			if tc.chainID != "" {
				args = append(args, "--chain-id", tc.chainID)
			}
			out := executeErr(t, args...)
			if !strings.Contains(out, tc.want) {
				t.Fatalf("error output missing %q:\n%s", tc.want, out)
			}
			if _, err := os.Stat(objectRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("object store root stat err=%v, want not exist", err)
			}
		})
	}
}

func TestCommandsValidateDBBackendBeforeOpeningObjectStore(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{
			name: migrateCommandName,
			args: []string{migrateCommandName, "--chain-id", cliTestChainID, "--db-dir", t.TempDir(), "--db-backend", "unknown"},
		},
		{
			name: archiveReadyCommandName,
			args: []string{archiveReadyCommandName, "--chain-id", cliTestChainID, "--db-dir", t.TempDir(), "--db-backend", "unknown", "--ready-height", "1"},
		},
		{
			name: pruneHotCommandName,
			args: []string{pruneHotCommandName, "--chain-id", cliTestChainID, "--db-dir", t.TempDir(), "--db-backend", "unknown"},
		},
		{
			name: serveCommandName,
			args: []string{serveCommandName, "--dry-run=false", "--chain-id", cliTestChainID, "--db-dir", t.TempDir(), "--node-key-file", filepath.Join(t.TempDir(), "node_key.json"), "--db-backend", "unknown"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objectRoot := filepath.Join(t.TempDir(), "objects")
			args := append(append([]string{}, tc.args...), "--store", "file://"+objectRoot)
			out := executeErr(t, args...)
			if !strings.Contains(out, `unsupported db backend "unknown"`) {
				t.Fatalf("error output missing backend validation:\n%s", out)
			}
			if _, err := os.Stat(objectRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("object store root stat err=%v, want not exist", err)
			}
		})
	}
}

func TestOfflineCommandsRequireExistingBlockStoreBeforeOpeningObjectStore(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{
			name: migrateCommandName,
			args: []string{migrateCommandName, "--chain-id", cliTestChainID, "--db-dir", filepath.Join(t.TempDir(), "missing")},
		},
		{
			name: archiveReadyCommandName,
			args: []string{archiveReadyCommandName, "--chain-id", cliTestChainID, "--db-dir", filepath.Join(t.TempDir(), "missing"), "--ready-height", "1"},
		},
		{
			name: pruneHotCommandName,
			args: []string{pruneHotCommandName, "--chain-id", cliTestChainID, "--db-dir", filepath.Join(t.TempDir(), "missing")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objectRoot := filepath.Join(t.TempDir(), "objects")
			args := append(append([]string{}, tc.args...), "--store", "file://"+objectRoot)
			out := executeErr(t, args...)
			if !strings.Contains(out, "blockstore database") {
				t.Fatalf("error output missing blockstore validation:\n%s", out)
			}
			if _, err := os.Stat(objectRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("object store root stat err=%v, want not exist", err)
			}
		})
	}
}

func TestCommandsValidateCompressionBeforeOpeningObjectStore(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{
			name: migrateCommandName,
			args: []string{migrateCommandName, "--chain-id", cliTestChainID, "--db-dir", t.TempDir(), "--compression", "zstd"},
		},
		{
			name: archiveReadyCommandName,
			args: []string{archiveReadyCommandName, "--chain-id", cliTestChainID, "--db-dir", t.TempDir(), "--ready-height", "1", "--compression", "zstd"},
		},
		{
			name: serveCommandName,
			args: []string{serveCommandName, "--chain-id", cliTestChainID, "--compression", "zstd"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objectRoot := filepath.Join(t.TempDir(), "objects")
			args := append(append([]string{}, tc.args...), "--store", "file://"+objectRoot)
			out := executeErr(t, args...)
			if !strings.Contains(out, `unsupported compression "zstd"`) {
				t.Fatalf("error output missing compression validation:\n%s", out)
			}
			if _, err := os.Stat(objectRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("object store root stat err=%v, want not exist", err)
			}
		})
	}
}

func TestServeConfigPopulatesDryRunAndFlagsOverride(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "serve.json")
	storeURL := "file://" + filepath.Join(dir, "objects")
	config := fmt.Sprintf(`{
  "store": %q,
	  "chain_id": %q,
	  "p2p_listen": "tcp://127.0.0.1:26656",
	  "moniker": "archive-peer-1",
	  "request_limit": 5,
	  "cold_workers": 2,
	  "cold_manifest_cache_ttl": "250ms",
	  "request_timeout": "750ms",
	  "status_request_interval": "250ms",
  "pex": true,
  "addr_book_file": %q,
  "addr_book_strict": false,
  "seeds": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.1:26656"],
  "metrics_listen": "127.0.0.1:0",
  "safety_window": 7,
  "archive_interval": "3s",
  "prune_interval": "0s",
  "retain_blocks": 42,
  "segment_blocks": 3,
  "compression": "none",
  "validation": "validator-set",
  "validator_set_rpc": "http://127.0.0.1:26657",
  "validator_set_timeout": "2s",
  "dry_run": true
}`, storeURL, cliTestChainID, filepath.Join(dir, "addrbook.json"))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	out := execute(t, "serve", "--config", configPath, "--safety-window", "9")
	for _, want := range []string{
		`"status": "dry-run"`,
		`"chain_id": "archive-test-chain"`,
		`"moniker": "archive-peer-1"`,
		`"archive_interval": "3s"`,
		`"prune_interval": "0s"`,
		`"retain_blocks": 42`,
		`"safety_window": 9`,
		`"metrics_listen": "127.0.0.1:0"`,
		`"request_limit": 5`,
		`"cold_workers": 2`,
		`"cold_manifest_cache_ttl": "250ms"`,
		`"request_timeout": "750ms"`,
		`"status_interval": "250ms"`,
		`"pex": true`,
		`"seeds": [`,
		`"validation_trust_model": "rpc-trusted-validator-set-source"`,
		`"validator_set_rpc": "http://127.0.0.1:26657"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("serve config output missing %s:\n%s", want, out)
		}
	}
}

func TestServeConfigRejectsTrailingJSON(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "serve.json")
	storeURL := "file://" + filepath.Join(dir, "objects")
	config := fmt.Sprintf(`{"store": %q, "chain_id": %q} {"dry_run": true}`, storeURL, cliTestChainID)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	out := executeErr(t, "serve", "--config", configPath)
	if !strings.Contains(out, "trailing JSON") {
		t.Fatalf("error output missing trailing JSON validation:\n%s", out)
	}
}

func TestServeConfigRejectsEmptyListEntries(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "serve.json")
	storeURL := "file://" + filepath.Join(dir, "objects")
	config := fmt.Sprintf(`{
  "store": %q,
  "chain_id": %q,
  "persistent_peers": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.1:26656", " "]
}`, storeURL, cliTestChainID)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	out := executeErr(t, "serve", "--config", configPath)
	if !strings.Contains(out, "config persistent_peers[1] must not be empty") {
		t.Fatalf("error output missing empty list entry validation:\n%s", out)
	}
}

func TestServeConfigRejectsCommaInListEntries(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "serve.json")
	storeURL := "file://" + filepath.Join(dir, "objects")
	config := fmt.Sprintf(`{
  "store": %q,
  "chain_id": %q,
  "seeds": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.1:26656,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb@127.0.0.2:26656"]
}`, storeURL, cliTestChainID)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	out := executeErr(t, "serve", "--config", configPath)
	if !strings.Contains(out, "config seeds[0] must not contain commas") {
		t.Fatalf("error output missing comma list entry validation:\n%s", out)
	}
}

func TestServeExampleConfigStaysValid(t *testing.T) {
	configPath := filepath.Join("..", "..", "docs", "serve.config.example.json")
	out := execute(t, "serve", "--config", configPath)
	for _, want := range []string{
		`"status": "dry-run"`,
		`"chain_id": "example-chain-1"`,
		`"blocksync_peer": true`,
		`"custom_sync": true`,
		`"consensus_node": false`,
		`"validation": "validator-set"`,
		`"validation_trust_model": "rpc-trusted-validator-set-source"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("serve example config output missing %s:\n%s", want, out)
		}
	}
}

func TestValidationTrustModel(t *testing.T) {
	for _, tc := range []struct {
		name            string
		mode            string
		checkpoints     []string
		validatorSets   []string
		validatorSetRPC string
		want            string
	}{
		{
			name: "storage-only",
			mode: string(blocksyncarchive.ValidationStorageOnly),
			want: "storage-only",
		},
		{
			name:        "checkpoint",
			mode:        string(blocksyncarchive.ValidationCheckpoint),
			checkpoints: []string{"1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			want:        "trusted-checkpoints",
		},
		{
			name:          "validator-set-files",
			mode:          string(blocksyncarchive.ValidationValidatorSet),
			validatorSets: []string{"1:/tmp/validators.pb"},
			want:          "trusted-validator-set-files",
		},
		{
			name:            "validator-set-rpc",
			mode:            string(blocksyncarchive.ValidationValidatorSet),
			validatorSetRPC: "http://127.0.0.1:26657",
			want:            "rpc-trusted-validator-set-source",
		},
		{
			name:            "validator-set-files-rpc",
			mode:            string(blocksyncarchive.ValidationValidatorSet),
			validatorSets:   []string{"1:/tmp/validators.pb"},
			validatorSetRPC: "http://127.0.0.1:26657",
			want:            "trusted-validator-set-files-with-rpc-backfill",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := validationTrustModel(tc.mode, tc.checkpoints, tc.validatorSets, tc.validatorSetRPC)
			if got != tc.want {
				t.Fatalf("validationTrustModel() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLiveIngestorMaintenanceLoopRetriesArchiveErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dbDir := filepath.Join(t.TempDir(), "db")
	createCLIBlockStoreFixture(t, dbDir, 5)
	reader, err := archive.OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ingestor, err := blocksyncarchive.NewHotIngestor(reader, blocksyncarchive.IngestOptions{ChainID: cliTestChainID})
	if err != nil {
		t.Fatal(err)
	}
	localStore, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	objectStore := &transientPutStore{ObjectStore: localStore}
	objectStore.failures.Store(1)
	stats := newLiveMaintenanceStats()
	errCh := startLiveIngestorMaintenanceLoop(ctx, ingestor, objectStore, archive.LiveArchiveOptions{
		ChainID:       cliTestChainID,
		Prefix:        "archive",
		SegmentBlocks: 2,
		Compression:   archive.CompressionGzip,
	}, 1, 10*time.Millisecond, archive.PruneHotOptions{}, 0, stats)

	deadline := time.After(500 * time.Millisecond)
	for {
		verify, verifyErr := archive.Verify(context.Background(), localStore, archive.VerifyOptions{
			ManifestKey: archive.ManifestKey("archive", cliTestChainID, archive.DefaultManifest),
		})
		if verifyErr == nil && verify.BlocksChecked == 4 {
			break
		}
		select {
		case loopErr := <-errCh:
			t.Fatalf("maintenance loop exited after transient archive error: %v", loopErr)
		case <-deadline:
			t.Fatalf("archive was not retried successfully; last verify error: %v", verifyErr)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if got := stats.snapshot()[liveMetricArchiveErrors]; got != int64(1) {
		t.Fatalf("archive_errors = %v, want 1", got)
	}
}

func TestLiveIngestorMaintenanceLoopDoesNotExitOrPruneOnCorruptArchive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dbDir := filepath.Join(t.TempDir(), "db")
	createCLIBlockStoreFixture(t, dbDir, 4)
	reader, err := archive.OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ingestor, err := blocksyncarchive.NewHotIngestor(reader, blocksyncarchive.IngestOptions{ChainID: cliTestChainID})
	if err != nil {
		t.Fatal(err)
	}
	objectStore, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	archived, err := archive.ArchiveReady(ctx, reader, objectStore, archive.LiveArchiveOptions{
		ChainID:       cliTestChainID,
		Prefix:        "archive",
		ReadyHeight:   3,
		SegmentBlocks: 3,
		Compression:   archive.CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	segmentKey := archived.Manifest.Segments[0].Key
	data, err := objectStore.Get(ctx, segmentKey)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := objectStore.Put(ctx, segmentKey, data); err != nil {
		t.Fatal(err)
	}

	stats := newLiveMaintenanceStats()
	errCh := startLiveIngestorMaintenanceLoop(ctx, ingestor, objectStore, archive.LiveArchiveOptions{
		ChainID:       cliTestChainID,
		Prefix:        "archive",
		SegmentBlocks: 3,
		Compression:   archive.CompressionGzip,
	}, 1, 10*time.Millisecond, archive.PruneHotOptions{
		ManifestKey:  archived.ManifestKey,
		RetainBlocks: 1,
	}, time.Nanosecond, stats)

	deadline := time.After(500 * time.Millisecond)
	for {
		pruneErrors, ok := stats.snapshot()[liveMetricPruneErrors].(int64)
		if ok && pruneErrors > 0 {
			break
		}
		select {
		case loopErr := <-errCh:
			t.Fatalf("maintenance loop exited after corrupt archive: %v", loopErr)
		case <-deadline:
			t.Fatal("expected maintenance loop to record prune error")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if reader.Base() != 1 {
		t.Fatalf("base changed after failed live prune: %d", reader.Base())
	}
}

func TestLiveIngestorMaintenanceLoopArchivesThenPrunes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dbDir := filepath.Join(t.TempDir(), "db")
	createCLIBlockStoreFixture(t, dbDir, 5)
	reader, err := archive.OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ingestor, err := blocksyncarchive.NewHotIngestor(reader, blocksyncarchive.IngestOptions{ChainID: cliTestChainID})
	if err != nil {
		t.Fatal(err)
	}
	objectStore, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := archive.ManifestKey("archive", cliTestChainID, archive.DefaultManifest)
	errCh := startLiveIngestorMaintenanceLoop(ctx, ingestor, objectStore, archive.LiveArchiveOptions{
		ChainID:       cliTestChainID,
		Prefix:        "archive",
		SegmentBlocks: 2,
		Compression:   archive.CompressionGzip,
	}, 1, time.Hour, archive.PruneHotOptions{
		ManifestKey:            manifestKey,
		RetainBlocks:           1,
		EvidenceMaxAgeBlocks:   1,
		EvidenceMaxAgeDuration: time.Nanosecond,
	}, time.Hour, newLiveMaintenanceStats())
	deadline := time.After(500 * time.Millisecond)
	for reader.Base() != 5 {
		select {
		case loopErr := <-errCh:
			t.Fatalf("maintenance loop failed: %v", loopErr)
		case <-deadline:
			t.Fatalf("base after maintenance = %d, want 5", reader.Base())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	verify, err := archive.Verify(context.Background(), objectStore, archive.VerifyOptions{ManifestKey: manifestKey})
	if err != nil {
		t.Fatal(err)
	}
	if verify.BlocksChecked != 4 {
		t.Fatalf("verified %d archived blocks, want 4", verify.BlocksChecked)
	}
}

func TestLiveIngestorMaintenanceLoopDoesNotBlockIngestDuringArchiveUpload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dbDir := filepath.Join(t.TempDir(), "db")
	createCLIBlockStoreFixture(t, dbDir, 5)
	reader, err := archive.OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ingestor, err := blocksyncarchive.NewHotIngestor(reader, blocksyncarchive.IngestOptions{ChainID: cliTestChainID})
	if err != nil {
		t.Fatal(err)
	}
	localStore, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	objectStore := &blockingSegmentStore{
		ObjectStore: localStore,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	released := false
	defer func() {
		if !released {
			close(objectStore.release)
		}
	}()
	manifestKey := archive.ManifestKey("archive", cliTestChainID, archive.DefaultManifest)
	errCh := startLiveIngestorMaintenanceLoop(ctx, ingestor, objectStore, archive.LiveArchiveOptions{
		ChainID:       cliTestChainID,
		Prefix:        "archive",
		SegmentBlocks: 2,
		Compression:   archive.CompressionGzip,
	}, 1, time.Hour, archive.PruneHotOptions{}, 0, newLiveMaintenanceStats())

	select {
	case <-objectStore.started:
	case loopErr := <-errCh:
		t.Fatalf("maintenance loop failed before segment upload blocked: %v", loopErr)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for segment upload to block")
	}

	submitted := make(chan error, 1)
	go func() {
		_, submitErr := ingestor.Submit(makeCLITestBlock(6))
		submitted <- submitErr
	}()
	select {
	case submitErr := <-submitted:
		if submitErr != nil {
			t.Fatalf("submit while archive upload blocked: %v", submitErr)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ingest submit blocked behind object-store archive upload")
	}
	close(objectStore.release)
	released = true

	deadline := time.After(500 * time.Millisecond)
	for {
		verify, verifyErr := archive.Verify(context.Background(), localStore, archive.VerifyOptions{ManifestKey: manifestKey})
		if verifyErr == nil && verify.BlocksChecked == 4 {
			break
		}
		select {
		case loopErr := <-errCh:
			t.Fatalf("maintenance loop failed after segment upload released: %v", loopErr)
		case <-deadline:
			t.Fatalf("archive did not finish after release; last verify error: %v", verifyErr)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestMetricsServerReportsMaintenanceStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats := newLiveMaintenanceStats()
	stats.recordArchive(3, nil)
	stats.recordPrune(2, nil)
	addr, err := startMetricsServer(ctx, "127.0.0.1:0", stats, func() map[string]any {
		return map[string]any{
			liveMetricP2PPeers:             2,
			liveMetricPeerBestHeight:       int64(12),
			"blocksync_inflight_requests":  4,
			"blocksync_request_timeouts":   int64(1),
			"blocksync_hot_responses":      int64(5),
			liveMetricColdResponses:        int64(3),
			"blocksync_no_block_responses": int64(2),
			liveMetricColdErrors:           int64(1),
			liveMetricColdQueue:            1,
			liveMetricColdQueueFull:        int64(2),
			liveMetricBuffered:             0,
			liveMetricColdActive:           int64(1),
			"hot_base":                     int64(3),
			"hot_height":                   int64(10),
			liveMetricNextHeight:           int64(11),
			"pending_height":               int64(10),
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{Timeout: time.Second}
	health, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", health.StatusCode)
	}
	ready, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = ready.Body.Close()
	if ready.StatusCode != http.StatusOK {
		t.Fatalf("ready status = %d", ready.StatusCode)
	}
	resp, err := client.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	archiveRuns, _ := numericMetric(got["archive_runs"])
	blocksArchived, _ := numericMetric(got["blocks_archived"])
	pruneRuns, _ := numericMetric(got["prune_runs"])
	blocksPruned, _ := numericMetric(got["blocks_pruned"])
	if archiveRuns != 1 || blocksArchived != 3 || pruneRuns != 1 || blocksPruned != 2 {
		t.Fatalf("unexpected metrics: %+v", got)
	}
	lastArchiveAttempt, _ := numericMetric(got["last_archive_attempt_unix_nano"])
	lastArchiveSuccess, _ := numericMetric(got["last_archive_success_unix_nano"])
	lastPruneAttempt, _ := numericMetric(got["last_prune_attempt_unix_nano"])
	lastPruneSuccess, _ := numericMetric(got["last_prune_success_unix_nano"])
	if lastArchiveAttempt <= 0 || lastArchiveSuccess <= 0 || lastPruneAttempt <= 0 || lastPruneSuccess <= 0 {
		t.Fatalf("expected maintenance attempt and success timestamps: %+v", got)
	}
	p2pPeers, _ := numericMetric(got[liveMetricP2PPeers])
	peerBestHeight, _ := numericMetric(got[liveMetricPeerBestHeight])
	hotHeight, _ := numericMetric(got["hot_height"])
	nextHeight, _ := numericMetric(got[liveMetricNextHeight])
	if p2pPeers != 2 || peerBestHeight != 12 || hotHeight != 10 || nextHeight != 11 {
		t.Fatalf("unexpected live status metrics: %+v", got)
	}
	inflightRequests, _ := numericMetric(got["blocksync_inflight_requests"])
	bufferedResponses, _ := numericMetric(got[liveMetricBuffered])
	requestTimeouts, _ := numericMetric(got["blocksync_request_timeouts"])
	if inflightRequests != 4 || bufferedResponses != 0 || requestTimeouts != 1 {
		t.Fatalf("unexpected blocksync request metrics: %+v", got)
	}
	hotResponses, _ := numericMetric(got["blocksync_hot_responses"])
	coldResponses, _ := numericMetric(got[liveMetricColdResponses])
	noBlockResponses, _ := numericMetric(got["blocksync_no_block_responses"])
	coldErrors, _ := numericMetric(got[liveMetricColdErrors])
	coldQueue, _ := numericMetric(got[liveMetricColdQueue])
	coldQueueFull, _ := numericMetric(got[liveMetricColdQueueFull])
	coldActive, _ := numericMetric(got[liveMetricColdActive])
	if hotResponses != 5 || coldResponses != 3 || noBlockResponses != 2 || coldErrors != 1 || coldQueue != 1 || coldQueueFull != 2 || coldActive != 1 {
		t.Fatalf("unexpected blocksync serving metrics: %+v", got)
	}
	if got[liveMetricArchiveError] != "" || got[liveMetricPruneError] != "" {
		t.Fatalf("unexpected last error metrics: %+v", got)
	}
}

func TestMetricsServerReportsAndClearsLastErrors(t *testing.T) {
	stats := newLiveMaintenanceStats()
	stats.recordArchive(0, errors.New("archive unavailable"))
	stats.recordPrune(0, errors.New("manifest corrupt"))
	failed := stats.snapshot()
	if failed[liveMetricArchiveError] != "archive unavailable" || failed[liveMetricPruneError] != "manifest corrupt" {
		t.Fatalf("unexpected error metrics: %+v", failed)
	}
	archiveAttempt, _ := numericMetric(failed["last_archive_attempt_unix_nano"])
	archiveSuccess, _ := numericMetric(failed["last_archive_success_unix_nano"])
	pruneAttempt, _ := numericMetric(failed["last_prune_attempt_unix_nano"])
	pruneSuccess, _ := numericMetric(failed["last_prune_success_unix_nano"])
	if archiveAttempt <= 0 || pruneAttempt <= 0 || archiveSuccess != 0 || pruneSuccess != 0 {
		t.Fatalf("unexpected failed attempt timestamps: %+v", failed)
	}
	stats.recordArchive(1, nil)
	stats.recordPrune(1, nil)
	recovered := stats.snapshot()
	if recovered[liveMetricArchiveError] != "" || recovered[liveMetricPruneError] != "" {
		t.Fatalf("expected cleared error metrics after success: %+v", recovered)
	}
	archiveSuccess, _ = numericMetric(recovered["last_archive_success_unix_nano"])
	pruneSuccess, _ = numericMetric(recovered["last_prune_success_unix_nano"])
	if archiveSuccess <= 0 || pruneSuccess <= 0 {
		t.Fatalf("expected success timestamps after recovery: %+v", recovered)
	}
}

func TestMetricsServerReadinessRequiresLiveP2PStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, err := startMetricsServer(ctx, "127.0.0.1:0", newLiveMaintenanceStats(), func() map[string]any {
		return map[string]any{
			liveMetricP2PPeers:       0,
			liveMetricPeerBestHeight: int64(0),
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{Timeout: time.Second}
	health, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", health.StatusCode)
	}
	ready, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = ready.Body.Close()
	if ready.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d, want %d", ready.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestMetricsServerReadinessFailsClosedWithoutP2PStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, err := startMetricsServer(ctx, "127.0.0.1:0", newLiveMaintenanceStats())
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{Timeout: time.Second}
	ready, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = ready.Body.Close()
	if ready.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d, want %d", ready.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestSoakCommandPassesAdvancingReadyMetrics(t *testing.T) {
	var height atomic.Int64
	height.Store(10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("ok\n")); err != nil {
				t.Errorf("write ready response: %v", err)
			}
		case "/metrics":
			currentHeight := height.Add(1)
			if err := json.NewEncoder(w).Encode(map[string]any{
				liveMetricP2PPeers:       2,
				liveMetricPeerBestHeight: currentHeight,
				liveMetricNextHeight:     currentHeight + 1,
				liveMetricArchiveErrors:  0,
				liveMetricPruneErrors:    0,
				liveMetricColdResponses:  currentHeight - 10,
				liveMetricColdErrors:     0,
				liveMetricColdQueue:      0,
				liveMetricColdQueueFull:  0,
				liveMetricBuffered:       0,
				liveMetricArchiveError:   "",
				liveMetricPruneError:     "",
			}); err != nil {
				t.Errorf("encode metrics: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	out := execute(t, soakCommandName, "--metrics-url", server.URL, "--duration", "25ms", "--interval", "5ms")
	if !strings.Contains(out, `"status": "passed"`) || !strings.Contains(out, `"ready_samples"`) {
		t.Fatalf("unexpected soak output:\n%s", out)
	}
}

func TestSoakCheckRejectsInvalidThresholds(t *testing.T) {
	base := soakOptions{
		MetricsURL: "http://127.0.0.1:26660",
		Duration:   time.Second,
		Interval:   time.Second,
	}
	for _, tc := range []struct {
		name string
		mut  func(*soakOptions)
		want string
	}{
		{
			name: "min-peers",
			mut:  func(opts *soakOptions) { opts.MinPeers = -1 },
			want: "min peers cannot be negative",
		},
		{
			name: "max-archive-errors",
			mut:  func(opts *soakOptions) { opts.MaxArchiveErrorsDelta = -1 },
			want: "max archive errors delta cannot be negative",
		},
		{
			name: "max-prune-errors",
			mut:  func(opts *soakOptions) { opts.MaxPruneErrorsDelta = -1 },
			want: "max prune errors delta cannot be negative",
		},
		{
			name: "min-cold-responses",
			mut:  func(opts *soakOptions) { opts.MinColdResponsesDelta = -1 },
			want: "min cold responses delta cannot be negative",
		},
		{
			name: "max-cold-errors",
			mut:  func(opts *soakOptions) { opts.MaxColdErrorsDelta = -1 },
			want: "max cold errors delta cannot be negative",
		},
		{
			name: "max-cold-queue-full",
			mut:  func(opts *soakOptions) { opts.MaxColdQueueFullDelta = -1 },
			want: "max cold queue full delta cannot be negative",
		},
		{
			name: "max-cold-queue",
			mut:  func(opts *soakOptions) { opts.MaxColdQueue = -2 },
			want: "max cold queue must be -1 or greater",
		},
		{
			name: "max-buffered-responses",
			mut:  func(opts *soakOptions) { opts.MaxBufferedResponses = -2 },
			want: "max buffered responses must be -1 or greater",
		},
		{
			name: "max-head-lag",
			mut:  func(opts *soakOptions) { opts.MaxHeadLag = -2 },
			want: "max head lag must be -1 or greater",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := base
			tc.mut(&opts)
			_, err := runSoakCheck(context.Background(), opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("soak err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestSoakCheckFailsWithoutHeadAdvance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("ok\n")); err != nil {
				t.Errorf("write ready response: %v", err)
			}
		case "/metrics":
			if err := json.NewEncoder(w).Encode(map[string]any{
				liveMetricP2PPeers:       1,
				liveMetricPeerBestHeight: 10,
				liveMetricArchiveErrors:  0,
				liveMetricPruneErrors:    0,
				liveMetricColdResponses:  0,
				liveMetricColdErrors:     0,
				liveMetricColdQueue:      0,
				liveMetricColdQueueFull:  0,
				liveMetricBuffered:       0,
				liveMetricArchiveError:   "",
				liveMetricPruneError:     "",
			}); err != nil {
				t.Errorf("encode metrics: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	_, err := runSoakCheck(context.Background(), soakOptions{
		MetricsURL:         server.URL,
		Duration:           20 * time.Millisecond,
		Interval:           5 * time.Millisecond,
		RequireHeadAdvance: true,
	})
	if err == nil || !strings.Contains(err.Error(), "peer best height did not advance") {
		t.Fatalf("soak err=%v, want head advance failure", err)
	}
}

func TestSoakCheckFailsOnColdServingErrors(t *testing.T) {
	var height atomic.Int64
	height.Store(10)
	var samples atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			sample := samples.Add(1)
			if err := json.NewEncoder(w).Encode(map[string]any{
				liveMetricP2PPeers:       1,
				liveMetricPeerBestHeight: height.Add(1),
				liveMetricArchiveErrors:  0,
				liveMetricPruneErrors:    0,
				liveMetricColdResponses:  0,
				liveMetricColdErrors:     sample - 1,
				liveMetricColdQueue:      0,
				liveMetricColdQueueFull:  0,
				liveMetricBuffered:       0,
				liveMetricArchiveError:   "",
				liveMetricPruneError:     "",
			}); err != nil {
				t.Errorf("encode metrics: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	_, err := runSoakCheck(context.Background(), soakOptions{
		MetricsURL:         server.URL,
		Duration:           20 * time.Millisecond,
		Interval:           5 * time.Millisecond,
		RequireHeadAdvance: true,
	})
	if err == nil || !strings.Contains(err.Error(), "cold serving errors increased") {
		t.Fatalf("soak err=%v, want cold serving error failure", err)
	}
}

func TestSoakCheckFailsWithoutRequiredColdResponses(t *testing.T) {
	var height atomic.Int64
	height.Store(10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			if err := json.NewEncoder(w).Encode(map[string]any{
				liveMetricP2PPeers:       1,
				liveMetricPeerBestHeight: height.Add(1),
				liveMetricArchiveErrors:  0,
				liveMetricPruneErrors:    0,
				liveMetricColdResponses:  0,
				liveMetricColdErrors:     0,
				liveMetricColdQueue:      0,
				liveMetricColdQueueFull:  0,
				liveMetricBuffered:       0,
				liveMetricArchiveError:   "",
				liveMetricPruneError:     "",
			}); err != nil {
				t.Errorf("encode metrics: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	_, err := runSoakCheck(context.Background(), soakOptions{
		MetricsURL:            server.URL,
		Duration:              20 * time.Millisecond,
		Interval:              5 * time.Millisecond,
		RequireHeadAdvance:    true,
		MinColdResponsesDelta: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "cold responses increased") {
		t.Fatalf("soak err=%v, want cold responses failure", err)
	}
}

func TestSoakCheckFailsOnColdQueueFullEvents(t *testing.T) {
	var height atomic.Int64
	height.Store(10)
	var samples atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			sample := samples.Add(1)
			if err := json.NewEncoder(w).Encode(map[string]any{
				liveMetricP2PPeers:       1,
				liveMetricPeerBestHeight: height.Add(1),
				liveMetricArchiveErrors:  0,
				liveMetricPruneErrors:    0,
				liveMetricColdResponses:  0,
				liveMetricColdErrors:     0,
				liveMetricColdQueue:      0,
				liveMetricColdQueueFull:  sample - 1,
				liveMetricBuffered:       0,
				liveMetricArchiveError:   "",
				liveMetricPruneError:     "",
			}); err != nil {
				t.Errorf("encode metrics: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	_, err := runSoakCheck(context.Background(), soakOptions{
		MetricsURL:         server.URL,
		Duration:           20 * time.Millisecond,
		Interval:           5 * time.Millisecond,
		RequireHeadAdvance: true,
	})
	if err == nil || !strings.Contains(err.Error(), "cold queue full events increased") {
		t.Fatalf("soak err=%v, want cold queue full failure", err)
	}
}

func TestSoakCheckFailsOnColdQueueThreshold(t *testing.T) {
	var height atomic.Int64
	height.Store(10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			if err := json.NewEncoder(w).Encode(map[string]any{
				liveMetricP2PPeers:       1,
				liveMetricPeerBestHeight: height.Add(1),
				liveMetricArchiveErrors:  0,
				liveMetricPruneErrors:    0,
				liveMetricColdResponses:  0,
				liveMetricColdErrors:     0,
				liveMetricColdQueue:      3,
				liveMetricColdQueueFull:  0,
				liveMetricBuffered:       0,
				liveMetricArchiveError:   "",
				liveMetricPruneError:     "",
			}); err != nil {
				t.Errorf("encode metrics: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	_, err := runSoakCheck(context.Background(), soakOptions{
		MetricsURL:         server.URL,
		Duration:           20 * time.Millisecond,
		Interval:           5 * time.Millisecond,
		RequireHeadAdvance: true,
		MaxColdQueue:       2,
	})
	if err == nil || !strings.Contains(err.Error(), "max cold queue") {
		t.Fatalf("soak err=%v, want cold queue failure", err)
	}
}

func TestSoakCheckFailsOnBufferedResponseThreshold(t *testing.T) {
	var height atomic.Int64
	height.Store(10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			if err := json.NewEncoder(w).Encode(map[string]any{
				liveMetricP2PPeers:       1,
				liveMetricPeerBestHeight: height.Add(1),
				liveMetricArchiveErrors:  0,
				liveMetricPruneErrors:    0,
				liveMetricColdResponses:  0,
				liveMetricColdErrors:     0,
				liveMetricColdQueue:      0,
				liveMetricColdQueueFull:  0,
				liveMetricBuffered:       3,
				liveMetricArchiveError:   "",
				liveMetricPruneError:     "",
			}); err != nil {
				t.Errorf("encode metrics: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	_, err := runSoakCheck(context.Background(), soakOptions{
		MetricsURL:           server.URL,
		Duration:             20 * time.Millisecond,
		Interval:             5 * time.Millisecond,
		RequireHeadAdvance:   true,
		MaxBufferedResponses: 2,
	})
	if err == nil || !strings.Contains(err.Error(), "max buffered responses") {
		t.Fatalf("soak err=%v, want buffered response failure", err)
	}
}

func TestSoakCheckFailsOnHeadLagThreshold(t *testing.T) {
	var height atomic.Int64
	height.Store(10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			currentHeight := height.Add(1)
			if err := json.NewEncoder(w).Encode(map[string]any{
				liveMetricP2PPeers:       1,
				liveMetricPeerBestHeight: currentHeight,
				liveMetricNextHeight:     currentHeight - 4,
				liveMetricArchiveErrors:  0,
				liveMetricPruneErrors:    0,
				liveMetricColdResponses:  0,
				liveMetricColdErrors:     0,
				liveMetricColdQueue:      0,
				liveMetricColdQueueFull:  0,
				liveMetricBuffered:       0,
				liveMetricArchiveError:   "",
				liveMetricPruneError:     "",
			}); err != nil {
				t.Errorf("encode metrics: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	result, err := runSoakCheck(context.Background(), soakOptions{
		MetricsURL:         server.URL,
		Duration:           20 * time.Millisecond,
		Interval:           5 * time.Millisecond,
		RequireHeadAdvance: true,
		MaxHeadLag:         3,
	})
	if err == nil || !strings.Contains(err.Error(), "max head lag") {
		t.Fatalf("soak err=%v, want max head lag failure", err)
	}
	if result.MaxHeadLag != 5 || result.LastNextHeight == 0 {
		t.Fatalf("unexpected head lag result: %+v", result)
	}
}

func TestSoakCheckHeadLagThresholdRequiresNextHeightMetric(t *testing.T) {
	var height atomic.Int64
	height.Store(10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			if err := json.NewEncoder(w).Encode(map[string]any{
				liveMetricP2PPeers:       1,
				liveMetricPeerBestHeight: height.Add(1),
				liveMetricArchiveErrors:  0,
				liveMetricPruneErrors:    0,
				liveMetricColdResponses:  0,
				liveMetricColdErrors:     0,
				liveMetricColdQueue:      0,
				liveMetricColdQueueFull:  0,
				liveMetricBuffered:       0,
				liveMetricArchiveError:   "",
				liveMetricPruneError:     "",
			}); err != nil {
				t.Errorf("encode metrics: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	_, err := runSoakCheck(context.Background(), soakOptions{
		MetricsURL:         server.URL,
		Duration:           20 * time.Millisecond,
		Interval:           5 * time.Millisecond,
		RequireHeadAdvance: true,
		MaxHeadLag:         3,
	})
	if err == nil || !strings.Contains(err.Error(), "max-head-lag requires numeric metric") {
		t.Fatalf("soak err=%v, want missing next height failure", err)
	}
}

func TestSoakCheckReportsFailedStatusOnSampleError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	result, err := runSoakCheck(context.Background(), soakOptions{
		MetricsURL: server.URL,
		Duration:   time.Millisecond,
		Interval:   time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "metrics status") {
		t.Fatalf("soak err=%v, want metrics status failure", err)
	}
	if result.Status != soakStatusFailed || result.MetricsURL != server.URL {
		t.Fatalf("unexpected failed result: %+v", result)
	}
}

func TestParseCheckpoints(t *testing.T) {
	got, err := parseCheckpoints([]string{
		"1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		" 2 : BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got[1] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("unexpected checkpoint 1: %q", got[1])
	}
	if got[2] != "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" {
		t.Fatalf("unexpected checkpoint 2: %q", got[2])
	}
	if _, err := parseCheckpoints([]string{"bad"}); err == nil {
		t.Fatal("expected malformed checkpoint error")
	}
	if _, err := parseCheckpoints([]string{"0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}); err == nil {
		t.Fatal("expected invalid checkpoint height error")
	}
	if _, err := parseCheckpoints([]string{"1:abcd"}); err == nil {
		t.Fatal("expected short checkpoint hash error")
	}
	if _, err := parseCheckpoints([]string{"1:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}); err == nil {
		t.Fatal("expected non-hex checkpoint hash error")
	}
	if _, err := parseCheckpoints([]string{
		"1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}); err == nil {
		t.Fatal("expected duplicate checkpoint height error")
	}
}

func TestLoadValidatorSetFiles(t *testing.T) {
	vals, _ := ctypes.RandValidatorSet(2, 1)
	pb, err := vals.ToProto()
	if err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(pb)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "validators.pb")
	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	loaded, err := loadValidatorSetFiles([]string{"7:" + path})
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded[7].Hash(); string(got) != string(vals.Hash()) {
		t.Fatal("loaded validator set hash mismatch")
	}
	if _, err := loadValidatorSetFiles([]string{"bad"}); err == nil {
		t.Fatal("expected malformed validator set arg error")
	}
	if _, err := loadValidatorSetFiles([]string{"7:" + path, "7:" + path}); err == nil {
		t.Fatal("expected duplicate validator set height error")
	}
}

type transientPutStore struct {
	archive.ObjectStore
	failures atomic.Int64
}

func (s *transientPutStore) Put(ctx context.Context, key string, data []byte) error {
	for {
		remaining := s.failures.Load()
		if remaining <= 0 {
			return s.ObjectStore.Put(ctx, key, data)
		}
		if s.failures.CompareAndSwap(remaining, remaining-1) {
			return errors.New("transient put failure")
		}
	}
}

type blockingSegmentStore struct {
	archive.ObjectStore
	started chan struct{}
	release chan struct{}
	blocked atomic.Bool
}

func (s *blockingSegmentStore) Put(ctx context.Context, key string, data []byte) error {
	s.blockSegmentPut(ctx, key)
	return s.ObjectStore.Put(ctx, key, data)
}

func (s *blockingSegmentStore) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	s.blockSegmentPut(ctx, key)
	immutableStore, ok := s.ObjectStore.(archive.ImmutableObjectStore)
	if !ok {
		return s.ObjectStore.Put(ctx, key, data)
	}
	return immutableStore.PutIfAbsent(ctx, key, data)
}

func (s *blockingSegmentStore) blockSegmentPut(ctx context.Context, key string) {
	if !strings.Contains(key, "/segments/") || !s.blocked.CompareAndSwap(false, true) {
		return
	}
	close(s.started)
	select {
	case <-s.release:
	case <-ctx.Done():
	}
}

func createCLIBlockStoreFixture(t *testing.T, dir string, heights int) {
	t.Helper()
	db, err := dbm.NewDB("blockstore", dbm.BackendType("goleveldb"), dir)
	if err != nil {
		t.Fatal(err)
	}
	bs := store.NewBlockStore(db)
	defer bs.Close()
	for h := int64(1); h <= int64(heights); h++ {
		block := makeCLITestBlock(h)
		parts, err := block.MakePartSet(ctypes.BlockPartSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		seen := &ctypes.Commit{Height: h, Signatures: []ctypes.CommitSig{}}
		bs.SaveBlock(block, parts, seen)
	}
}

func makeCLITestBlock(height int64) *ctypes.Block {
	block := ctypes.MakeBlock(height, ctypes.Data{Txs: []ctypes.Tx{ctypes.Tx(fmt.Sprintf("tx-%d", height))}}, &ctypes.Commit{}, nil)
	block.ChainID = cliTestChainID
	block.ProposerAddress = testAddress(byte(height))
	block.ValidatorsHash = testBytes(0x11, 32)
	block.NextValidatorsHash = testBytes(0x22, 32)
	block.ConsensusHash = testBytes(0x33, 32)
	return block
}

func testAddress(value byte) []byte {
	return testBytes(value, crypto.AddressSize)
}

func testBytes(value byte, size int) []byte {
	bz := make([]byte, crypto.AddressSize)
	if size != crypto.AddressSize {
		bz = make([]byte, size)
	}
	for i := range bz {
		bz[i] = value
	}
	return bz
}
