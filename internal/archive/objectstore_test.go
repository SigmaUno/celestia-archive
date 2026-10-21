package archive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLocalObjectStorePublicationSync(t *testing.T) {
	for _, immutable := range []bool{false, true} {
		name := "put"
		if immutable {
			name = "put-if-absent"
		}
		t.Run(name, func(t *testing.T) {
			for _, failAt := range []string{"", "publication", "root", "ancestor"} {
				t.Run("failure="+failAt, func(t *testing.T) {
					base := t.TempDir()
					root := filepath.Join(base, "new", "archive")
					store, err := NewLocalObjectStore(root)
					if err != nil {
						t.Fatal(err)
					}
					key := "chains/test/segments/1.cba"
					path := filepath.Join(root, filepath.FromSlash(key))
					dir := filepath.Dir(path)
					failureDir := map[string]string{"publication": dir, "root": root, "ancestor": base}[failAt]
					injected := errors.New("directory sync failed")
					originalSync := syncLocalDirectory
					t.Cleanup(func() { syncLocalDirectory = originalSync })
					var synced []string
					syncLocalDirectory = func(dir string) error {
						data, err := os.ReadFile(path)
						if err != nil || string(data) != "segment" {
							t.Fatalf("object at directory sync = %q, %v", data, err)
						}
						synced = append(synced, dir)
						if dir == failureDir {
							return injected
						}
						return originalSync(dir)
					}
					put := store.Put
					if immutable {
						put = store.PutIfAbsent
					}
					err = put(context.Background(), key, []byte("segment"))
					if failureDir != "" {
						if !errors.Is(err, injected) || !strings.Contains(err.Error(), failureDir) {
							t.Fatalf("publication error = %v, want directory sync error for %s", err, failureDir)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					var want []string
					for {
						want = append(want, dir)
						parent := filepath.Dir(dir)
						if dir == failureDir || dir == parent {
							break
						}
						dir = parent
					}
					if !slices.Equal(synced, want) {
						t.Fatalf("synced directories = %v, want %v", synced, want)
					}
				})
			}
		})
	}
}

func TestLocalObjectStorePutReplacesAtomically(t *testing.T) {
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "manifests/manifest.json"
	if err := store.Put(ctx, key, []byte("old manifest")); err != nil {
		t.Fatal(err)
	}
	old, err := os.Open(filepath.Join(store.root, filepath.FromSlash(key)))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err := store.Put(ctx, key, []byte("new manifest")); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, key)
	if err != nil || string(got) != "new manifest" {
		t.Fatalf("published manifest = %q, %v", got, err)
	}
	data := make([]byte, len("old manifest"))
	if _, err := old.Read(data); err != nil || string(data) != "old manifest" {
		t.Fatalf("open previous manifest = %q, %v", data, err)
	}
}

func TestLocalObjectStorePutIfAbsentDoesNotOverwrite(t *testing.T) {
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if putErr := store.PutIfAbsent(ctx, "segments/1.cba", []byte("first")); putErr != nil {
		t.Fatal(putErr)
	}
	if putErr := store.PutIfAbsent(ctx, "segments/1.cba", []byte("replacement")); !errors.Is(putErr, ErrObjectAlreadyExists) {
		t.Fatalf("PutIfAbsent err=%v, want ErrObjectAlreadyExists", putErr)
	}
	got, err := store.Get(ctx, "segments/1.cba")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first" {
		t.Fatalf("object content = %q, want first", got)
	}
}

func TestLocalObjectStoreListSkipsUnsafeExistingKeys(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalObjectStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if putErr := store.Put(ctx, "segments/1.cba", []byte("first")); putErr != nil {
		t.Fatal(putErr)
	}
	if writeErr := os.WriteFile(filepath.Join(root, `bad\key`), []byte("unsafe"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	infos, err := store.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := objectInfoKeys(infos); len(got) != 1 || got[0] != "segments/1.cba" {
		t.Fatalf("list keys = %v, want only safe key", got)
	}
}

func TestLocalObjectStoreRejectsSymlinkObjects(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalObjectStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if putErr := store.Put(ctx, "segments/1.cba", []byte("first")); putErr != nil {
		t.Fatal(putErr)
	}
	if mkdirErr := os.MkdirAll(filepath.Join(root, "segments"), 0o755); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	outside := filepath.Join(t.TempDir(), "outside.cba")
	if writeErr := os.WriteFile(outside, []byte("outside"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	linkPath := filepath.Join(root, "segments", "link.cba")
	if symlinkErr := os.Symlink(outside, linkPath); symlinkErr != nil {
		t.Skipf("symlink unavailable: %v", symlinkErr)
	}
	if _, getErr := store.Get(ctx, "segments/link.cba"); getErr == nil || !strings.Contains(getErr.Error(), "symlink") {
		t.Fatalf("Get symlink err=%v, want symlink error", getErr)
	}
	if _, existsErr := store.Exists(ctx, "segments/link.cba"); existsErr == nil || !strings.Contains(existsErr.Error(), "symlink") {
		t.Fatalf("Exists symlink err=%v, want symlink error", existsErr)
	}
	if _, statErr := store.Stat(ctx, "segments/link.cba"); statErr == nil || !strings.Contains(statErr.Error(), "symlink") {
		t.Fatalf("Stat symlink err=%v, want symlink error", statErr)
	}
	infos, err := store.List(ctx, "segments")
	if err != nil {
		t.Fatal(err)
	}
	if got := objectInfoKeys(infos); strings.Join(got, ",") != "segments/1.cba" {
		t.Fatalf("list keys = %v, want only non-symlink object", got)
	}

	outsideDir := t.TempDir()
	if writeErr := os.WriteFile(filepath.Join(outsideDir, "escape.cba"), []byte("escaped"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	prefixLinkPath := filepath.Join(root, "linked")
	if symlinkErr := os.Symlink(outsideDir, prefixLinkPath); symlinkErr != nil {
		t.Skipf("symlink directory unavailable: %v", symlinkErr)
	}
	if _, getErr := store.Get(ctx, "linked/escape.cba"); getErr == nil || !strings.Contains(getErr.Error(), "symlink") {
		t.Fatalf("Get symlink parent err=%v, want symlink error", getErr)
	}
	if putErr := store.Put(ctx, "linked/new.cba", []byte("new")); putErr == nil || !strings.Contains(putErr.Error(), "symlink") {
		t.Fatalf("Put symlink parent err=%v, want symlink error", putErr)
	}
	if _, statErr := os.Stat(filepath.Join(outsideDir, "new.cba")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside write stat err=%v, want not exist", statErr)
	}
}

func TestLocalObjectStoreRejectsSymlinkRoot(t *testing.T) {
	target := t.TempDir()
	root := filepath.Join(t.TempDir(), "root-link")
	if symlinkErr := os.Symlink(target, root); symlinkErr != nil {
		t.Skipf("symlink unavailable: %v", symlinkErr)
	}
	if _, err := NewLocalObjectStore(root); err == nil || !strings.Contains(err.Error(), "root") || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("NewLocalObjectStore symlink root err=%v, want symlink root error", err)
	}
	if _, err := NewLocalObjectStoreReadOnly(root); err == nil || !strings.Contains(err.Error(), "root") || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("NewLocalObjectStoreReadOnly symlink root err=%v, want symlink root error", err)
	}
}

func TestLocalObjectStoreListStaysWithinRequestedPrefix(t *testing.T) {
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for key, data := range map[string][]byte{
		"segments/1.cba":        []byte("inside"),
		"segments/nested/2.cba": []byte("nested"),
		"segments-extra/1.cba":  []byte("sibling"),
	} {
		if putErr := store.Put(ctx, key, data); putErr != nil {
			t.Fatal(putErr)
		}
	}
	infos, err := store.List(ctx, "segments")
	if err != nil {
		t.Fatal(err)
	}
	if got := objectInfoKeys(infos); strings.Join(got, ",") != "segments/1.cba,segments/nested/2.cba" {
		t.Fatalf("list keys = %v, want only segments prefix", got)
	}
}

func TestOpenObjectStoreReadOnlyDoesNotCreateLocalRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	store, err := OpenObjectStoreReadOnly("file://" + root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("local root stat err=%v, want not exist", err)
	}
	if err := store.Put(context.Background(), "segments/1.cba", []byte("x")); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("read-only Put err=%v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("local root stat after Put err=%v, want not exist", err)
	}
}

func TestValidateObjectStoreURL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "file", url: "file:///tmp/archive"},
		{name: "s3", url: "s3://bucket/root?region=us-east-1"},
		{name: "empty", wantErr: true},
		{name: "file-empty-root", url: "file://", wantErr: true},
		{name: "s3-missing-bucket", url: "s3:///root", wantErr: true},
		{name: "unsupported", url: "gs://bucket/root", wantErr: true},
		{name: "bad-s3-query", url: "s3://bucket/root?path_style=maybe", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateObjectStoreURL(tc.url)
			if tc.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateObjectKey(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "valid", key: "archive/chains/test/segments/1.cba"},
		{name: "empty", wantErr: true},
		{name: "relative-parent", key: "../escape", wantErr: true},
		{name: "nested-parent", key: "archive/../../escape", wantErr: true},
		{name: "absolute", key: "/tmp/escape", wantErr: true},
		{name: "backslash", key: `archive\escape`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateObjectKey(tc.key)
			if tc.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestArchiveAPIsRejectNilDependencies(t *testing.T) {
	ctx := context.Background()
	manifestKey := ManifestKey("archive", testChainID, DefaultManifest)
	if _, err := LoadManifest(ctx, nil, "../escape"); err == nil || !strings.Contains(err.Error(), "manifest key") {
		t.Fatalf("LoadManifest err=%v, want manifest key validation", err)
	}
	if err := SaveManifest(ctx, nil, "../escape", Manifest{}); err == nil || !strings.Contains(err.Error(), "manifest key") {
		t.Fatalf("SaveManifest err=%v, want manifest key validation", err)
	}
	if _, err := LoadManifest(ctx, nil, manifestKey); err == nil || !strings.Contains(err.Error(), "object store is required") {
		t.Fatalf("LoadManifest err=%v, want object store required", err)
	}
	if err := SaveManifest(ctx, nil, manifestKey, Manifest{}); err == nil || !strings.Contains(err.Error(), "object store is required") {
		t.Fatalf("SaveManifest err=%v, want object store required", err)
	}
	if _, err := Verify(ctx, nil, VerifyOptions{ManifestKey: manifestKey}); err == nil || !strings.Contains(err.Error(), "object store is required") {
		t.Fatalf("Verify err=%v, want object store required", err)
	}
	if _, err := Verify(ctx, nil, VerifyOptions{ManifestKey: manifestKey, SampleEvery: -1}); err == nil || !strings.Contains(err.Error(), "sample every cannot be negative") {
		t.Fatalf("Verify err=%v, want sample every validation", err)
	}
	if _, err := Inspect(ctx, nil, manifestKey); err == nil || !strings.Contains(err.Error(), "object store is required") {
		t.Fatalf("Inspect err=%v, want object store required", err)
	}
	if _, err := Inspect(ctx, nil, ""); err == nil || !strings.Contains(err.Error(), "manifest key is required") {
		t.Fatalf("Inspect err=%v, want manifest key required", err)
	}
	if _, err := Hydrate(ctx, nil, HydrateOptions{ManifestKey: manifestKey, CacheDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "object store is required") {
		t.Fatalf("Hydrate err=%v, want object store required", err)
	}
	if _, err := Hydrate(ctx, nil, HydrateOptions{ManifestKey: manifestKey, CacheDir: t.TempDir(), MaxBytes: -1}); err == nil || !strings.Contains(err.Error(), "max cache bytes cannot be negative") {
		t.Fatalf("Hydrate err=%v, want max cache bytes validation", err)
	}
	if _, _, err := ClampRange(nil, 0, 0); err == nil || !strings.Contains(err.Error(), "block reader is required") {
		t.Fatalf("ClampRange err=%v, want block reader required", err)
	}
	if _, err := Migrate(ctx, nil, nil, MigrationOptions{ChainID: testChainID, SegmentBlocks: 1}); err == nil || !strings.Contains(err.Error(), "block reader is required") {
		t.Fatalf("Migrate err=%v, want block reader required", err)
	}
	if _, err := ArchiveReady(ctx, nil, nil, LiveArchiveOptions{ChainID: testChainID, ReadyHeight: 1, SegmentBlocks: 1}); err == nil || !strings.Contains(err.Error(), "block reader is required") {
		t.Fatalf("ArchiveReady err=%v, want block reader required", err)
	}
	if _, err := ArchiveReadyFromHead(ctx, nil, nil, LiveArchiveOptions{ChainID: testChainID, SegmentBlocks: 1}, 10); err == nil || !strings.Contains(err.Error(), "block reader is required") {
		t.Fatalf("ArchiveReadyFromHead err=%v, want block reader required", err)
	}
}

func TestValidateObjectPrefix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{name: "empty"},
		{name: "plain", prefix: "segments"},
		{name: "trailing-slash", prefix: "segments/"},
		{name: "relative-parent", prefix: "../escape", wantErr: true},
		{name: "nested-parent", prefix: "archive/../escape", wantErr: true},
		{name: "double-slash", prefix: "archive//escape", wantErr: true},
		{name: "absolute", prefix: "/archive", wantErr: true},
		{name: "backslash", prefix: `archive\escape`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateObjectPrefix(tc.prefix)
			if tc.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestManifestValidationRejectsUnsafeSegmentKey(t *testing.T) {
	records := []BlockRecord{makeTestRecord(t, 1)}
	_, segment, err := EncodeSegment(records, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = "../escape"
	manifest := Manifest{
		Version:     ManifestVersion,
		ChainID:     testChainID,
		FirstHeight: 1,
		LastHeight:  1,
		Segments:    []SegmentManifest{segment},
	}
	if err := manifest.Validate(); err == nil {
		t.Fatal("expected unsafe segment key validation error")
	}
}

func TestValidateCometDBBackend(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backend string
		wantErr bool
	}{
		{name: "default"},
		{name: "goleveldb", backend: "goleveldb"},
		{name: "pebbledb", backend: "pebbledb"},
		{name: "unsupported", backend: "unknown", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCometDBBackend(tc.backend)
			if tc.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateExistingCometBlockStoreConfigRequiresExistingDB(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "missing")
	err := ValidateExistingCometBlockStoreConfig(dbDir, DefaultDBBackend)
	if err == nil {
		t.Fatal("expected missing blockstore error")
	}
	if !strings.Contains(err.Error(), "blockstore database") {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ValidateExistingCometBlockStoreConfig(dbDir, "memdb"); err != nil {
		t.Fatalf("memdb should not require on-disk blockstore: %v", err)
	}
}
