package archive

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestArchiveReadyRetriesVisibleSegmentDirectorySync(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 4)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	root := filepath.Join(t.TempDir(), "objects")
	store, err := NewLocalObjectStore(root)
	if err != nil {
		t.Fatal(err)
	}
	opts := LiveArchiveOptions{
		ChainID: testChainID, Prefix: "archive", ReadyHeight: 2,
		SegmentBlocks: 2, Compression: CompressionGzip,
	}
	manifestKey := ManifestKey(opts.Prefix, opts.ChainID, DefaultManifest)
	pruneOpts := PruneHotOptions{
		ManifestKey: manifestKey, RetainBlocks: 1,
		EvidenceMaxAgeBlocks: 1, EvidenceMaxAgeDuration: time.Nanosecond,
	}
	originalSync := syncLocalDirectory
	t.Cleanup(func() { syncLocalDirectory = originalSync })
	injected := errors.New("segment directory sync failed")
	fail := true
	segmentSyncs := 0
	syncLocalDirectory = func(dir string) error {
		if filepath.Base(dir) == "segments" {
			segmentSyncs++
			if fail {
				return injected
			}
		}
		return originalSync(dir)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if _, err := ArchiveReady(ctx, reader, store, opts); !errors.Is(err, injected) {
			t.Fatalf("attempt %d: archive error = %v, want sync failure", attempt, err)
		}
		if segmentSyncs != attempt {
			t.Fatalf("attempt %d: segment syncs = %d, want %d", attempt, segmentSyncs, attempt)
		}
		objects, err := store.List(ctx, "archive")
		if err != nil || len(objects) != 1 {
			t.Fatalf("visible objects after failed publication = %v, %v", objects, err)
		}
		beforeRead := segmentSyncs
		if _, err := store.Get(ctx, objects[0].Key); err != nil {
			t.Fatal(err)
		}
		if segmentSyncs != beforeRead {
			t.Fatal("cold read retried directory sync")
		}
		if _, err := LoadManifest(ctx, store, manifestKey); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("manifest after failed archive = %v, want missing", err)
		}
		if _, err := PruneVerifiedHotStore(ctx, reader, store, pruneOpts); err == nil {
			t.Fatal("prune succeeded after failed archive")
		}
		if reader.Base() != 1 || reader.LoadBlock(1) == nil {
			t.Fatal("failed archive allowed hot blocks to be pruned")
		}
		store, err = NewLocalObjectStore(root)
		if err != nil {
			t.Fatal(err)
		}
	}
	fail = false
	archived, err := ArchiveReady(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if segmentSyncs != 4 || archived.Reused != 1 || archived.Uploaded != 0 || archived.Manifest.LastHeight != 2 {
		t.Fatalf("recovered archive = %+v, segment syncs = %d", archived, segmentSyncs)
	}
	pruned, err := PruneVerifiedHotStore(ctx, reader, store, pruneOpts)
	if err != nil {
		t.Fatal(err)
	}
	if pruned.Pruned != 2 || reader.Base() != 3 {
		t.Fatalf("prune after durable reuse = %+v", pruned)
	}
}

func TestLocalObjectStoreCollisionRetriesDirectorySync(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	originalSync := syncLocalDirectory
	t.Cleanup(func() { syncLocalDirectory = originalSync })
	injected := errors.New("directory sync failed")
	fail := true
	syncs := 0
	syncLocalDirectory = func(dir string) error {
		if filepath.Base(dir) == "segments" {
			syncs++
			if fail {
				return injected
			}
		}
		return originalSync(dir)
	}
	key := "segments/1.cba"
	if err := store.PutIfAbsent(ctx, key, []byte("original")); !errors.Is(err, injected) {
		t.Fatalf("initial publication = %v, want sync failure", err)
	}
	if err := store.PutIfAbsent(ctx, key, []byte("replacement")); !errors.Is(err, injected) || errors.Is(err, ErrObjectAlreadyExists) {
		t.Fatalf("collision = %v, want sync failure without reusable collision sentinel", err)
	}
	fail = false
	if err := store.PutIfAbsent(ctx, key, []byte("replacement")); !errors.Is(err, ErrObjectAlreadyExists) {
		t.Fatalf("durable collision = %v, want ErrObjectAlreadyExists", err)
	}
	if syncs != 3 {
		t.Fatalf("publication and collision syncs = %d, want 3", syncs)
	}
	data, err := store.Get(ctx, key)
	if err != nil || string(data) != "original" {
		t.Fatalf("existing object = %q, %v", data, err)
	}
}
