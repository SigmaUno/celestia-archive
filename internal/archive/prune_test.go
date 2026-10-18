package archive

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestPruneVerifiedHotStorePrunesOnlyArchivedAndOutsideRetention(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 6)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectStore, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	archived, err := ArchiveReady(ctx, reader, objectStore, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   5,
		SegmentBlocks: 2,
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := PruneVerifiedHotStore(ctx, reader, objectStore, PruneHotOptions{
		ManifestKey:            archived.ManifestKey,
		RetainBlocks:           2,
		EvidenceMaxAgeBlocks:   1,
		EvidenceMaxAgeDuration: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.BaseBefore != 1 || result.PruneToHeight != 5 || result.BaseAfter != 5 || result.Pruned != 4 {
		t.Fatalf("unexpected prune result: %+v", result)
	}
	if reader.LoadBlock(4) != nil {
		t.Fatal("expected block 4 to be pruned")
	}
	if reader.LoadBlock(5) == nil || reader.LoadBlock(6) == nil {
		t.Fatal("expected retained hot blocks 5 and 6")
	}
}

func TestPruneVerifiedHotStoreDoesNotPruneUnverifiedArchive(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 4)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectStore, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	archived, err := ArchiveReady(ctx, reader, objectStore, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   3,
		SegmentBlocks: 3,
		Compression:   CompressionGzip,
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
	if _, err := PruneVerifiedHotStore(ctx, reader, objectStore, PruneHotOptions{
		ManifestKey:  archived.ManifestKey,
		RetainBlocks: 1,
	}); err == nil {
		t.Fatal("expected corrupt archive to block pruning")
	}
	if reader.Base() != 1 {
		t.Fatalf("base changed after failed prune: %d", reader.Base())
	}
}

func TestPruneVerifiedHotStoreNoopsWhenRetentionProtectsRange(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 4)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectStore, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	archived, err := ArchiveReady(ctx, reader, objectStore, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   4,
		SegmentBlocks: 2,
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := PruneVerifiedHotStore(ctx, reader, objectStore, PruneHotOptions{
		ManifestKey:  archived.ManifestKey,
		RetainBlocks: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pruned != 0 || result.BaseAfter != 1 {
		t.Fatalf("unexpected retention-protected prune result: %+v", result)
	}
}

func TestVerifyArchiveForPruneChecksOnlyNewSegments(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 6)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectStore, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	archiveOpts := LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   2,
		SegmentBlocks: 2,
		Compression:   CompressionGzip,
	}
	archived, err := ArchiveReady(ctx, reader, objectStore, archiveOpts)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	if verified != 2 {
		t.Fatalf("verified through %d, want 2", verified)
	}

	// Corrupt the already verified segment; incremental checks must skip it.
	firstKey := archived.Manifest.Segments[0].Key
	data, err := objectStore.Get(ctx, firstKey)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if putErr := objectStore.Put(ctx, firstKey, data); putErr != nil {
		t.Fatal(putErr)
	}
	same, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, verified+1)
	if err != nil {
		t.Fatalf("incremental verify with no new segments: %v", err)
	}
	if same != 2 {
		t.Fatalf("verified through %d with no new segments, want 2", same)
	}

	archiveOpts.ReadyHeight = 4
	if _, archiveErr := ArchiveReady(ctx, reader, objectStore, archiveOpts); archiveErr != nil {
		t.Fatal(archiveErr)
	}
	next, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, verified+1)
	if err != nil {
		t.Fatalf("incremental verify of new segment: %v", err)
	}
	if next != 4 {
		t.Fatalf("verified through %d, want 4", next)
	}
	if _, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, 0); err == nil {
		t.Fatal("expected full verify to detect corrupt first segment")
	}
	if _, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, 10); err == nil {
		t.Fatal("expected error when manifest is below previously verified height")
	}
}

func TestPruneHotStoreThroughUsesVerifiedHeight(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 6)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	result, err := PruneHotStoreThrough(reader, PruneHotOptions{
		ManifestKey:            "unused",
		RetainBlocks:           1,
		EvidenceMaxAgeBlocks:   1,
		EvidenceMaxAgeDuration: time.Nanosecond,
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if result.PruneToHeight != 4 || result.BaseAfter != 4 || result.ArchivedThrough != 3 {
		t.Fatalf("unexpected prune result: %+v", result)
	}
}
