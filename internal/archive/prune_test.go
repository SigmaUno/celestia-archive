package archive

import (
	"context"
	"path/filepath"
	"strings"
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
	verified, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	if verified.manifest.LastHeight != 2 {
		t.Fatalf("verified through %d, want 2", verified.manifest.LastHeight)
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
	same, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, verified)
	if err != nil {
		t.Fatalf("incremental verify with no new segments: %v", err)
	}
	if same.manifest.LastHeight != 2 {
		t.Fatalf("verified through %d with no new segments, want 2", same.manifest.LastHeight)
	}

	archiveOpts.ReadyHeight = 4
	if _, archiveErr := ArchiveReady(ctx, reader, objectStore, archiveOpts); archiveErr != nil {
		t.Fatal(archiveErr)
	}
	next, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, verified)
	if err != nil {
		t.Fatalf("incremental verify of new segment: %v", err)
	}
	if next.manifest.LastHeight != 4 {
		t.Fatalf("verified through %d, want 4", next.manifest.LastHeight)
	}
	if _, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, nil); err == nil {
		t.Fatal("expected full verify to detect corrupt first segment")
	}
	// Changing a previously verified index must force object verification again.
	manifest, err := LoadManifest(ctx, objectStore, archived.ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Segments[0].Blocks[0].Hash = strings.Repeat("a", 64)
	if err := SaveManifest(ctx, objectStore, archived.ManifestKey, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyArchiveForPrune(ctx, objectStore, archived.ManifestKey, next); err == nil {
		t.Fatal("changed manifest prefix reused previous verification")
	}
}

func TestPruningRequiresMatchingCompleteCoverage(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		first, last, hotBase int64
		chain                string
		divergent            bool
		wantBase             int64
	}{
		{name: "full", first: 1, last: 5, hotBase: 1, chain: testChainID, wantBase: 5},
		{name: "partial covers hot base", first: 3, last: 4, hotBase: 3, chain: testChainID, wantBase: 5},
		{name: "partial leaves gap", first: 3, last: 5, hotBase: 1, chain: testChainID},
		{name: "wrong chain", first: 1, last: 5, hotBase: 1, chain: "other-chain"},
		{name: "divergent last candidate", first: 1, last: 5, hotBase: 1, chain: testChainID, divergent: true},
	} {
		for _, standalone := range []bool{true, false} {
			name := tc.name + "/shared"
			if standalone {
				name = tc.name + "/standalone"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				dbDir := filepath.Join(t.TempDir(), "db")
				createBlockStoreFixture(t, dbDir, 6)
				reader, err := OpenCometBlockStore(dbDir, "goleveldb")
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				opts := PruneHotOptions{ManifestKey: "manifest.json", RetainBlocks: 2, EvidenceMaxAgeBlocks: 1, EvidenceMaxAgeDuration: time.Nanosecond}
				if tc.hotBase > 1 {
					state, err := pruneState(reader, opts)
					if err != nil {
						t.Fatal(err)
					}
					if _, _, err := reader.PruneBlocks(tc.hotBase, state); err != nil {
						t.Fatal(err)
					}
				}
				objectStore, err := NewLocalObjectStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				var records []BlockRecord
				for h := tc.first; h <= tc.last; h++ {
					block := makeTestBlock(t, h)
					block.ChainID = tc.chain
					if tc.divergent && h == 4 {
						block.AppHash = []byte("divergent")
					}
					record, err := BlockToRecord(block)
					if err != nil {
						t.Fatal(err)
					}
					records = append(records, record)
				}
				data, segment, err := EncodeSegment(records, CompressionGzip)
				if err != nil {
					t.Fatal(err)
				}
				segment.Key = "segment"
				if err := objectStore.Put(ctx, segment.Key, data); err != nil {
					t.Fatal(err)
				}
				manifest, err := NewManifest(tc.chain, []SegmentManifest{segment})
				if err != nil {
					t.Fatal(err)
				}
				if err := SaveManifest(ctx, objectStore, opts.ManifestKey, manifest); err != nil {
					t.Fatal(err)
				}
				verified, err := VerifyArchiveForPrune(ctx, objectStore, opts.ManifestKey, nil)
				if err != nil {
					t.Fatalf("archive must be valid: %v", err)
				}
				if standalone {
					_, err = PruneVerifiedHotStore(ctx, reader, objectStore, opts)
				} else {
					if tc.name == "full" {
						manifest.ChainID = "replacement-chain"
						manifest.Segments[0].Blocks[0].Hash = strings.Repeat("a", 64)
						if err := SaveManifest(ctx, objectStore, opts.ManifestKey, manifest); err != nil {
							t.Fatal(err)
						}
					}
					_, err = PruneHotStoreThrough(reader, opts, verified)
				}
				if tc.wantBase == 0 {
					if err == nil {
						t.Fatal("expected unsafe prune to fail")
					}
					if reader.Base() != tc.hotBase {
						t.Fatalf("base changed: %d", reader.Base())
					}
					for h := tc.hotBase; h <= 6; h++ {
						if reader.LoadBlock(h) == nil {
							t.Fatalf("hot block %d was deleted", h)
						}
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if reader.Base() != tc.wantBase {
						t.Fatalf("base = %d, want %d", reader.Base(), tc.wantBase)
					}
					if reader.LoadBlock(tc.wantBase-1) != nil {
						t.Fatal("covered block was not pruned")
					}
				}
			})
		}
	}
}
