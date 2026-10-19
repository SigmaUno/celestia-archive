package blocksyncarchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/01builders/cometbft-archive/internal/archive"
	"github.com/cometbft/cometbft/types"
)

type segmentTestStore struct {
	archive.ObjectStore
	mu      sync.Mutex
	objects map[string][]byte
	gets    int
	hook    func(context.Context, string) error
}

func (s *segmentTestStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	data := bytes.Clone(s.objects[key])
	hook := s.hook
	if key != "manifest.json" {
		s.gets++
	} else {
		hook = nil
	}
	s.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, key); err != nil {
			return nil, err
		}
	}
	return data, ctx.Err()
}

func (s *segmentTestStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

func segmentFixture(t testing.TB, counts ...int) (*ArchiveBlockSource, *segmentTestStore, []archive.SegmentManifest) {
	t.Helper()
	store := &segmentTestStore{objects: make(map[string][]byte)}
	var segments []archive.SegmentManifest
	height := int64(1)
	for _, count := range counts {
		var records []archive.BlockRecord
		for i := 0; i < count; i++ {
			block := types.MakeBlock(height, types.Data{Txs: []types.Tx{bytes.Repeat([]byte(fmt.Sprintf("tx-%d-", height)), 1024)}}, &types.Commit{}, nil)
			block.ChainID = ingestTestChainID
			block.ProposerAddress = bytes.Repeat([]byte{1}, 20)
			block.ValidatorsHash = bytes.Repeat([]byte{2}, 32)
			block.NextValidatorsHash = bytes.Repeat([]byte{3}, 32)
			block.ConsensusHash = bytes.Repeat([]byte{4}, 32)
			record, err := archive.BlockToRecord(block)
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
			height++
		}
		data, segment, err := archive.EncodeSegment(records, archive.CompressionGzip)
		if err != nil {
			t.Fatal(err)
		}
		segment.Key = archive.SegmentKey("archive", ingestTestChainID, segment)
		store.objects[segment.Key] = data
		segments = append(segments, segment)
	}
	manifest, err := archive.NewManifest(ingestTestChainID, segments)
	if err != nil {
		t.Fatal(err)
	}
	store.objects["manifest.json"], err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewArchiveBlockSource(store, "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	if err := source.SetManifestCacheTTL(time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := source.AdvertisedRange(context.Background()); err != nil {
		t.Fatal(err)
	}
	return source, store, segments
}

func loadHeight(t *testing.T, source *ArchiveBlockSource, height int64) {
	t.Helper()
	block, err := source.LoadBlock(context.Background(), height)
	if err != nil {
		t.Fatal(err)
	}
	if block.Height != height {
		t.Fatalf("height=%d, want %d", block.Height, height)
	}
}

func awaitWaiters(t *testing.T, source *ArchiveBlockSource, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		source.mu.Lock()
		got := 0
		for _, load := range source.segmentLoads {
			got += load.waiters
		}
		source.mu.Unlock()
		if got == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("did not reach %d waiters", want)
}

func receiveError(t *testing.T, result <-chan error, want error) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, want) {
			t.Fatalf("error=%v, want %v", err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request stuck")
	}
}

func TestColdSegmentsSequentialAndConcurrent(t *testing.T) {
	source, store, _ := segmentFixture(t, 100)
	release := make(chan struct{})
	store.hook = func(ctx context.Context, _ string) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for height := int64(1); height <= 100; height++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := source.LoadBlock(context.Background(), height); errs <- err }()
	}
	awaitWaiters(t, source, 100)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for height := int64(1); height <= 100; height++ {
		loadHeight(t, source, height)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("segment fetches=%d, want 1 for 200 requests", got)
	}
	block, err := source.LoadBlock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	block.Data.Txs[0][0] ^= 0xff
	loadHeight(t, source, 1)
}

func TestColdSegmentsBudget(t *testing.T) {
	source, store, segments := segmentFixture(t, 1, 1, 1)
	var budget int64
	for _, segment := range segments {
		records, err := archive.DecodeSegment(store.objects[segment.Key], segment)
		if err != nil {
			t.Fatal(err)
		}
		budget = max(budget, segmentRecordBytes(records))
	}
	if err := source.SetSegmentCacheBytes(2 * budget); err != nil {
		t.Fatal(err)
	}
	for _, height := range []int64{1, 2, 1, 3, 1, 2} {
		loadHeight(t, source, height)
	}
	if got := store.count(); got != 4 {
		t.Fatalf("LRU fetches=%d, want 4", got)
	}
	source.mu.Lock()
	if source.segmentBytes > 2*budget {
		t.Fatal("cache exceeded budget")
	}
	source.mu.Unlock()
	if err := source.SetSegmentCacheBytes(1); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		loadHeight(t, source, 1)
	}
	if got := store.count(); got != 6 {
		t.Fatalf("oversized fetches=%d, want 6", got)
	}
	if source.segmentBytes != 0 || source.segmentLRU.Len() != 0 {
		t.Fatal("oversized segment retained")
	}
	if err := source.SetSegmentCacheBytes(segments[0].SizeBytes); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		loadHeight(t, source, 1)
	}
	if store.count() != 8 || source.segmentBytes != 0 {
		t.Fatal("decompressed records exceeding budget were retained")
	}
	if err := source.SetSegmentCacheBytes(0); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		loadHeight(t, source, 1)
	}
	if got := store.count(); got != 10 {
		t.Fatalf("disabled cache fetches=%d, want 10", got)
	}
	for _, invalid := range []int64{-1, 1<<30 + 1} {
		if source.SetSegmentCacheBytes(invalid) == nil {
			t.Fatal("accepted invalid budget")
		}
	}
}

func TestColdSegmentsRejectCorruptionAndRetry(t *testing.T) {
	for _, mode := range []string{"size", "checksum", "magic", "gzip", "index hash", "block hash"} {
		t.Run(mode, func(t *testing.T) {
			source, store, segments := segmentFixture(t, 2)
			segment := segments[0]
			good := bytes.Clone(store.objects[segment.Key])
			data := bytes.Clone(good)
			switch mode {
			case "size":
				data = data[:len(data)-1]
			case "checksum":
				data[len(data)/2] ^= 1
			case "magic":
				data[0] ^= 1
			case "gzip":
				data[len(data)-1] ^= 1
			case "index hash":
				segment.Blocks[1].Hash = strings.Repeat("a", 64)
			case "block hash":
				records, err := archive.DecodeSegment(good, segment)
				if err != nil {
					t.Fatal(err)
				}
				records[1].Hash = strings.Repeat("a", 64)
				data, segment, err = archive.EncodeSegment(records, archive.CompressionGzip)
				if err != nil {
					t.Fatal(err)
				}
				segment.Key = segments[0].Key
			}
			if mode == "magic" || mode == "gzip" {
				sum := sha256.Sum256(data)
				segment.SHA256 = hex.EncodeToString(sum[:])
			}
			store.objects[segment.Key] = data
			for range 2 {
				if _, err := source.loadSegment(context.Background(), segment); err == nil {
					t.Fatal("corrupt object accepted")
				}
			}
			if source.segmentBytes != 0 || store.count() != 2 {
				t.Fatal("corrupt segment cached")
			}
			store.objects[segment.Key] = good
			loadHeight(t, source, 1)
			if store.count() != 3 {
				t.Fatal("repair did not retry")
			}
		})
	}
}

func TestColdSegmentsFailureAndIdentity(t *testing.T) {
	source, store, segments := segmentFixture(t, 1)
	failure := errors.New("temporary fetch failure")
	release := make(chan struct{})
	store.hook = func(ctx context.Context, _ string) error { <-release; return failure }
	results := make(chan error, 2)
	for range 2 {
		go func() { _, err := source.LoadBlock(context.Background(), 1); results <- err }()
	}
	awaitWaiters(t, source, 2)
	close(release)
	for range 2 {
		receiveError(t, results, failure)
	}
	store.mu.Lock()
	store.hook = nil
	store.mu.Unlock()
	loadHeight(t, source, 1)
	if store.count() != 2 {
		t.Fatal("failed fetch cached or not coalesced")
	}
	changed := segments[0]
	changed.SizeBytes++
	if _, err := source.loadSegment(context.Background(), changed); err == nil {
		t.Fatal("size change reused cache")
	}
	changed = segments[0]
	changed.Blocks = append([]archive.BlockIndex(nil), changed.Blocks...)
	changed.Blocks[0].Hash = strings.Repeat("a", 64)
	if _, err := source.loadSegment(context.Background(), changed); err == nil {
		t.Fatal("index change reused cache")
	}
	changed = segments[0]
	changed.SHA256 = strings.Repeat("a", 64)
	if _, err := source.loadSegment(context.Background(), changed); err == nil {
		t.Fatal("checksum change reused cache")
	}
	loadHeight(t, source, 1)
	if store.count() != 5 {
		t.Fatalf("fetches=%d, want 5", store.count())
	}
}

func TestColdSegmentsFirstCallerCancellation(t *testing.T) {
	source, store, _ := segmentFixture(t, 1)
	release := make(chan struct{})
	store.hook = func(ctx context.Context, _ string) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	first, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstResult, secondResult := make(chan error, 1), make(chan error, 1)
	go func() { _, err := source.LoadBlock(first, 1); firstResult <- err }()
	awaitWaiters(t, source, 1)
	go func() { _, err := source.LoadBlock(context.Background(), 1); secondResult <- err }()
	awaitWaiters(t, source, 2)
	cancel()
	receiveError(t, firstResult, context.Canceled)
	close(release)
	receiveError(t, secondResult, nil)
	loadHeight(t, source, 1)
	if store.count() != 1 {
		t.Fatal("first cancellation poisoned shared fetch")
	}
}

func TestColdSegmentsAbandonAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprint(shutdown), func(t *testing.T) {
			source, store, _ := segmentFixture(t, 1)
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			store.hook = func(ctx context.Context, _ string) error {
				close(started)
				<-ctx.Done()
				close(canceled)
				<-release // A slow backend must not hold up a later retry or Close.
				return ctx.Err()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := source.LoadBlock(ctx, 1); result <- err }()
			<-started
			if shutdown {
				if err := source.Close(); err != nil {
					t.Fatal(err)
				}
				receiveError(t, result, errColdSourceClosed)
			} else {
				cancel()
				receiveError(t, result, context.Canceled)
			}
			select {
			case <-canceled:
			case <-time.After(5 * time.Second):
				t.Fatal("backend context not canceled")
			}
			store.mu.Lock()
			store.hook = nil
			store.mu.Unlock()
			if shutdown {
				if _, err := source.LoadBlock(context.Background(), 1); !errors.Is(err, errColdSourceClosed) {
					t.Fatalf("closed source error=%v", err)
				}
			} else {
				loadHeight(t, source, 1)
			}
			close(release)
			if shutdown && source.segmentBytes != 0 {
				t.Fatal("closed source retained records")
			}
			if source.Close() != nil {
				t.Fatal("Close not idempotent")
			}
		})
	}
}

func TestColdSegmentsCanceledCacheHit(t *testing.T) {
	source, store, _ := segmentFixture(t, 1)
	loadHeight(t, source, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.LoadBlock(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if store.count() != 1 {
		t.Fatal("canceled hit fetched object")
	}
}

func BenchmarkColdSegmentsSequential100(b *testing.B) {
	for _, cached := range []bool{false, true} {
		b.Run(fmt.Sprintf("cached=%t", cached), func(b *testing.B) {
			source, store, segments := segmentFixture(b, 100)
			segment := segments[0]
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for height := int64(1); height <= 100; height++ {
					if cached {
						if _, err := source.LoadBlock(context.Background(), height); err != nil {
							b.Fatal(err)
						}
					} else {
						data, err := store.Get(context.Background(), segment.Key)
						if err != nil {
							b.Fatal(err)
						}
						if _, err := archive.DecodeSegmentBlock(data, segment, height); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(store.count())/float64(b.N*100), "fetches/block")
			b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "blocks/s")
		})
	}
}
