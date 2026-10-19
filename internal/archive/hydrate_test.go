package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type hydrateGetStore struct {
	ObjectStore
	get func(context.Context, string) ([]byte, error)
}

func (s hydrateGetStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.get(ctx, key)
}

func hydrateFixture(t *testing.T, count int) (ObjectStore, string, []SegmentManifest, []BlockRecord) {
	t.Helper()
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var segments []SegmentManifest
	var records []BlockRecord
	for height := int64(1); height <= int64(count); height++ {
		record := makeTestRecord(t, height)
		data, segment, err := EncodeSegment([]BlockRecord{record}, CompressionGzip)
		if err != nil {
			t.Fatal(err)
		}
		segment.Key = SegmentKey("archive", testChainID, segment)
		if err := store.Put(context.Background(), segment.Key, data); err != nil {
			t.Fatal(err)
		}
		segments = append(segments, segment)
		records = append(records, record)
	}
	manifest, err := NewManifest(testChainID, segments)
	if err != nil {
		t.Fatal(err)
	}
	key := ManifestKey("archive", testChainID, DefaultManifest)
	if err := SaveManifest(context.Background(), store, key, manifest); err != nil {
		t.Fatal(err)
	}
	return store, key, segments, records
}

func TestHydrateBoundsOutstandingSegments(t *testing.T) {
	store, key, segments, records := hydrateFixture(t, 14)
	for _, concurrency := range []int{1, 3} {
		for _, limited := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers=%d/limited=%t", concurrency, limited), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				release := make(chan struct{})
				started := make(chan string, len(segments))
				wrapped := hydrateGetStore{ObjectStore: store, get: func(ctx context.Context, objectKey string) ([]byte, error) {
					if objectKey != key {
						started <- objectKey
						if objectKey == segments[1].Key {
							select {
							case <-release:
							case <-ctx.Done():
								return nil, ctx.Err()
							}
						}
					}
					return store.Get(ctx, objectKey)
				}}
				cacheDir := t.TempDir()
				opts := HydrateOptions{ManifestKey: key, CacheDir: cacheDir, StartHeight: 2, EndHeight: 13, Concurrency: concurrency}
				if limited {
					opts.MaxBytes = int64(len(records[12].Bytes))
				}
				done := make(chan struct{})
				var result HydrateResult
				var hydrateErr error
				go func() {
					defer close(done)
					result, hydrateErr = Hydrate(ctx, wrapped, opts)
				}()
				t.Cleanup(func() {
					cancel()
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("hydrate did not stop after cancellation")
					}
				})
				seen := make(map[string]bool)
				for range concurrency {
					select {
					case fetched := <-started:
						seen[fetched] = true
					case <-time.After(5 * time.Second):
						t.Fatal("prefetch window did not fill")
					}
				}
				for _, segment := range segments[1 : 1+concurrency] {
					if !seen[segment.Key] {
						t.Fatalf("initial window did not fetch %s", segment.Key)
					}
				}
				select {
				case fetched := <-started:
					t.Fatalf("fetched beyond window of %d while first segment blocked: %s", concurrency, fetched)
				case <-time.After(100 * time.Millisecond):
				}
				blockDir := filepath.Join(cacheDir, "chains", testChainID, "blocks")
				entries, err := os.ReadDir(blockDir)
				if err != nil || len(entries) != 0 {
					t.Fatalf("output before first segment: %v, %v", entries, err)
				}
				close(release)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("hydrate did not advance after releasing first segment")
				}
				if hydrateErr != nil {
					t.Fatal(hydrateErr)
				}
				var wantBytes int64
				for _, record := range records[1:13] {
					wantBytes += int64(len(record.Bytes))
					path := filepath.Join(blockDir, fmt.Sprintf("%012d.block", record.Height))
					data, err := os.ReadFile(path)
					if limited && record.Height != 13 {
						if !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("evicted block %d: %v", record.Height, err)
						}
					} else if err != nil || !bytes.Equal(data, record.Bytes) {
						t.Fatalf("block %d bytes mismatch or read error: %v", record.Height, err)
					}
				}
				if result.BlocksWritten != 12 || result.BytesWritten != wantBytes {
					t.Fatalf("result = %+v, want 12 blocks and %d bytes", result, wantBytes)
				}
			})
		}
	}
}

func TestHydrateWindowStopsOnCancellationAndErrors(t *testing.T) {
	store, key, segments, _ := hydrateFixture(t, 12)
	fetchErr := errors.New("fetch failed")
	for _, mode := range []string{"cancel", "fetch", "decode", "later-fetch", "later-decode", "write"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			trigger := make(chan struct{})
			started := make(chan struct{}, len(segments))
			exited := make(chan struct{}, len(segments))
			failureKey := segments[0].Key
			if strings.HasPrefix(mode, "later-") {
				failureKey = segments[1].Key
			}
			wrapped := hydrateGetStore{ObjectStore: store, get: func(ctx context.Context, objectKey string) ([]byte, error) {
				if objectKey == key {
					return store.Get(ctx, objectKey)
				}
				started <- struct{}{}
				defer func() { exited <- struct{}{} }()
				if objectKey == failureKey && mode != "cancel" {
					select {
					case <-trigger:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					switch mode {
					case "fetch", "later-fetch":
						return nil, fetchErr
					case "decode", "later-decode":
						return []byte("corrupt"), nil
					case "write":
						return store.Get(ctx, objectKey)
					}
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			cacheDir := t.TempDir()
			if mode == "write" {
				path := filepath.Join(cacheDir, "chains", testChainID, "blocks", "000000000001.block")
				if err := os.MkdirAll(path, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() {
				_, err := Hydrate(ctx, wrapped, HydrateOptions{ManifestKey: key, CacheDir: cacheDir, Concurrency: 3})
				done <- err
				close(done)
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("hydrate did not stop after cancellation")
				}
			})
			for range 3 {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("workers did not start")
				}
			}
			if mode == "cancel" {
				cancel()
			} else {
				close(trigger)
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("hydrate succeeded despite cancellation or error")
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v, want context.Canceled", err)
				}
				if mode == "fetch" && !errors.Is(err, fetchErr) {
					t.Fatalf("error = %v, want fetch error", err)
				}
				if mode == "decode" && !strings.Contains(err.Error(), "segment size mismatch") {
					t.Fatalf("error = %v, want decode error", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("hydrate deadlocked after cancellation or error")
			}
			if len(exited) != 3 || len(started) != 0 {
				t.Fatalf("workers still active or fetched beyond window: exited=%d, extra starts=%d", len(exited), len(started))
			}
		})
	}
}
