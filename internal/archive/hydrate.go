package archive

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

type InspectSummary struct {
	ManifestKey string `json:"manifest_key"`
	ChainID     string `json:"chain_id"`
	FirstHeight int64  `json:"first_height"`
	LastHeight  int64  `json:"last_height"`
	Segments    int    `json:"segments"`
	Blocks      int    `json:"blocks"`
	SizeBytes   int64  `json:"size_bytes"`
}

func Inspect(ctx context.Context, store ObjectStore, manifestKey string) (InspectSummary, error) {
	if manifestKey == "" {
		return InspectSummary{}, errors.New("manifest key is required")
	}
	if store == nil {
		return InspectSummary{}, errors.New("object store is required")
	}
	manifest, err := LoadManifest(ctx, store, manifestKey)
	if err != nil {
		return InspectSummary{}, err
	}
	summary := InspectSummary{
		ManifestKey: manifestKey,
		ChainID:     manifest.ChainID,
		FirstHeight: manifest.FirstHeight,
		LastHeight:  manifest.LastHeight,
		Segments:    len(manifest.Segments),
	}
	for _, segment := range manifest.Segments {
		summary.Blocks += len(segment.Blocks)
		summary.SizeBytes += segment.SizeBytes
	}
	return summary, nil
}

func (s InspectSummary) JSON() ([]byte, error) {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

type HydrateOptions struct {
	ManifestKey string
	CacheDir    string
	StartHeight int64
	EndHeight   int64
	MaxBytes    int64
	// Concurrency bounds fetched and decoded segments outstanding until written,
	// including active fetches. Disk writes remain serialized in manifest order
	// so the cache limit and atomic-rename ordering are preserved. Non-positive values
	// select a sensible default (min(GOMAXPROCS, 8)).
	Concurrency int
}

type HydrateResult struct {
	BlocksWritten int
	BytesWritten  int64
	CacheDir      string
}

func Hydrate(ctx context.Context, store ObjectStore, opts HydrateOptions) (HydrateResult, error) {
	if opts.ManifestKey == "" {
		return HydrateResult{}, errors.New("manifest key is required")
	}
	if opts.CacheDir == "" {
		return HydrateResult{}, errors.New("cache-dir is required")
	}
	if opts.MaxBytes < 0 {
		return HydrateResult{}, errors.New("max cache bytes cannot be negative")
	}
	if store == nil {
		return HydrateResult{}, errors.New("object store is required")
	}
	manifest, err := LoadManifest(ctx, store, opts.ManifestKey)
	if err != nil {
		return HydrateResult{}, err
	}
	start, end := opts.StartHeight, opts.EndHeight
	if start == 0 {
		start = manifest.FirstHeight
	}
	if end == 0 {
		end = manifest.LastHeight
	}
	if start < manifest.FirstHeight || end > manifest.LastHeight || end < start {
		return HydrateResult{}, fmt.Errorf("hydrate range %d-%d outside archive range %d-%d", start, end, manifest.FirstHeight, manifest.LastHeight)
	}
	if err := ctx.Err(); err != nil {
		return HydrateResult{}, err
	}
	blockDir := filepath.Join(opts.CacheDir, "chains", manifest.ChainID, "blocks")
	if err := os.MkdirAll(blockDir, 0o755); err != nil {
		return HydrateResult{}, err
	}
	result := HydrateResult{CacheDir: opts.CacheDir}

	// Collect segments that overlap the requested range so workers and the
	// writer agree on iteration order.
	var relevant []int
	for i, segment := range manifest.Segments {
		if segment.LastHeight < start || segment.FirstHeight > end {
			continue
		}
		relevant = append(relevant, i)
	}
	if len(relevant) == 0 {
		return result, nil
	}

	type segResult struct {
		records []BlockRecord
		err     error
	}

	workers := segmentConcurrency(opts.Concurrency)
	if workers > len(relevant) {
		workers = len(relevant)
	}

	// A slot is reused only after the ordered writer finishes its segment.
	window := make(chan struct{}, workers)
	results := make([]chan segResult, workers)
	for i := range results {
		results[i] = make(chan segResult, 1)
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for slot := range jobs {
				resultCh := results[slot%workers]
				if err := workerCtx.Err(); err != nil {
					resultCh <- segResult{err: err}
					continue
				}
				segment := manifest.Segments[relevant[slot]]
				data, err := store.Get(workerCtx, segment.Key)
				if err != nil {
					resultCh <- segResult{err: err}
					cancel()
					continue
				}
				records, err := DecodeSegment(data, segment)
				if err != nil {
					resultCh <- segResult{err: err}
					cancel()
					continue
				}
				resultCh <- segResult{records: records}
			}
		})
	}

	// Submit jobs in a separate goroutine so the main goroutine can consume
	// results in order without deadlocking on a full job channel.
	wg.Go(func() {
		defer close(jobs)
		for slot := range relevant {
			select {
			case <-workerCtx.Done():
				return
			case window <- struct{}{}:
			}
			select {
			case <-workerCtx.Done():
				return
			case jobs <- slot:
			}
		}
	})

	// Consume results in manifest order and perform disk writes sequentially so
	// enforceCacheLimit observes a deterministic on-disk state after every
	// block.
	var writeErr error
	for slot := range relevant {
		var res segResult
		select {
		case <-ctx.Done():
			writeErr = ctx.Err()
		case res = <-results[slot%workers]:
		}
		if writeErr != nil {
			cancel()
			break
		}
		if res.err != nil {
			writeErr = res.err
			cancel()
			break
		}
		for _, record := range res.records {
			if err := ctx.Err(); err != nil {
				writeErr = err
				break
			}
			if record.Height < start || record.Height > end {
				continue
			}
			path := filepath.Join(blockDir, fmt.Sprintf("%012d.block", record.Height))
			if err := atomicWriteFile(path, record.Bytes, 0o600); err != nil {
				writeErr = err
				break
			}
			result.BlocksWritten++
			result.BytesWritten += int64(len(record.Bytes))
			if opts.MaxBytes > 0 {
				if err := enforceCacheLimit(blockDir, opts.MaxBytes); err != nil {
					writeErr = err
					break
				}
			}
		}
		if writeErr != nil {
			cancel()
			break
		}
		<-window
	}

	cancel()
	wg.Wait()

	if writeErr != nil {
		return HydrateResult{}, writeErr
	}
	return result, nil
}

func atomicWriteFile(target string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(target)
	tmp, err := os.CreateTemp(dir, ".hydrate-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, target)
}

func enforceCacheLimit(blockDir string, maxBytes int64) error {
	entries, err := os.ReadDir(blockDir)
	if err != nil {
		return err
	}
	type fileInfo struct {
		path string
		name string
		size int64
	}
	var files []fileInfo
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !isHydratedBlockFile(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		path := filepath.Join(blockDir, entry.Name())
		files = append(files, fileInfo{path: path, name: entry.Name(), size: info.Size()})
		total += info.Size()
	}
	slices.SortFunc(files, func(a, b fileInfo) int {
		return cmp.Compare(a.name, b.name)
	})
	for _, file := range files {
		if total <= maxBytes {
			return nil
		}
		if err := os.Remove(file.path); err != nil {
			return err
		}
		total -= file.size
	}
	return nil
}

func isHydratedBlockFile(name string) bool {
	if len(name) != len("000000000001.block") || name[len(name)-len(".block"):] != ".block" {
		return false
	}
	for _, ch := range name[:12] {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}
