package archive

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type VerifyOptions struct {
	ManifestKey string
	SampleEvery int64
	// Concurrency bounds the worker pool used to verify segments in parallel.
	// Non-positive values select a sensible default (min(GOMAXPROCS, 8)).
	Concurrency int
	// FromHeight skips segments that end below this height. Zero or one
	// verifies every segment in the manifest.
	FromHeight int64
}

type VerifyResult struct {
	SegmentsChecked int
	BlocksChecked   int
	// VerifiedThrough is the manifest last height that this run covered.
	VerifiedThrough int64
}

func Verify(ctx context.Context, store ObjectStore, opts VerifyOptions) (VerifyResult, error) {
	if opts.ManifestKey == "" {
		return VerifyResult{}, errors.New("manifest key is required")
	}
	if opts.SampleEvery < 0 {
		return VerifyResult{}, errors.New("sample every cannot be negative")
	}
	if store == nil {
		return VerifyResult{}, errors.New("object store is required")
	}
	manifest, err := LoadManifest(ctx, store, opts.ManifestKey)
	if err != nil {
		return VerifyResult{}, err
	}
	return verifyManifest(ctx, store, manifest, opts)
}

func verifyManifest(ctx context.Context, store ObjectStore, manifest Manifest, opts VerifyOptions) (VerifyResult, error) {
	if err := ctx.Err(); err != nil {
		return VerifyResult{}, err
	}
	selected := make([]int, 0, len(manifest.Segments))
	for i, segment := range manifest.Segments {
		if segment.LastHeight >= opts.FromHeight {
			selected = append(selected, i)
		}
	}
	if len(selected) == 0 {
		return VerifyResult{VerifiedThrough: manifest.LastHeight}, nil
	}

	workers := segmentConcurrency(opts.Concurrency)
	if workers > len(selected) {
		workers = len(selected)
	}

	errs := make([]error, len(manifest.Segments))
	var blocksChecked int64
	var segmentsChecked int64

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for i := range jobs {
				if err := workerCtx.Err(); err != nil {
					errs[i] = err
					continue
				}
				segment := manifest.Segments[i]
				blocks, err := verifySegment(workerCtx, store, segment, manifest.FirstHeight, opts.SampleEvery)
				if err != nil {
					errs[i] = err
					cancel()
					continue
				}
				atomic.AddInt64(&blocksChecked, int64(blocks))
				atomic.AddInt64(&segmentsChecked, 1)
			}
		})
	}

	for _, i := range selected {
		select {
		case <-workerCtx.Done():
			close(jobs)
			wg.Wait()
			if err := firstSegmentError(errs, ctx.Err()); err != nil {
				return VerifyResult{}, err
			}
			// no errors recorded — context was canceled externally
			return VerifyResult{}, ctx.Err()
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	if err := firstSegmentError(errs, ctx.Err()); err != nil {
		return VerifyResult{}, err
	}
	return VerifyResult{
		SegmentsChecked: int(segmentsChecked),
		BlocksChecked:   int(blocksChecked),
		VerifiedThrough: manifest.LastHeight,
	}, nil
}

// verifySegment performs the per-segment verification work and returns the
// number of blocks counted toward BlocksChecked.
func verifySegment(ctx context.Context, store ObjectStore, segment SegmentManifest, manifestFirstHeight int64, sampleEvery int64) (int, error) {
	info, err := store.Stat(ctx, segment.Key)
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", segment.Key, err)
	}
	if info.Size != segment.SizeBytes {
		return 0, fmt.Errorf("object %s size %d, expected %d", segment.Key, info.Size, segment.SizeBytes)
	}
	data, err := store.Get(ctx, segment.Key)
	if err != nil {
		return 0, fmt.Errorf("get %s: %w", segment.Key, err)
	}
	validateAllBlocks := sampleEvery <= 0
	var validateRecord func(BlockRecord) error
	if validateAllBlocks {
		validateRecord = validateDecodedBlockRecord
	}
	records, err := decodeSegmentRecords(data, segment, validateRecord)
	if err != nil {
		return 0, fmt.Errorf("decode %s: %w", segment.Key, err)
	}
	if validateAllBlocks {
		return len(records), nil
	}
	blocks := 0
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if (record.Height-manifestFirstHeight)%sampleEvery != 0 {
			continue
		}
		if _, err := RecordToBlock(record); err != nil {
			return 0, fmt.Errorf("block %d: %w", record.Height, err)
		}
		blocks++
	}
	return blocks, nil
}
