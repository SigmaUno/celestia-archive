package blocksyncarchive

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/01builders/cometbft-archive/internal/archive"
)

const DefaultColdSegmentCacheBytes int64 = 64 << 20

var errColdSourceClosed = errors.New("cold block source closed")

type cachedSegment struct {
	key     [32]byte
	records []archive.BlockRecord
	bytes   int64
}

type segmentLoad struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	records []archive.BlockRecord
	err     error
}

// SetSegmentCacheBytes bounds retained records, with conservative allowance for
// allocation and index overhead. Zero disables retention, but still coalesces loads.
func (s *ArchiveBlockSource) SetSegmentCacheBytes(budget int64) error {
	if budget < 0 || budget > 1<<30 {
		return errors.New("segment cache bytes must be between 0 and 1 GiB")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segmentCacheBytes = budget
	s.evictSegments(0)
	entries := make(map[[32]byte]*list.Element, s.segmentLRU.Len())
	for e := s.segmentLRU.Front(); e != nil; e = e.Next() {
		entries[e.Value.(*cachedSegment).key] = e
	}
	s.segments = entries
	return nil
}

func (s *ArchiveBlockSource) evictSegments(incoming int64) {
	for s.segmentBytes+incoming > s.segmentCacheBytes && s.segmentLRU.Len() > 0 {
		oldest := s.segmentLRU.Back()
		entry := oldest.Value.(*cachedSegment)
		delete(s.segments, entry.key)
		s.segmentLRU.Remove(oldest)
		s.segmentBytes -= entry.bytes
	}
}

// Close cancels outstanding segment fetches and releases retained records.
// Reactor shutdown also cancels loads through each request's context.
func (s *ArchiveBlockSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
		return nil
	default:
		close(s.closed)
	}
	for key, load := range s.segmentLoads {
		load.cancel()
		delete(s.segmentLoads, key)
	}
	s.segments = make(map[[32]byte]*list.Element)
	s.segmentLRU.Init()
	s.segmentBytes = 0
	s.cachedManifest = archive.Manifest{}
	s.cachedManifestAt = time.Time{}
	return nil
}

func (s *ArchiveBlockSource) loadSegment(ctx context.Context, segment archive.SegmentManifest) ([]archive.BlockRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Include the entire index so a changed manifest cannot reuse earlier validation.
	identity, err := json.Marshal(segment)
	if err != nil {
		return nil, err
	}
	key := sha256.Sum256(identity)
	s.mu.Lock()
	select {
	case <-s.closed:
		s.mu.Unlock()
		return nil, errColdSourceClosed
	default:
	}
	if element := s.segments[key]; element != nil {
		s.segmentLRU.MoveToFront(element)
		records := element.Value.(*cachedSegment).records
		s.mu.Unlock()
		return records, nil
	}
	load := s.segmentLoads[key]
	if load == nil {
		fetchCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		load = &segmentLoad{done: make(chan struct{}), cancel: cancel}
		s.segmentLoads[key] = load
		go s.fetchSegment(fetchCtx, key, segment, load)
	}
	load.waiters++
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		load.waiters--
		if load.waiters == 0 {
			load.cancel()
			if s.segmentLoads[key] == load {
				delete(s.segmentLoads, key)
			}
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, errColdSourceClosed
	case <-load.done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return load.records, load.err
	}
}

func (s *ArchiveBlockSource) fetchSegment(ctx context.Context, key [32]byte, segment archive.SegmentManifest, load *segmentLoad) {
	defer load.cancel()
	data, err := s.store.Get(ctx, segment.Key)
	var records []archive.BlockRecord
	if err == nil && ctx.Err() == nil {
		records, err = archive.DecodeSegment(data, segment)
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	load.records, load.err = records, err
	if s.segmentLoads[key] == load {
		delete(s.segmentLoads, key)
		if err == nil {
			size := segmentRecordBytes(records)
			if segment.SizeBytes <= s.segmentCacheBytes && size <= s.segmentCacheBytes {
				s.evictSegments(size)
				entry := &cachedSegment{key: key, records: records, bytes: size}
				s.segments[key] = s.segmentLRU.PushFront(entry)
				s.segmentBytes += size
			}
		}
	}
	close(load.done)
}

func segmentRecordBytes(records []archive.BlockRecord) int64 {
	// Charge spare slice capacity and round payload allocations up, plus fixed
	// allowance for the LRU entry, map bucket, key and allocator overhead.
	size := int64(512 + cap(records)*128)
	for _, record := range records {
		size += int64(cap(record.Bytes)+len(record.Hash)) * 2
	}
	return size
}
