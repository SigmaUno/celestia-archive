package blocksyncarchive

import (
	"container/list"
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/01builders/cometbft-archive/internal/archive"
	"github.com/cometbft/cometbft/types"
)

var ErrColdBlockNotFound = errors.New("cold block not found")

const DefaultColdManifestCacheTTL = 5 * time.Second

type ColdBlockSource interface {
	AdvertisedRange(ctx context.Context) (PeerRange, error)
	LoadBlock(ctx context.Context, height int64) (*types.Block, error)
}

type ArchiveBlockSource struct {
	store            archive.ObjectStore
	manifestKey      string
	manifestCacheTTL time.Duration

	mu                sync.Mutex
	cachedManifest    archive.Manifest
	cachedManifestAt  time.Time
	segmentCacheBytes int64
	segmentBytes      int64
	segments          map[[32]byte]*list.Element
	segmentLRU        list.List
	segmentLoads      map[[32]byte]*segmentLoad
	closed            chan struct{}
}

func NewArchiveBlockSource(store archive.ObjectStore, manifestKey string) (*ArchiveBlockSource, error) {
	if store == nil {
		return nil, errors.New("object store is required")
	}
	if manifestKey == "" {
		return nil, errors.New("manifest key is required")
	}
	if err := archive.ValidateObjectKey(manifestKey); err != nil {
		return nil, err
	}
	return &ArchiveBlockSource{
		store:             store,
		manifestKey:       manifestKey,
		manifestCacheTTL:  DefaultColdManifestCacheTTL,
		segmentCacheBytes: DefaultColdSegmentCacheBytes,
		segments:          make(map[[32]byte]*list.Element),
		segmentLoads:      make(map[[32]byte]*segmentLoad),
		closed:            make(chan struct{}),
	}, nil
}

func (s *ArchiveBlockSource) SetManifestCacheTTL(ttl time.Duration) error {
	if ttl < 0 {
		return errors.New("manifest cache TTL cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manifestCacheTTL = ttl
	if ttl == 0 {
		s.cachedManifestAt = time.Time{}
	}
	return nil
}

func (s *ArchiveBlockSource) AdvertisedRange(ctx context.Context) (PeerRange, error) {
	manifest, err := s.loadManifest(ctx)
	if errors.Is(err, archive.ErrObjectNotFound) {
		return PeerRange{}, nil
	}
	if err != nil {
		return PeerRange{}, err
	}
	if manifest.LastHeight == 0 {
		return PeerRange{}, nil
	}
	return PeerRange{Base: manifest.FirstHeight, Height: manifest.LastHeight}, nil
}

func (s *ArchiveBlockSource) LoadBlock(ctx context.Context, height int64) (*types.Block, error) {
	if height <= 0 {
		return nil, ErrColdBlockNotFound
	}
	manifest, err := s.loadManifest(ctx)
	if errors.Is(err, archive.ErrObjectNotFound) {
		return nil, ErrColdBlockNotFound
	}
	if err != nil {
		return nil, err
	}
	segment, ok := segmentForHeight(manifest.Segments, height)
	if !ok {
		return nil, ErrColdBlockNotFound
	}
	records, err := s.loadSegment(ctx, segment)
	if err != nil {
		return nil, err
	}
	return archive.RecordToBlock(records[height-segment.FirstHeight])
}

func (s *ArchiveBlockSource) loadManifest(ctx context.Context) (archive.Manifest, error) {
	if err := ctx.Err(); err != nil {
		return archive.Manifest{}, err
	}
	now := time.Now()
	s.mu.Lock()
	select {
	case <-s.closed:
		s.mu.Unlock()
		return archive.Manifest{}, errColdSourceClosed
	default:
	}
	if !s.cachedManifestAt.IsZero() && s.manifestCacheTTL > 0 && now.Sub(s.cachedManifestAt) < s.manifestCacheTTL {
		manifest := s.cachedManifest
		s.mu.Unlock()
		return manifest, nil
	}
	s.mu.Unlock()

	manifest, err := archive.LoadManifest(ctx, s.store, s.manifestKey)
	if err != nil {
		return archive.Manifest{}, err
	}
	s.mu.Lock()
	select {
	case <-s.closed:
		s.mu.Unlock()
		return archive.Manifest{}, errColdSourceClosed
	default:
	}
	s.cachedManifest = manifest
	s.cachedManifestAt = now
	s.mu.Unlock()
	return manifest, nil
}

func segmentForHeight(segments []archive.SegmentManifest, height int64) (archive.SegmentManifest, bool) {
	index, ok := slices.BinarySearchFunc(segments, height, func(segment archive.SegmentManifest, target int64) int {
		if target < segment.FirstHeight {
			return 1
		}
		if target > segment.LastHeight {
			return -1
		}
		return 0
	})
	if ok {
		return segments[index], true
	}
	return archive.SegmentManifest{}, false
}

func mergeAdvertisedRanges(cold, hot PeerRange) PeerRange {
	if cold.Height == 0 {
		return hot
	}
	if hot.Height == 0 {
		return cold
	}
	if cold.Height+1 < hot.Base || hot.Height+1 < cold.Base {
		if hot.Height >= cold.Height {
			return hot
		}
		return cold
	}
	base := cold.Base
	if hot.Base < base {
		base = hot.Base
	}
	height := cold.Height
	if hot.Height > height {
		height = hot.Height
	}
	return PeerRange{Base: base, Height: height}
}
