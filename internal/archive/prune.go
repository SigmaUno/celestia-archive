package archive

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"time"

	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
)

type PruneHotOptions struct {
	ManifestKey            string
	RetainBlocks           int64
	EvidenceMaxAgeBlocks   int64
	EvidenceMaxAgeDuration time.Duration
}

type PruneHotResult struct {
	BaseBefore      int64
	BaseAfter       int64
	Head            int64
	ArchivedThrough int64
	PruneToHeight   int64
	Pruned          uint64
	EvidenceRetains int64
}

// PruneVerifiedHotStore verifies the whole archive and then prunes the hot
// store through the verified height.
func PruneVerifiedHotStore(ctx context.Context, blockStore *store.BlockStore, objectStore ObjectStore, opts PruneHotOptions) (PruneHotResult, error) {
	if blockStore == nil {
		return PruneHotResult{}, errors.New("block store is required")
	}
	if err := validatePruneHotOptions(opts); err != nil {
		return PruneHotResult{}, err
	}
	verified, err := VerifyArchiveForPrune(ctx, objectStore, opts.ManifestKey, nil)
	if err != nil {
		return PruneHotResult{}, err
	}
	return PruneHotStoreThrough(blockStore, opts, verified)
}

// VerifiedArchive binds pruning authority to a fully verified manifest snapshot.
// Its contents are private so callers cannot alter the verified evidence.
type VerifiedArchive struct {
	manifestKey string
	manifest    Manifest
}

// VerifyArchiveForPrune verifies a single manifest snapshot. Previous evidence
// skips object reads only for an unchanged prefix of that manifest.
// An empty manifest returns nil evidence.
func VerifyArchiveForPrune(ctx context.Context, objectStore ObjectStore, manifestKey string, previous *VerifiedArchive) (*VerifiedArchive, error) {
	if manifestKey == "" {
		return nil, errors.New("manifest key is required")
	}
	if objectStore == nil {
		return nil, errors.New("object store is required")
	}
	manifest, err := LoadManifest(ctx, objectStore, manifestKey)
	if err != nil {
		return nil, fmt.Errorf("verify archive before prune: %w", err)
	}
	if len(manifest.Segments) == 0 {
		return nil, nil //nolint:nilnil // No archive means no pruning authority.
	}
	var fromHeight int64
	if previous != nil && previous.manifestKey == manifestKey &&
		previous.manifest.ChainID == manifest.ChainID && previous.manifest.FirstHeight == manifest.FirstHeight &&
		len(previous.manifest.Segments) <= len(manifest.Segments) &&
		reflect.DeepEqual(previous.manifest.Segments, manifest.Segments[:len(previous.manifest.Segments)]) {
		fromHeight = previous.manifest.LastHeight + 1
	}
	if _, err := verifyManifest(ctx, objectStore, manifest, VerifyOptions{FromHeight: fromHeight}); err != nil {
		return nil, fmt.Errorf("verify archive before prune: %w", err)
	}
	return &VerifiedArchive{manifestKey: manifestKey, manifest: manifest}, nil
}

// PruneHotStoreThrough compares verified coverage and hashes before deleting.
// Callers must hold exclusive access to the hot store for this entire call;
// this function performs no object store I/O.
func PruneHotStoreThrough(blockStore *store.BlockStore, opts PruneHotOptions, verified *VerifiedArchive) (PruneHotResult, error) {
	if blockStore == nil {
		return PruneHotResult{}, errors.New("block store is required")
	}
	if err := validatePruneHotOptions(opts); err != nil {
		return PruneHotResult{}, err
	}
	if verified == nil || verified.manifestKey != opts.ManifestKey {
		return PruneHotResult{}, errors.New("verified archive snapshot is required for this manifest")
	}
	manifest := verified.manifest
	archivedThrough := manifest.LastHeight
	base := blockStore.Base()
	head := blockStore.Height()
	result := PruneHotResult{
		BaseBefore:      base,
		BaseAfter:       base,
		Head:            head,
		ArchivedThrough: archivedThrough,
	}
	if base == 0 || head == 0 {
		return result, nil
	}
	retainPruneTo := head - opts.RetainBlocks + 1
	archivePruneTo := archivedThrough + 1
	pruneTo := min(retainPruneTo, archivePruneTo)
	if pruneTo <= base {
		return result, nil
	}
	if pruneTo > head {
		pruneTo = head
	}
	if manifest.FirstHeight > base {
		return PruneHotResult{}, fmt.Errorf("archive starts at %d, leaving hot blocks from %d uncovered", manifest.FirstHeight, base)
	}
	latest := blockStore.LoadBlockMeta(head)
	if latest == nil || latest.Header.ChainID != manifest.ChainID {
		return PruneHotResult{}, errors.New("archive chain does not match hot store")
	}
	for _, segment := range manifest.Segments {
		if segment.LastHeight < base {
			continue
		}
		if segment.FirstHeight >= pruneTo {
			break
		}
		for _, index := range segment.Blocks {
			if index.Height < base || index.Height >= pruneTo {
				continue
			}
			meta := blockStore.LoadBlockMeta(index.Height)
			if meta == nil || meta.Header.ChainID != manifest.ChainID || hex.EncodeToString(meta.BlockID.Hash) != index.Hash {
				return PruneHotResult{}, fmt.Errorf("archive does not match hot block %d", index.Height)
			}
		}
	}
	state, err := pruneState(blockStore, opts)
	if err != nil {
		return PruneHotResult{}, err
	}
	pruned, evidencePoint, err := blockStore.PruneBlocks(pruneTo, state)
	if err != nil {
		return PruneHotResult{}, err
	}
	result.PruneToHeight = pruneTo
	result.Pruned = pruned
	result.EvidenceRetains = evidencePoint
	result.BaseAfter = blockStore.Base()
	return result, nil
}

func validatePruneHotOptions(opts PruneHotOptions) error {
	if opts.ManifestKey == "" {
		return errors.New("manifest key is required")
	}
	if opts.RetainBlocks <= 0 {
		return errors.New("retain blocks must be positive")
	}
	return nil
}

func pruneState(blockStore *store.BlockStore, opts PruneHotOptions) (sm.State, error) {
	head := blockStore.Height()
	latest := blockStore.LoadBlock(head)
	if latest == nil {
		return sm.State{}, fmt.Errorf("latest block %d is missing", head)
	}
	params := *types.DefaultConsensusParams()
	if opts.EvidenceMaxAgeBlocks > 0 {
		params.Evidence.MaxAgeNumBlocks = opts.EvidenceMaxAgeBlocks
	}
	if opts.EvidenceMaxAgeDuration > 0 {
		params.Evidence.MaxAgeDuration = opts.EvidenceMaxAgeDuration
	}
	return sm.State{
		ChainID:         latest.ChainID,
		InitialHeight:   blockStore.Base(),
		LastBlockHeight: head,
		LastBlockTime:   latest.Time,
		ConsensusParams: params,
		LastBlockID:     latest.LastBlockID,
		AppHash:         latest.AppHash,
		LastResultsHash: latest.LastResultsHash,
	}, nil
}
