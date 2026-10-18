package archive

import (
	"context"
	"errors"
	"fmt"
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
	verifiedThrough, err := VerifyArchiveForPrune(ctx, objectStore, opts.ManifestKey, 0)
	if err != nil {
		return PruneHotResult{}, err
	}
	return PruneHotStoreThrough(blockStore, opts, verifiedThrough)
}

// VerifyArchiveForPrune verifies archive segments that end at or above
// fromHeight and returns the manifest height the archive is verified through.
// It only reads object storage, so callers can run it without exclusive
// access to the hot store. Pass the previous result plus one to verify only
// segments archived since the last run.
func VerifyArchiveForPrune(ctx context.Context, objectStore ObjectStore, manifestKey string, fromHeight int64) (int64, error) {
	if manifestKey == "" {
		return 0, errors.New("manifest key is required")
	}
	if objectStore == nil {
		return 0, errors.New("object store is required")
	}
	result, err := Verify(ctx, objectStore, VerifyOptions{ManifestKey: manifestKey, FromHeight: fromHeight})
	if err != nil {
		return 0, fmt.Errorf("verify archive before prune: %w", err)
	}
	if fromHeight <= 1 && result.BlocksChecked == 0 {
		return 0, errors.New("archive verification checked no blocks")
	}
	if result.VerifiedThrough < fromHeight-1 {
		return 0, fmt.Errorf("archive manifest last height %d is below previously verified height %d", result.VerifiedThrough, fromHeight-1)
	}
	return result.VerifiedThrough, nil
}

// PruneHotStoreThrough prunes hot blocks at or below archivedThrough while
// keeping opts.RetainBlocks at the head. archivedThrough must come from
// VerifyArchiveForPrune; this function performs no object store I/O.
func PruneHotStoreThrough(blockStore *store.BlockStore, opts PruneHotOptions, archivedThrough int64) (PruneHotResult, error) {
	if blockStore == nil {
		return PruneHotResult{}, errors.New("block store is required")
	}
	if err := validatePruneHotOptions(opts); err != nil {
		return PruneHotResult{}, err
	}
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
