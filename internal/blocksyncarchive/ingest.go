package blocksyncarchive

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
)

type ValidationMode string

const (
	ValidationStorageOnly  ValidationMode = "storage-only"
	ValidationCheckpoint   ValidationMode = "checkpoint"
	ValidationValidatorSet ValidationMode = "validator-set"
)

var errValidationSource = errors.New("validation source unavailable")
var errPredecessorMismatch = errors.New("predecessor mismatch")

type pendingConflictError struct {
	height int64
	peerID PeerID
	err    error
}

func (e *pendingConflictError) Error() string { return e.err.Error() }
func (e *pendingConflictError) Unwrap() error { return e.err }

type IngestOptions struct {
	ChainID            string
	StartHeight        int64
	Predecessor        *types.Block // Trusted archived tip for an empty hot store.
	SafetyWindow       int64
	Validation         ValidationMode
	Checkpoints        map[int64]string
	ValidatorSets      map[int64]*types.ValidatorSet
	ValidatorSetSource ValidatorSetSource
}

type ValidatorSetSource interface {
	ValidatorSet(height int64) (*types.ValidatorSet, error)
}

type IngestResult struct {
	Accepted              int64
	Persisted             int64
	HotBase               int64
	HotHeight             int64
	ReadyToArchiveThrough int64
}

// maxFetchedValidatorSets bounds the runtime cache of validator sets fetched
// from ValidatorSetSource so a long-running node cannot accumulate one entry
// per validated height indefinitely.
const maxFetchedValidatorSets = 256

type HotIngestor struct {
	// mu serializes ingestion writes (Submit, persistPending) while allowing
	// concurrent readers (LoadBlock, AdvertisedRange, NextHeight, BlockReader)
	// so the reactor's p2p Receive goroutine isn't blocked by SaveBlock/leveldb
	// writes. Mutations to i.pending and i.fetchedSets only happen under the
	// write lock; reads under the read lock observe a consistent snapshot.
	mu             sync.RWMutex
	store          *store.BlockStore
	opts           IngestOptions
	pending        *types.Block
	pendingPeer    PeerID
	fetchedSets    map[int64]*types.ValidatorSet
	fetchedHeights []int64
}

type HotBlockReader struct {
	ingestor *HotIngestor
}

func NewHotIngestor(blockStore *store.BlockStore, opts IngestOptions) (*HotIngestor, error) {
	if blockStore == nil {
		return nil, errors.New("block store is required")
	}
	if opts.ChainID == "" {
		return nil, errors.New("chain ID is required")
	}
	if height := blockStore.Height(); height > 0 {
		tip := blockStore.LoadBlock(height)
		if tip == nil || tip.ChainID != opts.ChainID {
			return nil, fmt.Errorf("hot store tip %d does not match chain %q", height, opts.ChainID)
		}
	}
	if opts.StartHeight < 0 {
		return nil, errors.New("start height cannot be negative")
	}
	if tip := opts.Predecessor; tip != nil {
		if blockStore.Height() != 0 || tip.Height <= 0 || tip.ChainID != opts.ChainID || len(tip.Hash()) == 0 {
			return nil, errors.New("invalid archive predecessor for empty hot store")
		}
		if opts.StartHeight != 0 && opts.StartHeight != tip.Height+1 {
			return nil, errors.New("start height must follow archive predecessor")
		}
		opts.StartHeight = tip.Height + 1
	}
	if opts.StartHeight <= 0 {
		opts.StartHeight = blockStore.Height() + 1
		if opts.StartHeight == 1 && blockStore.Base() > 0 {
			opts.StartHeight = blockStore.Height() + 1
		}
	}
	if opts.Validation == "" {
		opts.Validation = ValidationStorageOnly
	}
	switch opts.Validation {
	case ValidationStorageOnly:
	case ValidationCheckpoint:
		if len(opts.Checkpoints) == 0 {
			return nil, errors.New("checkpoint validation requires at least one checkpoint")
		}
		normalized, err := normalizeCheckpoints(opts.Checkpoints)
		if err != nil {
			return nil, err
		}
		opts.Checkpoints = normalized
	case ValidationValidatorSet:
		if len(opts.ValidatorSets) == 0 && opts.ValidatorSetSource == nil {
			return nil, errors.New("validator-set validation requires at least one validator set or validator set source")
		}
		if err := validateValidatorSets(opts.ValidatorSets); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported validation mode %q", opts.Validation)
	}
	if opts.SafetyWindow < 0 {
		return nil, errors.New("safety window cannot be negative")
	}
	ingestor := &HotIngestor{store: blockStore, opts: opts}
	if opts.Predecessor != nil {
		if err := ingestor.validateBlock(opts.Predecessor); err != nil {
			return nil, fmt.Errorf("archive predecessor: %w", err)
		}
	}
	return ingestor, nil
}

func (i *HotIngestor) NextHeight() int64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.nextHeightLocked()
}

func (i *HotIngestor) nextHeightLocked() int64 {
	if i.pending != nil {
		return i.pending.Height + 1
	}
	if height := i.store.Height(); height > 0 {
		return height + 1
	}
	return i.opts.StartHeight
}

func (i *HotIngestor) AdvertisedRange() PeerRange {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.store.Height() == 0 {
		return PeerRange{}
	}
	return PeerRange{Base: i.store.Base(), Height: i.store.Height()}
}

func (i *HotIngestor) LoadBlock(height int64) *types.Block {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.store.LoadBlock(height)
}

// WithHotStore retains an exclusive lock because callers may perform writes
// (compaction/pruning) on the underlying block store.
func (i *HotIngestor) WithHotStore(fn func(*store.BlockStore) error) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return fn(i.store)
}

func (i *HotIngestor) BlockReader() *HotBlockReader {
	return &HotBlockReader{ingestor: i}
}

func (r *HotBlockReader) Base() int64 {
	r.ingestor.mu.RLock()
	defer r.ingestor.mu.RUnlock()
	return r.ingestor.store.Base()
}

func (r *HotBlockReader) Height() int64 {
	r.ingestor.mu.RLock()
	defer r.ingestor.mu.RUnlock()
	return r.ingestor.store.Height()
}

func (r *HotBlockReader) LoadBlock(height int64) *types.Block {
	r.ingestor.mu.RLock()
	defer r.ingestor.mu.RUnlock()
	return r.ingestor.store.LoadBlock(height)
}

func (*HotBlockReader) Close() error {
	return nil
}

func (i *HotIngestor) Submit(block *types.Block) (IngestResult, error) {
	return i.submit(block, "")
}

func (i *HotIngestor) submit(block *types.Block, peerID PeerID) (IngestResult, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.validateBlock(block); err != nil {
		return IngestResult{}, err
	}
	expected := i.nextHeightLocked()
	if block.Height != expected {
		return IngestResult{}, fmt.Errorf("unexpected block height %d, expected %d", block.Height, expected)
	}
	result := IngestResult{Accepted: block.Height}
	if i.pending == nil {
		previous := i.opts.Predecessor
		if height := i.store.Height(); height > 0 {
			previous = i.store.LoadBlock(height)
		}
		if previous != nil {
			if _, err := i.validatePredecessor(previous, block); err != nil {
				return IngestResult{}, err
			}
		}
		// With no hot or archived predecessor, bootstrap is intentionally unanchored.
		i.pendingPeer = peerID
		i.pending = block
		return i.withStoreState(result), nil
	}
	if err := i.persistPending(block); err != nil {
		if errors.Is(err, errPredecessorMismatch) {
			conflict := &pendingConflictError{height: i.pending.Height, peerID: i.pendingPeer, err: err}
			i.pending, i.pendingPeer = nil, ""
			return IngestResult{}, conflict
		}
		return IngestResult{}, err
	}
	result.Persisted = i.pending.Height
	i.pendingPeer = peerID
	i.pending = block
	return i.withStoreState(result), nil
}

func (i *HotIngestor) discardPending() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.pending, i.pendingPeer = nil, ""
}

func (i *HotIngestor) PendingHeight() int64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.pending == nil {
		return 0
	}
	return i.pending.Height
}

func (i *HotIngestor) validateBlock(block *types.Block) error {
	if block == nil {
		return errors.New("nil block")
	}
	if block.ChainID != i.opts.ChainID {
		return fmt.Errorf("block %d chain ID %q, expected %q", block.Height, block.ChainID, i.opts.ChainID)
	}
	if block.Height <= 0 {
		return fmt.Errorf("invalid block height %d", block.Height)
	}
	if len(block.Hash()) == 0 {
		return fmt.Errorf("block %d has empty hash", block.Height)
	}
	if i.opts.Validation == ValidationCheckpoint {
		if want, ok := i.opts.Checkpoints[block.Height]; ok {
			got := strings.ToLower(hex.EncodeToString(block.Hash()))
			if got != want {
				return fmt.Errorf("block %d checkpoint hash mismatch: got %s want %s", block.Height, got, want)
			}
		}
	}
	if i.opts.Validation == ValidationValidatorSet {
		vals := i.opts.ValidatorSets[block.Height]
		if vals == nil && i.opts.ValidatorSetSource != nil {
			var err error
			vals, err = i.validatorSetFor(block.Height)
			if err != nil {
				return err
			}
		}
		if vals != nil {
			if !bytes.Equal(block.ValidatorsHash, vals.Hash()) {
				return fmt.Errorf("block %d validators hash mismatch", block.Height)
			}
		}
	}
	return nil
}

func normalizeCheckpoints(checkpoints map[int64]string) (map[int64]string, error) {
	normalized := make(map[int64]string, len(checkpoints))
	for height, hash := range checkpoints {
		if height <= 0 {
			return nil, fmt.Errorf("invalid checkpoint height %d", height)
		}
		hash = strings.ToLower(strings.TrimSpace(hash))
		if len(hash) != 64 {
			return nil, fmt.Errorf("checkpoint at height %d must be a 64-character hex hash", height)
		}
		if _, err := hex.DecodeString(hash); err != nil {
			return nil, fmt.Errorf("checkpoint at height %d is not valid hex: %w", height, err)
		}
		normalized[height] = hash
	}
	return normalized, nil
}

func (i *HotIngestor) persistPending(next *types.Block) error {
	parts, err := i.validatePredecessor(i.pending, next)
	if err != nil {
		return err
	}
	i.store.SaveBlock(i.pending, parts, next.LastCommit)
	return nil
}

func (i *HotIngestor) validatePredecessor(previous, next *types.Block) (*types.PartSet, error) {
	if next.LastCommit == nil || next.LastCommit.Height != previous.Height {
		return nil, fmt.Errorf("block %d must commit height %d", next.Height, previous.Height)
	}
	if !next.LastBlockID.Equals(next.LastCommit.BlockID) {
		return nil, fmt.Errorf("block %d last block ID differs from last commit", next.Height)
	}
	parts, err := previous.MakePartSet(types.BlockPartSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: make part set for block %d: %v", errValidationSource, previous.Height, err)
	}
	blockID := types.BlockID{Hash: previous.Hash(), PartSetHeader: parts.Header()}
	if !next.LastBlockID.Equals(blockID) {
		return nil, fmt.Errorf("%w: block %d does not reference block %d", errPredecessorMismatch, next.Height, previous.Height)
	}
	if i.opts.Validation == ValidationValidatorSet {
		vals, err := i.validatorSetFor(previous.Height)
		if err != nil {
			return nil, err
		}
		if err := vals.VerifyCommit(i.opts.ChainID, blockID, previous.Height, next.LastCommit); err != nil {
			return nil, fmt.Errorf("verify commit for block %d: %w", previous.Height, err)
		}
	}
	return parts, nil
}

func (i *HotIngestor) validatorSetFor(height int64) (*types.ValidatorSet, error) {
	if vals := i.opts.ValidatorSets[height]; vals != nil {
		return vals, nil
	}
	if vals := i.fetchedSets[height]; vals != nil {
		return vals, nil
	}
	if i.opts.ValidatorSetSource == nil {
		return nil, fmt.Errorf("%w: missing validator set for height %d", errValidationSource, height)
	}
	vals, err := i.opts.ValidatorSetSource.ValidatorSet(height)
	if err != nil {
		return nil, fmt.Errorf("%w: load validator set for height %d: %w", errValidationSource, height, err)
	}
	if vals == nil {
		return nil, fmt.Errorf("%w: validator set source returned nil for height %d", errValidationSource, height)
	}
	if err := vals.ValidateBasic(); err != nil {
		return nil, fmt.Errorf("%w: validator set from source at height %d: %w", errValidationSource, height, err)
	}
	if i.fetchedSets == nil {
		i.fetchedSets = make(map[int64]*types.ValidatorSet)
	}
	if len(i.fetchedHeights) >= maxFetchedValidatorSets {
		evict := i.fetchedHeights[0]
		i.fetchedHeights = i.fetchedHeights[1:]
		delete(i.fetchedSets, evict)
	}
	i.fetchedSets[height] = vals
	i.fetchedHeights = append(i.fetchedHeights, height)
	return vals, nil
}

func validateValidatorSets(sets map[int64]*types.ValidatorSet) error {
	for height, vals := range sets {
		if height <= 0 {
			return fmt.Errorf("invalid validator-set height %d", height)
		}
		if vals == nil {
			return fmt.Errorf("nil validator set at height %d", height)
		}
		if err := vals.ValidateBasic(); err != nil {
			return fmt.Errorf("validator set at height %d: %w", height, err)
		}
	}
	return nil
}

func (i *HotIngestor) withStoreState(result IngestResult) IngestResult {
	result.HotBase = i.store.Base()
	result.HotHeight = i.store.Height()
	result.ReadyToArchiveThrough = i.readyToArchiveThrough()
	return result
}

func (i *HotIngestor) readyToArchiveThrough() int64 {
	height := i.store.Height()
	if height <= i.opts.SafetyWindow {
		return 0
	}
	return height - i.opts.SafetyWindow
}
