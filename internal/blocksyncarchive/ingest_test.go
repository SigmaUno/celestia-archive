package blocksyncarchive

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/crypto"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
)

const ingestTestChainID = "archive-blocksync-test"

func TestHotIngestorPersistsCommittedPendingBlock(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:      ingestTestChainID,
		StartHeight:  1,
		SafetyWindow: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := ingestor.Submit(makeIngestBlock(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	if first.Persisted != 0 || first.HotHeight != 0 || ingestor.PendingHeight() != 1 {
		t.Fatalf("unexpected first ingest result: %+v pending=%d", first, ingestor.PendingHeight())
	}
	second, err := ingestor.Submit(makeIngestBlock(t, 2))
	if err != nil {
		t.Fatal(err)
	}
	if second.Persisted != 1 || second.HotBase != 1 || second.HotHeight != 1 {
		t.Fatalf("unexpected second ingest result: %+v", second)
	}
	if got := blockStore.LoadBlock(1); got == nil || got.Height != 1 {
		t.Fatalf("block 1 was not persisted: %+v", got)
	}
	if advertised := ingestor.AdvertisedRange(); advertised.Base != 1 || advertised.Height != 1 {
		t.Fatalf("unexpected advertised range: %+v", advertised)
	}
	third, err := ingestor.Submit(makeIngestBlock(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	if third.Persisted != 2 || third.ReadyToArchiveThrough != 1 {
		t.Fatalf("unexpected third ingest result: %+v", third)
	}
}

func TestHotIngestorRejectsNonContiguousAndWrongChain(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(makeIngestBlock(t, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(makeIngestBlock(t, 3)); err == nil {
		t.Fatal("expected non-contiguous height error")
	}
	wrongChain := makeIngestBlock(t, 2)
	wrongChain.ChainID = "other-chain"
	if _, err := ingestor.Submit(wrongChain); err == nil {
		t.Fatal("expected wrong chain error")
	}
}

func TestHotIngestorRejectsInvalidCommitHeight(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(makeIngestBlock(t, 1)); err != nil {
		t.Fatal(err)
	}
	next := makeIngestBlock(t, 2)
	next.LastCommit.Height = 99
	if _, err := ingestor.Submit(next); err == nil {
		t.Fatal("expected invalid commit height error")
	}
}

func TestHotIngestorRejectsNilCommitForPendingBlock(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(makeIngestBlock(t, 1)); err != nil {
		t.Fatal(err)
	}
	next := makeIngestBlock(t, 2)
	next.LastCommit = nil
	if _, err := ingestor.Submit(next); err == nil {
		t.Fatal("expected nil commit error")
	}
}

func TestHotIngestorRejectsCommitForDifferentBlock(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(makeIngestBlock(t, 1)); err != nil {
		t.Fatal(err)
	}
	next := makeIngestBlock(t, 2)
	next.LastCommit.BlockID.Hash = bytesOf(0x99, 32)
	if _, err := ingestor.Submit(next); err == nil {
		t.Fatal("expected mismatched commit block ID error")
	}
}

func TestHotIngestorRejectsCommitForDifferentPartSet(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(makeIngestBlock(t, 1)); err != nil {
		t.Fatal(err)
	}
	next := makeIngestBlock(t, 2)
	next.LastCommit.BlockID.PartSetHeader.Hash = bytesOf(0x88, 32)
	if _, err := ingestor.Submit(next); err == nil {
		t.Fatal("expected mismatched commit part set error")
	}
}

func TestHotIngestorCheckpointValidation(t *testing.T) {
	blockStore := newTestBlockStore(t)
	checkpointed := makeIngestBlock(t, 1)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:     ingestTestChainID,
		StartHeight: 1,
		Validation:  ValidationCheckpoint,
		Checkpoints: map[int64]string{
			1: hex.EncodeToString(checkpointed.Hash()),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(checkpointed); err != nil {
		t.Fatal(err)
	}
}

func TestHotIngestorRejectsCheckpointMismatch(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:     ingestTestChainID,
		StartHeight: 1,
		Validation:  ValidationCheckpoint,
		Checkpoints: map[int64]string{
			1: hex.EncodeToString(bytesOf(0xff, 32)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(makeIngestBlock(t, 1)); err == nil {
		t.Fatal("expected checkpoint mismatch")
	}
}

func TestHotIngestorRequiresValidCheckpoints(t *testing.T) {
	blockStore := newTestBlockStore(t)
	if _, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:     ingestTestChainID,
		Validation:  ValidationCheckpoint,
		Checkpoints: map[int64]string{0: "not-hex"},
	}); err == nil {
		t.Fatal("expected invalid checkpoint error")
	}
	if _, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:    ingestTestChainID,
		Validation: ValidationCheckpoint,
	}); err == nil {
		t.Fatal("expected missing checkpoint error")
	}
}

func TestHotIngestorRejectsNegativeStartHeight(t *testing.T) {
	blockStore := newTestBlockStore(t)
	if _, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:     ingestTestChainID,
		StartHeight: -1,
	}); err == nil {
		t.Fatal("expected negative start height error")
	}
}

func TestHotIngestorValidatorSetValidation(t *testing.T) {
	blockStore := newTestBlockStore(t)
	vals, privVals := types.RandValidatorSet(4, 1)
	first := makeSignedIngestBlock(t, 1, vals, nil)
	second := makeSignedIngestBlock(t, 2, vals, makeCommitForBlock(t, first, vals, privVals, ingestTestChainID))
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:       ingestTestChainID,
		StartHeight:   1,
		Validation:    ValidationValidatorSet,
		ValidatorSets: map[int64]*types.ValidatorSet{1: vals},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, submitErr := ingestor.Submit(first); submitErr != nil {
		t.Fatal(submitErr)
	}
	result, err := ingestor.Submit(second)
	if err != nil {
		t.Fatal(err)
	}
	if result.Persisted != 1 || blockStore.LoadBlock(1) == nil {
		t.Fatalf("expected verified block 1 to persist: %+v", result)
	}
}

func TestHotIngestorValidatorSetValidationLoadsFromSource(t *testing.T) {
	blockStore := newTestBlockStore(t)
	vals, privVals := types.RandValidatorSet(4, 1)
	first := makeSignedIngestBlock(t, 1, vals, nil)
	second := makeSignedIngestBlock(t, 2, vals, makeCommitForBlock(t, first, vals, privVals, ingestTestChainID))
	source := &mockValidatorSetSource{sets: map[int64]*types.ValidatorSet{
		1: vals,
		2: vals,
	}}
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:            ingestTestChainID,
		StartHeight:        1,
		Validation:         ValidationValidatorSet,
		ValidatorSetSource: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(first); err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(second); err != nil {
		t.Fatal(err)
	}
	if source.loads[1] == 0 || source.loads[2] == 0 {
		t.Fatalf("validator set source was not used: %+v", source.loads)
	}
}

func TestHotIngestorValidatorSetRejectsBadCommit(t *testing.T) {
	blockStore := newTestBlockStore(t)
	vals, privVals := types.RandValidatorSet(4, 1)
	first := makeSignedIngestBlock(t, 1, vals, nil)
	badCommit := makeCommitForBlock(t, first, vals, privVals, ingestTestChainID)
	badCommit.Signatures[0].Signature[0] ^= 0xff
	second := makeSignedIngestBlock(t, 2, vals, badCommit)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:       ingestTestChainID,
		StartHeight:   1,
		Validation:    ValidationValidatorSet,
		ValidatorSets: map[int64]*types.ValidatorSet{1: vals},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(first); err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(second); err == nil {
		t.Fatal("expected bad commit to be rejected")
	}
	if blockStore.LoadBlock(1) != nil {
		t.Fatal("bad commit persisted block")
	}
}

func TestHotIngestorValidatorSetRequiresValidSets(t *testing.T) {
	blockStore := newTestBlockStore(t)
	if _, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:       ingestTestChainID,
		Validation:    ValidationValidatorSet,
		ValidatorSets: map[int64]*types.ValidatorSet{0: types.NewValidatorSet(nil)},
	}); err == nil {
		t.Fatal("expected invalid validator set error")
	}
	if _, err := NewHotIngestor(blockStore, IngestOptions{
		ChainID:    ingestTestChainID,
		Validation: ValidationValidatorSet,
	}); err == nil {
		t.Fatal("expected missing validator set error")
	}
}

func newTestBlockStore(t *testing.T) *store.BlockStore {
	t.Helper()
	db, err := dbm.NewDB("blockstore", dbm.BackendType("goleveldb"), filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return store.NewBlockStore(db)
}

func makeSignedIngestBlock(t *testing.T, height int64, vals *types.ValidatorSet, lastCommit *types.Commit) *types.Block {
	t.Helper()
	if lastCommit == nil {
		lastCommit = &types.Commit{Height: 0, Signatures: []types.CommitSig{}}
	}
	block := types.MakeBlock(height, types.Data{Txs: []types.Tx{types.Tx(fmt.Sprintf("tx-%d", height))}}, lastCommit, nil)
	block.LastBlockID = lastCommit.BlockID
	block.ChainID = ingestTestChainID
	block.ProposerAddress = vals.GetProposer().Address
	block.ValidatorsHash = vals.Hash()
	block.NextValidatorsHash = vals.Hash()
	block.ConsensusHash = bytesOf(0x33, 32)
	return block
}

func makeCommitForBlock(t *testing.T, block *types.Block, vals *types.ValidatorSet, privVals []types.PrivValidator, chainID string) *types.Commit {
	t.Helper()
	parts, err := block.MakePartSet(types.BlockPartSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	blockID := types.BlockID{Hash: block.Hash(), PartSetHeader: parts.Header()}
	signatures := make([]types.CommitSig, vals.Size())
	for index, privVal := range privVals {
		address, _ := vals.GetByIndex(int32(index))
		vote := &cmtproto.Vote{
			Type:             cmtproto.PrecommitType,
			Height:           block.Height,
			Round:            0,
			BlockID:          blockID.ToProto(),
			Timestamp:        time.Unix(block.Height, int64(index)).UTC(),
			ValidatorAddress: address,
			ValidatorIndex:   int32(index),
		}
		if err := privVal.SignVote(chainID, vote); err != nil {
			t.Fatal(err)
		}
		domainVote, err := types.VoteFromProto(vote)
		if err != nil {
			t.Fatal(err)
		}
		signatures[index] = domainVote.CommitSig()
	}
	return &types.Commit{
		Height:     block.Height,
		Round:      0,
		BlockID:    blockID,
		Signatures: signatures,
	}
}

func makeIngestBlock(t *testing.T, height int64) *types.Block {
	t.Helper()
	var lastCommit *types.Commit
	if height > 1 {
		previous := makeIngestBlock(t, height-1)
		parts, err := previous.MakePartSet(types.BlockPartSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		lastCommit = &types.Commit{
			Height: height - 1,
			BlockID: types.BlockID{
				Hash:          previous.Hash(),
				PartSetHeader: parts.Header(),
			},
			Signatures: []types.CommitSig{
				{
					BlockIDFlag:      types.BlockIDFlagCommit,
					ValidatorAddress: bytesOf(byte(height), crypto.AddressSize),
					Timestamp:        time.Unix(height, 0).UTC(),
					Signature:        bytesOf(0x66, 64),
				},
			},
		}
	} else {
		lastCommit = &types.Commit{Height: 0, Signatures: []types.CommitSig{}}
	}
	block := types.MakeBlock(height, types.Data{Txs: []types.Tx{types.Tx(fmt.Sprintf("tx-%d", height))}}, lastCommit, nil)
	block.LastBlockID = lastCommit.BlockID
	block.ChainID = ingestTestChainID
	block.ProposerAddress = bytesOf(byte(height), crypto.AddressSize)
	block.ValidatorsHash = bytesOf(0x11, 32)
	block.NextValidatorsHash = bytesOf(0x22, 32)
	block.ConsensusHash = bytesOf(0x33, 32)
	return block
}

func bytesOf(value byte, size int) []byte {
	return bytes.Repeat([]byte{value}, size)
}

type mockValidatorSetSource struct {
	sets  map[int64]*types.ValidatorSet
	loads map[int64]int
}

func (s *mockValidatorSetSource) ValidatorSet(height int64) (*types.ValidatorSet, error) {
	if s.loads == nil {
		s.loads = make(map[int64]int)
	}
	s.loads[height]++
	return s.sets[height], nil
}

func TestHotIngestorRestartBoundary(t *testing.T) {
	for _, mutation := range []string{"normal", "divergent", "last-block-id", "commit-id", "commit-height", "part-set"} {
		t.Run(mutation, func(t *testing.T) {
			bs := newTestBlockStore(t)
			ingestor, err := NewHotIngestor(bs, IngestOptions{ChainID: ingestTestChainID})
			if err != nil {
				t.Fatal(err)
			}
			for h := int64(1); h <= 2; h++ {
				if _, err := ingestor.Submit(makeIngestBlock(t, h)); err != nil {
					t.Fatal(err)
				}
			}
			restarted, err := NewHotIngestor(bs, IngestOptions{ChainID: ingestTestChainID})
			if err != nil {
				t.Fatal(err)
			}
			next := makeIngestBlock(t, 2)
			switch mutation {
			case "divergent":
				next.LastBlockID.Hash = bytesOf(0x99, 32)
				next.LastCommit.BlockID = next.LastBlockID
			case "last-block-id":
				next.LastBlockID = types.BlockID{}
			case "commit-id":
				next.LastCommit.BlockID = types.BlockID{}
			case "commit-height":
				next.LastCommit.Height = 99
			case "part-set":
				next.LastBlockID.PartSetHeader.Total++
				next.LastCommit.BlockID = next.LastBlockID
			}
			_, err = restarted.Submit(next)
			if mutation != "normal" {
				if err == nil || restarted.PendingHeight() != 0 || bs.Height() != 1 {
					t.Fatalf("divergent restart accepted: %v", err)
				}
				if _, err := restarted.Submit(makeIngestBlock(t, 2)); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if _, err := restarted.Submit(makeIngestBlock(t, 3)); err != nil {
				t.Fatal(err)
			}
			if bs.Height() != 2 {
				t.Fatalf("persisted height = %d", bs.Height())
			}
			if _, err := NewHotIngestor(bs, IngestOptions{ChainID: "different-chain"}); err == nil {
				t.Fatal("accepted wrong hot chain")
			}
		})
	}
}

func TestHotIngestorAdjacentLastBlockID(t *testing.T) {
	i, err := NewHotIngestor(newTestBlockStore(t), IngestOptions{ChainID: ingestTestChainID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := i.Submit(makeIngestBlock(t, 1)); err != nil {
		t.Fatal(err)
	}
	next := makeIngestBlock(t, 2)
	next.LastBlockID = types.BlockID{}
	if _, err := i.Submit(next); err == nil {
		t.Fatal("accepted inconsistent predecessor IDs")
	}
	if i.PendingHeight() != 1 {
		t.Fatal("unambiguous successor error discarded pending")
	}
	if _, err := i.Submit(makeIngestBlock(t, 2)); err != nil {
		t.Fatal(err)
	}
}

func TestHotIngestorUnanchoredBootstrap(t *testing.T) {
	i, err := NewHotIngestor(newTestBlockStore(t), IngestOptions{ChainID: ingestTestChainID, StartHeight: 10})
	if err != nil {
		t.Fatal(err)
	}
	for h := int64(10); h <= 11; h++ {
		if _, err := i.Submit(makeIngestBlock(t, h)); err != nil {
			t.Fatal(err)
		}
	}
	if i.AdvertisedRange() != (PeerRange{Base: 10, Height: 10}) {
		t.Fatal(i.AdvertisedRange())
	}
}

func TestHotIngestorRestartVerifiesCommit(t *testing.T) {
	bs := newTestBlockStore(t)
	vals, keys := types.RandValidatorSet(4, 1)
	first := makeSignedIngestBlock(t, 1, vals, nil)
	second := makeSignedIngestBlock(t, 2, vals, makeCommitForBlock(t, first, vals, keys, ingestTestChainID))
	opts := IngestOptions{ChainID: ingestTestChainID, Validation: ValidationValidatorSet, ValidatorSets: map[int64]*types.ValidatorSet{1: vals, 2: vals}}
	i, err := NewHotIngestor(bs, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []*types.Block{first, second} {
		if _, err := i.Submit(b); err != nil {
			t.Fatal(err)
		}
	}
	i, err = NewHotIngestor(bs, opts)
	if err != nil {
		t.Fatal(err)
	}
	badCommit := makeCommitForBlock(t, first, vals, keys, ingestTestChainID)
	badCommit.Signatures[0].Signature[0] ^= 0xff
	bad := makeSignedIngestBlock(t, 2, vals, badCommit)
	if _, err := i.Submit(bad); err == nil {
		t.Fatal("restart accepted forged commit")
	}
	if _, err := i.Submit(second); err != nil {
		t.Fatal(err)
	}
}

func TestHotIngestorArchivePredecessorValidation(t *testing.T) {
	vals, keys := types.RandValidatorSet(4, 1)
	tip := makeSignedIngestBlock(t, 5, vals, nil)
	next := makeSignedIngestBlock(t, 6, vals, makeCommitForBlock(t, tip, vals, keys, ingestTestChainID))
	for _, scenario := range []string{"valid", "wrong-chain", "wrong-start", "checkpoint", "forged-commit"} {
		t.Run(scenario, func(t *testing.T) {
			opts := IngestOptions{ChainID: ingestTestChainID, Predecessor: tip, Validation: ValidationValidatorSet, ValidatorSets: map[int64]*types.ValidatorSet{5: vals, 6: vals}}
			switch scenario {
			case "wrong-chain":
				opts.ChainID = "different-chain"
			case "wrong-start":
				opts.StartHeight = 8
			case "checkpoint":
				opts.Validation = ValidationCheckpoint
				opts.Checkpoints = map[int64]string{5: hex.EncodeToString(bytesOf(0x99, 32))}
			}
			i, err := NewHotIngestor(newTestBlockStore(t), opts)
			if scenario == "wrong-chain" || scenario == "wrong-start" || scenario == "checkpoint" {
				if err == nil {
					t.Fatal("accepted invalid archive anchor")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "forged-commit" {
				commit := makeCommitForBlock(t, tip, vals, keys, ingestTestChainID)
				commit.Signatures[0].Signature[0] ^= 0xff
				bad := makeSignedIngestBlock(t, 6, vals, commit)
				if _, err := i.Submit(bad); err == nil {
					t.Fatal("accepted forged archive boundary commit")
				}
			}
			if _, err := i.Submit(next); err != nil {
				t.Fatal(err)
			}
		})
	}
}
