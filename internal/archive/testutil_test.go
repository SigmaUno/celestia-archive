package archive

import (
	"fmt"
	"testing"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/store"
	ctypes "github.com/cometbft/cometbft/types"
)

const testChainID = "archive-test-chain"

func makeTestBlock(t *testing.T, height int64) *ctypes.Block {
	t.Helper()
	block := ctypes.MakeBlock(height, ctypes.Data{Txs: []ctypes.Tx{ctypes.Tx(fmt.Sprintf("tx-%d", height))}}, &ctypes.Commit{}, nil)
	block.ChainID = testChainID
	block.ProposerAddress = bytesOf(byte(height), crypto.AddressSize)
	block.ValidatorsHash = bytesOf(0x11, 32)
	block.NextValidatorsHash = bytesOf(0x22, 32)
	block.ConsensusHash = bytesOf(0x33, 32)
	block.Time = block.Time.UTC()
	return block
}

func makeTestRecord(t *testing.T, height int64) BlockRecord {
	t.Helper()
	record, err := BlockToRecord(makeTestBlock(t, height))
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func bytesOf(value byte, size int) []byte {
	bz := make([]byte, size)
	for i := range bz {
		bz[i] = value
	}
	return bz
}

func createBlockStoreFixture(t *testing.T, dir string, heights int) {
	t.Helper()
	db, err := dbm.NewDB("blockstore", dbm.BackendType("goleveldb"), dir)
	if err != nil {
		t.Fatal(err)
	}
	bs := store.NewBlockStore(db)
	defer bs.Close()
	for h := int64(1); h <= int64(heights); h++ {
		block := makeTestBlock(t, h)
		parts, err := block.MakePartSet(ctypes.BlockPartSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		seen := &ctypes.Commit{Height: h, Signatures: []ctypes.CommitSig{}}
		bs.SaveBlock(block, parts, seen)
	}
}
