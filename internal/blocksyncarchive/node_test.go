package blocksyncarchive

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/01builders/cometbft-archive/internal/archive"
	cmtblocksync "github.com/cometbft/cometbft/blocksync"
	cmtcfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/trace"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
	bcproto "github.com/cometbft/cometbft/proto/tendermint/blocksync"
	"github.com/cometbft/cometbft/types"
	"github.com/cometbft/cometbft/version"
)

func TestArchiveNodeMountsOnlyArchiveBlocksyncReactor(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	node, err := NewArchiveNode(ingestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: "tcp://127.0.0.1:0",
		NodeKeyFile:   filepath.Join(t.TempDir(), "node_key.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if node.Switch.Reactor(ReactorName) == nil {
		t.Fatalf("switch missing %s reactor", ReactorName)
	}
	for _, absent := range []string{"CONSENSUS", "MEMPOOL", "EVIDENCE", "STATESYNC"} {
		if node.Switch.Reactor(absent) != nil {
			t.Fatalf("switch unexpectedly mounted %s reactor", absent)
		}
	}
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	if !node.Switch.IsRunning() {
		t.Fatal("switch did not start")
	}
	if err := node.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveNodeCanMountPEXWithoutConsensusReactors(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	node, err := NewArchiveNode(ingestor, nil, NodeOptions{
		ChainID:        ingestTestChainID,
		ListenAddress:  "tcp://127.0.0.1:0",
		NodeKeyFile:    filepath.Join(t.TempDir(), "node_key.json"),
		PEX:            true,
		AddrBookFile:   filepath.Join(t.TempDir(), "addrbook.json"),
		AddrBookStrict: false,
		Seeds:          []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@127.0.0.1:26656"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if node.Switch.Reactor(ReactorName) == nil {
		t.Fatalf("switch missing %s reactor", ReactorName)
	}
	if node.Switch.Reactor("PEX") == nil {
		t.Fatal("switch missing PEX reactor")
	}
	for _, absent := range []string{"CONSENSUS", "MEMPOOL", "EVIDENCE", "STATESYNC"} {
		if node.Switch.Reactor(absent) != nil {
			t.Fatalf("switch unexpectedly mounted %s reactor", absent)
		}
	}
	if string(node.NodeInfo.Channels) != string(append(append([]byte{cmtblocksync.BlocksyncChannel}, compatChannels...), pex.PexChannel)) {
		t.Fatalf("unexpected channels: %v", node.NodeInfo.Channels)
	}
}

func TestArchiveNodeRequiresPEXSeedsOrAddrBook(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewArchiveNode(ingestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: "tcp://127.0.0.1:0",
		NodeKeyFile:   filepath.Join(t.TempDir(), "node_key.json"),
		PEX:           true,
	})
	if err == nil {
		t.Fatal("expected PEX config error")
	}
}

func TestArchiveNodeRejectsNilIngestorBeforeNodeKeySideEffects(t *testing.T) {
	nodeKeyFile := filepath.Join(t.TempDir(), "node_key.json")
	_, err := NewArchiveNode(nil, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: "tcp://127.0.0.1:0",
		NodeKeyFile:   nodeKeyFile,
	})
	if err == nil {
		t.Fatal("expected nil ingestor error")
	}
	if _, statErr := os.Stat(nodeKeyFile); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("node key stat err=%v, want not exist", statErr)
	}
}

func TestArchiveNodeRejectsReactorOptionsBeforeNodeKeySideEffects(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	nodeKeyFile := filepath.Join(t.TempDir(), "node_key.json")
	_, err = NewArchiveNode(ingestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: "tcp://127.0.0.1:0",
		NodeKeyFile:   nodeKeyFile,
		RequestLimit:  MaxRequestLimit + 1,
	})
	if err == nil || !strings.Contains(err.Error(), "request limit") {
		t.Fatalf("NewArchiveNode err=%v, want request limit error", err)
	}
	if _, statErr := os.Stat(nodeKeyFile); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("node key stat err=%v, want not exist", statErr)
	}
}

func TestArchiveNodesSyncBlocksOverP2P(t *testing.T) {
	providerStore := newTestBlockStore(t)
	providerIngestor, err := NewHotIngestor(providerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	for height := int64(1); height <= 5; height++ {
		if _, submitErr := providerIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	if providerStore.Height() != 4 {
		t.Fatalf("provider store height %d, want 4", providerStore.Height())
	}
	providerListen := freeListenAddress(t)
	provider, err := NewArchiveNode(providerIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: providerListen,
		NodeKeyFile:   filepath.Join(t.TempDir(), "provider_key.json"),
		RequestLimit:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := provider.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := provider.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	consumerStore := newTestBlockStore(t)
	consumerIngestor, err := NewHotIngestor(consumerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewArchiveNode(consumerIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: freeListenAddress(t),
		NodeKeyFile:   filepath.Join(t.TempDir(), "consumer_key.json"),
		RequestLimit:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := consumer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := consumer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	providerAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(provider.NodeKey.ID(), providerListen))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := consumer.Switch.DialPeerWithAddress(providerAddr); dialErr != nil {
		t.Fatal(dialErr)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if consumerStore.Height() >= 3 {
			objectStore, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
			if err != nil {
				t.Fatal(err)
			}
			result, err := archive.ArchiveReadyFromHead(contextWithTimeout(t), consumerStore, objectStore, archive.LiveArchiveOptions{
				ChainID:       ingestTestChainID,
				Prefix:        "archive",
				SegmentBlocks: 2,
				Compression:   archive.CompressionGzip,
			}, 1)
			if err != nil {
				t.Fatal(err)
			}
			if result.BlocksArchived < 2 {
				t.Fatalf("archived %d blocks after P2P sync, want at least 2", result.BlocksArchived)
			}
			verify, err := archive.Verify(contextWithTimeout(t), objectStore, archive.VerifyOptions{
				ManifestKey: archive.ManifestKey("archive", ingestTestChainID, archive.DefaultManifest),
			})
			if err != nil {
				t.Fatal(err)
			}
			if verify.BlocksChecked != result.BlocksArchived {
				t.Fatalf("verified %d blocks, archived %d", verify.BlocksChecked, result.BlocksArchived)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("consumer store height %d, want at least 3", consumerStore.Height())
}

func TestArchiveNodeFollowsPeerHeadWithStatusPolling(t *testing.T) {
	providerStore := newTestBlockStore(t)
	providerIngestor, err := NewHotIngestor(providerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	for height := int64(1); height <= 5; height++ {
		if _, submitErr := providerIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	providerListen := freeListenAddress(t)
	provider, err := NewArchiveNode(providerIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: providerListen,
		NodeKeyFile:   filepath.Join(t.TempDir(), "provider_key.json"),
		RequestLimit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := provider.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := provider.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	consumerStore := newTestBlockStore(t)
	consumerIngestor, err := NewHotIngestor(consumerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewArchiveNode(consumerIngestor, nil, NodeOptions{
		ChainID:        ingestTestChainID,
		ListenAddress:  freeListenAddress(t),
		NodeKeyFile:    filepath.Join(t.TempDir(), "consumer_key.json"),
		RequestLimit:   2,
		StatusInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := consumer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := consumer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	providerAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(provider.NodeKey.ID(), providerListen))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := consumer.Switch.DialPeerWithAddress(providerAddr); dialErr != nil {
		t.Fatal(dialErr)
	}
	waitForStoreHeight(t, consumerStore, 3)

	for height := int64(6); height <= 12; height++ {
		if _, submitErr := providerIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	waitForStoreHeight(t, consumerStore, 10)
}

func TestArchiveNodeContinuesAfterProviderChurn(t *testing.T) {
	providerAStore := newTestBlockStore(t)
	providerAIngestor, err := NewHotIngestor(providerAStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	providerBStore := newTestBlockStore(t)
	providerBIngestor, err := NewHotIngestor(providerBStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	for height := int64(1); height <= 5; height++ {
		block := makeIngestBlock(t, height)
		if _, submitErr := providerAIngestor.Submit(block); submitErr != nil {
			t.Fatal(submitErr)
		}
		if _, submitErr := providerBIngestor.Submit(block); submitErr != nil {
			t.Fatal(submitErr)
		}
	}

	providerAListen := freeListenAddress(t)
	providerA, err := NewArchiveNode(providerAIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: providerAListen,
		NodeKeyFile:   filepath.Join(t.TempDir(), "provider_a_key.json"),
		RequestLimit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	providerA.Config.AllowDuplicateIP = true
	if startErr := providerA.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	providerAStopped := false
	t.Cleanup(func() {
		if !providerAStopped {
			if stopErr := providerA.Stop(); stopErr != nil {
				t.Fatal(stopErr)
			}
		}
	})

	providerBListen := freeListenAddress(t)
	providerB, err := NewArchiveNode(providerBIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: providerBListen,
		NodeKeyFile:   filepath.Join(t.TempDir(), "provider_b_key.json"),
		RequestLimit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	providerB.Config.AllowDuplicateIP = true
	if startErr := providerB.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := providerB.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	consumerStore := newTestBlockStore(t)
	consumerIngestor, err := NewHotIngestor(consumerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewArchiveNode(consumerIngestor, nil, NodeOptions{
		ChainID:        ingestTestChainID,
		ListenAddress:  freeListenAddress(t),
		NodeKeyFile:    filepath.Join(t.TempDir(), "consumer_key.json"),
		RequestLimit:   2,
		StatusInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer.Config.AllowDuplicateIP = true
	if startErr := consumer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := consumer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	providerAAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(providerA.NodeKey.ID(), providerAListen))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := consumer.Switch.DialPeerWithAddress(providerAAddr); dialErr != nil {
		t.Fatal(dialErr)
	}
	providerBAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(providerB.NodeKey.ID(), providerBListen))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := consumer.Switch.DialPeerWithAddress(providerBAddr); dialErr != nil {
		t.Fatal(dialErr)
	}

	waitForStoreHeight(t, consumerStore, 3)
	if stopErr := providerA.Stop(); stopErr != nil {
		t.Fatal(stopErr)
	}
	providerAStopped = true

	for height := int64(6); height <= 12; height++ {
		if _, submitErr := providerBIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	waitForStoreHeight(t, consumerStore, 10)
	if best := consumer.Reactor.BestPeerHeight(); best < 11 {
		t.Fatalf("best peer height %d, want at least 11 after provider churn", best)
	}
}

func TestArchiveNodeSyncsFromStockBlocksyncOverP2P(t *testing.T) {
	stockListen := freeListenAddress(t)
	stock := newStockBlocksyncP2PNode(t, stockListen,
		makeIngestBlock(t, 1),
		makeIngestBlock(t, 2),
		makeIngestBlock(t, 3),
		makeIngestBlock(t, 4),
		makeIngestBlock(t, 5),
		makeIngestBlock(t, 6),
		makeIngestBlock(t, 7),
	)
	if err := stock.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stock.Stop(); err != nil {
			t.Fatal(err)
		}
	})

	consumerStore := newTestBlockStore(t)
	consumerIngestor, err := NewHotIngestor(consumerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewArchiveNode(consumerIngestor, nil, NodeOptions{
		ChainID:        ingestTestChainID,
		ListenAddress:  freeListenAddress(t),
		NodeKeyFile:    filepath.Join(t.TempDir(), "consumer_key.json"),
		RequestLimit:   2,
		StatusInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := consumer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := consumer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	stockAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(stock.NodeKey.ID(), stockListen))
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Switch.DialPeerWithAddress(stockAddr); err != nil {
		t.Fatal(err)
	}
	waitForStoreHeight(t, consumerStore, 6)
}

func TestArchiveNodeServesHotBlocksOverP2P(t *testing.T) {
	providerStore := newTestBlockStore(t)
	providerIngestor, err := NewHotIngestor(providerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	for height := int64(1); height <= 4; height++ {
		if _, submitErr := providerIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	providerListen := freeListenAddress(t)
	provider, err := NewArchiveNode(providerIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: providerListen,
		NodeKeyFile:   filepath.Join(t.TempDir(), "provider_key.json"),
		RequestLimit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := provider.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := provider.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	requester := newBlockRequesterP2PNode(t, freeListenAddress(t), 2)
	if startErr := requester.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := requester.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})
	providerAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(provider.NodeKey.ID(), providerListen))
	if err != nil {
		t.Fatal(err)
	}
	if err := requester.Switch.DialPeerWithAddress(providerAddr); err != nil {
		t.Fatal(err)
	}
	select {
	case block := <-requester.Reactor.blocks:
		if block.Height != 2 || block.ChainID != ingestTestChainID {
			t.Fatalf("unexpected served block: height=%d chain=%s", block.Height, block.ChainID)
		}
	case err := <-requester.Reactor.errs:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for archive block response")
	}
}

func TestArchiveNodeAdvertisesColdAndHotBlocksAsOneRange(t *testing.T) {
	coldSource := newColdArchiveSource(t, 1, 3)
	providerStore := newTestBlockStore(t)
	providerIngestor, err := NewHotIngestor(providerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 4})
	if err != nil {
		t.Fatal(err)
	}
	for height := int64(4); height <= 5; height++ {
		if _, submitErr := providerIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	reactor, err := NewReactor(providerIngestor, nil, ReactorOptions{ColdBlockSource: coldSource})
	if err != nil {
		t.Fatal(err)
	}
	if advertised := reactor.AdvertisedRange(); advertised.Base != 1 || advertised.Height != 4 {
		t.Fatalf("advertised range %+v, want 1-4", advertised)
	}
}

func TestArchiveNodeServesColdBlocksOverP2P(t *testing.T) {
	coldSource := newColdArchiveSource(t, 1, 3)
	providerStore := newTestBlockStore(t)
	providerIngestor, err := NewHotIngestor(providerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 4})
	if err != nil {
		t.Fatal(err)
	}
	providerListen := freeListenAddress(t)
	provider, err := NewArchiveNode(providerIngestor, nil, NodeOptions{
		ChainID:         ingestTestChainID,
		ListenAddress:   providerListen,
		NodeKeyFile:     filepath.Join(t.TempDir(), "provider_key.json"),
		RequestLimit:    2,
		ColdBlockSource: coldSource,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := provider.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := provider.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	requester := newBlockRequesterP2PNode(t, freeListenAddress(t), 2)
	if startErr := requester.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := requester.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})
	providerAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(provider.NodeKey.ID(), providerListen))
	if err != nil {
		t.Fatal(err)
	}
	if err := requester.Switch.DialPeerWithAddress(providerAddr); err != nil {
		t.Fatal(err)
	}
	select {
	case block := <-requester.Reactor.blocks:
		if block.Height != 2 || block.ChainID != ingestTestChainID {
			t.Fatalf("unexpected served block: height=%d chain=%s", block.Height, block.ChainID)
		}
	case err := <-requester.Reactor.errs:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for archive cold block response")
	}
}

func TestArchiveNodeFailsOverAfterRealP2PNoBlock(t *testing.T) {
	badListen := freeListenAddress(t)
	badPeer := newNoBlockP2PNode(t, badListen, PeerRange{Base: 1, Height: 4})
	if startErr := badPeer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := badPeer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	goodStore := newTestBlockStore(t)
	goodIngestor, err := NewHotIngestor(goodStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	for height := int64(1); height <= 5; height++ {
		if _, submitErr := goodIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	goodListen := freeListenAddress(t)
	goodPeer, err := NewArchiveNode(goodIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: goodListen,
		NodeKeyFile:   filepath.Join(t.TempDir(), "good_key.json"),
		RequestLimit:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	goodPeer.Config.AllowDuplicateIP = true
	if startErr := goodPeer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := goodPeer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	consumerStore := newTestBlockStore(t)
	consumerIngestor, err := NewHotIngestor(consumerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewArchiveNode(consumerIngestor, nil, NodeOptions{
		ChainID:        ingestTestChainID,
		ListenAddress:  freeListenAddress(t),
		NodeKeyFile:    filepath.Join(t.TempDir(), "consumer_key.json"),
		RequestLimit:   1,
		StatusInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer.Config.AllowDuplicateIP = true
	if startErr := consumer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := consumer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	badAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(badPeer.NodeKey.ID(), badListen))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := consumer.Switch.DialPeerWithAddress(badAddr); dialErr != nil {
		t.Fatal(dialErr)
	}
	waitForNoBlockResponses(t, badPeer.Reactor, 1)

	goodAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(goodPeer.NodeKey.ID(), goodListen))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := consumer.Switch.DialPeerWithAddress(goodAddr); dialErr != nil {
		t.Fatal(dialErr)
	}
	waitForStoreHeight(t, consumerStore, 3)
}

func TestArchiveNodeFailsOverAfterRealP2PRequestTimeout(t *testing.T) {
	silentListen := freeListenAddress(t)
	silentPeer := newSilentBlockP2PNode(t, silentListen, PeerRange{Base: 1, Height: 4})
	if startErr := silentPeer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := silentPeer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	goodStore := newTestBlockStore(t)
	goodIngestor, err := NewHotIngestor(goodStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	for height := int64(1); height <= 5; height++ {
		if _, submitErr := goodIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	goodListen := freeListenAddress(t)
	goodPeer, err := NewArchiveNode(goodIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: goodListen,
		NodeKeyFile:   filepath.Join(t.TempDir(), "good_key.json"),
		RequestLimit:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	goodPeer.Config.AllowDuplicateIP = true
	if startErr := goodPeer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := goodPeer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	consumerStore := newTestBlockStore(t)
	consumerIngestor, err := NewHotIngestor(consumerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewArchiveNode(consumerIngestor, nil, NodeOptions{
		ChainID:        ingestTestChainID,
		ListenAddress:  freeListenAddress(t),
		NodeKeyFile:    filepath.Join(t.TempDir(), "consumer_key.json"),
		RequestLimit:   1,
		RequestTimeout: 500 * time.Millisecond,
		StatusInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer.Config.AllowDuplicateIP = true
	if startErr := consumer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := consumer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	silentAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(silentPeer.NodeKey.ID(), silentListen))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := consumer.Switch.DialPeerWithAddress(silentAddr); dialErr != nil {
		t.Fatal(dialErr)
	}
	waitForSilentBlockRequests(t, silentPeer.Reactor, 1)
	waitForRequestTimeouts(t, consumer.Reactor, 1)

	goodAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(goodPeer.NodeKey.ID(), goodListen))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := consumer.Switch.DialPeerWithAddress(goodAddr); dialErr != nil {
		t.Fatal(dialErr)
	}
	waitForPeerCount(t, consumer.Reactor, 2)
	waitForBestPeerHeight(t, consumer.Reactor, 4)
	waitForStoreHeightWithRequestExpiry(t, consumerStore, consumer.Reactor, 3)
	if timeouts := consumer.Reactor.RequestTimeouts(); timeouts == 0 {
		t.Fatal("expected at least one request timeout")
	}
}

func TestArchiveNodeResumesP2PSyncFromExistingHotStore(t *testing.T) {
	stockListen := freeListenAddress(t)
	stock := newStockBlocksyncP2PNode(t, stockListen,
		makeIngestBlock(t, 1),
		makeIngestBlock(t, 2),
		makeIngestBlock(t, 3),
		makeIngestBlock(t, 4),
		makeIngestBlock(t, 5),
		makeIngestBlock(t, 6),
		makeIngestBlock(t, 7),
	)
	if err := stock.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stock.Stop(); err != nil {
			t.Fatal(err)
		}
	})

	consumerStore := newTestBlockStore(t)
	initialIngestor, err := NewHotIngestor(consumerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	for height := int64(1); height <= 4; height++ {
		if _, submitErr := initialIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	if consumerStore.Height() != 3 {
		t.Fatalf("seeded consumer height %d, want 3", consumerStore.Height())
	}
	restartedIngestor, err := NewHotIngestor(consumerStore, IngestOptions{ChainID: ingestTestChainID})
	if err != nil {
		t.Fatal(err)
	}
	if next := restartedIngestor.NextHeight(); next != 4 {
		t.Fatalf("restart next height %d, want 4", next)
	}
	consumer, err := NewArchiveNode(restartedIngestor, nil, NodeOptions{
		ChainID:        ingestTestChainID,
		ListenAddress:  freeListenAddress(t),
		NodeKeyFile:    filepath.Join(t.TempDir(), "consumer_key.json"),
		RequestLimit:   2,
		StatusInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := consumer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := consumer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	stockAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(stock.NodeKey.ID(), stockListen))
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Switch.DialPeerWithAddress(stockAddr); err != nil {
		t.Fatal(err)
	}
	waitForStoreHeight(t, consumerStore, 6)
	if consumerStore.LoadBlock(3) == nil || consumerStore.LoadBlock(6) == nil {
		t.Fatalf("unexpected hot store after resume: base=%d height=%d", consumerStore.Base(), consumerStore.Height())
	}
}

func TestArchiveNodeBlocksyncArchiveAndPruneSoak(t *testing.T) {
	providerStore := newTestBlockStore(t)
	providerIngestor, err := NewHotIngestor(providerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	for height := int64(1); height <= 15; height++ {
		if _, submitErr := providerIngestor.Submit(makeIngestBlock(t, height)); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	if providerStore.Height() != 14 {
		t.Fatalf("provider store height %d, want 14", providerStore.Height())
	}
	providerListen := freeListenAddress(t)
	provider, err := NewArchiveNode(providerIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: providerListen,
		NodeKeyFile:   filepath.Join(t.TempDir(), "provider_key.json"),
		RequestLimit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := provider.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := provider.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	consumerStore := newTestBlockStore(t)
	consumerIngestor, err := NewHotIngestor(consumerStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewArchiveNode(consumerIngestor, nil, NodeOptions{
		ChainID:       ingestTestChainID,
		ListenAddress: freeListenAddress(t),
		NodeKeyFile:   filepath.Join(t.TempDir(), "consumer_key.json"),
		RequestLimit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := consumer.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := consumer.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})

	providerAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(provider.NodeKey.ID(), providerListen))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := consumer.Switch.DialPeerWithAddress(providerAddr); dialErr != nil {
		t.Fatal(dialErr)
	}

	objectStore, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := archive.ManifestKey("archive", ingestTestChainID, archive.DefaultManifest)
	deadline := time.Now().Add(8 * time.Second)
	var lastArchive archive.LiveArchiveResult
	var lastPrune archive.PruneHotResult
	var totalPruned uint64
	for time.Now().Before(deadline) {
		if consumerStore.Height() >= 4 {
			lastArchive, err = archive.ArchiveReadyFromHead(contextWithTimeout(t), consumerStore, objectStore, archive.LiveArchiveOptions{
				ChainID:       ingestTestChainID,
				Prefix:        "archive",
				SegmentBlocks: 3,
				Compression:   archive.CompressionGzip,
			}, 2)
			if err != nil {
				t.Fatal(err)
			}
			if lastArchive.Manifest.LastHeight > 0 {
				lastPrune, err = archive.PruneVerifiedHotStore(contextWithTimeout(t), consumerStore, objectStore, archive.PruneHotOptions{
					ManifestKey:            manifestKey,
					RetainBlocks:           3,
					EvidenceMaxAgeBlocks:   1,
					EvidenceMaxAgeDuration: time.Nanosecond,
				})
				if err != nil {
					t.Fatal(err)
				}
				totalPruned += lastPrune.Pruned
			}
		}
		if consumerStore.Height() >= 12 {
			lastArchive, err = archive.ArchiveReadyFromHead(contextWithTimeout(t), consumerStore, objectStore, archive.LiveArchiveOptions{
				ChainID:       ingestTestChainID,
				Prefix:        "archive",
				SegmentBlocks: 3,
				Compression:   archive.CompressionGzip,
			}, 2)
			if err != nil {
				t.Fatal(err)
			}
			lastPrune, err = archive.PruneVerifiedHotStore(contextWithTimeout(t), consumerStore, objectStore, archive.PruneHotOptions{
				ManifestKey:            manifestKey,
				RetainBlocks:           3,
				EvidenceMaxAgeBlocks:   1,
				EvidenceMaxAgeDuration: time.Nanosecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			totalPruned += lastPrune.Pruned
			verify, err := archive.Verify(contextWithTimeout(t), objectStore, archive.VerifyOptions{ManifestKey: manifestKey})
			if err != nil {
				t.Fatal(err)
			}
			if verify.BlocksChecked < 10 {
				t.Fatalf("verified %d blocks after soak, want at least 10", verify.BlocksChecked)
			}
			if lastArchive.Manifest.LastHeight < 10 {
				t.Fatalf("archived through %d after soak, want at least 10", lastArchive.Manifest.LastHeight)
			}
			if totalPruned == 0 || consumerStore.Base() <= 1 {
				t.Fatalf("expected verified prune to advance base, last prune=%+v total pruned=%d base=%d", lastPrune, totalPruned, consumerStore.Base())
			}
			if consumerStore.LoadBlock(consumerStore.Base()) == nil {
				t.Fatalf("base block %d is missing after prune", consumerStore.Base())
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("soak timed out: consumer height=%d base=%d archive=%+v prune=%+v", consumerStore.Height(), consumerStore.Base(), lastArchive, lastPrune)
}

func contextWithTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newColdArchiveSource(t *testing.T, firstHeight, lastHeight int64) *ArchiveBlockSource {
	t.Helper()
	store, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	records := make([]archive.BlockRecord, 0, lastHeight-firstHeight+1)
	for height := firstHeight; height <= lastHeight; height++ {
		record, recordErr := archive.BlockToRecord(makeIngestBlock(t, height))
		if recordErr != nil {
			t.Fatal(recordErr)
		}
		records = append(records, record)
	}
	data, segment, err := archive.EncodeSegment(records, archive.CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = archive.SegmentKey("archive", ingestTestChainID, segment)
	if putErr := store.Put(context.Background(), segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	manifest, err := archive.NewManifest(ingestTestChainID, []archive.SegmentManifest{segment})
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := archive.ManifestKey("archive", ingestTestChainID, archive.DefaultManifest)
	if saveErr := archive.SaveManifest(context.Background(), store, manifestKey, manifest); saveErr != nil {
		t.Fatal(saveErr)
	}
	source, err := NewArchiveBlockSource(store, manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func freeListenAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return "tcp://" + listener.Addr().String()
}

func waitForStoreHeight(t *testing.T, store interface{ Height() int64 }, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.Height() >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("store height %d, want at least %d", store.Height(), want)
}

func waitForStoreHeightWithRequestExpiry(t *testing.T, store interface{ Height() int64 }, reactor *Reactor, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.Height() >= want {
			return
		}
		reactor.requestPeerStatuses()
		reactor.expireRequestsAndPlan()
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("store height %d, want at least %d", store.Height(), want)
}

func waitForRequestTimeouts(t *testing.T, reactor *Reactor, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reactor.RequestTimeouts() >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("request timeouts %d, want at least %d", reactor.RequestTimeouts(), want)
}

func waitForPeerCount(t *testing.T, reactor *Reactor, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reactor.PeerCount() >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peer count %d, want at least %d", reactor.PeerCount(), want)
}

func waitForBestPeerHeight(t *testing.T, reactor *Reactor, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reactor.BestPeerHeight() >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("best peer height %d, want at least %d", reactor.BestPeerHeight(), want)
}

type stockBlocksyncP2PNode struct {
	Config    *cmtcfg.P2PConfig
	NodeKey   *p2p.NodeKey
	Transport *p2p.MultiplexTransport
	Switch    *p2p.Switch
}

type blockRequesterP2PNode struct {
	Config    *cmtcfg.P2PConfig
	NodeKey   *p2p.NodeKey
	Transport *p2p.MultiplexTransport
	Switch    *p2p.Switch
	Reactor   *blockRequesterReactor
}

type noBlockP2PNode struct {
	Config    *cmtcfg.P2PConfig
	NodeKey   *p2p.NodeKey
	Transport *p2p.MultiplexTransport
	Switch    *p2p.Switch
	Reactor   *noBlockReactor
}

type silentBlockP2PNode struct {
	Config    *cmtcfg.P2PConfig
	NodeKey   *p2p.NodeKey
	Transport *p2p.MultiplexTransport
	Switch    *p2p.Switch
	Reactor   *silentBlockReactor
}

func newNoBlockP2PNode(t *testing.T, listenAddress string, advertised PeerRange) *noBlockP2PNode {
	t.Helper()
	nodeKey := &p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddress,
		Network:         ingestTestChainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{cmtblocksync.BlocksyncChannel},
		Moniker:         "no-block-peer",
	}
	if err := nodeInfo.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg := cmtcfg.DefaultP2PConfig()
	cfg.ListenAddress = listenAddress
	cfg.AllowDuplicateIP = true
	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, p2p.MConnConfig(cfg), trace.NoOpTracer())
	transport.AddChannel(cmtblocksync.BlocksyncChannel)
	sw := p2p.NewSwitch(cfg, transport)
	sw.SetLogger(log.NewNopLogger())
	reactor := newNoBlockReactor(advertised)
	sw.AddReactor("NO_BLOCK", reactor)
	sw.SetNodeInfo(nodeInfo)
	sw.SetNodeKey(nodeKey)
	return &noBlockP2PNode{
		Config:    cfg,
		NodeKey:   nodeKey,
		Transport: transport,
		Switch:    sw,
		Reactor:   reactor,
	}
}

func newSilentBlockP2PNode(t *testing.T, listenAddress string, advertised PeerRange) *silentBlockP2PNode {
	t.Helper()
	nodeKey := &p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddress,
		Network:         ingestTestChainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{cmtblocksync.BlocksyncChannel},
		Moniker:         "silent-block-peer",
	}
	if err := nodeInfo.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg := cmtcfg.DefaultP2PConfig()
	cfg.ListenAddress = listenAddress
	cfg.AllowDuplicateIP = true
	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, p2p.MConnConfig(cfg), trace.NoOpTracer())
	transport.AddChannel(cmtblocksync.BlocksyncChannel)
	sw := p2p.NewSwitch(cfg, transport)
	sw.SetLogger(log.NewNopLogger())
	reactor := newSilentBlockReactor(advertised)
	sw.AddReactor("SILENT_BLOCK", reactor)
	sw.SetNodeInfo(nodeInfo)
	sw.SetNodeKey(nodeKey)
	return &silentBlockP2PNode{
		Config:    cfg,
		NodeKey:   nodeKey,
		Transport: transport,
		Switch:    sw,
		Reactor:   reactor,
	}
}

func (n *noBlockP2PNode) Start() error {
	addr, err := p2p.NewNetAddressString(p2p.IDAddressString(n.NodeKey.ID(), n.Config.ListenAddress))
	if err != nil {
		return err
	}
	if err := n.Transport.Listen(*addr); err != nil {
		return err
	}
	if err := n.Switch.Start(); err != nil {
		_ = n.Transport.Close()
		return err
	}
	return nil
}

func (n *noBlockP2PNode) Stop() error {
	var stopErr error
	if n.Switch.IsRunning() {
		stopErr = n.Switch.Stop()
	}
	if err := n.Transport.Close(); err != nil && stopErr == nil {
		stopErr = err
	}
	return stopErr
}

func (n *silentBlockP2PNode) Start() error {
	addr, err := p2p.NewNetAddressString(p2p.IDAddressString(n.NodeKey.ID(), n.Config.ListenAddress))
	if err != nil {
		return err
	}
	if err := n.Transport.Listen(*addr); err != nil {
		return err
	}
	if err := n.Switch.Start(); err != nil {
		_ = n.Transport.Close()
		return err
	}
	return nil
}

func (n *silentBlockP2PNode) Stop() error {
	var stopErr error
	if n.Switch.IsRunning() {
		stopErr = n.Switch.Stop()
	}
	if err := n.Transport.Close(); err != nil && stopErr == nil {
		stopErr = err
	}
	return stopErr
}

type noBlockReactor struct {
	p2p.BaseReactor
	advertised PeerRange
	noBlocks   chan int64
}

func newNoBlockReactor(advertised PeerRange) *noBlockReactor {
	r := &noBlockReactor{
		advertised: advertised,
		noBlocks:   make(chan int64, 16),
	}
	r.BaseReactor = *p2p.NewBaseReactor("NO_BLOCK", r)
	return r
}

func (*noBlockReactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:                  cmtblocksync.BlocksyncChannel,
			Priority:            5,
			SendQueueCapacity:   1000,
			RecvBufferCapacity:  50 * 4096,
			RecvMessageCapacity: cmtblocksync.MaxMsgSize,
			MessageType:         &bcproto.Message{},
		},
	}
}

func (r *noBlockReactor) AddPeer(peer p2p.Peer) {
	r.sendStatus(peer)
}

func (r *noBlockReactor) Receive(e p2p.Envelope) {
	switch msg := e.Message.(type) {
	case *bcproto.StatusRequest:
		r.sendStatus(e.Src)
	case *bcproto.BlockRequest:
		e.Src.TrySend(p2p.Envelope{
			ChannelID: cmtblocksync.BlocksyncChannel,
			Message:   &bcproto.NoBlockResponse{Height: msg.Height},
		})
		r.noBlocks <- msg.Height
	default:
		return
	}
}

func (r *noBlockReactor) sendStatus(peer p2p.Peer) {
	peer.TrySend(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message: &bcproto.StatusResponse{
			Base:   r.advertised.Base,
			Height: r.advertised.Height,
		},
	})
}

type silentBlockReactor struct {
	p2p.BaseReactor
	advertised PeerRange
	requests   chan int64
}

func newSilentBlockReactor(advertised PeerRange) *silentBlockReactor {
	r := &silentBlockReactor{
		advertised: advertised,
		requests:   make(chan int64, 16),
	}
	r.BaseReactor = *p2p.NewBaseReactor("SILENT_BLOCK", r)
	return r
}

func (*silentBlockReactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:                  cmtblocksync.BlocksyncChannel,
			Priority:            5,
			SendQueueCapacity:   1000,
			RecvBufferCapacity:  50 * 4096,
			RecvMessageCapacity: cmtblocksync.MaxMsgSize,
			MessageType:         &bcproto.Message{},
		},
	}
}

func (r *silentBlockReactor) AddPeer(peer p2p.Peer) {
	r.sendStatus(peer)
}

func (r *silentBlockReactor) Receive(e p2p.Envelope) {
	switch msg := e.Message.(type) {
	case *bcproto.StatusRequest:
		r.sendStatus(e.Src)
	case *bcproto.BlockRequest:
		r.requests <- msg.Height
	default:
		return
	}
}

func (r *silentBlockReactor) sendStatus(peer p2p.Peer) {
	peer.TrySend(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message: &bcproto.StatusResponse{
			Base:   r.advertised.Base,
			Height: r.advertised.Height,
		},
	})
}

func waitForNoBlockResponses(t *testing.T, reactor *noBlockReactor, want int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	seen := 0
	for seen < want {
		select {
		case <-reactor.noBlocks:
			seen++
		case <-deadline:
			t.Fatalf("saw %d no-block responses, want %d", seen, want)
		}
	}
}

func waitForSilentBlockRequests(t *testing.T, reactor *silentBlockReactor, want int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	seen := 0
	for seen < want {
		select {
		case <-reactor.requests:
			seen++
		case <-deadline:
			t.Fatalf("saw %d silent block requests, want %d", seen, want)
		}
	}
}

func newBlockRequesterP2PNode(t *testing.T, listenAddress string, requestHeight int64) *blockRequesterP2PNode {
	t.Helper()
	nodeKey := &p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddress,
		Network:         ingestTestChainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{cmtblocksync.BlocksyncChannel},
		Moniker:         "block-requester",
	}
	if err := nodeInfo.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg := cmtcfg.DefaultP2PConfig()
	cfg.ListenAddress = listenAddress
	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, p2p.MConnConfig(cfg), trace.NoOpTracer())
	transport.AddChannel(cmtblocksync.BlocksyncChannel)
	sw := p2p.NewSwitch(cfg, transport)
	sw.SetLogger(log.NewNopLogger())
	reactor := newBlockRequesterReactor(requestHeight)
	sw.AddReactor("BLOCK_REQUESTER", reactor)
	sw.SetNodeInfo(nodeInfo)
	sw.SetNodeKey(nodeKey)
	return &blockRequesterP2PNode{
		Config:    cfg,
		NodeKey:   nodeKey,
		Transport: transport,
		Switch:    sw,
		Reactor:   reactor,
	}
}

func (n *blockRequesterP2PNode) Start() error {
	addr, err := p2p.NewNetAddressString(p2p.IDAddressString(n.NodeKey.ID(), n.Config.ListenAddress))
	if err != nil {
		return err
	}
	if err := n.Transport.Listen(*addr); err != nil {
		return err
	}
	if err := n.Switch.Start(); err != nil {
		_ = n.Transport.Close()
		return err
	}
	return nil
}

func (n *blockRequesterP2PNode) Stop() error {
	var stopErr error
	if n.Switch.IsRunning() {
		stopErr = n.Switch.Stop()
	}
	if err := n.Transport.Close(); err != nil && stopErr == nil {
		stopErr = err
	}
	return stopErr
}

type blockRequesterReactor struct {
	p2p.BaseReactor
	height int64
	blocks chan *types.Block
	errs   chan error
}

func newBlockRequesterReactor(height int64) *blockRequesterReactor {
	r := &blockRequesterReactor{
		height: height,
		blocks: make(chan *types.Block, 1),
		errs:   make(chan error, 1),
	}
	r.BaseReactor = *p2p.NewBaseReactor("BLOCK_REQUESTER", r)
	return r
}

func (*blockRequesterReactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:                  cmtblocksync.BlocksyncChannel,
			Priority:            5,
			SendQueueCapacity:   1000,
			RecvBufferCapacity:  50 * 4096,
			RecvMessageCapacity: cmtblocksync.MaxMsgSize,
			MessageType:         &bcproto.Message{},
		},
	}
}

func (r *blockRequesterReactor) AddPeer(peer p2p.Peer) {
	peer.TrySend(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: r.height},
	})
}

func (r *blockRequesterReactor) Receive(e p2p.Envelope) {
	switch msg := e.Message.(type) {
	case *bcproto.BlockResponse:
		block, err := types.BlockFromProto(msg.Block)
		if err != nil {
			r.errs <- err
			return
		}
		r.blocks <- block
	case *bcproto.NoBlockResponse:
		r.errs <- fmt.Errorf("archive peer returned no block at height %d", msg.Height)
	default:
		return
	}
}

func newStockBlocksyncP2PNode(t *testing.T, listenAddress string, blocks ...*types.Block) *stockBlocksyncP2PNode {
	t.Helper()
	nodeKey := &p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddress,
		Network:         ingestTestChainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{cmtblocksync.BlocksyncChannel},
		Moniker:         "stock-blocksync",
	}
	if err := nodeInfo.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg := cmtcfg.DefaultP2PConfig()
	cfg.ListenAddress = listenAddress
	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, p2p.MConnConfig(cfg), trace.NoOpTracer())
	transport.AddChannel(cmtblocksync.BlocksyncChannel)
	sw := p2p.NewSwitch(cfg, transport)
	logger := log.NewNopLogger()
	sw.SetLogger(logger)
	reactor := newStockBlocksyncServingReactor(t, blocks...)
	reactor.SetLogger(logger.With("module", "stock_blocksync"))
	sw.AddReactor("BLOCKSYNC", reactor)
	sw.SetNodeInfo(nodeInfo)
	sw.SetNodeKey(nodeKey)
	return &stockBlocksyncP2PNode{
		Config:    cfg,
		NodeKey:   nodeKey,
		Transport: transport,
		Switch:    sw,
	}
}

func (n *stockBlocksyncP2PNode) Start() error {
	addr, err := p2p.NewNetAddressString(p2p.IDAddressString(n.NodeKey.ID(), n.Config.ListenAddress))
	if err != nil {
		return err
	}
	if err := n.Transport.Listen(*addr); err != nil {
		return err
	}
	if err := n.Switch.Start(); err != nil {
		_ = n.Transport.Close()
		return err
	}
	return nil
}

func (n *stockBlocksyncP2PNode) Stop() error {
	var stopErr error
	if n.Switch.IsRunning() {
		stopErr = n.Switch.Stop()
	}
	if err := n.Transport.Close(); err != nil && stopErr == nil {
		stopErr = err
	}
	return stopErr
}
