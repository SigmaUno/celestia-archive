package blocksyncarchive

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	cmtblocksync "github.com/cometbft/cometbft/blocksync"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	cmtconn "github.com/cometbft/cometbft/p2p/conn"
	bcproto "github.com/cometbft/cometbft/proto/tendermint/blocksync"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
)

func TestReactorRequestsIngestsAndServesBlocks(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, NewRequestPlanner(), ReactorOptions{RequestLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	peer := newFakePeer("peer")
	reactor.AddPeer(peer)
	if responses := collectStatusResponses(peer.sentEnvelopes()); len(responses) != 1 {
		t.Fatalf("AddPeer sent %d status responses, want 1", len(responses))
	}
	if requests := collectStatusRequests(peer.sentEnvelopes()); len(requests) != 1 {
		t.Fatalf("AddPeer sent %d status requests, want 1", len(requests))
	}
	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: 0x40,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 2},
	})
	requests := collectRequests(peer.sentEnvelopes())
	if len(requests) != 2 || requests[0].Height != 1 || requests[1].Height != 2 {
		t.Fatalf("unexpected block requests: %+v", requests)
	}
	for _, height := range []int64{1, 2} {
		block := makeIngestBlock(t, height)
		pb, err := block.ToProto()
		if err != nil {
			t.Fatal(err)
		}
		reactor.Receive(p2p.Envelope{
			Src:       peer,
			ChannelID: 0x40,
			Message:   &bcproto.BlockResponse{Block: pb},
		})
	}
	if got := blockStore.LoadBlock(1); got == nil || got.Height != 1 {
		t.Fatalf("block 1 was not persisted: %+v", got)
	}

	requester := newFakePeer("requester")
	reactor.Receive(p2p.Envelope{
		Src:       requester,
		ChannelID: 0x40,
		Message:   &bcproto.BlockRequest{Height: 1},
	})
	if responses := collectBlockResponses(requester.sentEnvelopes()); len(responses) != 1 {
		t.Fatalf("got %d block responses, want 1", len(responses))
	}
	if responses := reactor.HotBlockResponses(); responses != 1 {
		t.Fatalf("hot block responses = %d, want 1", responses)
	}
}

func TestReactorReturnsNoBlockForColdOrMissingHeights(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	peer := newFakePeer("peer")
	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: 0x40,
		Message:   &bcproto.BlockRequest{Height: 99},
	})
	if noBlocks := collectNoBlockResponses(peer.sentEnvelopes()); len(noBlocks) != 1 || noBlocks[0].Height != 99 {
		t.Fatalf("unexpected no-block responses: %+v", noBlocks)
	}
	if responses := reactor.NoBlockResponses(); responses != 1 {
		t.Fatalf("no-block responses = %d, want 1", responses)
	}
}

func TestReactorDropsNilPeerBlockRequestBeforeColdLookup(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	source := &scriptedColdBlockSource{advertised: PeerRange{Base: 1, Height: 1}, block: makeIngestBlock(t, 1)}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		ColdBlockSource: source,
		RequestLimit:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	reactor.Receive(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: 1},
	})
	if queued := reactor.QueuedColdBlockRequests(); queued != 0 {
		t.Fatalf("queued cold requests = %d, want 0", queued)
	}
	if source.advertisedCalls != 0 || source.loadCalls != 0 {
		t.Fatalf("cold source calls advertised=%d load=%d, want 0/0", source.advertisedCalls, source.loadCalls)
	}
}

func TestReactorRejectsExcessiveRequestSettings(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: MaxRequestLimit + 1}); err == nil || !strings.Contains(err.Error(), "request limit") {
		t.Fatalf("NewReactor request limit err=%v, want request limit error", err)
	}
	if _, err := NewReactor(ingestor, nil, ReactorOptions{ColdRequestWorkers: MaxRequestLimit + 1}); err == nil || !strings.Contains(err.Error(), "cold request workers") {
		t.Fatalf("NewReactor cold workers err=%v, want cold workers error", err)
	}
}

func TestReactorIgnoresPeerlessStatusMessages(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("Receive panicked for peerless status message: %v", recovered)
		}
	}()
	reactor.Receive(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusRequest{},
	})
	reactor.Receive(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 1},
	})
}

func TestReactorIgnoresUnsolicitedBlockResponses(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	block := makeIngestBlock(t, 1)
	pb, err := block.ToProto()
	if err != nil {
		t.Fatal(err)
	}
	reactor.Receive(p2p.Envelope{
		Src:       newFakePeer("peer-a"),
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{Block: pb},
	})
	if got := blockStore.LoadBlock(1); got != nil {
		t.Fatalf("unsolicited block was persisted: %+v", got)
	}
	if pending := ingestor.PendingHeight(); pending != 0 {
		t.Fatalf("unsolicited block became pending at height %d", pending)
	}

	peerA := newFakePeer("peer-a")
	peerB := newFakePeer("peer-b")
	reactor.AddPeer(peerA)
	reactor.AddPeer(peerB)
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 1},
	})
	peerA.resetSent()
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{Block: pb},
	})
	if got := blockStore.LoadBlock(1); got != nil {
		t.Fatalf("wrong-peer block was persisted: %+v", got)
	}
	if pending := ingestor.PendingHeight(); pending != 0 {
		t.Fatalf("wrong-peer block became pending at height %d", pending)
	}
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{Block: pb},
	})
	if pending := ingestor.PendingHeight(); pending != 1 {
		t.Fatalf("requested block pending height = %d, want 1", pending)
	}
}

func TestReactorBuffersOutOfOrderBlockResponses(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	peer := newFakePeer("peer")
	reactor.AddPeer(peer)
	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 3},
	})
	requests := collectRequests(peer.sentEnvelopes())
	if len(requests) != 3 {
		t.Fatalf("block requests = %d, want 3: %+v", len(requests), requests)
	}

	for _, height := range []int64{2, 1, 3} {
		block := makeIngestBlock(t, height)
		pb, err := block.ToProto()
		if err != nil {
			t.Fatal(err)
		}
		reactor.Receive(p2p.Envelope{
			Src:       peer,
			ChannelID: cmtblocksync.BlocksyncChannel,
			Message:   &bcproto.BlockResponse{Block: pb},
		})
	}
	if got := blockStore.LoadBlock(1); got == nil || got.Height != 1 {
		t.Fatalf("block 1 was not persisted after out-of-order responses: %+v", got)
	}
	if got := blockStore.LoadBlock(2); got == nil || got.Height != 2 {
		t.Fatalf("block 2 was not persisted after out-of-order responses: %+v", got)
	}
	if pending := ingestor.PendingHeight(); pending != 3 {
		t.Fatalf("pending height = %d, want 3", pending)
	}
	if inflight := reactor.InflightRequests(); inflight != 0 {
		t.Fatalf("inflight requests = %d, want 0", inflight)
	}
}

func TestReactorRetriesImmediatelyAfterInvalidBlockResponse(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	peerA := newFakePeer("peer-a")
	peerB := newFakePeer("peer-b")
	reactor.AddPeer(peerA)
	reactor.AddPeer(peerB)
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 1},
	})
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 1},
	})
	initial := collectRequests(peerA.sentEnvelopes())
	if len(initial) == 0 || initial[0].Height != 1 {
		t.Fatalf("unexpected initial request to peer-a: %+v", initial)
	}
	peerB.resetSent()
	badBlock := makeIngestBlock(t, 1)
	badBlock.ChainID = "wrong-chain"
	pb, err := badBlock.ToProto()
	if err != nil {
		t.Fatal(err)
	}
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{Block: pb},
	})
	retry := collectRequests(peerB.sentEnvelopes())
	if len(retry) != 1 || retry[0].Height != 1 {
		t.Fatalf("expected immediate retry to peer-b after invalid response, got %+v", retry)
	}
}

func TestReactorRetriesImmediatelyAfterMalformedBlockResponse(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	peerA := newFakePeer("peer-a")
	peerB := newFakePeer("peer-b")
	reactor.AddPeer(peerA)
	reactor.AddPeer(peerB)
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 1},
	})
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 1},
	})
	initial := collectRequests(peerA.sentEnvelopes())
	if len(initial) == 0 || initial[0].Height != 1 {
		t.Fatalf("unexpected initial request to peer-a: %+v", initial)
	}
	peerB.resetSent()
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{},
	})
	retry := collectRequests(peerB.sentEnvelopes())
	if len(retry) != 1 || retry[0].Height != 1 {
		t.Fatalf("expected immediate retry to peer-b after malformed response, got %+v", retry)
	}
}

func TestReactorRetriesImmediatelyAfterInvalidBufferedBlockResponse(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	peerA := newFakePeer("peer-a")
	peerB := newFakePeer("peer-b")
	reactor.AddPeer(peerA)
	reactor.AddPeer(peerB)
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 2},
	})
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 2},
	})
	requests := collectRequests(peerA.sentEnvelopes())
	if len(requests) != 2 {
		t.Fatalf("block requests = %d, want 2: %+v", len(requests), requests)
	}

	badFuture := makeIngestBlock(t, 2)
	badFuture.ChainID = "wrong-chain"
	badPB, err := badFuture.ToProto()
	if err != nil {
		t.Fatal(err)
	}
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{Block: badPB},
	})
	if buffered := reactor.BufferedBlockResponses(); buffered != 1 {
		t.Fatalf("buffered responses = %d, want 1", buffered)
	}

	peerB.resetSent()
	goodFirst := makeIngestBlock(t, 1)
	goodPB, err := goodFirst.ToProto()
	if err != nil {
		t.Fatal(err)
	}
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{Block: goodPB},
	})
	retry := collectRequests(peerB.sentEnvelopes())
	if len(retry) != 1 || retry[0].Height != 2 {
		t.Fatalf("expected immediate retry of invalid buffered height to peer-b, got %+v", retry)
	}
}

func TestReactorDropsBufferedResponsesWhenPeerIsRemoved(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	peerA := newFakePeer("peer-a")
	peerB := newFakePeer("peer-b")
	reactor.AddPeer(peerA)
	reactor.AddPeer(peerB)
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 2},
	})
	requests := collectRequests(peerA.sentEnvelopes())
	if len(requests) != 2 {
		t.Fatalf("block requests = %d, want 2: %+v", len(requests), requests)
	}
	block := makeIngestBlock(t, 2)
	pb, err := block.ToProto()
	if err != nil {
		t.Fatal(err)
	}
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{Block: pb},
	})
	if peerID, ok := reactor.planner.InflightPeer(2); ok {
		t.Fatalf("height 2 still inflight after buffering from %s", peerID)
	}
	reactor.RemovePeer(peerA, nil)

	peerB.resetSent()
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 2},
	})
	failover := collectRequests(peerB.sentEnvelopes())
	if len(failover) != 2 || failover[0].Height != 1 || failover[1].Height != 2 {
		t.Fatalf("unexpected failover requests after dropping buffer: %+v", failover)
	}
}

func TestReactorDropsBufferedResponsesWhenPeerRangeContracts(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	peerA := newFakePeer("peer-a")
	peerB := newFakePeer("peer-b")
	reactor.AddPeer(peerA)
	reactor.AddPeer(peerB)
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 2},
	})
	requests := collectRequests(peerA.sentEnvelopes())
	if len(requests) != 2 {
		t.Fatalf("block requests = %d, want 2: %+v", len(requests), requests)
	}
	block := makeIngestBlock(t, 2)
	pb, err := block.ToProto()
	if err != nil {
		t.Fatal(err)
	}
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{Block: pb},
	})
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 1},
	})

	peerB.resetSent()
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 2},
	})
	failover := collectRequests(peerB.sentEnvelopes())
	if len(failover) != 1 || failover[0].Height != 2 {
		t.Fatalf("unexpected failover request after range contraction: %+v", failover)
	}
}

func TestReactorCountsColdBlockResponsesAndErrors(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	source := &scriptedColdBlockSource{advertised: PeerRange{Base: 1, Height: 2}, block: makeIngestBlock(t, 1)}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		ColdBlockSource: source,
		RequestTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := newFakePeer("peer")
	reactor.respondToColdBlockRequest(peer, 1)
	if responses := collectBlockResponses(peer.sentEnvelopes()); len(responses) != 1 {
		t.Fatalf("cold block responses = %d, want 1", len(responses))
	}
	if responses := reactor.ColdBlockResponses(); responses != 1 {
		t.Fatalf("cold response metric = %d, want 1", responses)
	}

	peer.resetSent()
	source.block = nil
	source.err = errors.New("cold store unavailable")
	reactor.respondToColdBlockRequest(peer, 2)
	if noBlocks := collectNoBlockResponses(peer.sentEnvelopes()); len(noBlocks) != 1 || noBlocks[0].Height != 2 {
		t.Fatalf("unexpected no-block responses: %+v", noBlocks)
	}
	if coldErrors := reactor.ColdBlockErrors(); coldErrors != 1 {
		t.Fatalf("cold error metric = %d, want 1", coldErrors)
	}
	if noBlocks := reactor.NoBlockResponses(); noBlocks != 1 {
		t.Fatalf("no-block metric = %d, want 1", noBlocks)
	}
}

func TestReactorBoundsQueuedColdBlockRequests(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		ColdBlockSource: &scriptedColdBlockSource{
			advertised: PeerRange{Base: 1, Height: 2},
			block:      makeIngestBlock(t, 1),
		},
		RequestLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := newFakePeer("peer")
	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: 1},
	})
	if queued := reactor.QueuedColdBlockRequests(); queued != 1 {
		t.Fatalf("queued cold requests = %d, want 1", queued)
	}
	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: 2},
	})
	if noBlocks := collectNoBlockResponses(peer.sentEnvelopes()); len(noBlocks) != 0 {
		t.Fatalf("unexpected no-block responses for queued cold request: %+v", noBlocks)
	}
	if queueFull := reactor.ColdQueueFull(); queueFull != 1 {
		t.Fatalf("cold queue full = %d, want 1", queueFull)
	}
}

func TestReactorRejectsColdRequestOutsideAdvertisedRange(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		ColdBlockSource: &scriptedColdBlockSource{
			advertised: PeerRange{Base: 10, Height: 20},
			block:      makeIngestBlock(t, 10),
		},
		RequestLimit:   1,
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := reactor.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := reactor.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})
	peer := newFakePeer("peer")
	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: 9},
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if reactor.NoBlockResponses() == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if noBlocks := collectNoBlockResponses(peer.sentEnvelopes()); len(noBlocks) != 1 || noBlocks[0].Height != 9 {
		t.Fatalf("unexpected no-block responses: %+v", noBlocks)
	}
	if responses := reactor.NoBlockResponses(); responses != 1 {
		t.Fatalf("no-block responses = %d, want 1", responses)
	}
}

func TestReactorCountsColdAdvertisedRangeErrors(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		ColdBlockSource: &scriptedColdBlockSource{err: errors.New("manifest unavailable")},
		RequestLimit:    1,
		RequestTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := reactor.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := reactor.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})
	peer := newFakePeer("peer")
	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: 1},
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if reactor.ColdBlockErrors() == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if coldErrors := reactor.ColdBlockErrors(); coldErrors != 1 {
		t.Fatalf("cold errors = %d, want 1", coldErrors)
	}
	if noBlocks := collectNoBlockResponses(peer.sentEnvelopes()); len(noBlocks) != 1 || noBlocks[0].Height != 1 {
		t.Fatalf("unexpected no-block responses: %+v", noBlocks)
	}
}

func TestReactorStatusExchangeBroadcastsUpdatedColdRange(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	source := &scriptedColdBlockSource{advertised: PeerRange{Base: 1, Height: 5}}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{ColdBlockSource: source})
	if err != nil {
		t.Fatal(err)
	}
	peer := newFakePeer("peer")
	reactor.AddPeer(peer)
	source.advertised = PeerRange{Base: 1, Height: 10}
	reactor.exchangeStatuses()
	statuses := collectStatusResponses(peer.sentEnvelopes())
	if len(statuses) < 2 {
		t.Fatalf("status responses = %d, want at least 2", len(statuses))
	}
	last := statuses[len(statuses)-1]
	if last.Base != 1 || last.Height != 10 {
		t.Fatalf("last status range = %d-%d, want 1-10", last.Base, last.Height)
	}
	if requests := collectStatusRequests(peer.sentEnvelopes()); len(requests) < 2 {
		t.Fatalf("status requests = %d, want at least 2", len(requests))
	}
}

func TestReactorStartSeedsColdRangeWhenStatusPollingDisabled(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	source := &scriptedColdBlockSource{advertised: PeerRange{Base: 1, Height: 7}}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		ColdBlockSource:       source,
		RequestTimeout:        time.Second,
		StatusRequestInterval: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := reactor.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := reactor.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})
	peer := newFakePeer("peer")
	reactor.AddPeer(peer)
	statuses := collectStatusResponses(peer.sentEnvelopes())
	if len(statuses) == 0 {
		t.Fatal("expected status response")
	}
	last := statuses[len(statuses)-1]
	if last.Base != 1 || last.Height != 7 {
		t.Fatalf("status range = %d-%d, want 1-7", last.Base, last.Height)
	}
}

func TestReactorTracksActiveColdBlockRequests(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	source := &blockingColdBlockSource{
		block:   makeIngestBlock(t, 1),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		ColdBlockSource: source,
		RequestLimit:    1,
		RequestTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := reactor.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := reactor.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})
	reactor.Receive(p2p.Envelope{
		Src:       newFakePeer("peer"),
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: 1},
	})
	select {
	case <-source.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cold block source")
	}
	if active := reactor.ActiveColdBlockRequests(); active != 1 {
		t.Fatalf("active cold requests = %d, want 1", active)
	}
	close(source.release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if reactor.ActiveColdBlockRequests() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("active cold requests = %d, want 0", reactor.ActiveColdBlockRequests())
}

func TestReactorColdWorkersLimitConcurrentColdReads(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	source := &multiBlockingColdBlockSource{
		block:   makeIngestBlock(t, 1),
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		ColdBlockSource:    source,
		RequestLimit:       2,
		ColdRequestWorkers: 1,
		RequestTimeout:     time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := reactor.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		close(source.release)
		if stopErr := reactor.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	})
	peer := newFakePeer("peer")
	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: 1},
	})
	select {
	case <-source.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first cold block source call")
	}
	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: 2},
	})
	select {
	case <-source.entered:
		t.Fatal("second cold block source call started despite cold worker limit")
	case <-time.After(25 * time.Millisecond):
	}
	if active := reactor.ActiveColdBlockRequests(); active != 1 {
		t.Fatalf("active cold requests = %d, want 1", active)
	}
	if queued := reactor.QueuedColdBlockRequests(); queued != 1 {
		t.Fatalf("queued cold requests = %d, want 1", queued)
	}
}

func TestReactorRetriesNoBlockHeightWithAnotherPeer(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	peerA := newFakePeer("peer-a")
	peerB := newFakePeer("peer-b")
	reactor.AddPeer(peerA)
	reactor.AddPeer(peerB)
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 3},
	})
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 3},
	})
	initial := collectRequests(peerA.sentEnvelopes())
	if len(initial) == 0 || initial[0].Height != 1 {
		t.Fatalf("unexpected initial request to peer-a: %+v", initial)
	}
	peerB.resetSent()
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.NoBlockResponse{Height: 1},
	})
	failover := collectRequests(peerB.sentEnvelopes())
	if len(failover) != 1 || failover[0].Height != 1 {
		t.Fatalf("expected height 1 failover request to peer-b, got %+v", failover)
	}
}

func TestReactorIgnoresNoBlockFromWrongPeer(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	peerA := newFakePeer("peer-a")
	peerB := newFakePeer("peer-b")
	reactor.AddPeer(peerA)
	reactor.AddPeer(peerB)
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 3},
	})
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 3},
	})
	initial := collectRequests(peerA.sentEnvelopes())
	if len(initial) == 0 || initial[0].Height != 1 {
		t.Fatalf("unexpected initial request to peer-a: %+v", initial)
	}
	peerB.resetSent()
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.NoBlockResponse{Height: 1},
	})
	if peerID, ok := reactor.planner.InflightPeer(1); !ok || peerID != "peer-a" {
		t.Fatalf("inflight peer after wrong no-block = %q/%v, want peer-a/true", peerID, ok)
	}
	if failover := collectRequests(peerB.sentEnvelopes()); len(failover) != 0 {
		t.Fatalf("wrong-peer no-block caused retry: %+v", failover)
	}
}

func TestReactorRetriesTimedOutRequestWithAnotherPeer(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		RequestLimit:   1,
		RequestTimeout: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	peerA := newFakePeer("peer-a")
	peerB := newFakePeer("peer-b")
	reactor.AddPeer(peerA)
	reactor.AddPeer(peerB)
	reactor.Receive(p2p.Envelope{
		Src:       peerA,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 3},
	})
	reactor.Receive(p2p.Envelope{
		Src:       peerB,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 3},
	})
	initial := collectRequests(peerA.sentEnvelopes())
	if len(initial) == 0 || initial[0].Height != 1 {
		t.Fatalf("unexpected initial request to peer-a: %+v", initial)
	}
	if inflight := reactor.InflightRequests(); inflight != 1 {
		t.Fatalf("inflight requests = %d, want 1", inflight)
	}
	peerA.resetSent()
	peerB.resetSent()
	time.Sleep(2 * time.Millisecond)
	reactor.expireRequestsAndPlan()
	if timeouts := reactor.RequestTimeouts(); timeouts != 1 {
		t.Fatalf("request timeouts = %d, want 1", timeouts)
	}
	retry := collectRequests(peerB.sentEnvelopes())
	if len(retry) != 1 || retry[0].Height != 1 {
		t.Fatalf("expected timed-out height 1 retry to peer-b, got %+v", retry)
	}
}

func TestArchiveBlockRequestIsCompatibleWithStockBlocksyncReactor(t *testing.T) {
	block := makeIngestBlock(t, 1)
	stock := newStockBlocksyncServingReactor(t, block)
	requester := newFakePeer("archive-peer")
	stock.Receive(p2p.Envelope{
		Src:       requester,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: 1},
	})
	responses := collectBlockResponses(requester.sentEnvelopes())
	if len(responses) != 1 {
		t.Fatalf("got %d stock responses, want 1", len(responses))
	}
	got, err := types.BlockFromProto(responses[0].Block)
	if err != nil {
		t.Fatal(err)
	}
	if got.Height != 1 || got.ChainID != ingestTestChainID {
		t.Fatalf("unexpected stock response block: height=%d chain=%s", got.Height, got.ChainID)
	}
}

func TestArchiveReactorIngestsStockBlocksyncResponses(t *testing.T) {
	stock := newStockBlocksyncServingReactor(t, makeIngestBlock(t, 1), makeIngestBlock(t, 2))
	archiveStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(archiveStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	archiveReactor, err := NewReactor(ingestor, NewRequestPlanner(), ReactorOptions{RequestLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	peer := newFakePeer("stock-peer")
	archiveReactor.AddPeer(peer)
	archiveReactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusResponse{Base: 1, Height: 2},
	})
	requests := collectRequests(peer.sentEnvelopes())
	if len(requests) != 2 {
		t.Fatalf("archive reactor requested %d blocks, want 2", len(requests))
	}
	for _, request := range requests {
		peer.resetSent()
		stock.Receive(p2p.Envelope{
			Src:       peer,
			ChannelID: cmtblocksync.BlocksyncChannel,
			Message:   request,
		})
		responses := collectBlockResponses(peer.sentEnvelopes())
		if len(responses) != 1 {
			t.Fatalf("stock reactor returned %d responses for height %d", len(responses), request.Height)
		}
		archiveReactor.Receive(p2p.Envelope{
			Src:       peer,
			ChannelID: cmtblocksync.BlocksyncChannel,
			Message:   responses[0],
		})
	}
	if got := archiveStore.LoadBlock(1); got == nil || got.Height != 1 {
		t.Fatalf("archive reactor did not persist stock block response: %+v", got)
	}
}

func newStockBlocksyncServingReactor(t *testing.T, blocks ...*types.Block) *cmtblocksync.Reactor {
	t.Helper()
	db := dbm.NewMemDB()
	blockStore := store.NewBlockStore(db)
	for _, block := range blocks {
		parts, err := block.MakePartSet(types.BlockPartSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		blockStore.SaveBlock(block, parts, &types.Commit{Height: block.Height, Signatures: []types.CommitSig{}})
	}
	stateDB := dbm.NewMemDB()
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{})
	vals, _ := types.RandValidatorSet(1, 1)
	state := sm.State{
		ChainID:                          ingestTestChainID,
		InitialHeight:                    1,
		LastBlockHeight:                  int64(len(blocks)),
		Validators:                       vals,
		NextValidators:                   vals.Copy(),
		LastValidators:                   vals.Copy(),
		LastHeightValidatorsChanged:      1,
		ConsensusParams:                  *types.DefaultConsensusParams(),
		LastHeightConsensusParamsChanged: 1,
	}
	if saveErr := stateStore.Save(state); saveErr != nil {
		t.Fatal(saveErr)
	}
	blockExec := sm.NewBlockExecutor(stateStore, log.NewNopLogger(), nil, nil, sm.EmptyEvidencePool{}, blockStore)
	return cmtblocksync.NewReactor(state, blockExec, blockStore, false, cmtblocksync.NopMetrics(), 0)
}

func collectRequests(envelopes []p2p.Envelope) []*bcproto.BlockRequest {
	var requests []*bcproto.BlockRequest
	for _, envelope := range envelopes {
		if request, ok := envelope.Message.(*bcproto.BlockRequest); ok {
			requests = append(requests, request)
		}
	}
	return requests
}

func collectBlockResponses(envelopes []p2p.Envelope) []*bcproto.BlockResponse {
	var responses []*bcproto.BlockResponse
	for _, envelope := range envelopes {
		if response, ok := envelope.Message.(*bcproto.BlockResponse); ok {
			responses = append(responses, response)
		}
	}
	return responses
}

func collectNoBlockResponses(envelopes []p2p.Envelope) []*bcproto.NoBlockResponse {
	var responses []*bcproto.NoBlockResponse
	for _, envelope := range envelopes {
		if response, ok := envelope.Message.(*bcproto.NoBlockResponse); ok {
			responses = append(responses, response)
		}
	}
	return responses
}

func collectStatusRequests(envelopes []p2p.Envelope) []*bcproto.StatusRequest {
	var requests []*bcproto.StatusRequest
	for _, envelope := range envelopes {
		if request, ok := envelope.Message.(*bcproto.StatusRequest); ok {
			requests = append(requests, request)
		}
	}
	return requests
}

func collectStatusResponses(envelopes []p2p.Envelope) []*bcproto.StatusResponse {
	var responses []*bcproto.StatusResponse
	for _, envelope := range envelopes {
		if response, ok := envelope.Message.(*bcproto.StatusResponse); ok {
			responses = append(responses, response)
		}
	}
	return responses
}

type scriptedColdBlockSource struct {
	advertised      PeerRange
	block           *types.Block
	err             error
	advertisedCalls int
	loadCalls       int
}

func (s *scriptedColdBlockSource) AdvertisedRange(context.Context) (PeerRange, error) {
	s.advertisedCalls++
	return s.advertised, s.err
}

func (s *scriptedColdBlockSource) LoadBlock(context.Context, int64) (*types.Block, error) {
	s.loadCalls++
	if s.err != nil {
		return nil, s.err
	}
	if s.block == nil {
		return nil, ErrColdBlockNotFound
	}
	return s.block, nil
}

type blockingColdBlockSource struct {
	block   *types.Block
	entered chan struct{}
	release chan struct{}
}

func (s *blockingColdBlockSource) AdvertisedRange(context.Context) (PeerRange, error) {
	return PeerRange{Base: s.block.Height, Height: s.block.Height}, nil
}

func (s *blockingColdBlockSource) LoadBlock(ctx context.Context, _ int64) (*types.Block, error) {
	close(s.entered)
	select {
	case <-s.release:
		return s.block, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type multiBlockingColdBlockSource struct {
	block   *types.Block
	entered chan struct{}
	release chan struct{}
}

func (s *multiBlockingColdBlockSource) AdvertisedRange(context.Context) (PeerRange, error) {
	return PeerRange{Base: s.block.Height, Height: s.block.Height + 1}, nil
}

func (s *multiBlockingColdBlockSource) LoadBlock(ctx context.Context, _ int64) (*types.Block, error) {
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return s.block, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type fakePeer struct {
	id   p2p.ID
	quit chan struct{}
	mu   sync.Mutex
	sent []p2p.Envelope
}

func newFakePeer(id p2p.ID) *fakePeer {
	return &fakePeer{id: id, quit: make(chan struct{})}
}

func (*fakePeer) Start() error                      { return nil }
func (*fakePeer) OnStart() error                    { return nil }
func (p *fakePeer) Stop() error                     { close(p.quit); return nil }
func (*fakePeer) OnStop()                           {}
func (*fakePeer) Reset() error                      { return nil }
func (*fakePeer) OnReset() error                    { return nil }
func (*fakePeer) IsRunning() bool                   { return true }
func (p *fakePeer) Quit() <-chan struct{}           { return p.quit }
func (p *fakePeer) String() string                  { return string(p.id) }
func (*fakePeer) SetLogger(log.Logger)              {}
func (*fakePeer) FlushStop()                        {}
func (p *fakePeer) ID() p2p.ID                      { return p.id }
func (p *fakePeer) HasIPChanged() bool              { return false }
func (*fakePeer) RemoteIP() net.IP                  { return nil }
func (*fakePeer) RemoteAddr() net.Addr              { return nil }
func (*fakePeer) IsOutbound() bool                  { return false }
func (*fakePeer) IsPersistent() bool                { return false }
func (*fakePeer) CloseConn() error                  { return nil }
func (*fakePeer) NodeInfo() p2p.NodeInfo            { return nil }
func (*fakePeer) Status() cmtconn.ConnectionStatus  { return cmtconn.ConnectionStatus{} }
func (*fakePeer) SocketAddr() *p2p.NetAddress       { return nil }
func (p *fakePeer) Send(envelope p2p.Envelope) bool { return p.TrySend(envelope) }
func (p *fakePeer) TrySend(envelope p2p.Envelope) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, envelope)
	return true
}

func (p *fakePeer) sentEnvelopes() []p2p.Envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]p2p.Envelope(nil), p.sent...)
}

func (p *fakePeer) resetSent() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = nil
}

func (*fakePeer) Set(string, any)        {}
func (*fakePeer) Get(string) any         { return nil }
func (*fakePeer) SetRemovalFailed()      {}
func (*fakePeer) GetRemovalFailed() bool { return false }
