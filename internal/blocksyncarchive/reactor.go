package blocksyncarchive

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	cmtblocksync "github.com/cometbft/cometbft/blocksync"
	"github.com/cometbft/cometbft/p2p"
	bcproto "github.com/cometbft/cometbft/proto/tendermint/blocksync"
	"github.com/cometbft/cometbft/types"
)

const (
	ReactorName         = "ARCHIVE_BLOCKSYNC"
	DefaultRequestLimit = 32
	MaxRequestLimit     = 1024
)

type ReactorOptions struct {
	RequestLimit          int
	ColdRequestWorkers    int
	RequestTimeout        time.Duration
	StatusRequestInterval time.Duration
	ColdBlockSource       ColdBlockSource
}

type Reactor struct {
	p2p.BaseReactor

	ingestor *HotIngestor
	planner  *RequestPlanner
	opts     ReactorOptions
	coldWork chan coldBlockRequest

	mu                sync.Mutex
	peers             map[PeerID]p2p.Peer
	bufferedResponses map[int64]bufferedBlockResponse

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	requestTimeouts  atomic.Int64
	hotResponses     atomic.Int64
	coldResponses    atomic.Int64
	noBlockResponses atomic.Int64
	coldErrors       atomic.Int64
	coldQueueFull    atomic.Int64
	coldActive       atomic.Int64

	// cachedRange holds the most recently computed advertised range (hot + cold).
	// It is refreshed by the statusRequestLoop and on ingestion progress
	// (broadcastStatus) so that sendStatus / AddPeer never block the p2p
	// receive goroutine on a cold-source roundtrip.
	cachedRange atomic.Value // PeerRange
}

type coldBlockRequest struct {
	peer   p2p.Peer
	height int64
}

type bufferedBlockResponse struct {
	peerID PeerID
	block  *types.Block
}

func NewReactor(ingestor *HotIngestor, planner *RequestPlanner, opts ReactorOptions) (*Reactor, error) {
	if ingestor == nil {
		return nil, errors.New("hot ingestor is required")
	}
	if planner == nil {
		planner = NewRequestPlanner()
	}
	if opts.RequestLimit <= 0 {
		opts.RequestLimit = DefaultRequestLimit
	}
	if opts.RequestLimit > MaxRequestLimit {
		return nil, fmt.Errorf("request limit cannot exceed %d", MaxRequestLimit)
	}
	if opts.ColdRequestWorkers < 0 {
		return nil, errors.New("cold request workers cannot be negative")
	}
	if opts.ColdRequestWorkers == 0 {
		opts.ColdRequestWorkers = opts.RequestLimit
	}
	if opts.ColdRequestWorkers > MaxRequestLimit {
		return nil, fmt.Errorf("cold request workers cannot exceed %d", MaxRequestLimit)
	}
	if opts.RequestTimeout < 0 {
		return nil, errors.New("request timeout cannot be negative")
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = 30 * time.Second
	}
	if opts.StatusRequestInterval < 0 {
		return nil, errors.New("status request interval cannot be negative")
	}
	r := &Reactor{
		ingestor:          ingestor,
		planner:           planner,
		opts:              opts,
		coldWork:          make(chan coldBlockRequest, opts.RequestLimit),
		peers:             make(map[PeerID]p2p.Peer),
		bufferedResponses: make(map[int64]bufferedBlockResponse),
	}
	r.BaseReactor = *p2p.NewBaseReactor(ReactorName, r)
	return r, nil
}

func (r *Reactor) OnStart() error {
	r.ctx, r.cancel = context.WithCancel(context.Background())
	// Seed the cached advertised range so the first sendStatus call from
	// AddPeer doesn't see a zero cold range.
	r.cachedRange.Store(PeerRange{})
	if r.opts.ColdBlockSource != nil {
		r.refreshAdvertisedRangeWithTimeout()
	}
	if r.opts.ColdBlockSource != nil {
		for range r.opts.ColdRequestWorkers {
			r.wg.Add(1)
			go func() {
				defer r.wg.Done()
				r.coldBlockWorker()
			}()
		}
	}
	if r.opts.RequestTimeout > 0 {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.requestTimeoutLoop()
		}()
	}
	if r.opts.StatusRequestInterval > 0 {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.statusRequestLoop()
		}()
	}
	return nil
}

func (r *Reactor) OnStop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

func (*Reactor) GetChannels() []*p2p.ChannelDescriptor {
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

func (r *Reactor) AddPeer(peer p2p.Peer) {
	if peer == nil {
		return
	}
	r.mu.Lock()
	r.peers[PeerID(peer.ID())] = peer
	r.mu.Unlock()
	r.sendStatus(peer)
	requestStatus(peer)
}

func (r *Reactor) RemovePeer(peer p2p.Peer, _ any) {
	if peer == nil {
		return
	}
	r.mu.Lock()
	peerID := PeerID(peer.ID())
	delete(r.peers, peerID)
	r.planner.RemovePeer(peerID)
	r.dropBufferedResponsesFromPeerLocked(peerID)
	r.mu.Unlock()
}

func (r *Reactor) PeerCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.peers)
}

func (r *Reactor) BestPeerHeight() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.planner.BestHeight()
}

func (r *Reactor) InflightRequests() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.planner.InflightCount()
}

func (r *Reactor) BufferedBlockResponses() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bufferedResponses)
}

func (r *Reactor) RequestTimeouts() int64 {
	return r.requestTimeouts.Load()
}

func (r *Reactor) HotBlockResponses() int64 {
	return r.hotResponses.Load()
}

func (r *Reactor) ColdBlockResponses() int64 {
	return r.coldResponses.Load()
}

func (r *Reactor) NoBlockResponses() int64 {
	return r.noBlockResponses.Load()
}

func (r *Reactor) ColdBlockErrors() int64 {
	return r.coldErrors.Load()
}

func (r *Reactor) ColdQueueFull() int64 {
	return r.coldQueueFull.Load()
}

func (r *Reactor) QueuedColdBlockRequests() int {
	return len(r.coldWork)
}

func (r *Reactor) ActiveColdBlockRequests() int64 {
	return r.coldActive.Load()
}

// AdvertisedRange returns the currently cached advertised range, refreshing it
// synchronously. External callers (CLI, tests) use this; the p2p Receive
// goroutine reads cachedAdvertisedRange directly via sendStatus.
func (r *Reactor) AdvertisedRange() PeerRange {
	return r.refreshAdvertisedRangeWithTimeout()
}

func (r *Reactor) refreshAdvertisedRangeWithTimeout() PeerRange {
	ctx, cancel := context.WithTimeout(r.lifetimeContext(), r.opts.RequestTimeout)
	defer cancel()
	return r.refreshAdvertisedRange(ctx)
}

// cachedAdvertisedRange returns the most recently cached advertised range
// without performing any blocking I/O. Safe to call from the p2p receive
// goroutine. The hot component is always re-read so newly persisted heights
// become visible immediately even before the next cache refresh.
func (r *Reactor) cachedAdvertisedRange() PeerRange {
	hot := r.ingestor.AdvertisedRange()
	if r.opts.ColdBlockSource == nil {
		return hot
	}
	cached, ok := r.cachedRange.Load().(PeerRange)
	if !ok {
		return hot
	}
	return mergeAdvertisedRanges(cached, hot)
}

func (r *Reactor) lifetimeContext() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

func (r *Reactor) Receive(e p2p.Envelope) {
	if err := cmtblocksync.ValidateMsg(e.Message); err != nil {
		return
	}
	switch msg := e.Message.(type) {
	case *bcproto.BlockRequest:
		r.respondToBlockRequest(e.Src, msg.Height)
	case *bcproto.BlockResponse:
		r.handleBlockResponse(e.Src, msg)
	case *bcproto.StatusRequest:
		r.sendStatus(e.Src)
	case *bcproto.StatusResponse:
		r.handleStatus(e.Src, msg)
	case *bcproto.NoBlockResponse:
		var peerID PeerID
		if e.Src != nil {
			peerID = PeerID(e.Src.ID())
		}
		r.mu.Lock()
		r.planner.MarkNoBlock(peerID, msg.Height)
		r.mu.Unlock()
		r.planAndSend()
	default:
		return
	}
}

func (r *Reactor) respondToBlockRequest(peer p2p.Peer, height int64) {
	if peer == nil {
		return
	}
	block := r.ingestor.LoadBlock(height)
	if block != nil {
		if sendBlock(peer, block) {
			r.hotResponses.Add(1)
		}
		return
	}
	if r.opts.ColdBlockSource != nil {
		// Enqueue immediately without an inline cold-source advertised-range
		// check; the cold worker's LoadBlock already returns
		// ErrColdBlockNotFound for heights outside the manifest. This keeps
		// the p2p receive goroutine off any S3 round-trip.
		if !r.enqueueColdBlockRequest(peer, height) {
			r.coldQueueFull.Add(1)
		}
		return
	}
	if sendNoBlock(peer, height) {
		r.noBlockResponses.Add(1)
	}
}

func (r *Reactor) respondToColdBlockRequest(peer p2p.Peer, height int64) {
	ctx, cancel := context.WithTimeout(r.lifetimeContext(), r.opts.RequestTimeout)
	defer cancel()
	// Check advertised range first in the worker (off the p2p Receive
	// goroutine) so we can skip the LoadBlock fetch for unadvertised heights
	// and surface manifest errors as cold errors.
	advertised, err := r.opts.ColdBlockSource.AdvertisedRange(ctx)
	if err != nil {
		r.coldErrors.Add(1)
		if sendNoBlock(peer, height) {
			r.noBlockResponses.Add(1)
		}
		return
	}
	if !advertised.Contains(height) {
		if sendNoBlock(peer, height) {
			r.noBlockResponses.Add(1)
		}
		return
	}
	block, err := r.opts.ColdBlockSource.LoadBlock(ctx, height)
	if err != nil || block == nil {
		if err != nil && !errors.Is(err, ErrColdBlockNotFound) {
			r.coldErrors.Add(1)
		}
		if sendNoBlock(peer, height) {
			r.noBlockResponses.Add(1)
		}
		return
	}
	if sendBlock(peer, block) {
		r.coldResponses.Add(1)
	}
}

func (r *Reactor) enqueueColdBlockRequest(peer p2p.Peer, height int64) bool {
	select {
	case r.coldWork <- coldBlockRequest{peer: peer, height: height}:
		return true
	default:
		return false
	}
}

func (r *Reactor) coldBlockWorker() {
	for {
		select {
		case request := <-r.coldWork:
			r.coldActive.Add(1)
			r.respondToColdBlockRequest(request.peer, request.height)
			r.coldActive.Add(-1)
		case <-r.ctx.Done():
			return
		case <-r.Quit():
			return
		}
	}
}

func sendNoBlock(peer p2p.Peer, height int64) bool {
	if peer == nil {
		return false
	}
	return peer.TrySend(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.NoBlockResponse{Height: height},
	})
}

func sendBlock(peer p2p.Peer, block *types.Block) bool {
	if peer == nil || block == nil {
		return false
	}
	pb, err := block.ToProto()
	if err != nil {
		return false
	}
	return peer.TrySend(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockResponse{Block: pb},
	})
}

func (r *Reactor) handleBlockResponse(peer p2p.Peer, msg *bcproto.BlockResponse) {
	if peer == nil {
		return
	}
	peerID := PeerID(peer.ID())
	block, err := types.BlockFromProto(msg.Block)
	if err != nil {
		r.markPeerBlockFailed(peer, 0)
		r.planAndSend()
		return
	}
	r.mu.Lock()
	expectedPeer, requested := r.planner.InflightPeer(block.Height)
	if !requested || expectedPeer != peerID {
		r.mu.Unlock()
		return
	}
	nextHeight := r.ingestor.NextHeight()
	if block.Height < nextHeight {
		r.planner.MarkDone(block.Height)
		r.mu.Unlock()
		return
	}
	if block.Height > nextHeight {
		r.bufferedResponses[block.Height] = bufferedBlockResponse{peerID: peerID, block: block}
		r.planner.MarkReceived(block.Height)
		r.mu.Unlock()
		r.planAndSend()
		return
	}
	r.mu.Unlock()
	if !r.submitAndDrainBuffered(peerID, block) {
		return
	}
	r.broadcastStatus()
	r.planAndSend()
}

func (r *Reactor) submitAndDrainBuffered(peerID PeerID, block *types.Block) bool {
	r.mu.Lock()
	for block != nil {
		if _, err := r.ingestor.submit(block, peerID); err != nil {
			r.planner.MarkFailed(block.Height)
			delete(r.bufferedResponses, block.Height)
			var conflict *pendingConflictError
			switch {
			case errors.As(err, &conflict):
				// Neither side of a conflicting pair proves which provider lied.
				// Try each pending provider at most once at this height.
				r.planner.RejectAt(conflict.peerID, conflict.height)
				r.planner.MarkFailed(conflict.height)
				r.bufferedResponses[block.Height] = bufferedBlockResponse{peerID: peerID, block: block}
				r.planner.MarkReceived(block.Height)
			case errors.Is(err, errValidationSource):
				r.planner.RetryLater(peerID, block.Height)
			default:
				r.planner.RejectAt(peerID, block.Height)
			}
			r.mu.Unlock()
			r.planAndSend()
			return false
		}
		r.planner.MarkDone(block.Height)
		delete(r.bufferedResponses, block.Height)
		nextHeight := r.ingestor.NextHeight()
		buffered, ok := r.bufferedResponses[nextHeight]
		if ok {
			peerID = buffered.peerID
			block = buffered.block
		} else {
			block = nil
		}
	}
	r.mu.Unlock()
	return true
}

func (r *Reactor) handleStatus(peer p2p.Peer, msg *bcproto.StatusResponse) {
	if peer == nil {
		return
	}
	peerID := PeerID(peer.ID())
	advertised := PeerRange{Base: msg.Base, Height: msg.Height}
	r.mu.Lock()
	err := r.planner.UpsertPeer(peerID, advertised)
	if err == nil {
		r.dropBufferedResponsesOutsideRangeLocked(peerID, advertised)
	}
	r.mu.Unlock()
	if err != nil {
		return
	}
	r.planAndSend()
}

func (r *Reactor) sendStatus(peer p2p.Peer) {
	if peer == nil {
		return
	}
	advertised := r.cachedAdvertisedRange()
	peer.TrySend(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message: &bcproto.StatusResponse{
			Base:   advertised.Base,
			Height: advertised.Height,
		},
	})
}

// refreshAdvertisedRange consults the cold source synchronously, updates the
// cached cold range, and returns the merged hot+cold range. Called from the
// status loop and from external AdvertisedRange callers — never from Receive.
func (r *Reactor) refreshAdvertisedRange(ctx context.Context) PeerRange {
	hot := r.ingestor.AdvertisedRange()
	if r.opts.ColdBlockSource == nil {
		return hot
	}
	cold, err := r.opts.ColdBlockSource.AdvertisedRange(ctx)
	if err != nil {
		// Fall back to the last known cached cold range so we don't regress
		// the advertised heights on a transient cold-source error.
		if cached, ok := r.cachedRange.Load().(PeerRange); ok {
			return mergeAdvertisedRanges(cached, hot)
		}
		return hot
	}
	r.cachedRange.Store(cold)
	return mergeAdvertisedRanges(cold, hot)
}

func requestStatus(peer p2p.Peer) {
	if peer == nil {
		return
	}
	peer.TrySend(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.StatusRequest{},
	})
}

func (r *Reactor) requestPeerStatuses() {
	r.mu.Lock()
	peers := make([]p2p.Peer, 0, len(r.peers))
	for _, peer := range r.peers {
		peers = append(peers, peer)
	}
	r.mu.Unlock()
	for _, peer := range peers {
		requestStatus(peer)
	}
}

func (r *Reactor) broadcastStatus() {
	r.mu.Lock()
	peers := make([]p2p.Peer, 0, len(r.peers))
	for _, peer := range r.peers {
		peers = append(peers, peer)
	}
	r.mu.Unlock()
	for _, peer := range peers {
		r.sendStatus(peer)
	}
}

func (r *Reactor) statusRequestLoop() {
	r.exchangeStatuses()
	ticker := time.NewTicker(r.opts.StatusRequestInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.exchangeStatuses()
		case <-r.ctx.Done():
			return
		case <-r.Quit():
			return
		}
	}
}

func (r *Reactor) exchangeStatuses() {
	// Refresh the cached cold-source advertised range here so subsequent
	// sendStatus calls from Receive observe a recent snapshot without
	// blocking on S3.
	r.refreshAdvertisedRangeWithTimeout()
	r.requestPeerStatuses()
	r.broadcastStatus()
}

func (r *Reactor) requestTimeoutLoop() {
	interval := r.opts.RequestTimeout / 2
	if interval <= 0 {
		interval = r.opts.RequestTimeout
	}
	if interval > time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.expireRequestsAndPlan()
		case <-r.ctx.Done():
			return
		case <-r.Quit():
			return
		}
	}
}

func (r *Reactor) expireRequestsAndPlan() {
	r.mu.Lock()
	expired := r.planner.ExpireInflight(time.Now(), r.opts.RequestTimeout)
	r.mu.Unlock()
	if expired > 0 {
		r.requestTimeouts.Add(int64(expired))
		r.planAndSend()
	}
}

func (r *Reactor) planAndSend() {
	r.mu.Lock()
	requests, err := r.planner.PlanAt(r.ingestor.NextHeight(), r.opts.RequestLimit, time.Now())
	if err != nil {
		r.mu.Unlock()
		return
	}
	peers := make(map[PeerID]p2p.Peer, len(r.peers))
	for id, peer := range r.peers {
		peers[id] = peer
	}
	r.mu.Unlock()

	for _, request := range requests {
		peer := peers[request.PeerID]
		if peer == nil {
			r.mu.Lock()
			r.planner.MarkFailed(request.Height)
			r.mu.Unlock()
			continue
		}
		if !peer.TrySend(p2p.Envelope{
			ChannelID: cmtblocksync.BlocksyncChannel,
			Message:   &bcproto.BlockRequest{Height: request.Height},
		}) {
			r.mu.Lock()
			r.planner.MarkFailed(request.Height)
			r.mu.Unlock()
		}
	}
}

func (r *Reactor) markPeerBlockFailed(peer p2p.Peer, height int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if height > 0 {
		r.planner.MarkFailed(height)
	}
	if peer != nil {
		peerID := PeerID(peer.ID())
		r.planner.RemovePeer(peerID)
		r.dropBufferedResponsesFromPeerLocked(peerID)
	}
}

func (r *Reactor) dropBufferedResponsesFromPeerLocked(peerID PeerID) {
	for height, buffered := range r.bufferedResponses {
		if buffered.peerID == peerID {
			delete(r.bufferedResponses, height)
			r.planner.MarkFailed(height)
		}
	}
}

func (r *Reactor) dropBufferedResponsesOutsideRangeLocked(peerID PeerID, advertised PeerRange) {
	for height, buffered := range r.bufferedResponses {
		if buffered.peerID == peerID && !advertised.Contains(height) {
			delete(r.bufferedResponses, height)
			r.planner.MarkFailed(height)
		}
	}
}
