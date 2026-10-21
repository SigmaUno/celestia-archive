package blocksyncarchive

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

var (
	ErrNoPeers          = errors.New("no blocksync peers available")
	ErrNoPeerForHeight  = errors.New("no peer advertises requested height")
	ErrInvalidPeerRange = errors.New("invalid peer range")
)

type PeerID string

const MaxInflightPerPeer = 32

type PeerRange struct {
	Base   int64
	Height int64
}

func (r PeerRange) Validate() error {
	if r.Base <= 0 || r.Height <= 0 || r.Height < r.Base {
		return fmt.Errorf("%w: %d-%d", ErrInvalidPeerRange, r.Base, r.Height)
	}
	return nil
}

func (r PeerRange) Contains(height int64) bool {
	return height >= r.Base && height <= r.Height
}

type Request struct {
	PeerID PeerID
	Height int64
}

type RequestPlanner struct {
	peers          map[PeerID]PeerRange
	inflight       map[int64]inflightRequest
	received       map[int64]struct{}
	noBlocks       map[PeerID]map[int64]struct{}
	timeouts       map[PeerID]map[int64]struct{}
	rejected       map[PeerID]map[int64]struct{}
	recoveryHeight int64
	recoveryPeer   PeerID
	recoveryTried  map[PeerID]bool
}

type inflightRequest struct {
	PeerID      PeerID
	RequestedAt time.Time
	LocalRetry  bool
}

func NewRequestPlanner() *RequestPlanner {
	return &RequestPlanner{
		peers:    make(map[PeerID]PeerRange),
		inflight: make(map[int64]inflightRequest),
		received: make(map[int64]struct{}),
		noBlocks: make(map[PeerID]map[int64]struct{}),
		timeouts: make(map[PeerID]map[int64]struct{}),
		rejected: make(map[PeerID]map[int64]struct{}),
	}
}

func (p *RequestPlanner) UpsertPeer(id PeerID, advertised PeerRange) error {
	if id == "" {
		return errors.New("peer id is required")
	}
	if err := advertised.Validate(); err != nil {
		return err
	}
	if existing, ok := p.peers[id]; ok && existing != advertised {
		delete(p.noBlocks, id)
		delete(p.timeouts, id)
	}
	p.peers[id] = advertised
	for height, inflight := range p.inflight {
		if inflight.PeerID == id && !advertised.Contains(height) {
			delete(p.inflight, height)
		}
	}
	return nil
}

func (p *RequestPlanner) RemovePeer(id PeerID) {
	delete(p.peers, id)
	delete(p.noBlocks, id)
	delete(p.timeouts, id)
	for height, inflight := range p.inflight {
		if inflight.PeerID == id {
			delete(p.inflight, height)
		}
	}
}

func (p *RequestPlanner) MarkDone(height int64) {
	delete(p.inflight, height)
	delete(p.received, height)
}

func (p *RequestPlanner) MarkFailed(height int64) {
	delete(p.inflight, height)
	delete(p.received, height)
}

func (p *RequestPlanner) RejectAt(id PeerID, height int64) {
	if id == "" {
		return
	}
	if p.rejected[id] == nil {
		p.rejected[id] = make(map[int64]struct{})
	}
	p.rejected[id][height] = struct{}{}
}

func (p *RequestPlanner) RetryLater(id PeerID, height int64) {
	p.MarkFailed(height)
	p.inflight[height] = inflightRequest{PeerID: id, RequestedAt: time.Now(), LocalRetry: true}
}

// simplify: recovery needs one provider for both heights; cross-provider recovery requires independent fork evidence.
func (p *RequestPlanner) RecoverPair(height int64) {
	if p.recoveryHeight != height {
		p.recoveryTried = make(map[PeerID]bool)
	}
	p.recoveryHeight, p.recoveryPeer = height, ""
	for h := range p.inflight {
		if h >= height {
			delete(p.inflight, h)
		}
	}
	for h := range p.received {
		if h >= height {
			delete(p.received, h)
		}
	}
}

func (p *RequestPlanner) recoveryFailed(id PeerID) bool {
	h := p.recoveryHeight
	r := p.peers[id]
	for _, height := range []int64{h, h + 1} {
		if !r.Contains(height) || p.peerReturnedNoBlock(id, height) || p.peerTimedOut(id, height) {
			return true
		}
		if _, rejected := p.rejected[id][height]; rejected {
			return true
		}
	}
	return false
}

func (p *RequestPlanner) MarkReceived(height int64) {
	delete(p.inflight, height)
	if height > 0 {
		p.received[height] = struct{}{}
	}
}

func (p *RequestPlanner) MarkNoBlock(id PeerID, height int64) {
	if id == "" || height <= 0 {
		return
	}
	inflight, ok := p.inflight[height]
	if !ok || inflight.PeerID != id {
		return
	}
	delete(p.inflight, height)
	if p.noBlocks[id] == nil {
		p.noBlocks[id] = make(map[int64]struct{})
	}
	p.noBlocks[id][height] = struct{}{}
}

func (p *RequestPlanner) ExpireInflight(now time.Time, timeout time.Duration) int {
	if timeout <= 0 {
		return 0
	}
	var expired int
	for height, inflight := range p.inflight {
		if now.Sub(inflight.RequestedAt) < timeout {
			continue
		}
		delete(p.inflight, height)
		if inflight.LocalRetry {
			expired++
			continue
		}
		if p.timeouts[inflight.PeerID] == nil {
			p.timeouts[inflight.PeerID] = make(map[int64]struct{})
		}
		p.timeouts[inflight.PeerID][height] = struct{}{}
		expired++
	}
	return expired
}

func (p *RequestPlanner) InflightCount() int {
	return len(p.inflight)
}

func (p *RequestPlanner) InflightPeer(height int64) (PeerID, bool) {
	inflight, ok := p.inflight[height]
	if !ok {
		return "", false
	}
	return inflight.PeerID, true
}

func (p *RequestPlanner) BestHeight() int64 {
	var best int64
	for _, advertised := range p.peers {
		if advertised.Height > best {
			best = advertised.Height
		}
	}
	return best
}

func (p *RequestPlanner) Plan(nextHeight int64, limit int) ([]Request, error) {
	return p.PlanAt(nextHeight, limit, time.Now())
}

func (p *RequestPlanner) PlanAt(nextHeight int64, limit int, now time.Time) ([]Request, error) {
	if nextHeight <= 0 {
		return nil, errors.New("next height must be positive")
	}
	p.pruneMarkersBelow(nextHeight)
	// Pending height may be refetched until its successor persists it.
	for id, heights := range p.rejected {
		pruneHeightSet(heights, nextHeight-1)
		if len(heights) == 0 {
			delete(p.rejected, id)
		}
	}
	if limit <= 0 {
		return nil, nil
	}
	if limit > MaxRequestLimit {
		return nil, fmt.Errorf("request limit cannot exceed %d", MaxRequestLimit)
	}
	if len(p.peers) == 0 {
		return nil, ErrNoPeers
	}
	if p.recoveryHeight > 0 && nextHeight > p.recoveryHeight+1 {
		p.recoveryHeight, p.recoveryPeer, p.recoveryTried = 0, "", nil
	}
	if p.recoveryHeight > 0 && p.recoveryPeer == "" {
		ids := make([]PeerID, 0, len(p.peers))
		for id := range p.peers {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			if !p.recoveryTried[id] && !p.recoveryFailed(id) {
				p.recoveryPeer = id
				p.recoveryTried[id] = true
				break
			}
		}
		if p.recoveryPeer == "" {
			return nil, ErrNoPeerForHeight
		}
	}
	best := p.BestHeight()
	if p.recoveryHeight > 0 {
		best = p.recoveryHeight + 1
	}
	if best < nextHeight {
		return nil, nil
	}

	requests := make([]Request, 0, limit)
	loads := make(map[PeerID]int, len(p.peers))
	for _, request := range p.inflight {
		if !request.LocalRetry {
			loads[request.PeerID]++
		}
	}
	windowEnd := nextHeight + int64(limit) - 1
	if windowEnd < nextHeight {
		windowEnd = best
	}
	for height := nextHeight; height <= best && height <= windowEnd && len(requests) < limit; height++ {
		if _, exists := p.inflight[height]; exists {
			continue
		}
		if _, exists := p.received[height]; exists {
			continue
		}
		peerID, ok := p.recoveryPeer, p.recoveryPeer != ""
		if !ok {
			peerID, ok = p.peerForHeight(height, loads)
		}
		if !ok {
			if len(requests) == 0 {
				return nil, fmt.Errorf("%w: %d", ErrNoPeerForHeight, height)
			}
			return requests, nil
		}
		if p.recoveryPeer == "" && loads[peerID] >= MaxInflightPerPeer {
			return requests, nil
		}
		p.inflight[height] = inflightRequest{PeerID: peerID, RequestedAt: now}
		loads[peerID]++
		requests = append(requests, Request{PeerID: peerID, Height: height})
	}
	return requests, nil
}

func (p *RequestPlanner) pruneMarkersBelow(height int64) {
	for receivedHeight := range p.received {
		if receivedHeight < height {
			delete(p.received, receivedHeight)
		}
	}
	for peerID, heights := range p.noBlocks {
		pruneHeightSet(heights, height)
		if len(heights) == 0 {
			delete(p.noBlocks, peerID)
		}
	}
	for peerID, heights := range p.timeouts {
		pruneHeightSet(heights, height)
		if len(heights) == 0 {
			delete(p.timeouts, peerID)
		}
	}
}

func pruneHeightSet(heights map[int64]struct{}, below int64) {
	for height := range heights {
		if height < below {
			delete(heights, height)
		}
	}
}

func (p *RequestPlanner) peerForHeight(height int64, loads map[PeerID]int) (PeerID, bool) {
	for _, allowTimeout := range []bool{false, true} {
		var best PeerID
		for id, advertised := range p.peers {
			if _, rejected := p.rejected[id][height]; rejected {
				continue
			}
			if !advertised.Contains(height) || p.peerReturnedNoBlock(id, height) || (!allowTimeout && p.peerTimedOut(id, height)) {
				continue
			}
			if best == "" || loads[id] < loads[best] || (loads[id] == loads[best] && id < best) {
				best = id
			}
		}
		if best != "" {
			return best, true
		}
	}
	return "", false
}

func (p *RequestPlanner) peerReturnedNoBlock(peerID PeerID, height int64) bool {
	heights := p.noBlocks[peerID]
	if heights == nil {
		return false
	}
	_, ok := heights[height]
	return ok
}

func (p *RequestPlanner) peerTimedOut(peerID PeerID, height int64) bool {
	heights := p.timeouts[peerID]
	if heights == nil {
		return false
	}
	_, ok := heights[height]
	return ok
}
