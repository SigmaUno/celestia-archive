package blocksyncarchive

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRequestPlannerPlansContiguousMissingHeights(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 8}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 6}); err != nil {
		t.Fatal(err)
	}
	requests, err := planner.Plan(5, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []Request{
		{PeerID: "peer-a", Height: 5},
		{PeerID: "peer-b", Height: 6},
		{PeerID: "peer-b", Height: 7},
	}
	if len(requests) != len(want) {
		t.Fatalf("got %d requests, want %d: %+v", len(requests), len(want), requests)
	}
	for i := range want {
		if requests[i] != want[i] {
			t.Fatalf("request %d got %+v want %+v", i, requests[i], want[i])
		}
	}
}

func TestRequestPlannerTracksInflightAndRetriesFailures(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("got %d first requests, want 2", len(first))
	}
	if peerID, ok := planner.InflightPeer(1); !ok || peerID != "peer" {
		t.Fatalf("inflight peer for height 1 = %q/%v, want peer/true", peerID, ok)
	}
	second, err := planner.Plan(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 || second[0].Height != 3 || second[1].Height != 4 {
		t.Fatalf("unexpected second requests: %+v", second)
	}
	planner.MarkFailed(1)
	retry, err := planner.Plan(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].Height != 1 {
		t.Fatalf("unexpected retry requests: %+v", retry)
	}
}

func TestRequestPlannerFailsOverAfterNoBlockResponse(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer-a" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	planner.MarkNoBlock("peer-a", 2)
	second, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].PeerID != "peer-b" || second[0].Height != 2 {
		t.Fatalf("unexpected failover request: %+v", second)
	}
}

func TestRequestPlannerIgnoresNoBlockFromWrongPeer(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer-a" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	planner.MarkNoBlock("peer-b", 2)
	if peerID, ok := planner.InflightPeer(2); !ok || peerID != "peer-a" {
		t.Fatalf("inflight peer after wrong no-block = %q/%v, want peer-a/true", peerID, ok)
	}
	next, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 0 {
		t.Fatalf("unexpected next request after wrong no-block: %+v", next)
	}
	planner.RemovePeer("peer-a")
	failover, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(failover) != 1 || failover[0].PeerID != "peer-b" || failover[0].Height != 2 {
		t.Fatalf("unexpected failover after wrong no-block: %+v", failover)
	}
}

func TestRequestPlannerSkipsReceivedBufferedHeightsWithinWindow(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 || first[0].Height != 1 || first[1].Height != 2 || first[2].Height != 3 {
		t.Fatalf("unexpected first requests: %+v", first)
	}
	planner.MarkReceived(2)
	second, err := planner.Plan(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("unexpected duplicate requests while window full: %+v", second)
	}
	planner.MarkDone(1)
	third, err := planner.Plan(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 1 || third[0].Height != 4 {
		t.Fatalf("unexpected request after advancing window: %+v", third)
	}
}

func TestRequestPlannerExpiresInflightAndPrefersAlternatePeer(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	first, err := planner.PlanAt(2, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer-a" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	if expired := planner.ExpireInflight(now.Add(time.Second), 2*time.Second); expired != 0 {
		t.Fatalf("expired %d early requests, want 0", expired)
	}
	if expired := planner.ExpireInflight(now.Add(3*time.Second), 2*time.Second); expired != 1 {
		t.Fatalf("expired %d requests, want 1", expired)
	}
	second, err := planner.PlanAt(2, 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].PeerID != "peer-b" || second[0].Height != 2 {
		t.Fatalf("unexpected timeout failover request: %+v", second)
	}
}

func TestRequestPlannerRetriesTimedOutPeerWhenItIsTheOnlySource(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	first, err := planner.PlanAt(2, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer-a" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	if expired := planner.ExpireInflight(now.Add(3*time.Second), 2*time.Second); expired != 1 {
		t.Fatalf("expired %d requests, want 1", expired)
	}
	retry, err := planner.PlanAt(2, 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].PeerID != "peer-a" || retry[0].Height != 2 {
		t.Fatalf("unexpected timeout retry request: %+v", retry)
	}
}

func TestRequestPlannerPrunesStaleNoBlockAndTimeoutMarkers(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 6}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 6}); err != nil {
		t.Fatal(err)
	}
	planner.MarkNoBlock("peer-a", 2)
	now := time.Unix(100, 0)
	first, err := planner.PlanAt(3, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Height != 3 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	if expired := planner.ExpireInflight(now.Add(3*time.Second), 2*time.Second); expired != 1 {
		t.Fatalf("expired %d requests, want 1", expired)
	}
	if _, ok := planner.noBlocks["peer-a"][2]; ok {
		t.Fatal("no-block marker below next height was not pruned")
	}
	if _, ok := planner.timeouts["peer-a"][3]; !ok {
		t.Fatal("timeout marker for pending height was not recorded")
	}
	retry, err := planner.PlanAt(4, 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].Height != 4 {
		t.Fatalf("unexpected retry request: %+v", retry)
	}
	if _, ok := planner.timeouts["peer-a"][3]; ok {
		t.Fatal("timeout marker below next height was not pruned")
	}
}

func TestRequestPlannerClearsNoBlockMarkersWhenPeerRangeChanges(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	planner.MarkNoBlock("peer", 2)
	if upsertErr := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 5}); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	retry, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].PeerID != "peer" || retry[0].Height != 2 {
		t.Fatalf("unexpected retry after range update: %+v", retry)
	}
}

func TestRequestPlannerClearsInflightWhenPeerRangeContracts(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	requests, err := planner.Plan(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0].PeerID != "peer-a" || requests[0].Height != 3 {
		t.Fatalf("unexpected first request: %+v", requests)
	}
	if upsertErr := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 2}); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	retry, err := planner.Plan(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].PeerID != "peer-b" || retry[0].Height != 3 {
		t.Fatalf("unexpected retry after peer range contraction: %+v", retry)
	}
}

func TestRequestPlannerReportsUnfillableGap(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 3, Height: 5}); err != nil {
		t.Fatal(err)
	}
	_, err := planner.Plan(1, 1)
	if !errors.Is(err, ErrNoPeerForHeight) {
		t.Fatalf("got err %v, want ErrNoPeerForHeight", err)
	}
}

func TestRequestPlannerRejectsExcessiveLimit(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 5}); err != nil {
		t.Fatal(err)
	}
	_, err := planner.Plan(1, MaxRequestLimit+1)
	if err == nil || !strings.Contains(err.Error(), "request limit") {
		t.Fatalf("Plan err=%v, want request limit error", err)
	}
}

func TestRequestPlannerSharesLoadAndBoundsPeers(t *testing.T) {
	p := NewRequestPlanner()
	for _, id := range []PeerID{"c", "b", "a"} {
		if err := p.UpsertPeer(id, PeerRange{Base: 1, Height: 1024}); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(100, 0)
	requests, err := p.PlanAt(1, 128, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 3*MaxInflightPerPeer {
		t.Fatalf("got %d requests", len(requests))
	}
	counts := map[PeerID]int{}
	for n, request := range requests {
		if request.Height != int64(n+1) {
			t.Fatalf("out of order request: %+v", request)
		}
		counts[request.PeerID]++
	}
	for _, id := range []PeerID{"a", "b", "c"} {
		if counts[id] != MaxInflightPerPeer {
			t.Fatalf("distribution: %v", counts)
		}
	}
	// Free capacity on b while a still owns the earliest missing height.
	p.MarkReceived(2)
	p.MarkFailed(1)
	requests, err = p.PlanAt(1, 128, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || requests[0] != (Request{PeerID: "a", Height: 1}) || requests[1] != (Request{PeerID: "b", Height: 97}) {
		t.Fatalf("unexpected refill: %+v", requests)
	}
	requests, err = p.PlanAt(1, 128, now)
	if err != nil || len(requests) != 0 {
		t.Fatalf("cap not preserved across passes: %+v, %v", requests, err)
	}
}

func TestRequestPlannerPrioritizesGapOnFailover(t *testing.T) {
	for _, failure := range []string{"timeout", "no-block", "disconnect", "rejected"} {
		t.Run(failure, func(t *testing.T) {
			p := NewRequestPlanner()
			for _, id := range []PeerID{"a", "b"} {
				if err := p.UpsertPeer(id, PeerRange{Base: 1, Height: 10}); err != nil {
					t.Fatal(err)
				}
			}
			now := time.Unix(100, 0)
			if _, err := p.PlanAt(1, 4, now); err != nil {
				t.Fatal(err)
			}
			p.MarkReceived(2)
			p.MarkReceived(3)
			p.MarkReceived(4)
			switch failure {
			case "timeout":
				p.ExpireInflight(now.Add(time.Second), time.Second)
			case "no-block":
				p.MarkNoBlock("a", 1)
			case "disconnect":
				p.RemovePeer("a")
			case "rejected":
				p.RejectAt("a", 1)
				p.MarkFailed(1)
			}
			requests, err := p.PlanAt(1, 4, now.Add(time.Second))
			if err != nil || len(requests) != 1 || requests[0] != (Request{PeerID: "b", Height: 1}) {
				t.Fatalf("gap failover: %+v, %v", requests, err)
			}
		})
	}
}

func TestRequestPlannerLocalRetryDoesNotPenalizeProvider(t *testing.T) {
	p := NewRequestPlanner()
	for _, id := range []PeerID{"a", "b"} {
		if err := p.UpsertPeer(id, PeerRange{Base: 1, Height: 10}); err != nil {
			t.Fatal(err)
		}
	}
	p.RetryLater("a", 1)
	requests, err := p.Plan(1, 2)
	if err != nil || len(requests) != 1 || requests[0] != (Request{PeerID: "a", Height: 2}) {
		t.Fatalf("local retry affected load: %+v, %v", requests, err)
	}
	p.MarkReceived(2)
	p.ExpireInflight(time.Now().Add(time.Hour), time.Second)
	requests, err = p.Plan(1, 2)
	if err != nil || len(requests) != 1 || requests[0] != (Request{PeerID: "a", Height: 1}) || len(p.timeouts) != 0 {
		t.Fatalf("local retry penalized provider: %+v, %v, %v", requests, err, p.timeouts)
	}
}

func TestRequestPlannerRecoveryPairRemainsBoundedAndCoherent(t *testing.T) {
	p := NewRequestPlanner()
	for _, id := range []PeerID{"a", "b"} {
		if err := p.UpsertPeer(id, PeerRange{Base: 1, Height: 100}); err != nil {
			t.Fatal(err)
		}
	}
	p.RecoverPair(1)
	requests, err := p.Plan(1, 32)
	if err != nil || len(requests) != 2 || requests[0] != (Request{PeerID: "a", Height: 1}) || requests[1] != (Request{PeerID: "a", Height: 2}) {
		t.Fatalf("incoherent recovery: %+v, %v", requests, err)
	}
	p.MarkNoBlock("a", 2)
	p.RecoverPair(1)
	requests, err = p.Plan(1, 32)
	if err != nil || len(requests) != 2 || requests[0] != (Request{PeerID: "b", Height: 1}) || requests[1] != (Request{PeerID: "b", Height: 2}) {
		t.Fatalf("incoherent failover: %+v, %v", requests, err)
	}
	p.RecoverPair(1)
	if requests, err = p.Plan(1, 32); !errors.Is(err, ErrNoPeerForHeight) || len(requests) != 0 {
		t.Fatalf("recovery revisited exhausted providers: %+v, %v", requests, err)
	}
}

func BenchmarkRequestPlannerDistribution(b *testing.B) {
	for _, peers := range []int{1, 4} {
		b.Run(fmt.Sprintf("providers=%d", peers), func(b *testing.B) {
			counts := make(map[PeerID]int, peers)
			now := time.Unix(100, 0)
			for range b.N {
				p := NewRequestPlanner()
				for n := range peers {
					if err := p.UpsertPeer(PeerID(fmt.Sprintf("peer-%d", n)), PeerRange{Base: 1, Height: 32}); err != nil {
						b.Fatal(err)
					}
				}
				requests, err := p.PlanAt(1, 32, now)
				if err != nil || len(requests) != 32 {
					b.Fatalf("requests=%d err=%v", len(requests), err)
				}
				for _, request := range requests {
					counts[request.PeerID]++
				}
			}
			for id, count := range counts {
				b.ReportMetric(float64(count)/float64(b.N), "req/"+string(id))
			}
		})
	}
}

func TestRequestPlannerWaitsForEarliestProviderCapacity(t *testing.T) {
	p := NewRequestPlanner()
	if err := p.UpsertPeer("a", PeerRange{Base: 1, Height: 100}); err != nil {
		t.Fatal(err)
	}
	if requests, err := p.Plan(1, MaxInflightPerPeer); err != nil || len(requests) != MaxInflightPerPeer {
		t.Fatalf("initial load: %+v, %v", requests, err)
	}
	if err := p.UpsertPeer("b", PeerRange{Base: 34, Height: 100}); err != nil {
		t.Fatal(err)
	}
	if requests, err := p.Plan(33, 32); err != nil || len(requests) != 0 {
		t.Fatalf("skipped saturated provider at earliest gap: %+v, %v", requests, err)
	}
	p.MarkDone(1)
	requests, err := p.Plan(33, 32)
	if err != nil || len(requests) != 32 || requests[0] != (Request{PeerID: "a", Height: 33}) {
		t.Fatalf("did not resume at earliest gap: %+v, %v", requests, err)
	}
}
