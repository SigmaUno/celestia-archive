package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/01builders/cometbft-archive/internal/archive"
	dbm "github.com/cometbft/cometbft-db"
	cmtblocksync "github.com/cometbft/cometbft/blocksync"
	cmtcfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/trace"
	cmtnode "github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	bcproto "github.com/cometbft/cometbft/proto/tendermint/blocksync"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/cometbft/cometbft/version"
)

const e2eChainID = "archive-e2e-chain"

func TestBinaryHelpListsArchiveCommands(t *testing.T) {
	cmd := exec.Command("go", "run", "../cmd/cometbft-archive", "--help")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go run help failed: %v\n%s", err, string(out))
	}
	text := string(out)
	for _, want := range []string{"migrate", "verify", "inspect", "hydrate", "archive-ready", "prune-hot", "soak", "serve"} {
		if !strings.Contains(text, want) {
			t.Fatalf("help output missing %q:\n%s", want, text)
		}
	}
}

func TestBinaryMigrateVerifyInspectHydrateServe(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	createE2EBlockStoreFixture(t, dbDir, 4)
	objectRoot := filepath.Join(t.TempDir(), "objects")
	cacheDir := filepath.Join(t.TempDir(), "cache")
	storeURL := "file://" + objectRoot

	migrateOut := runArchive(t,
		"migrate",
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", storeURL,
		"--chain-id", e2eChainID,
		"--start-height", "1",
		"--end-height", "4",
		"--segment-blocks", "2",
	)
	if !strings.Contains(migrateOut, `"segments": 2`) || !strings.Contains(migrateOut, `"uploaded": 2`) {
		t.Fatalf("unexpected migrate output:\n%s", migrateOut)
	}

	verifyOut := runArchive(t, "verify", "--store", storeURL, "--chain-id", e2eChainID)
	if !strings.Contains(verifyOut, `"segments_checked": 2`) || !strings.Contains(verifyOut, `"blocks_checked": 4`) {
		t.Fatalf("unexpected verify output:\n%s", verifyOut)
	}

	inspectOut := runArchive(t, "inspect", "--store", storeURL, "--chain-id", e2eChainID)
	if !strings.Contains(inspectOut, `"chain_id": "archive-e2e-chain"`) || !strings.Contains(inspectOut, `"blocks": 4`) {
		t.Fatalf("unexpected inspect output:\n%s", inspectOut)
	}

	hydrateOut := runArchive(t,
		"hydrate",
		"--store", storeURL,
		"--chain-id", e2eChainID,
		"--cache-dir", cacheDir,
		"--start-height", "2",
		"--end-height", "3",
	)
	if !strings.Contains(hydrateOut, `"blocks_written": 2`) {
		t.Fatalf("unexpected hydrate output:\n%s", hydrateOut)
	}

	liveStoreURL := "file://" + filepath.Join(t.TempDir(), "live-objects")
	archiveReadyOut := runArchive(t,
		"archive-ready",
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", liveStoreURL,
		"--chain-id", e2eChainID,
		"--ready-height", "3",
		"--segment-blocks", "2",
	)
	if !strings.Contains(archiveReadyOut, `"blocks_archived": 3`) {
		t.Fatalf("unexpected archive-ready output:\n%s", archiveReadyOut)
	}
	pruneOut := runArchive(t,
		"prune-hot",
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", liveStoreURL,
		"--chain-id", e2eChainID,
		"--retain-blocks", "1",
		"--evidence-max-age-blocks", "1",
		"--evidence-max-age-duration", "1ns",
	)
	if !strings.Contains(pruneOut, `"pruned": 3`) {
		t.Fatalf("unexpected prune-hot output:\n%s", pruneOut)
	}

	serveOut := runArchive(t, "serve", "--store", storeURL, "--chain-id", e2eChainID)
	if !strings.Contains(serveOut, `"blocksync_peer": true`) || !strings.Contains(serveOut, `"status": "dry-run"`) {
		t.Fatalf("unexpected serve output:\n%s", serveOut)
	}
}

func TestBinaryServeLiveStartsDiagnostics(t *testing.T) {
	binary := buildArchiveBinary(t)
	dbDir := filepath.Join(t.TempDir(), "db")
	createE2EBlockStoreFixture(t, dbDir, 2)
	objectRoot := filepath.Join(t.TempDir(), "objects")
	storeURL := "file://" + objectRoot
	p2pListen := freeP2PListenAddress(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx,
		binary,
		"serve",
		"--dry-run=false",
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", storeURL,
		"--chain-id", e2eChainID,
		"--node-key-file", filepath.Join(t.TempDir(), "node_key.json"),
		"--p2p-listen", p2pListen,
		"--metrics-listen", "127.0.0.1:0",
		"--archive-interval", "100ms",
		"--prune-interval", "0s",
		"--safety-window", "2",
		"--segment-blocks", "2",
	)
	stdout, pipeErr := cmd.StdoutPipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if startErr := cmd.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	defer func() {
		cancel()
		if waitErr := cmd.Wait(); waitErr != nil && ctx.Err() == nil {
			t.Errorf("serve exited unexpectedly: %v\nstderr:\n%s", waitErr, stderr.String())
		}
	}()

	var started map[string]any
	if decodeErr := json.NewDecoder(stdout).Decode(&started); decodeErr != nil {
		t.Fatalf("decode serve startup JSON: %v\nstderr:\n%s", decodeErr, stderr.String())
	}
	consensusNode, hasConsensusNode := started["consensus_node"].(bool)
	blocksyncPeer, hasBlocksyncPeer := started["blocksync_peer"].(bool)
	if started["status"] != "running" || !hasConsensusNode || !hasBlocksyncPeer || consensusNode || !blocksyncPeer {
		t.Fatalf("unexpected serve startup JSON: %+v", started)
	}
	metricsListen, ok := started["metrics_listen"].(string)
	if !ok || metricsListen == "" {
		t.Fatalf("missing metrics listener in startup JSON: %+v", started)
	}
	client := http.Client{Timeout: time.Second}
	health, err := client.Get("http://" + metricsListen + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", health.StatusCode)
	}
	ready, err := client.Get("http://" + metricsListen + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = ready.Body.Close()
	if ready.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d, want %d", ready.StatusCode, http.StatusServiceUnavailable)
	}
	metrics, err := client.Get("http://" + metricsListen + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metrics.Body.Close()
	var got map[string]any
	if err := json.NewDecoder(metrics.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	p2pPeers, ok := jsonNumber(got["p2p_peers"])
	if !ok {
		t.Fatalf("missing p2p peer metric: %+v", got)
	}
	peerBestHeight, ok := jsonNumber(got["peer_best_height"])
	if !ok {
		t.Fatalf("missing peer best height metric: %+v", got)
	}
	if p2pPeers != 0 || peerBestHeight != 0 {
		t.Fatalf("unexpected live metrics without peers: %+v", got)
	}
	for _, key := range []string{
		"blocksync_hot_responses",
		"blocksync_cold_responses",
		"blocksync_no_block_responses",
		"blocksync_cold_errors",
		"blocksync_cold_queue",
		"blocksync_cold_queue_full",
		"blocksync_cold_active",
	} {
		if _, ok := jsonNumber(got[key]); !ok {
			t.Fatalf("missing numeric metric %q: %+v", key, got)
		}
	}
}

func TestBinaryServeSyncsFromRealCometBFTBlocksyncPeer(t *testing.T) {
	binary := buildArchiveBinary(t)
	stockListen := freeP2PListenAddress(t)
	stock := newE2EStockBlocksyncP2PNode(t, stockListen,
		makeE2EBlock(t, 1),
		makeE2EBlock(t, 2),
		makeE2EBlock(t, 3),
		makeE2EBlock(t, 4),
		makeE2EBlock(t, 5),
		makeE2EBlock(t, 6),
		makeE2EBlock(t, 7),
	)
	if err := stock.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stock.Stop(); err != nil {
			t.Fatal(err)
		}
	}()

	dbDir := filepath.Join(t.TempDir(), "archive-db")
	storeURL := "file://" + filepath.Join(t.TempDir(), "objects")
	archiveListen := freeP2PListenAddress(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx,
		binary,
		"serve",
		"--dry-run=false",
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", storeURL,
		"--chain-id", e2eChainID,
		"--node-key-file", filepath.Join(t.TempDir(), "node_key.json"),
		"--p2p-listen", archiveListen,
		"--persistent-peers", string(stock.NodeKey.ID())+"@"+strings.TrimPrefix(stockListen, "tcp://"),
		"--metrics-listen", "127.0.0.1:0",
		"--request-limit", "2",
		"--status-request-interval", "20ms",
		"--archive-interval", "1h",
		"--prune-interval", "0s",
	)
	stdout, pipeErr := cmd.StdoutPipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if startErr := cmd.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	waited := false
	defer func() {
		cancel()
		if waited {
			return
		}
		waitErr := cmd.Wait()
		if waitErr != nil && ctx.Err() == nil {
			t.Errorf("serve exited unexpectedly: %v\nstderr:\n%s", waitErr, stderr.String())
		}
	}()

	var started map[string]any
	if decodeErr := json.NewDecoder(stdout).Decode(&started); decodeErr != nil {
		t.Fatalf("decode serve startup JSON: %v\nstderr:\n%s", decodeErr, stderr.String())
	}
	metricsListen, ok := started["metrics_listen"].(string)
	if !ok || metricsListen == "" {
		t.Fatalf("missing metrics listener in startup JSON: %+v", started)
	}
	nodeID, ok := started["node_id"].(string)
	if !ok || nodeID == "" {
		t.Fatalf("missing archive node ID in startup JSON: %+v", started)
	}
	archiveAddr, err := p2p.NewNetAddressString(nodeID + "@" + strings.TrimPrefix(archiveListen, "tcp://"))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := stock.Switch.DialPeerWithAddress(archiveAddr); dialErr != nil {
		t.Fatal(dialErr)
	}
	waitForArchiveSyncMetrics(t, metricsListen, 1, 7, 8)
	cancel()
	if waitErr := cmd.Wait(); waitErr != nil && ctx.Err() == nil {
		t.Fatalf("serve exited unexpectedly after sync: %v\nstderr:\n%s", waitErr, stderr.String())
	}
	waited = true

	db, err := dbm.NewDB("blockstore", dbm.BackendType("goleveldb"), dbDir)
	if err != nil {
		t.Fatal(err)
	}
	blockStore := store.NewBlockStore(db)
	defer blockStore.Close()
	if got := blockStore.LoadBlock(1); got == nil || got.ChainID != e2eChainID {
		t.Fatalf("archive store did not persist real blocksync block 1: %+v", got)
	}
	if height := blockStore.Height(); height < 6 {
		t.Fatalf("archive store height = %d, want at least 6", height)
	}
}

func TestBinaryServeSyncsToHeadFromNormalCometBFTKVNode(t *testing.T) {
	binary := buildArchiveBinary(t)
	cometListen := freeP2PListenAddress(t)
	comet := newE2ENormalCometBFTKVNode(t, cometListen)
	if startErr := comet.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	defer func() {
		if comet.IsRunning() {
			if stopErr := comet.Stop(); stopErr != nil {
				t.Fatal(stopErr)
			}
		}
	}()
	waitForCometBFTBlockHeight(t, comet.BlockStore(), 4)

	dbDir := filepath.Join(t.TempDir(), "archive-db")
	objectRoot := filepath.Join(t.TempDir(), "objects")
	storeURL := "file://" + objectRoot
	archiveListen := freeP2PListenAddress(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx,
		binary,
		"serve",
		"--dry-run=false",
		"--db-dir", dbDir,
		"--db-backend", "goleveldb",
		"--store", storeURL,
		"--chain-id", e2eChainID,
		"--node-key-file", filepath.Join(t.TempDir(), "node_key.json"),
		"--p2p-listen", archiveListen,
		"--persistent-peers", string(comet.Switch().NodeInfo().ID())+"@"+strings.TrimPrefix(cometListen, "tcp://"),
		"--metrics-listen", "127.0.0.1:0",
		"--request-limit", "4",
		"--status-request-interval", "20ms",
		"--archive-interval", "100ms",
		"--prune-interval", "100ms",
		"--safety-window", "2",
		"--retain-blocks", "2",
		"--evidence-max-age-blocks", "1",
		"--evidence-max-age-duration", "1ns",
		"--segment-blocks", "2",
	)
	stdout, pipeErr := cmd.StdoutPipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if startErr := cmd.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	waited := false
	defer func() {
		cancel()
		if waited {
			return
		}
		waitErr := cmd.Wait()
		if waitErr != nil && ctx.Err() == nil {
			t.Errorf("serve exited unexpectedly: %v\nstderr:\n%s", waitErr, stderr.String())
		}
	}()

	var started map[string]any
	if decodeErr := json.NewDecoder(stdout).Decode(&started); decodeErr != nil {
		t.Fatalf("decode serve startup JSON: %v\nstderr:\n%s", decodeErr, stderr.String())
	}
	metricsListen, ok := started["metrics_listen"].(string)
	if !ok || metricsListen == "" {
		t.Fatalf("missing metrics listener in startup JSON: %+v", started)
	}
	nodeID, ok := started["node_id"].(string)
	if !ok || nodeID == "" {
		t.Fatalf("missing archive node ID in startup JSON: %+v", started)
	}
	archiveAddr, err := p2p.NewNetAddressString(nodeID + "@" + strings.TrimPrefix(archiveListen, "tcp://"))
	if err != nil {
		t.Fatal(err)
	}
	if dialErr := comet.Switch().DialPeerWithAddress(archiveAddr); dialErr != nil && !strings.Contains(dialErr.Error(), "already") {
		t.Fatal(dialErr)
	}
	syncedHead := waitForArchiveNearCometBFTHead(t, metricsListen, comet.BlockStore(), 4)
	waitForArchiveBlocksArchived(t, metricsListen, 2)
	waitForArchivePrunedColdRange(t, metricsListen, 2, 1)
	if stopErr := comet.Stop(); stopErr != nil {
		t.Fatal(stopErr)
	}
	requester := newE2EBlockRequesterP2PNode(t, freeP2PListenAddress(t), 1)
	if startErr := requester.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	defer func() {
		if stopErr := requester.Stop(); stopErr != nil {
			t.Fatal(stopErr)
		}
	}()
	if dialErr := requester.Switch.DialPeerWithAddress(archiveAddr); dialErr != nil {
		t.Fatal(dialErr)
	}
	select {
	case block := <-requester.Reactor.blocks:
		if block.Height != 1 || block.ChainID != e2eChainID {
			t.Fatalf("unexpected cold block response: height=%d chain=%s", block.Height, block.ChainID)
		}
	case requestErr := <-requester.Reactor.errs:
		t.Fatal(requestErr)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for archive cold block response")
	}
	cancel()
	if waitErr := cmd.Wait(); waitErr != nil && ctx.Err() == nil {
		t.Fatalf("serve exited unexpectedly after sync: %v\nstderr:\n%s", waitErr, stderr.String())
	}
	waited = true

	db, err := dbm.NewDB("blockstore", dbm.BackendType("goleveldb"), dbDir)
	if err != nil {
		t.Fatal(err)
	}
	blockStore := store.NewBlockStore(db)
	defer blockStore.Close()
	if got := blockStore.LoadBlock(1); got != nil {
		t.Fatalf("archive store retained hot block 1 after verified prune: %+v", got)
	}
	if height := blockStore.Height(); height < syncedHead-3 {
		t.Fatalf("archive store height = %d, want near CometBFT head %d", height, syncedHead)
	}
	objectStore, err := archive.NewLocalObjectStore(objectRoot)
	if err != nil {
		t.Fatal(err)
	}
	verify, err := archive.Verify(context.Background(), objectStore, archive.VerifyOptions{
		ManifestKey: archive.ManifestKey("archive", e2eChainID, archive.DefaultManifest),
	})
	if err != nil {
		t.Fatal(err)
	}
	if verify.BlocksChecked < 2 {
		t.Fatalf("verified archived blocks = %d, want at least 2", verify.BlocksChecked)
	}
}

func runArchive(t *testing.T, args ...string) string {
	t.Helper()
	allArgs := append([]string{"run", "../cmd/cometbft-archive"}, args...)
	cmd := exec.Command("go", allArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go run %v failed: %v\n%s", args, err, string(out))
	}
	return string(out)
}

func freeP2PListenAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return "tcp://" + addr
}

func buildArchiveBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "cometbft-archive")
	cmd := exec.Command("go", "build", "-o", binary, "../cmd/cometbft-archive")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build archive binary failed: %v\n%s", err, string(out))
	}
	return binary
}

func jsonNumber(value any) (float64, bool) {
	number, ok := value.(float64)
	return number, ok
}

func waitForArchiveSyncMetrics(t *testing.T, metricsListen string, minPeers, minPeerBestHeight, minNextHeight float64) {
	t.Helper()
	client := http.Client{Timeout: time.Second}
	deadline := time.Now().Add(15 * time.Second)
	var last map[string]any
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + metricsListen + "/metrics")
		if err == nil {
			var got map[string]any
			decodeErr := json.NewDecoder(resp.Body).Decode(&got)
			_ = resp.Body.Close()
			if decodeErr == nil {
				last = got
				peers, hasPeers := jsonNumber(got["p2p_peers"])
				best, hasBest := jsonNumber(got["peer_best_height"])
				next, hasNext := jsonNumber(got["next_height"])
				if hasPeers && hasBest && hasNext && peers >= minPeers && best >= minPeerBestHeight && next >= minNextHeight {
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("archive sync metrics did not reach peers>=%.0f best>=%.0f next>=%.0f; last=%+v", minPeers, minPeerBestHeight, minNextHeight, last)
}

func waitForArchiveNearCometBFTHead(t *testing.T, metricsListen string, cometStore *store.BlockStore, minHeight int64) int64 {
	t.Helper()
	client := http.Client{Timeout: time.Second}
	deadline := time.Now().Add(20 * time.Second)
	var last map[string]any
	var lastCometHeight int64
	for time.Now().Before(deadline) {
		lastCometHeight = cometStore.Height()
		resp, err := client.Get("http://" + metricsListen + "/metrics")
		if err == nil {
			var got map[string]any
			decodeErr := json.NewDecoder(resp.Body).Decode(&got)
			_ = resp.Body.Close()
			if decodeErr == nil {
				last = got
				peers, hasPeers := jsonNumber(got["p2p_peers"])
				best, hasBest := jsonNumber(got["peer_best_height"])
				next, hasNext := jsonNumber(got["next_height"])
				nearHead := float64(lastCometHeight)
				if lastCometHeight >= minHeight && hasPeers && hasBest && hasNext && peers >= 1 && best >= nearHead-10 && next >= nearHead-10 {
					return lastCometHeight
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("archive did not sync near normal CometBFT head >=%d; comet_height=%d last_metrics=%+v", minHeight, lastCometHeight, last)
	return 0
}

func waitForArchiveBlocksArchived(t *testing.T, metricsListen string, minBlocks float64) {
	t.Helper()
	client := http.Client{Timeout: time.Second}
	deadline := time.Now().Add(20 * time.Second)
	var last map[string]any
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + metricsListen + "/metrics")
		if err == nil {
			var got map[string]any
			decodeErr := json.NewDecoder(resp.Body).Decode(&got)
			_ = resp.Body.Close()
			if decodeErr == nil {
				last = got
				archived, hasArchived := jsonNumber(got["blocks_archived"])
				errors, hasErrors := jsonNumber(got["archive_errors"])
				if hasArchived && hasErrors && archived >= minBlocks && errors == 0 {
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("archive metrics did not reach blocks_archived>=%.0f with archive_errors=0; last=%+v", minBlocks, last)
}

func waitForArchivePrunedColdRange(t *testing.T, metricsListen string, minHotBase, wantServedBase float64) {
	t.Helper()
	client := http.Client{Timeout: time.Second}
	deadline := time.Now().Add(20 * time.Second)
	var last map[string]any
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + metricsListen + "/metrics")
		if err == nil {
			var got map[string]any
			decodeErr := json.NewDecoder(resp.Body).Decode(&got)
			_ = resp.Body.Close()
			if decodeErr == nil {
				last = got
				hotBase, hasHotBase := jsonNumber(got["hot_base"])
				servedBase, hasServedBase := jsonNumber(got["served_base"])
				pruned, hasPruned := jsonNumber(got["blocks_pruned"])
				pruneErrors, hasPruneErrors := jsonNumber(got["prune_errors"])
				if hasHotBase && hasServedBase && hasPruned && hasPruneErrors &&
					hotBase >= minHotBase && servedBase == wantServedBase && pruned > 0 && pruneErrors == 0 {
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("archive metrics did not reach hot_base>=%.0f served_base=%.0f with verified prune; last=%+v", minHotBase, wantServedBase, last)
}

func waitForCometBFTBlockHeight(t *testing.T, blockStore *store.BlockStore, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if blockStore.Height() >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("normal CometBFT height = %d, want at least %d", blockStore.Height(), want)
}

func createE2EBlockStoreFixture(t *testing.T, dir string, heights int) {
	t.Helper()
	db, err := dbm.NewDB("blockstore", dbm.BackendType("goleveldb"), dir)
	if err != nil {
		t.Fatal(err)
	}
	bs := store.NewBlockStore(db)
	defer bs.Close()
	for h := int64(1); h <= int64(heights); h++ {
		block := makeE2EBlock(t, h)
		parts, err := block.MakePartSet(ctypes.BlockPartSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		seen := &ctypes.Commit{Height: h, Signatures: []ctypes.CommitSig{}}
		bs.SaveBlock(block, parts, seen)
	}
}

func makeE2EBlock(t *testing.T, height int64) *ctypes.Block {
	t.Helper()
	var lastCommit *ctypes.Commit
	if height > 1 {
		previous := makeE2EBlock(t, height-1)
		parts, err := previous.MakePartSet(ctypes.BlockPartSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		lastCommit = &ctypes.Commit{
			Height: height - 1,
			BlockID: ctypes.BlockID{
				Hash:          previous.Hash(),
				PartSetHeader: parts.Header(),
			},
			Signatures: []ctypes.CommitSig{
				{
					BlockIDFlag:      ctypes.BlockIDFlagCommit,
					ValidatorAddress: e2eBytes(byte(height), crypto.AddressSize),
					Timestamp:        time.Unix(height, 0).UTC(),
					Signature:        e2eBytes(0x66, 64),
				},
			},
		}
	} else {
		lastCommit = &ctypes.Commit{Height: 0, Signatures: []ctypes.CommitSig{}}
	}
	block := ctypes.MakeBlock(height, ctypes.Data{Txs: []ctypes.Tx{ctypes.Tx(fmt.Sprintf("tx-%d", height))}}, lastCommit, nil)
	block.ChainID = e2eChainID
	block.ProposerAddress = e2eBytes(byte(height), crypto.AddressSize)
	block.ValidatorsHash = e2eBytes(0x11, 32)
	block.NextValidatorsHash = e2eBytes(0x22, 32)
	block.ConsensusHash = e2eBytes(0x33, 32)
	return block
}

func e2eBytes(value byte, size int) []byte {
	bz := make([]byte, size)
	for i := range bz {
		bz[i] = value
	}
	return bz
}

func newE2ENormalCometBFTKVNode(t *testing.T, listenAddress string) *cmtnode.Node {
	t.Helper()
	cfg := cmtcfg.TestConfig()
	cfg.SetRoot(t.TempDir())
	cfg.ProxyApp = "kvstore"
	cfg.P2P.ListenAddress = listenAddress
	cfg.P2P.PexReactor = false
	cfg.RPC.ListenAddress = ""
	cfg.RPC.GRPCListenAddress = ""
	cfg.RPC.PprofListenAddress = ""
	cfg.Consensus.TimeoutPropose = 100 * time.Millisecond
	cfg.Consensus.TimeoutProposeDelta = 10 * time.Millisecond
	cfg.Consensus.TimeoutPrevote = 50 * time.Millisecond
	cfg.Consensus.TimeoutPrevoteDelta = 10 * time.Millisecond
	cfg.Consensus.TimeoutPrecommit = 50 * time.Millisecond
	cfg.Consensus.TimeoutPrecommitDelta = 10 * time.Millisecond
	cfg.Consensus.TimeoutCommit = 250 * time.Millisecond
	cfg.Consensus.SkipTimeoutCommit = false
	cfg.Consensus.PeerGossipSleepDuration = 5 * time.Millisecond
	cfg.Consensus.PeerQueryMaj23SleepDuration = 50 * time.Millisecond
	cmtcfg.EnsureRoot(cfg.RootDir)

	pv := privval.LoadOrGenFilePV(cfg.PrivValidatorKeyFile(), cfg.PrivValidatorStateFile())
	pubKey, err := pv.GetPubKey()
	if err != nil {
		t.Fatal(err)
	}
	genesis := ctypes.GenesisDoc{
		ChainID:         e2eChainID,
		GenesisTime:     time.Now().Add(-time.Second).UTC(),
		ConsensusParams: ctypes.DefaultConsensusParams(),
		Validators: []ctypes.GenesisValidator{
			{
				Address: pubKey.Address(),
				PubKey:  pubKey,
				Power:   10,
			},
		},
	}
	if saveErr := genesis.SaveAs(cfg.GenesisFile()); saveErr != nil {
		t.Fatal(saveErr)
	}
	comet, err := cmtnode.DefaultNewNode(cfg, log.NewNopLogger())
	if err != nil {
		t.Fatal(err)
	}
	return comet
}

type e2eStockBlocksyncP2PNode struct {
	Config    *cmtcfg.P2PConfig
	NodeKey   *p2p.NodeKey
	Transport *p2p.MultiplexTransport
	Switch    *p2p.Switch
}

type e2eBlockRequesterP2PNode struct {
	Config    *cmtcfg.P2PConfig
	NodeKey   *p2p.NodeKey
	Transport *p2p.MultiplexTransport
	Switch    *p2p.Switch
	Reactor   *e2eBlockRequesterReactor
}

func newE2EBlockRequesterP2PNode(t *testing.T, listenAddress string, requestHeight int64) *e2eBlockRequesterP2PNode {
	t.Helper()
	nodeKey := &p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddress,
		Network:         e2eChainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{cmtblocksync.BlocksyncChannel},
		Moniker:         "e2e-block-requester",
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
	reactor := newE2EBlockRequesterReactor(requestHeight)
	sw.AddReactor("BLOCK_REQUESTER", reactor)
	sw.SetNodeInfo(nodeInfo)
	sw.SetNodeKey(nodeKey)
	return &e2eBlockRequesterP2PNode{
		Config:    cfg,
		NodeKey:   nodeKey,
		Transport: transport,
		Switch:    sw,
		Reactor:   reactor,
	}
}

func newE2EStockBlocksyncP2PNode(t *testing.T, listenAddress string, blocks ...*ctypes.Block) *e2eStockBlocksyncP2PNode {
	t.Helper()
	nodeKey := &p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddress,
		Network:         e2eChainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{cmtblocksync.BlocksyncChannel},
		Moniker:         "e2e-stock-blocksync",
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
	reactor := newE2EStockBlocksyncServingReactor(t, blocks...)
	reactor.SetLogger(logger.With("module", "stock_blocksync"))
	sw.AddReactor("BLOCKSYNC", reactor)
	sw.SetNodeInfo(nodeInfo)
	sw.SetNodeKey(nodeKey)
	return &e2eStockBlocksyncP2PNode{
		Config:    cfg,
		NodeKey:   nodeKey,
		Transport: transport,
		Switch:    sw,
	}
}

func newE2EStockBlocksyncServingReactor(t *testing.T, blocks ...*ctypes.Block) *cmtblocksync.Reactor {
	t.Helper()
	db := dbm.NewMemDB()
	blockStore := store.NewBlockStore(db)
	var lastHeight int64
	for _, block := range blocks {
		parts, err := block.MakePartSet(ctypes.BlockPartSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		blockStore.SaveBlock(block, parts, &ctypes.Commit{Height: block.Height, Signatures: []ctypes.CommitSig{}})
		if block.Height > lastHeight {
			lastHeight = block.Height
		}
	}
	stateDB := dbm.NewMemDB()
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{})
	vals, _ := ctypes.RandValidatorSet(1, 1)
	state := sm.State{
		ChainID:                          e2eChainID,
		InitialHeight:                    1,
		LastBlockHeight:                  lastHeight,
		Validators:                       vals,
		NextValidators:                   vals.Copy(),
		LastValidators:                   vals.Copy(),
		LastHeightValidatorsChanged:      1,
		ConsensusParams:                  *ctypes.DefaultConsensusParams(),
		LastHeightConsensusParamsChanged: 1,
	}
	if saveErr := stateStore.Save(state); saveErr != nil {
		t.Fatal(saveErr)
	}
	blockExec := sm.NewBlockExecutor(stateStore, log.NewNopLogger(), nil, nil, sm.EmptyEvidencePool{}, blockStore)
	return cmtblocksync.NewReactor(state, blockExec, blockStore, false, cmtblocksync.NopMetrics(), 0)
}

func (n *e2eStockBlocksyncP2PNode) Start() error {
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

func (n *e2eBlockRequesterP2PNode) Start() error {
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

func (n *e2eStockBlocksyncP2PNode) Stop() error {
	var stopErr error
	if n.Switch.IsRunning() {
		stopErr = n.Switch.Stop()
	}
	if err := n.Transport.Close(); err != nil && stopErr == nil {
		stopErr = err
	}
	return stopErr
}

func (n *e2eBlockRequesterP2PNode) Stop() error {
	var stopErr error
	if n.Switch.IsRunning() {
		stopErr = n.Switch.Stop()
	}
	if err := n.Transport.Close(); err != nil && stopErr == nil {
		stopErr = err
	}
	return stopErr
}

type e2eBlockRequesterReactor struct {
	p2p.BaseReactor
	height int64
	blocks chan *ctypes.Block
	errs   chan error
}

func newE2EBlockRequesterReactor(height int64) *e2eBlockRequesterReactor {
	reactor := &e2eBlockRequesterReactor{
		height: height,
		blocks: make(chan *ctypes.Block, 1),
		errs:   make(chan error, 1),
	}
	reactor.BaseReactor = *p2p.NewBaseReactor("BLOCK_REQUESTER", reactor)
	return reactor
}

func (*e2eBlockRequesterReactor) GetChannels() []*p2p.ChannelDescriptor {
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

func (r *e2eBlockRequesterReactor) AddPeer(peer p2p.Peer) {
	peer.TrySend(p2p.Envelope{
		ChannelID: cmtblocksync.BlocksyncChannel,
		Message:   &bcproto.BlockRequest{Height: r.height},
	})
}

func (r *e2eBlockRequesterReactor) Receive(envelope p2p.Envelope) {
	switch msg := envelope.Message.(type) {
	case *bcproto.BlockResponse:
		block, err := ctypes.BlockFromProto(msg.Block)
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
