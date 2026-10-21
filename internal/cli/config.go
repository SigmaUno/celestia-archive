package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type serveConfig struct {
	Store                  string   `json:"store"`
	Prefix                 string   `json:"prefix"`
	ChainID                string   `json:"chain_id"`
	ManifestName           string   `json:"manifest_name"`
	ManifestKey            string   `json:"manifest_key"`
	DBDir                  string   `json:"db_dir"`
	DBBackend              string   `json:"db_backend"`
	P2PSendRate            *int64   `json:"p2p_send_rate"`
	P2PRecvRate            *int64   `json:"p2p_recv_rate"`
	P2PListen              string   `json:"p2p_listen"`
	Moniker                string   `json:"moniker"`
	NodeKeyFile            string   `json:"node_key_file"`
	PersistentPeers        []string `json:"persistent_peers"`
	RequestLimit           *int     `json:"request_limit"`
	ColdWorkers            *int     `json:"cold_workers"`
	ColdManifestCacheTTL   string   `json:"cold_manifest_cache_ttl"`
	RequestTimeout         string   `json:"request_timeout"`
	StatusRequestInterval  string   `json:"status_request_interval"`
	PEX                    *bool    `json:"pex"`
	AddrBookFile           string   `json:"addr_book_file"`
	AddrBookStrict         *bool    `json:"addr_book_strict"`
	Seeds                  []string `json:"seeds"`
	SeedMode               *bool    `json:"seed_mode"`
	PrivatePeerIDs         []string `json:"private_peer_ids"`
	MetricsListen          string   `json:"metrics_listen"`
	SafetyWindow           *int64   `json:"safety_window"`
	ArchiveInterval        string   `json:"archive_interval"`
	PruneInterval          string   `json:"prune_interval"`
	RetainBlocks           *int64   `json:"retain_blocks"`
	EvidenceMaxAgeBlocks   *int64   `json:"evidence_max_age_blocks"`
	EvidenceMaxAgeDuration string   `json:"evidence_max_age_duration"`
	SegmentBlocks          *int     `json:"segment_blocks"`
	Compression            string   `json:"compression"`
	Validation             string   `json:"validation"`
	Checkpoints            []string `json:"checkpoints"`
	ValidatorSets          []string `json:"validator_sets"`
	ValidatorSetRPC        string   `json:"validator_set_rpc"`
	ValidatorSetTimeout    string   `json:"validator_set_timeout"`
	DryRun                 *bool    `json:"dry_run"`
}

type serveSettings struct {
	common               *commonFlags
	dbDir                *string
	dbBackend            *string
	listenAddress        *string
	p2pSendRate          *int64
	p2pRecvRate          *int64
	moniker              *string
	nodeKeyFile          *string
	persistentPeers      *string
	requestLimit         *int
	coldWorkers          *int
	coldManifestCacheTTL *time.Duration
	requestTimeout       *time.Duration
	statusInterval       *time.Duration
	pexEnabled           *bool
	addrBookFile         *string
	addrBookStrict       *bool
	seeds                *string
	seedMode             *bool
	privatePeerIDs       *string
	metricsListen        *string
	safetyWindow         *int64
	archiveInterval      *time.Duration
	pruneInterval        *time.Duration
	retainBlocks         *int64
	evidenceBlocks       *int64
	evidenceDuration     *time.Duration
	segmentBlocks        *int
	compression          *string
	validation           *string
	checkpoints          *[]string
	validatorSets        *[]string
	validatorSetRPC      *string
	validatorSetTimeout  *time.Duration
	dryRun               *bool
}

func applyServeConfig(cmd *cobra.Command, path string, settings serveSettings) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg serveConfig
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return fmt.Errorf("decode config %q: %w", path, err)
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		if err != nil {
			return fmt.Errorf("decode config %q: %w", path, err)
		}
		return fmt.Errorf("decode config %q: trailing JSON after top-level object", path)
	}
	overlayString(cmd, "store", cfg.Store, &settings.common.storeURL)
	overlayString(cmd, "prefix", cfg.Prefix, &settings.common.prefix)
	overlayString(cmd, "chain-id", cfg.ChainID, &settings.common.chainID)
	overlayString(cmd, "manifest-name", cfg.ManifestName, &settings.common.manifestName)
	overlayString(cmd, "manifest-key", cfg.ManifestKey, &settings.common.manifestKey)
	overlayString(cmd, "db-dir", cfg.DBDir, settings.dbDir)
	overlayString(cmd, "db-backend", cfg.DBBackend, settings.dbBackend)
	overlayPtr(cmd, "p2p-send-rate", cfg.P2PSendRate, settings.p2pSendRate)
	overlayPtr(cmd, "p2p-recv-rate", cfg.P2PRecvRate, settings.p2pRecvRate)
	overlayString(cmd, "p2p-listen", cfg.P2PListen, settings.listenAddress)
	overlayString(cmd, "moniker", cfg.Moniker, settings.moniker)
	overlayString(cmd, "node-key-file", cfg.NodeKeyFile, settings.nodeKeyFile)
	if err := overlayCSV(cmd, "persistent-peers", "persistent_peers", cfg.PersistentPeers, settings.persistentPeers); err != nil {
		return err
	}
	overlayPtr(cmd, "request-limit", cfg.RequestLimit, settings.requestLimit)
	overlayPtr(cmd, "cold-workers", cfg.ColdWorkers, settings.coldWorkers)
	if err := overlayDuration(cmd, "cold-manifest-cache-ttl", "cold_manifest_cache_ttl", cfg.ColdManifestCacheTTL, settings.coldManifestCacheTTL); err != nil {
		return err
	}
	if err := overlayDuration(cmd, "request-timeout", "request_timeout", cfg.RequestTimeout, settings.requestTimeout); err != nil {
		return err
	}
	if err := overlayDuration(cmd, "status-request-interval", "status_request_interval", cfg.StatusRequestInterval, settings.statusInterval); err != nil {
		return err
	}
	overlayPtr(cmd, "pex", cfg.PEX, settings.pexEnabled)
	overlayString(cmd, "addr-book-file", cfg.AddrBookFile, settings.addrBookFile)
	overlayPtr(cmd, "addr-book-strict", cfg.AddrBookStrict, settings.addrBookStrict)
	if err := overlayCSV(cmd, "seeds", "seeds", cfg.Seeds, settings.seeds); err != nil {
		return err
	}
	overlayPtr(cmd, "seed-mode", cfg.SeedMode, settings.seedMode)
	if err := overlayCSV(cmd, "private-peer-ids", "private_peer_ids", cfg.PrivatePeerIDs, settings.privatePeerIDs); err != nil {
		return err
	}
	overlayString(cmd, "metrics-listen", cfg.MetricsListen, settings.metricsListen)
	overlayPtr(cmd, "safety-window", cfg.SafetyWindow, settings.safetyWindow)
	if err := overlayDuration(cmd, "archive-interval", "archive_interval", cfg.ArchiveInterval, settings.archiveInterval); err != nil {
		return err
	}
	if err := overlayDuration(cmd, "prune-interval", "prune_interval", cfg.PruneInterval, settings.pruneInterval); err != nil {
		return err
	}
	overlayPtr(cmd, "retain-blocks", cfg.RetainBlocks, settings.retainBlocks)
	overlayPtr(cmd, "evidence-max-age-blocks", cfg.EvidenceMaxAgeBlocks, settings.evidenceBlocks)
	if err := overlayDuration(cmd, "evidence-max-age-duration", "evidence_max_age_duration", cfg.EvidenceMaxAgeDuration, settings.evidenceDuration); err != nil {
		return err
	}
	overlayPtr(cmd, "segment-blocks", cfg.SegmentBlocks, settings.segmentBlocks)
	overlayString(cmd, "compression", cfg.Compression, settings.compression)
	overlayString(cmd, "validation", cfg.Validation, settings.validation)
	if len(cfg.Checkpoints) > 0 && !flagChanged(cmd, "checkpoint") {
		*settings.checkpoints = cfg.Checkpoints
	}
	if len(cfg.ValidatorSets) > 0 && !flagChanged(cmd, "validator-set") {
		*settings.validatorSets = cfg.ValidatorSets
	}
	overlayString(cmd, "validator-set-rpc", cfg.ValidatorSetRPC, settings.validatorSetRPC)
	if err := overlayDuration(cmd, "validator-set-timeout", "validator_set_timeout", cfg.ValidatorSetTimeout, settings.validatorSetTimeout); err != nil {
		return err
	}
	overlayPtr(cmd, "dry-run", cfg.DryRun, settings.dryRun)
	return nil
}

func overlayString(cmd *cobra.Command, flag, value string, dst *string) {
	if value != "" && !flagChanged(cmd, flag) {
		*dst = value
	}
}

func overlayPtr[T any](cmd *cobra.Command, flag string, src *T, dst *T) {
	if src != nil && !flagChanged(cmd, flag) {
		*dst = *src
	}
}

func overlayDuration(cmd *cobra.Command, flag, configName, raw string, dst *time.Duration) error {
	if raw == "" || flagChanged(cmd, flag) {
		return nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("config %s: %w", configName, err)
	}
	*dst = d
	return nil
}

func overlayCSV(cmd *cobra.Command, flag, configName string, values []string, dst *string) error {
	if len(values) == 0 || flagChanged(cmd, flag) {
		return nil
	}
	value, err := configCSV(configName, values)
	if err != nil {
		return err
	}
	*dst = value
	return nil
}

func configCSV(name string, values []string) (string, error) {
	normalized := make([]string, 0, len(values))
	for i, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return "", fmt.Errorf("config %s[%d] must not be empty", name, i)
		}
		if strings.Contains(value, ",") {
			return "", fmt.Errorf("config %s[%d] must not contain commas", name, i)
		}
		normalized = append(normalized, value)
	}
	return strings.Join(normalized, ","), nil
}

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flags().Lookup(name)
	return flag != nil && flag.Changed
}
