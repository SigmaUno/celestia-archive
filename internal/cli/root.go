package cli

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	cmtflags "github.com/cometbft/cometbft/libs/cli/flags"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	cmtstore "github.com/cometbft/cometbft/store"
	"github.com/spf13/cobra"

	"github.com/01builders/cometbft-archive/internal/archive"
	"github.com/01builders/cometbft-archive/internal/blocksyncarchive"
)

const (
	manifestKeyField        = "manifest_key"
	migrateCommandName      = "migrate"
	verifyCommandName       = "verify"
	inspectCommandName      = "inspect"
	hydrateCommandName      = "hydrate"
	archiveReadyCommandName = "archive-ready"
	pruneHotCommandName     = "prune-hot"
	soakCommandName         = "soak"
	serveCommandName        = "serve"

	trustModelStorageOnly                     = "storage-only"
	trustModelTrustedCheckpoints              = "trusted-checkpoints"
	trustModelTrustedValidatorSetFiles        = "trusted-validator-set-files"
	trustModelTrustedValidatorSetFilesWithRPC = "trusted-validator-set-files-with-rpc-backfill"
	trustModelRPCTrustedValidatorSetSource    = "rpc-trusted-validator-set-source"
	trustModelMissingValidatorSetSource       = "missing-validator-set-source"
	trustModelUnknownWithCheckpoints          = "unknown-with-checkpoints"
	trustModelUnknown                         = "unknown"

	peerRolePersistent = "persistent peer"
	peerRoleSeed       = "seed"
)

type commonFlags struct {
	storeURL     string
	prefix       string
	chainID      string
	manifestName string
	manifestKey  string
}

func NewRootCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:          "cometbft-archive",
		Short:        "Archive CometBFT blockstores into immutable object-store segments",
		SilenceUsage: true,
	}
	subs := []func() (*cobra.Command, error){
		newMigrateCommand,
		newVerifyCommand,
		newInspectCommand,
		newHydrateCommand,
		newArchiveReadyCommand,
		newPruneHotCommand,
		newSoakCommand,
		newServeCommand,
	}
	for _, factory := range subs {
		sub, err := factory()
		if err != nil {
			return nil, err
		}
		cmd.AddCommand(sub)
	}
	return cmd, nil
}

func newMigrateCommand() (*cobra.Command, error) {
	var flags commonFlags
	var dbDir, backend, compression string
	var start, end int64
	var segmentBlocks int
	cmd := &cobra.Command{
		Use:   migrateCommandName,
		Short: "Migrate a local CometBFT blockstore height range into object storage",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireObjectStore(flags); err != nil {
				return err
			}
			if err := validateSegmentBlocks(segmentBlocks); err != nil {
				return err
			}
			if err := archive.ValidateCompression(compression); err != nil {
				return err
			}
			if err := archive.ValidateExistingCometBlockStoreConfig(dbDir, backend); err != nil {
				return err
			}
			if _, err := archive.ResolveManifestKey(flags.prefix, flags.chainID, flags.manifestName, flags.manifestKey); err != nil {
				return err
			}
			store, err := archive.OpenObjectStore(flags.storeURL)
			if err != nil {
				return err
			}
			blockStore, err := archive.OpenCometBlockStore(dbDir, backend)
			if err != nil {
				return err
			}
			defer blockStore.Close()
			result, err := archive.Migrate(cmd.Context(), blockStore, store, archive.MigrationOptions{
				ChainID:       flags.chainID,
				StartHeight:   start,
				EndHeight:     end,
				SegmentBlocks: segmentBlocks,
				Prefix:        flags.prefix,
				ManifestName:  flags.manifestName,
				ManifestKey:   flags.manifestKey,
				Compression:   compression,
			})
			if err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), map[string]any{
				manifestKeyField: result.ManifestKey,
				"segments":       result.Segments,
				"uploaded":       result.Uploaded,
				"reused":         result.Reused,
				"first_height":   result.Manifest.FirstHeight,
				"last_height":    result.Manifest.LastHeight,
			})
		},
	}
	addCommonFlags(cmd, &flags)
	cmd.Flags().StringVar(&dbDir, "db-dir", "", "CometBFT DB directory containing blockstore.db")
	cmd.Flags().StringVar(&backend, "db-backend", "goleveldb", "CometBFT DB backend")
	cmd.Flags().Int64Var(&start, "start-height", 0, "First height to export; defaults to blockstore base")
	cmd.Flags().Int64Var(&end, "end-height", 0, "Last height to export; defaults to blockstore height")
	cmd.Flags().IntVar(&segmentBlocks, "segment-blocks", archive.DefaultSegmentBlocks, "Maximum blocks per immutable segment")
	cmd.Flags().StringVar(&compression, "compression", archive.DefaultCompression, "Segment compression: gzip or none")
	if err := markRequired(cmd, "db-dir", "chain-id"); err != nil {
		return nil, err
	}
	return cmd, nil
}

func newVerifyCommand() (*cobra.Command, error) {
	var flags commonFlags
	var sampleEvery int64
	cmd := &cobra.Command{
		Use:   verifyCommandName,
		Short: "Verify manifest consistency, object checksums, and archived block records",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireObjectStore(flags); err != nil {
				return err
			}
			if sampleEvery < 0 {
				return errors.New("sample every cannot be negative")
			}
			key, err := resolveManifestKey(flags)
			if err != nil {
				return err
			}
			store, err := archive.OpenObjectStoreReadOnly(flags.storeURL)
			if err != nil {
				return err
			}
			result, err := archive.Verify(cmd.Context(), store, archive.VerifyOptions{
				ManifestKey: key,
				SampleEvery: sampleEvery,
			})
			if err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), map[string]any{
				manifestKeyField:   key,
				"segments_checked": result.SegmentsChecked,
				"blocks_checked":   result.BlocksChecked,
			})
		},
	}
	addCommonFlags(cmd, &flags)
	cmd.Flags().Int64Var(&sampleEvery, "sample-every", 0, "Verify one block every N heights after segment checksum validation; 0 verifies all")
	return cmd, nil
}

func newInspectCommand() (*cobra.Command, error) {
	var flags commonFlags
	cmd := &cobra.Command{
		Use:   inspectCommandName,
		Short: "Print manifest summary without hydrating archive data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireObjectStore(flags); err != nil {
				return err
			}
			key, err := resolveManifestKey(flags)
			if err != nil {
				return err
			}
			store, err := archive.OpenObjectStoreReadOnly(flags.storeURL)
			if err != nil {
				return err
			}
			summary, err := archive.Inspect(cmd.Context(), store, key)
			if err != nil {
				return err
			}
			data, err := summary.JSON()
			if err != nil {
				return err
			}
			_, err = cmd.OutOrStdout().Write(data)
			return err
		},
	}
	addCommonFlags(cmd, &flags)
	return cmd, nil
}

func newHydrateCommand() (*cobra.Command, error) {
	var flags commonFlags
	var cacheDir string
	var start, end, maxBytes int64
	cmd := &cobra.Command{
		Use:   hydrateCommandName,
		Short: "Hydrate an archived block range into a bounded local cache",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireObjectStore(flags); err != nil {
				return err
			}
			if maxBytes < 0 {
				return errors.New("max cache bytes cannot be negative")
			}
			key, err := resolveManifestKey(flags)
			if err != nil {
				return err
			}
			store, err := archive.OpenObjectStoreReadOnly(flags.storeURL)
			if err != nil {
				return err
			}
			result, err := archive.Hydrate(cmd.Context(), store, archive.HydrateOptions{
				ManifestKey: key,
				CacheDir:    cacheDir,
				StartHeight: start,
				EndHeight:   end,
				MaxBytes:    maxBytes,
			})
			if err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), map[string]any{
				manifestKeyField: key,
				"cache_dir":      result.CacheDir,
				"blocks_written": result.BlocksWritten,
				"bytes_written":  result.BytesWritten,
			})
		},
	}
	addCommonFlags(cmd, &flags)
	cmd.Flags().StringVar(&cacheDir, "cache-dir", "", "Local cache directory")
	cmd.Flags().Int64Var(&start, "start-height", 0, "First height to hydrate; defaults to archive first height")
	cmd.Flags().Int64Var(&end, "end-height", 0, "Last height to hydrate; defaults to archive last height")
	cmd.Flags().Int64Var(&maxBytes, "max-cache-bytes", 10<<30, "Maximum bytes retained in local cache")
	if err := markRequired(cmd, "cache-dir"); err != nil {
		return nil, err
	}
	return cmd, nil
}

func newArchiveReadyCommand() (*cobra.Command, error) {
	var flags commonFlags
	var dbDir, backend, compression string
	var start, ready int64
	var segmentBlocks int
	cmd := &cobra.Command{
		Use:   archiveReadyCommandName,
		Short: "Archive a ready hot blockstore range into object storage",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireObjectStore(flags); err != nil {
				return err
			}
			if err := validateSegmentBlocks(segmentBlocks); err != nil {
				return err
			}
			if err := archive.ValidateCompression(compression); err != nil {
				return err
			}
			if err := archive.ValidateExistingCometBlockStoreConfig(dbDir, backend); err != nil {
				return err
			}
			if _, err := archive.ResolveManifestKey(flags.prefix, flags.chainID, flags.manifestName, flags.manifestKey); err != nil {
				return err
			}
			store, err := archive.OpenObjectStore(flags.storeURL)
			if err != nil {
				return err
			}
			blockStore, err := archive.OpenCometBlockStore(dbDir, backend)
			if err != nil {
				return err
			}
			defer blockStore.Close()
			result, err := archive.ArchiveReady(cmd.Context(), blockStore, store, archive.LiveArchiveOptions{
				ChainID:       flags.chainID,
				StartHeight:   start,
				ReadyHeight:   ready,
				SegmentBlocks: segmentBlocks,
				Prefix:        flags.prefix,
				ManifestName:  flags.manifestName,
				ManifestKey:   flags.manifestKey,
				Compression:   compression,
			})
			if err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), map[string]any{
				manifestKeyField:  result.ManifestKey,
				"segments":        result.Segments,
				"uploaded":        result.Uploaded,
				"reused":          result.Reused,
				"blocks_archived": result.BlocksArchived,
				"first_archived":  result.FirstArchived,
				"last_archived":   result.LastArchived,
				"first_height":    result.Manifest.FirstHeight,
				"last_height":     result.Manifest.LastHeight,
			})
		},
	}
	addCommonFlags(cmd, &flags)
	cmd.Flags().StringVar(&dbDir, "db-dir", "", "CometBFT DB directory containing blockstore.db")
	cmd.Flags().StringVar(&backend, "db-backend", "goleveldb", "CometBFT DB backend")
	cmd.Flags().Int64Var(&start, "start-height", 0, "First height to archive; defaults to blockstore base or manifest continuation")
	cmd.Flags().Int64Var(&ready, "ready-height", 0, "Highest safety-windowed hot height eligible for archiving")
	cmd.Flags().IntVar(&segmentBlocks, "segment-blocks", archive.DefaultSegmentBlocks, "Maximum blocks per immutable segment")
	cmd.Flags().StringVar(&compression, "compression", archive.DefaultCompression, "Segment compression: gzip or none")
	if err := markRequired(cmd, "db-dir", "chain-id", "ready-height"); err != nil {
		return nil, err
	}
	return cmd, nil
}

func newPruneHotCommand() (*cobra.Command, error) {
	var flags commonFlags
	var dbDir, backend string
	var retainBlocks, evidenceBlocks int64
	var evidenceDuration time.Duration
	cmd := &cobra.Command{
		Use:   pruneHotCommandName,
		Short: "Prune verified archived blocks from the local hot blockstore",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireObjectStore(flags); err != nil {
				return err
			}
			if retainBlocks <= 0 {
				return errors.New("retain blocks must be positive")
			}
			if evidenceBlocks < 0 {
				return errors.New("evidence max age blocks cannot be negative")
			}
			if evidenceDuration < 0 {
				return errors.New("evidence max age duration cannot be negative")
			}
			if err := archive.ValidateExistingCometBlockStoreConfig(dbDir, backend); err != nil {
				return err
			}
			key, err := resolveManifestKey(flags)
			if err != nil {
				return err
			}
			store, err := archive.OpenObjectStoreReadOnly(flags.storeURL)
			if err != nil {
				return err
			}
			blockStore, err := archive.OpenCometBlockStore(dbDir, backend)
			if err != nil {
				return err
			}
			defer blockStore.Close()
			result, err := archive.PruneVerifiedHotStore(cmd.Context(), blockStore, store, archive.PruneHotOptions{
				ManifestKey:            key,
				RetainBlocks:           retainBlocks,
				EvidenceMaxAgeBlocks:   evidenceBlocks,
				EvidenceMaxAgeDuration: evidenceDuration,
			})
			if err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), map[string]any{
				manifestKeyField:   key,
				"base_before":      result.BaseBefore,
				"base_after":       result.BaseAfter,
				"head":             result.Head,
				"archived_through": result.ArchivedThrough,
				"prune_to_height":  result.PruneToHeight,
				"pruned":           result.Pruned,
				"evidence_retains": result.EvidenceRetains,
			})
		},
	}
	addCommonFlags(cmd, &flags)
	cmd.Flags().StringVar(&dbDir, "db-dir", "", "CometBFT DB directory containing blockstore.db")
	cmd.Flags().StringVar(&backend, "db-backend", "goleveldb", "CometBFT DB backend")
	cmd.Flags().Int64Var(&retainBlocks, "retain-blocks", 1000, "Number of latest local hot blocks to retain")
	cmd.Flags().Int64Var(&evidenceBlocks, "evidence-max-age-blocks", 0, "Override evidence max age blocks for pruning state; 0 uses CometBFT default")
	cmd.Flags().DurationVar(&evidenceDuration, "evidence-max-age-duration", 0, "Override evidence max age duration for pruning state; 0 uses CometBFT default")
	if err := markRequired(cmd, "db-dir"); err != nil {
		return nil, err
	}
	return cmd, nil
}

func newSoakCommand() (*cobra.Command, error) {
	var metricsURL string
	var duration, interval time.Duration
	var minPeers, maxArchiveErrorsDelta, maxPruneErrorsDelta, minColdResponsesDelta, maxColdErrorsDelta, maxColdQueueFullDelta, maxColdQueue, maxBufferedResponses, maxHeadLag int64
	var requireHeadAdvance bool
	cmd := &cobra.Command{
		Use:   soakCommandName,
		Short: "Run an operator soak check against a live serve diagnostics endpoint",
		RunE: func(cmd *cobra.Command, _ []string) error {
			result, err := runSoakCheck(cmd.Context(), soakOptions{
				MetricsURL:            metricsURL,
				Duration:              duration,
				Interval:              interval,
				MinPeers:              minPeers,
				RequireHeadAdvance:    requireHeadAdvance,
				MaxArchiveErrorsDelta: maxArchiveErrorsDelta,
				MaxPruneErrorsDelta:   maxPruneErrorsDelta,
				MinColdResponsesDelta: minColdResponsesDelta,
				MaxColdErrorsDelta:    maxColdErrorsDelta,
				MaxColdQueueFullDelta: maxColdQueueFullDelta,
				MaxColdQueue:          maxColdQueue,
				MaxBufferedResponses:  maxBufferedResponses,
				MaxHeadLag:            maxHeadLag,
			})
			if writeErr := writeJSON(cmd.OutOrStdout(), result); writeErr != nil {
				return writeErr
			}
			return err
		},
	}
	cmd.Flags().StringVar(&metricsURL, "metrics-url", "", "Base URL for serve diagnostics, for example http://127.0.0.1:26660")
	cmd.Flags().DurationVar(&duration, "duration", 10*time.Minute, "How long to poll diagnostics before passing")
	cmd.Flags().DurationVar(&interval, "interval", 10*time.Second, "Diagnostics polling interval")
	cmd.Flags().Int64Var(&minPeers, "min-peers", 1, "Minimum P2P peer count required during the soak")
	cmd.Flags().BoolVar(&requireHeadAdvance, "require-head-advance", true, "Require peer_best_height to advance during the soak")
	cmd.Flags().Int64Var(&maxArchiveErrorsDelta, "max-archive-errors-delta", 0, "Maximum allowed increase in archive_errors during the soak")
	cmd.Flags().Int64Var(&maxPruneErrorsDelta, "max-prune-errors-delta", 0, "Maximum allowed increase in prune_errors during the soak")
	cmd.Flags().Int64Var(&minColdResponsesDelta, "min-cold-responses-delta", 0, "Minimum required increase in blocksync_cold_responses during the soak")
	cmd.Flags().Int64Var(&maxColdErrorsDelta, "max-cold-errors-delta", 0, "Maximum allowed increase in blocksync_cold_errors during the soak")
	cmd.Flags().Int64Var(&maxColdQueueFullDelta, "max-cold-queue-full-delta", 0, "Maximum allowed increase in blocksync_cold_queue_full during the soak")
	cmd.Flags().Int64Var(&maxColdQueue, "max-cold-queue", -1, "Maximum observed blocksync_cold_queue allowed during the soak; -1 disables the queue-depth gate")
	cmd.Flags().Int64Var(&maxBufferedResponses, "max-buffered-responses", -1, "Maximum observed blocksync_buffered_responses allowed during the soak; -1 disables the buffered-response gate")
	cmd.Flags().Int64Var(&maxHeadLag, "max-head-lag", -1, "Maximum allowed peer_best_height - next_height + 1 during the soak; -1 disables the head-lag gate")
	if err := markRequired(cmd, "metrics-url"); err != nil {
		return nil, err
	}
	return cmd, nil
}

func newServeCommand() (*cobra.Command, error) {
	var flags commonFlags
	var dbDir, backend, listenAddress, moniker, nodeKeyFile, persistentPeers, compression string
	var addrBookFile, seeds, privatePeerIDs string
	var metricsListen, logLevel string
	var configPath string
	var validationMode, validatorSetRPC string
	var checkpointValues, validatorSetValues []string
	var safetyWindow, retainBlocks, evidenceBlocks int64
	var archiveInterval, pruneInterval, evidenceDuration, requestTimeout, coldManifestCacheTTL, validatorSetTimeout time.Duration
	var segmentBlocks, requestLimit, coldWorkers int
	var statusRequestInterval time.Duration
	var dryRun, pexEnabled, addrBookStrict, seedMode bool
	cmd := &cobra.Command{
		Use:   serveCommandName,
		Short: "Run or describe the custom blocksync archive peer",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := applyServeConfig(cmd, configPath, serveSettings{
				common:               &flags,
				dbDir:                &dbDir,
				dbBackend:            &backend,
				listenAddress:        &listenAddress,
				moniker:              &moniker,
				nodeKeyFile:          &nodeKeyFile,
				persistentPeers:      &persistentPeers,
				requestLimit:         &requestLimit,
				coldWorkers:          &coldWorkers,
				coldManifestCacheTTL: &coldManifestCacheTTL,
				requestTimeout:       &requestTimeout,
				statusInterval:       &statusRequestInterval,
				pexEnabled:           &pexEnabled,
				addrBookFile:         &addrBookFile,
				addrBookStrict:       &addrBookStrict,
				seeds:                &seeds,
				seedMode:             &seedMode,
				privatePeerIDs:       &privatePeerIDs,
				metricsListen:        &metricsListen,
				safetyWindow:         &safetyWindow,
				archiveInterval:      &archiveInterval,
				pruneInterval:        &pruneInterval,
				retainBlocks:         &retainBlocks,
				evidenceBlocks:       &evidenceBlocks,
				evidenceDuration:     &evidenceDuration,
				segmentBlocks:        &segmentBlocks,
				compression:          &compression,
				validation:           &validationMode,
				checkpoints:          &checkpointValues,
				validatorSets:        &validatorSetValues,
				validatorSetRPC:      &validatorSetRPC,
				validatorSetTimeout:  &validatorSetTimeout,
				dryRun:               &dryRun,
			}); err != nil {
				return err
			}
			if err := requireObjectStore(flags); err != nil {
				return err
			}
			if err := archive.ValidateObjectStoreURL(flags.storeURL); err != nil {
				return err
			}
			if err := validateServeChainID(flags.chainID); err != nil {
				return err
			}
			key, err := resolveManifestKey(flags)
			if err != nil {
				return err
			}
			if pexEnabled && addrBookFile == "" && len(splitCSV(seeds)) == 0 {
				return errors.New("PEX requires at least one seed or an address book file")
			}
			if err := validateServeRuntimeConfig(serveRuntimeConfig{
				requestLimit:          requestLimit,
				coldWorkers:           coldWorkers,
				coldManifestCacheTTL:  coldManifestCacheTTL,
				requestTimeout:        requestTimeout,
				statusRequestInterval: statusRequestInterval,
				safetyWindow:          safetyWindow,
				archiveInterval:       archiveInterval,
				pruneInterval:         pruneInterval,
				retainBlocks:          retainBlocks,
				evidenceBlocks:        evidenceBlocks,
				evidenceDuration:      evidenceDuration,
				segmentBlocks:         segmentBlocks,
				validatorSetTimeout:   validatorSetTimeout,
			}); err != nil {
				return err
			}
			if err := archive.ValidateCompression(compression); err != nil {
				return err
			}
			if !dryRun {
				if err := validateServeLiveConfig(dbDir, nodeKeyFile); err != nil {
					return err
				}
			}
			if err := archive.ValidateCometDBBackend(backend); err != nil {
				return err
			}
			if err := validateServeP2PConfig(listenAddress, splitCSV(persistentPeers), splitCSV(seeds), splitCSV(privatePeerIDs)); err != nil {
				return err
			}
			if err := validateServeMetricsConfig(metricsListen); err != nil {
				return err
			}
			if err := validateServeValidationConfig(validationMode, checkpointValues, validatorSetValues, validatorSetRPC); err != nil {
				return err
			}
			if !dryRun {
				store, err := archive.OpenObjectStore(flags.storeURL)
				if err != nil {
					return err
				}
				summary, summaryErr := archive.Inspect(cmd.Context(), store, key)
				if summaryErr != nil && !errors.Is(summaryErr, archive.ErrObjectNotFound) {
					return summaryErr
				}
				blockStore, err := archive.OpenCometBlockStore(dbDir, backend)
				if err != nil {
					return err
				}
				defer blockStore.Close()
				checkpoints, err := parseCheckpoints(checkpointValues)
				if err != nil {
					return err
				}
				validatorSets, err := loadValidatorSetFiles(validatorSetValues)
				if err != nil {
					return err
				}
				var validatorSetSource blocksyncarchive.ValidatorSetSource
				if validatorSetRPC != "" {
					source, sourceErr := newHTTPValidatorSetSource(flags.chainID, validatorSetRPC, validatorSetTimeout)
					if sourceErr != nil {
						return sourceErr
					}
					validatorSetSource = source
				}
				ingestor, err := blocksyncarchive.NewHotIngestor(blockStore, blocksyncarchive.IngestOptions{
					ChainID:            flags.chainID,
					StartHeight:        liveIngestStartHeight(blockStore, summary),
					SafetyWindow:       safetyWindow,
					Validation:         blocksyncarchive.ValidationMode(validationMode),
					Checkpoints:        checkpoints,
					ValidatorSets:      validatorSets,
					ValidatorSetSource: validatorSetSource,
				})
				if err != nil {
					return err
				}
				coldSource, err := blocksyncarchive.NewArchiveBlockSource(store, key)
				if err != nil {
					return err
				}
				if ttlErr := coldSource.SetManifestCacheTTL(coldManifestCacheTTL); ttlErr != nil {
					return ttlErr
				}
				nodeLogger, err := cmtflags.ParseLogLevel(logLevel, cmtlog.NewTMLogger(cmtlog.NewSyncWriter(cmd.ErrOrStderr())), "info")
				if err != nil {
					return fmt.Errorf("log-level: %w", err)
				}
				node, err := blocksyncarchive.NewArchiveNode(ingestor, nil, blocksyncarchive.NodeOptions{
					ChainID:         flags.chainID,
					ListenAddress:   listenAddress,
					Moniker:         moniker,
					NodeKeyFile:     nodeKeyFile,
					PersistentPeers: splitCSV(persistentPeers),
					RequestLimit:    requestLimit,
					ColdWorkers:     coldWorkers,
					RequestTimeout:  requestTimeout,
					StatusInterval:  statusRequestInterval,
					PEX:             pexEnabled,
					AddrBookFile:    addrBookFile,
					AddrBookStrict:  addrBookStrict,
					Seeds:           splitCSV(seeds),
					SeedMode:        seedMode,
					PrivatePeerIDs:  splitCSV(privatePeerIDs),
					ColdBlockSource: coldSource,
					Logger:          nodeLogger,
				})
				if err != nil {
					return err
				}
				if startErr := node.Start(); startErr != nil {
					return startErr
				}
				defer func() {
					if stopErr := node.Stop(); stopErr != nil {
						cmd.PrintErrf("stop archive node: %v\n", stopErr)
					}
				}()
				archiveCtx, cancelArchive := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
				defer cancelArchive()
				stats := newLiveMaintenanceStats()
				metricsAddress, metricsErr := startMetricsServer(archiveCtx, metricsListen, stats, func() map[string]any {
					advertised := ingestor.AdvertisedRange()
					served := node.Reactor.AdvertisedRange()
					return map[string]any{
						liveMetricP2PPeers:             node.Reactor.PeerCount(),
						liveMetricPeerBestHeight:       node.Reactor.BestPeerHeight(),
						liveMetricInflight:             node.Reactor.InflightRequests(),
						liveMetricBuffered:             node.Reactor.BufferedBlockResponses(),
						"blocksync_request_timeouts":   node.Reactor.RequestTimeouts(),
						"blocksync_hot_responses":      node.Reactor.HotBlockResponses(),
						"blocksync_cold_responses":     node.Reactor.ColdBlockResponses(),
						"blocksync_no_block_responses": node.Reactor.NoBlockResponses(),
						liveMetricColdErrors:           node.Reactor.ColdBlockErrors(),
						liveMetricColdQueue:            node.Reactor.QueuedColdBlockRequests(),
						liveMetricColdQueueFull:        node.Reactor.ColdQueueFull(),
						liveMetricColdActive:           node.Reactor.ActiveColdBlockRequests(),
						"hot_base":                     advertised.Base,
						"hot_height":                   advertised.Height,
						"served_base":                  served.Base,
						"served_height":                served.Height,
						liveMetricNextHeight:           ingestor.NextHeight(),
						"pending_height":               ingestor.PendingHeight(),
						"node_id":                      string(node.NodeKey.ID()),
						"chain_id":                     flags.chainID,
						"moniker":                      moniker,
						"p2p_listen":                   listenAddress,
						"validation":                   validationMode,
						"segment_blocks":               segmentBlocks,
						"compression":                  compression,
						"safety_window":                safetyWindow,
						"retain_blocks":                retainBlocks,
					}
				})
				if metricsErr != nil {
					return metricsErr
				}
				maintenanceErrCh := startLiveIngestorMaintenanceLoop(archiveCtx, ingestor, store, archive.LiveArchiveOptions{
					ChainID:       flags.chainID,
					Prefix:        flags.prefix,
					ManifestName:  flags.manifestName,
					ManifestKey:   flags.manifestKey,
					SegmentBlocks: segmentBlocks,
					Compression:   compression,
				}, safetyWindow, archiveInterval, archive.PruneHotOptions{
					ManifestKey:            key,
					RetainBlocks:           retainBlocks,
					EvidenceMaxAgeBlocks:   evidenceBlocks,
					EvidenceMaxAgeDuration: evidenceDuration,
				}, pruneInterval, stats)
				if err := writeJSON(cmd.OutOrStdout(), map[string]any{
					"status":                  "running",
					manifestKeyField:          key,
					"chain_id":                flags.chainID,
					"archive_range":           archiveRange(summary),
					"listen_address":          listenAddress,
					"moniker":                 moniker,
					"node_id":                 string(node.NodeKey.ID()),
					"metrics_listen":          metricsAddress,
					"pex":                     pexEnabled,
					"addr_book_file":          addrBookFile,
					"seeds":                   splitCSV(seeds),
					"request_limit":           requestLimit,
					"cold_workers":            resolvedColdWorkers(requestLimit, coldWorkers),
					"cold_manifest_cache_ttl": coldManifestCacheTTL.String(),
					"request_timeout":         requestTimeout.String(),
					"status_interval":         statusRequestInterval.String(),
					"archive_interval":        archiveInterval.String(),
					"prune_interval":          pruneInterval.String(),
					"retain_blocks":           retainBlocks,
					"safety_window":           safetyWindow,
					"validation":              validationMode,
					"validation_trust_model":  validationTrustModel(validationMode, checkpointValues, validatorSetValues, validatorSetRPC),
					"blocksync_peer":          true,
					"custom_sync":             true,
					"cold_block_serve":        true,
					"consensus_node":          false,
					"implemented":             true,
				}); err != nil {
					return err
				}
				select {
				case err := <-maintenanceErrCh:
					return err
				case <-archiveCtx.Done():
					return nil
				}
			}
			return writeJSON(cmd.OutOrStdout(), map[string]any{
				"status":                  "dry-run",
				manifestKeyField:          key,
				"chain_id":                flags.chainID,
				"archive_range":           "",
				"p2p_model":               "CometBFT P2P and custom blocksync-only archive peer with archive upload worker",
				"moniker":                 moniker,
				"blocksync_peer":          true,
				"custom_sync":             true,
				"cold_block_serve":        true,
				"consensus_node":          false,
				"metrics_listen":          metricsListen,
				"pex":                     pexEnabled,
				"addr_book_file":          addrBookFile,
				"seeds":                   splitCSV(seeds),
				"request_limit":           requestLimit,
				"cold_workers":            resolvedColdWorkers(requestLimit, coldWorkers),
				"cold_manifest_cache_ttl": coldManifestCacheTTL.String(),
				"request_timeout":         requestTimeout.String(),
				"status_interval":         statusRequestInterval.String(),
				"archive_interval":        archiveInterval.String(),
				"prune_interval":          pruneInterval.String(),
				"retain_blocks":           retainBlocks,
				"safety_window":           safetyWindow,
				"validation":              validationMode,
				"validation_trust_model":  validationTrustModel(validationMode, checkpointValues, validatorSetValues, validatorSetRPC),
				"validator_sets":          validatorSetValues,
				"validator_set_rpc":       validatorSetRPC,
				"components": []string{
					"custom_blocksync_reactor",
					"hot_block_ingestor",
					"live_archive_worker",
					"verified_hot_pruner",
					"object_store_archive",
					"cold_archive_block_serving",
				},
				"implemented": true,
				"note":        "Dry-run mode does not start P2P; pass --dry-run=false to run the live archive peer.",
			})
		},
	}
	addCommonFlags(cmd, &flags)
	cmd.Flags().StringVar(&configPath, "config", "", "JSON config file for serve; explicit flags override config values")
	cmd.Flags().BoolVar(&dryRun, "dry-run", true, "Describe the live archive node without starting P2P")
	cmd.Flags().StringVar(&dbDir, "db-dir", "", "CometBFT DB directory containing blockstore.db")
	cmd.Flags().StringVar(&backend, "db-backend", "goleveldb", "CometBFT DB backend")
	cmd.Flags().StringVar(&listenAddress, "p2p-listen", "tcp://0.0.0.0:26656", "P2P listen address")
	cmd.Flags().StringVar(&moniker, "moniker", "cometbft-archive", "P2P node moniker advertised in node info")
	cmd.Flags().StringVar(&nodeKeyFile, "node-key-file", "", "Path to node_key.json; required when dry-run=false")
	cmd.Flags().StringVar(&persistentPeers, "persistent-peers", "", "Comma-delimited peer addresses to dial and keep connected")
	cmd.Flags().IntVar(&requestLimit, "request-limit", blocksyncarchive.DefaultRequestLimit, "Maximum new blocksync requests to issue per planning pass")
	cmd.Flags().IntVar(&coldWorkers, "cold-workers", 0, "Concurrent object-store-backed cold block requests to serve; 0 uses request-limit")
	cmd.Flags().DurationVar(&coldManifestCacheTTL, "cold-manifest-cache-ttl", blocksyncarchive.DefaultColdManifestCacheTTL, "Cold archive manifest cache TTL; 0 disables caching")
	cmd.Flags().DurationVar(&requestTimeout, "request-timeout", 30*time.Second, "Timeout before retrying an unanswered blocksync request")
	cmd.Flags().DurationVar(&statusRequestInterval, "status-request-interval", 10*time.Second, "Interval for polling peers with blocksync status requests; 0 disables periodic polling")
	cmd.Flags().BoolVar(&pexEnabled, "pex", false, "Enable CometBFT PEX reactor for peer discovery without consensus")
	cmd.Flags().StringVar(&addrBookFile, "addr-book-file", "", "PEX address book file; empty keeps the address book in memory")
	cmd.Flags().BoolVar(&addrBookStrict, "addr-book-strict", true, "Use strict routability checks for PEX address book entries")
	cmd.Flags().StringVar(&seeds, "seeds", "", "Comma-delimited seed node addresses for PEX")
	cmd.Flags().BoolVar(&seedMode, "seed-mode", false, "Run PEX in seed/crawler mode")
	cmd.Flags().StringVar(&privatePeerIDs, "private-peer-ids", "", "Comma-delimited peer IDs that PEX must not add")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "Log level, or module:level pairs such as p2p:debug,*:info (modules: p2p, blocksync)")
	cmd.Flags().StringVar(&metricsListen, "metrics-listen", "", "Optional TCP listen address for /healthz and /metrics")
	cmd.Flags().Int64Var(&safetyWindow, "safety-window", 100, "Number of hot blocks to keep ahead of archive uploads")
	cmd.Flags().DurationVar(&archiveInterval, "archive-interval", 30*time.Second, "Interval for archiving safety-windowed hot blocks while serving")
	cmd.Flags().DurationVar(&pruneInterval, "prune-interval", 5*time.Minute, "Interval for verified hot blockstore pruning while serving; 0 disables live pruning")
	cmd.Flags().Int64Var(&retainBlocks, "retain-blocks", 1000, "Number of latest local hot blocks to retain during live pruning")
	cmd.Flags().Int64Var(&evidenceBlocks, "evidence-max-age-blocks", 0, "Override evidence max age blocks for live pruning state; 0 uses CometBFT default")
	cmd.Flags().DurationVar(&evidenceDuration, "evidence-max-age-duration", 0, "Override evidence max age duration for live pruning state; 0 uses CometBFT default")
	cmd.Flags().IntVar(&segmentBlocks, "segment-blocks", archive.DefaultSegmentBlocks, "Maximum blocks per immutable archive segment")
	cmd.Flags().StringVar(&compression, "compression", archive.DefaultCompression, "Segment compression: gzip or none")
	cmd.Flags().StringVar(&validationMode, "validation", string(blocksyncarchive.ValidationStorageOnly), "Live ingest validation mode: storage-only, checkpoint, or validator-set")
	cmd.Flags().StringArrayVar(&checkpointValues, "checkpoint", nil, "Trusted checkpoint in height:hexhash form; repeatable")
	cmd.Flags().StringArrayVar(&validatorSetValues, "validator-set", nil, "Trusted validator set in height:path form; file must contain protobuf tendermint.types.ValidatorSet; repeatable")
	cmd.Flags().StringVar(&validatorSetRPC, "validator-set-rpc", "", "RPC endpoint used to fetch validator sets in validator-set mode")
	cmd.Flags().DurationVar(&validatorSetTimeout, "validator-set-timeout", 5*time.Second, "Timeout for fetching validator sets from --validator-set-rpc")
	return cmd, nil
}

func addCommonFlags(cmd *cobra.Command, flags *commonFlags) {
	cmd.Flags().StringVar(&flags.storeURL, "store", "", "Object store URL: file:///path or s3://bucket/prefix?region=us-east-1")
	cmd.Flags().StringVar(&flags.prefix, "prefix", "archive", "Archive object key prefix")
	cmd.Flags().StringVar(&flags.chainID, "chain-id", "", "CometBFT chain ID")
	cmd.Flags().StringVar(&flags.manifestKey, "manifest-key", "", "Full manifest object key; overrides prefix/chain-id/manifest-name")
	cmd.Flags().StringVar(&flags.manifestName, "manifest-name", archive.DefaultManifest, "Manifest object name")
}

func requireObjectStore(flags commonFlags) error {
	if flags.storeURL == "" {
		return errors.New("store is required")
	}
	return nil
}

func resolveManifestKey(flags commonFlags) (string, error) {
	if flags.manifestKey != "" {
		if err := archive.ValidateObjectKey(flags.manifestKey); err != nil {
			return "", err
		}
		return flags.manifestKey, nil
	}
	if flags.chainID == "" {
		return "", errors.New("chain-id is required unless manifest-key is provided")
	}
	if err := archive.ValidateArchiveKeys(flags.prefix, flags.chainID, flags.manifestName); err != nil {
		return "", err
	}
	return archive.ManifestKey(flags.prefix, flags.chainID, flags.manifestName), nil
}

type blockStoreHeightReader interface {
	Height() int64
}

func liveIngestStartHeight(blockStore blockStoreHeightReader, summary archive.InspectSummary) int64 {
	if blockStore != nil && blockStore.Height() == 0 && summary.LastHeight > 0 {
		return summary.LastHeight + 1
	}
	return 0
}

func markRequired(cmd *cobra.Command, names ...string) error {
	for _, name := range names {
		if err := cmd.MarkFlagRequired(name); err != nil {
			return fmt.Errorf("mark flag %q required: %w", name, err)
		}
	}
	return nil
}

func validateSegmentBlocks(segmentBlocks int) error {
	if segmentBlocks <= 0 {
		return errors.New("segment blocks must be positive")
	}
	if segmentBlocks > archive.MaxSegmentBlocks {
		return fmt.Errorf("segment blocks cannot exceed %d", archive.MaxSegmentBlocks)
	}
	return nil
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// tickLoop runs fn once and then re-runs it every interval until ctx is done
// or fn returns false.
func tickLoop(ctx context.Context, interval time.Duration, fn func() bool) {
	if !fn() {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !fn() {
				return
			}
		}
	}
}

func startLiveIngestorMaintenanceLoop(
	ctx context.Context,
	ingestor *blocksyncarchive.HotIngestor,
	objectStore archive.ObjectStore,
	archiveOpts archive.LiveArchiveOptions,
	safetyWindow int64,
	archiveInterval time.Duration,
	pruneOpts archive.PruneHotOptions,
	pruneInterval time.Duration,
	stats *liveMaintenanceStats,
) <-chan error {
	errCh := make(chan error, 1)
	if archiveInterval <= 0 {
		archiveInterval = 30 * time.Second
	}
	lastPrune := time.Time{}
	runMaintenance := func() bool {
		archived, archiveErr := archive.ArchiveReadyFromHead(ctx, ingestor.BlockReader(), objectStore, archiveOpts, safetyWindow)
		if archiveErr != nil {
			if ctx.Err() != nil {
				return false
			}
			stats.recordArchive(archived.BlocksArchived, archiveErr)
			return true
		}
		stats.recordArchive(archived.BlocksArchived, nil)
		if pruneInterval <= 0 || (!lastPrune.IsZero() && time.Since(lastPrune) < pruneInterval) {
			return true
		}
		var pruned archive.PruneHotResult
		pruneErr := ingestor.WithHotStore(func(blockStore *cmtstore.BlockStore) error {
			var err error
			pruned, err = runLivePrune(ctx, blockStore, objectStore, pruneOpts)
			return err
		})
		if pruneErr != nil {
			if ctx.Err() != nil {
				return false
			}
			stats.recordPrune(pruned.Pruned, pruneErr)
			return true
		}
		stats.recordPrune(pruned.Pruned, nil)
		lastPrune = time.Now()
		return true
	}
	go tickLoop(ctx, archiveInterval, runMaintenance)
	return errCh
}

func runLivePrune(
	ctx context.Context,
	blockStore *cmtstore.BlockStore,
	objectStore archive.ObjectStore,
	opts archive.PruneHotOptions,
) (archive.PruneHotResult, error) {
	manifest, err := archive.LoadManifest(ctx, objectStore, opts.ManifestKey)
	if errors.Is(err, archive.ErrObjectNotFound) {
		return archive.PruneHotResult{}, nil
	}
	if err != nil {
		return archive.PruneHotResult{}, err
	}
	if manifest.LastHeight == 0 {
		return archive.PruneHotResult{}, nil
	}
	return archive.PruneVerifiedHotStore(ctx, blockStore, objectStore, opts)
}

func archiveRange(summary archive.InspectSummary) string {
	if summary.FirstHeight == 0 && summary.LastHeight == 0 {
		return ""
	}
	return fmt.Sprintf("%d-%d", summary.FirstHeight, summary.LastHeight)
}

func parseCheckpoints(values []string) (map[int64]string, error) {
	if len(values) == 0 {
		return map[int64]string{}, nil
	}
	checkpoints := make(map[int64]string, len(values))
	for _, value := range values {
		heightText, hash, ok := strings.Cut(value, ":")
		if !ok {
			return nil, fmt.Errorf("checkpoint %q must be in height:hexhash form", value)
		}
		height, err := strconv.ParseInt(strings.TrimSpace(heightText), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("checkpoint %q has invalid height: %w", value, err)
		}
		if height <= 0 {
			return nil, fmt.Errorf("checkpoint %q has invalid height %d", value, height)
		}
		hash = strings.TrimSpace(hash)
		if hash == "" {
			return nil, fmt.Errorf("checkpoint %q has empty hash", value)
		}
		if len(hash) != 64 {
			return nil, fmt.Errorf("checkpoint %q hash must be 64 hex characters", value)
		}
		if _, err := hex.DecodeString(hash); err != nil {
			return nil, fmt.Errorf("checkpoint %q hash is not valid hex: %w", value, err)
		}
		if _, ok := checkpoints[height]; ok {
			return nil, fmt.Errorf("duplicate checkpoint height %d", height)
		}
		checkpoints[height] = hash
	}
	return checkpoints, nil
}

func validateServeValidationConfig(validationMode string, checkpointValues, validatorSetValues []string, validatorSetRPC string) error {
	switch blocksyncarchive.ValidationMode(validationMode) {
	case blocksyncarchive.ValidationStorageOnly:
		if len(checkpointValues) > 0 || len(validatorSetValues) > 0 || strings.TrimSpace(validatorSetRPC) != "" {
			return errors.New("storage-only validation does not accept checkpoint or validator-set options")
		}
		return nil
	case blocksyncarchive.ValidationCheckpoint:
		if len(validatorSetValues) > 0 || strings.TrimSpace(validatorSetRPC) != "" {
			return errors.New("checkpoint validation does not accept validator-set options")
		}
		if _, err := parseCheckpoints(checkpointValues); err != nil {
			return err
		}
		if len(checkpointValues) == 0 {
			return errors.New("checkpoint validation requires at least one checkpoint")
		}
		return nil
	case blocksyncarchive.ValidationValidatorSet:
		if len(checkpointValues) > 0 {
			return errors.New("validator-set validation does not accept checkpoint options")
		}
		if _, err := loadValidatorSetFiles(validatorSetValues); err != nil {
			return err
		}
		if len(validatorSetValues) == 0 && strings.TrimSpace(validatorSetRPC) == "" {
			return errors.New("validator-set validation requires at least one validator set or validator set RPC")
		}
		return nil
	default:
		return fmt.Errorf("unsupported validation mode %q", validationMode)
	}
}

func validationTrustModel(validationMode string, checkpointValues, validatorSetValues []string, validatorSetRPC string) string {
	switch blocksyncarchive.ValidationMode(validationMode) {
	case blocksyncarchive.ValidationStorageOnly:
		return trustModelStorageOnly
	case blocksyncarchive.ValidationCheckpoint:
		return trustModelTrustedCheckpoints
	case blocksyncarchive.ValidationValidatorSet:
		hasFiles := len(validatorSetValues) > 0
		hasRPC := strings.TrimSpace(validatorSetRPC) != ""
		switch {
		case hasFiles && hasRPC:
			return trustModelTrustedValidatorSetFilesWithRPC
		case hasFiles:
			return trustModelTrustedValidatorSetFiles
		case hasRPC:
			return trustModelRPCTrustedValidatorSetSource
		default:
			return trustModelMissingValidatorSetSource
		}
	default:
		if len(checkpointValues) > 0 {
			return trustModelUnknownWithCheckpoints
		}
		return trustModelUnknown
	}
}

type serveRuntimeConfig struct {
	requestLimit          int
	coldWorkers           int
	coldManifestCacheTTL  time.Duration
	requestTimeout        time.Duration
	statusRequestInterval time.Duration
	safetyWindow          int64
	archiveInterval       time.Duration
	pruneInterval         time.Duration
	retainBlocks          int64
	evidenceBlocks        int64
	evidenceDuration      time.Duration
	segmentBlocks         int
	validatorSetTimeout   time.Duration
}

func validateServeRuntimeConfig(c serveRuntimeConfig) error {
	if c.requestLimit <= 0 {
		return errors.New("request limit must be positive")
	}
	if c.requestLimit > blocksyncarchive.MaxRequestLimit {
		return fmt.Errorf("request limit cannot exceed %d", blocksyncarchive.MaxRequestLimit)
	}
	if c.coldWorkers < 0 {
		return errors.New("cold workers cannot be negative")
	}
	if c.coldWorkers > blocksyncarchive.MaxRequestLimit {
		return fmt.Errorf("cold workers cannot exceed %d", blocksyncarchive.MaxRequestLimit)
	}
	if c.coldManifestCacheTTL < 0 {
		return errors.New("cold manifest cache TTL cannot be negative")
	}
	if c.requestTimeout <= 0 {
		return errors.New("request timeout must be positive")
	}
	if c.statusRequestInterval < 0 {
		return errors.New("status request interval cannot be negative")
	}
	if c.safetyWindow < 0 {
		return errors.New("safety window cannot be negative")
	}
	if c.archiveInterval < 0 {
		return errors.New("archive interval cannot be negative")
	}
	if c.pruneInterval < 0 {
		return errors.New("prune interval cannot be negative")
	}
	if c.pruneInterval > 0 && c.retainBlocks <= 0 {
		return errors.New("retain blocks must be positive when live pruning is enabled")
	}
	if c.evidenceBlocks < 0 {
		return errors.New("evidence max age blocks cannot be negative")
	}
	if c.evidenceDuration < 0 {
		return errors.New("evidence max age duration cannot be negative")
	}
	if err := validateSegmentBlocks(c.segmentBlocks); err != nil {
		return err
	}
	if c.validatorSetTimeout <= 0 {
		return errors.New("validator set timeout must be positive")
	}
	return nil
}

func resolvedColdWorkers(requestLimit int, coldWorkers int) int {
	if coldWorkers == 0 {
		return requestLimit
	}
	return coldWorkers
}

func validateServeLiveConfig(dbDir string, nodeKeyFile string) error {
	if strings.TrimSpace(dbDir) == "" {
		return errors.New("db-dir is required when dry-run=false")
	}
	if strings.TrimSpace(nodeKeyFile) == "" {
		return errors.New("node-key-file is required when dry-run=false")
	}
	return nil
}

func validateServeChainID(chainID string) error {
	if strings.TrimSpace(chainID) == "" {
		return errors.New("chain-id is required for serve")
	}
	return archive.ValidateArchiveChainID(chainID)
}

func validateServeP2PConfig(listenAddress string, persistentPeers, seeds, privatePeerIDs []string) error {
	if err := validateHostPort("p2p listen address", listenAddress); err != nil {
		return err
	}
	configuredPeers := make(map[string]string, len(persistentPeers)+len(seeds))
	for _, peer := range persistentPeers {
		id, err := validatePeerAddress(peerRolePersistent, peer)
		if err != nil {
			return err
		}
		if existing := configuredPeers[id]; existing != "" {
			return fmt.Errorf("duplicate peer ID %q in persistent peer list", id)
		}
		configuredPeers[id] = peerRolePersistent
	}
	for _, seed := range seeds {
		id, err := validatePeerAddress(peerRoleSeed, seed)
		if err != nil {
			return err
		}
		if existing := configuredPeers[id]; existing != "" {
			if existing == peerRoleSeed {
				return fmt.Errorf("duplicate peer ID %q in seed list", id)
			}
			return fmt.Errorf("peer ID %q configured as both %s and seed", id, existing)
		}
		configuredPeers[id] = peerRoleSeed
	}
	privateIDs := make(map[string]struct{}, len(privatePeerIDs))
	for _, id := range privatePeerIDs {
		normalizedID, err := normalizePeerID(id)
		if err != nil {
			return fmt.Errorf("private peer ID %q: %w", id, err)
		}
		if _, ok := privateIDs[normalizedID]; ok {
			return fmt.Errorf("duplicate private peer ID %q", normalizedID)
		}
		privateIDs[normalizedID] = struct{}{}
	}
	return nil
}

func validateServeMetricsConfig(metricsListen string) error {
	if strings.TrimSpace(metricsListen) == "" {
		return nil
	}
	return validateHostPortAllowZero("metrics listen address", metricsListen)
}

func validatePeerAddress(label, address string) (string, error) {
	id, hostPort, ok := strings.Cut(stripProtocol(address), "@")
	if !ok {
		return "", fmt.Errorf("%s %q must be in id@host:port form", label, address)
	}
	id, err := normalizePeerID(id)
	if err != nil {
		return "", fmt.Errorf("%s %q has invalid ID: %w", label, address, err)
	}
	if err := validateHostPort(label, hostPort); err != nil {
		return "", err
	}
	return id, nil
}

func normalizePeerID(id string) (string, error) {
	if id == "" {
		return "", errors.New("empty ID")
	}
	decoded, err := hex.DecodeString(id)
	if err != nil {
		return "", err
	}
	if len(decoded) != p2p.IDByteLength {
		return "", fmt.Errorf("invalid hex length: got %d, expected %d", len(decoded), p2p.IDByteLength)
	}
	return strings.ToLower(id), nil
}

func validateHostPort(label, address string) error {
	port, err := parseHostPort(label, address)
	if err != nil {
		return err
	}
	if port == 0 {
		return fmt.Errorf("%s %q has invalid port: %w", label, address, errors.New("port must be positive"))
	}
	return nil
}

func validateHostPortAllowZero(label, address string) error {
	_, err := parseHostPort(label, address)
	return err
}

func parseHostPort(label, address string) (uint64, error) {
	host, portText, err := net.SplitHostPort(stripProtocol(address))
	if err != nil {
		return 0, fmt.Errorf("%s %q must include host and port: %w", label, address, err)
	}
	if strings.TrimSpace(host) == "" {
		return 0, fmt.Errorf("%s %q has empty host", label, address)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("%s %q has invalid port: %w", label, address, err)
	}
	return port, nil
}

func stripProtocol(address string) string {
	if _, rest, ok := strings.Cut(address, "://"); ok {
		return rest
	}
	return address
}
