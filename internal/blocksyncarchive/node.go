package blocksyncarchive

import (
	"errors"
	"fmt"
	"strings"
	"time"

	cmtblocksync "github.com/cometbft/cometbft/blocksync"
	cmtcfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/trace"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/version"
)

type NodeOptions struct {
	ChainID         string
	ListenAddress   string
	Moniker         string
	NodeKeyFile     string
	PersistentPeers []string
	RequestLimit    int
	ColdWorkers     int
	RequestTimeout  time.Duration
	StatusInterval  time.Duration
	PEX             bool
	AddrBookFile    string
	AddrBookStrict  bool
	Seeds           []string
	SeedMode        bool
	PrivatePeerIDs  []string
	ColdBlockSource ColdBlockSource
	Logger          log.Logger
}

type ArchiveNode struct {
	Config    *cmtcfg.P2PConfig
	NodeKey   *p2p.NodeKey
	NodeInfo  p2p.DefaultNodeInfo
	Transport *p2p.MultiplexTransport
	Switch    *p2p.Switch
	Reactor   *Reactor

	persistentPeers []string
}

func NewArchiveNode(ingestor *HotIngestor, planner *RequestPlanner, opts NodeOptions) (*ArchiveNode, error) {
	if opts.ChainID == "" {
		return nil, errors.New("chain ID is required")
	}
	if opts.ListenAddress == "" {
		opts.ListenAddress = "tcp://0.0.0.0:26656"
	}
	if opts.Moniker == "" {
		opts.Moniker = "cometbft-archive"
	}
	if ingestor == nil {
		return nil, errors.New("hot ingestor is required")
	}
	if opts.PEX && len(opts.Seeds) == 0 && opts.AddrBookFile == "" {
		return nil, errors.New("PEX requires at least one seed or an address book file")
	}
	reactor, err := NewReactor(ingestor, planner, ReactorOptions{
		RequestLimit:          opts.RequestLimit,
		ColdRequestWorkers:    opts.ColdWorkers,
		RequestTimeout:        opts.RequestTimeout,
		StatusRequestInterval: opts.StatusInterval,
		ColdBlockSource:       opts.ColdBlockSource,
	})
	if err != nil {
		return nil, err
	}
	nodeKey, err := loadOrCreateNodeKey(opts.NodeKeyFile)
	if err != nil {
		return nil, err
	}
	channels := append([]byte{cmtblocksync.BlocksyncChannel}, compatChannels...)
	if opts.PEX {
		channels = append(channels, pex.PexChannel)
	}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      opts.ListenAddress,
		Network:         opts.ChainID,
		Version:         version.TMCoreSemVer,
		Channels:        channels,
		Moniker:         opts.Moniker,
	}
	if validateErr := nodeInfo.Validate(); validateErr != nil {
		return nil, fmt.Errorf("validate node info: %w", validateErr)
	}
	cfg := cmtcfg.DefaultP2PConfig()
	cfg.ListenAddress = opts.ListenAddress
	cfg.PexReactor = opts.PEX
	cfg.AddrBook = opts.AddrBookFile
	cfg.AddrBookStrict = opts.AddrBookStrict
	cfg.Seeds = strings.Join(opts.Seeds, ",")
	cfg.SeedMode = opts.SeedMode
	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, p2p.MConnConfig(cfg), trace.NoOpTracer())
	transport.AddChannel(cmtblocksync.BlocksyncChannel)
	for _, ch := range compatChannels {
		transport.AddChannel(ch)
	}
	if opts.PEX {
		transport.AddChannel(pex.PexChannel)
	}
	sw := p2p.NewSwitch(cfg, transport)
	logger := opts.Logger
	if logger == nil {
		logger = log.NewNopLogger()
	}
	sw.SetLogger(logger.With("module", "p2p"))
	reactor.SetLogger(logger.With("module", "blocksync"))
	sw.AddReactor(ReactorName, reactor)
	sw.AddReactor(compatReactorName, newCompatReactor())
	if opts.PEX {
		addrBook := pex.NewAddrBook(opts.AddrBookFile, opts.AddrBookStrict)
		addrBook.SetLogger(logger.With("module", "addrbook"))
		if addr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), opts.ListenAddress)); err == nil {
			addrBook.AddOurAddress(addr)
		}
		addrBook.AddPrivateIDs(opts.PrivatePeerIDs)
		sw.SetAddrBook(addrBook)
		pexReactor := pex.NewReactor(addrBook, &pex.ReactorConfig{
			Seeds:                        opts.Seeds,
			SeedMode:                     opts.SeedMode,
			SeedDisconnectWaitPeriod:     28 * time.Hour,
			PersistentPeersMaxDialPeriod: cfg.PersistentPeersMaxDialPeriod,
		})
		pexReactor.SetLogger(logger.With("module", "pex"))
		sw.AddReactor("PEX", pexReactor)
	}
	sw.SetNodeInfo(nodeInfo)
	sw.SetNodeKey(nodeKey)
	if len(opts.PersistentPeers) > 0 {
		if err := sw.AddPersistentPeers(opts.PersistentPeers); err != nil {
			return nil, fmt.Errorf("add persistent peers: %w", err)
		}
	}
	return &ArchiveNode{
		Config:    cfg,
		NodeKey:   nodeKey,
		NodeInfo:  nodeInfo,
		Transport: transport,
		Switch:    sw,
		Reactor:   reactor,

		persistentPeers: opts.PersistentPeers,
	}, nil
}

func (n *ArchiveNode) Start() error {
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
	if len(n.persistentPeers) > 0 {
		return n.Switch.DialPeersAsync(n.persistentPeers)
	}
	return nil
}

func (n *ArchiveNode) Stop() error {
	var stopErr error
	if n.Switch.IsRunning() {
		stopErr = n.Switch.Stop()
	}
	if err := n.Transport.Close(); err != nil && stopErr == nil {
		stopErr = err
	}
	return stopErr
}

func loadOrCreateNodeKey(path string) (*p2p.NodeKey, error) {
	if path != "" {
		return p2p.LoadOrGenNodeKey(path)
	}
	return &p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}, nil
}
