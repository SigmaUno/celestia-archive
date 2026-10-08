package blocksyncarchive

import (
	"context"

	"github.com/cometbft/cometbft/consensus/propagation"
	"github.com/cometbft/cometbft/mempool/cat"
	"github.com/cometbft/cometbft/p2p"
)

const compatReactorName = "COMPAT"

// compatChannels are the channels Celestia full nodes require peers to
// advertise (see the peer filters in celestia-core node/setup.go). The archive
// node never uses them; it only needs to be accepted.
var compatChannels = []byte{
	propagation.DataChannel,
	propagation.WantChannel,
	cat.MempoolDataChannel,
	cat.MempoolWantsChannel,
}

const (
	compatPropMaxMsgSize = 512 * 1024
	compatCATMaxMsgSize  = 8 * 1024 * 1024
)

// compatReactor advertises the propagation and CAT mempool channels and
// discards everything received on them.
type compatReactor struct {
	p2p.BaseReactor
}

func newCompatReactor() *compatReactor {
	r := &compatReactor{}
	r.BaseReactor = *p2p.NewBaseReactor(compatReactorName, r, p2p.WithProcessor(drainIncoming))
	return r
}

func drainIncoming(ctx context.Context, incoming <-chan p2p.UnmarshalResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-incoming:
			if !ok {
				return
			}
		}
	}
}

func (*compatReactor) GetChannels() []*p2p.ChannelDescriptor {
	capacities := map[byte]int{
		propagation.DataChannel: compatPropMaxMsgSize,
		propagation.WantChannel: compatPropMaxMsgSize,
		cat.MempoolDataChannel:  compatCATMaxMsgSize,
		cat.MempoolWantsChannel: compatCATMaxMsgSize,
	}
	descs := make([]*p2p.ChannelDescriptor, 0, len(compatChannels))
	for _, id := range compatChannels {
		descs = append(descs, &p2p.ChannelDescriptor{
			ID:                  id,
			Priority:            1,
			SendQueueCapacity:   1,
			RecvMessageCapacity: capacities[id],
		})
	}
	return descs
}

func (*compatReactor) Receive(p2p.Envelope) {}
