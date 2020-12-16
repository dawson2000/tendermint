package p2p

import (
	"fmt"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/libs/service"
)

// Router manages peer connections and routes messages between
// peers and channels.
type Router struct {
	service.BaseService
	logger     log.Logger
	transports map[Protocol]Transport

	closeCh chan struct{}

	mtx         sync.RWMutex
	channels    map[ChannelID]*Channel
	peerUpdates map[*PeerUpdatesCh]*PeerUpdatesCh // keyed by struct identity (address)
}

// NewRouter creates a new Router.
func NewRouter(logger log.Logger, transports map[Protocol]Transport, peers []PeerAddress) *Router {
	return &Router{
		logger:     logger,
		transports: transports,
		channels:   map[ChannelID]*Channel{},
	}
}

// OpenChannel opens a new channel for the given message type.
func (r *Router) OpenChannel(id ChannelID, messageType proto.Message) (*Channel, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if _, ok := r.channels[id]; ok {
		return nil, fmt.Errorf("channel %v already in use", id)
	}
	ch := NewChannel(id, messageType, make(chan Envelope), make(chan Envelope), make(chan PeerError))
	r.channels[id] = ch
	return ch, nil
}

// SubscribePeerUpdates creates a new peer updates subscription.
func (r *Router) SubscribePeerUpdates() (*PeerUpdatesCh, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	peerUpdates := NewPeerUpdates()
	r.peerUpdates[peerUpdates] = peerUpdates
	go func() {
		select {
		case <-peerUpdates.Done():
			r.mtx.Lock()
			defer r.mtx.Unlock()
			delete(r.peerUpdates, peerUpdates)
		case <-r.closeCh:
		}
	}()
	return peerUpdates, nil
}

// OnStart implements service.Service.
func (r *Router) OnStart() error {
	return nil
}

// OnStop implements service.Service.
func (r *Router) OnStop() {

}
