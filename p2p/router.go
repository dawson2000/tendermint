package p2p

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/libs/service"
)

// Router manages peer connections and routes messages between peers and
// channels.
type Router struct {
	*service.BaseService
	logger     log.Logger
	transports map[Protocol]Transport
	store      *peerStore

	chClose chan struct{}
	wg      sync.WaitGroup

	mtx         sync.RWMutex
	channels    map[ChannelID]*Channel
	peerUpdates map[*PeerUpdatesCh]*PeerUpdatesCh // keyed by struct identity (address)
}

// NewRouter creates a new Router.
func NewRouter(logger log.Logger, transports map[Protocol]Transport, peers []PeerAddress) *Router {
	store := newPeerStore(logger)
	for _, address := range peers {
		if err := store.Add(address); err != nil {
			logger.Error("failed to add peer", "address", address, "err", err)
		}
	}
	router := &Router{
		logger:      logger,
		transports:  transports,
		store:       store,
		chClose:     make(chan struct{}),
		channels:    map[ChannelID]*Channel{},
		peerUpdates: map[*PeerUpdatesCh]*PeerUpdatesCh{},
	}
	router.BaseService = service.NewBaseService(logger, "router", router)
	return router
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
		case <-r.chClose:
		}
	}()
	return peerUpdates, nil
}

// processPeers maintains connections between peers.
func (r *Router) processPeers() {
	for {
		select {
		case <-r.chClose:
			return
		default:
		}

		peer := r.store.Dispense()
		if peer == nil {
			time.Sleep(time.Second)
			continue
		}
		go r.processPeer(peer)
	}
}

// processPeer manages a single peer.
func (r *Router) processPeer(peer *sPeer) {
	r.wg.Add(1)
	defer func() {
		r.store.Return(peer.ID)
		r.wg.Done()
	}()
	r.logger.Info("contacting peer", "peer", peer.ID)
	ctx := context.Background()

	r.logger.Info("resolving peer endpoints", "peer", peer.ID)
	endpoints := []Endpoint{}
	for _, address := range peer.Addresses {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		e, err := address.Resolve(ctx)
		if err != nil {
			r.logger.Error("failed to resolve address", "address", address, "err", err)
			continue
		}
		endpoints = append(endpoints, e...)
	}
	if len(endpoints) == 0 {
		r.logger.Error("unable to resolve network endpoints", "peer", peer.ID)
		return
	}

	var conn Connection
	for _, endpoint := range endpoints {
		t, ok := r.transports[endpoint.Protocol]
		if !ok {
			r.logger.Error("no transport found for protocol", "protocol", endpoint.Protocol)
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var err error
		conn, err = t.Dial(ctx, endpoint)
		if err != nil {
			r.logger.Error("failed to dial endpoint", "endpoint", endpoint)
			continue
		}
		r.logger.Info("connected to peer", "peer", peer.ID, "endpoint", endpoint)
		break
	}
	if conn == nil {
		r.logger.Error("failed to connect to peer", "peer", peer.ID)
		return
	}
}

// OnStart implements service.Service.
func (r *Router) OnStart() error {
	go r.processPeers()
	return nil
}

// OnStop implements service.Service.
func (r *Router) OnStop() {
	close(r.chClose)
	r.wg.Wait()
}
