package p2p

import (
	"context"
	"errors"
	"fmt"
	"io"
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

// acceptPeers accepts inbound connections from peers on the given transport.
func (r *Router) acceptPeers(transport Transport) {
	for {
		select {
		case <-r.chClose:
			return
		default:
		}

		conn, err := transport.Accept(context.Background())
		switch err {
		case ErrTransportClosed{}:
			return
		case io.EOF:
			return
		case nil:
		default:
			r.logger.Error("failed to accept connection", "err", err)
			return
		}

		peerID, err := conn.NodeInfo().DefaultNodeID.ToPeerID()
		if err != nil {
			r.logger.Error("invalid peer ID", "peer", conn.NodeInfo().DefaultNodeID)
			_ = conn.Close()
			continue
		}
		peer := r.store.Claim(peerID)
		if peer == nil {
			r.logger.Error("already connected to peer, rejecting connection", "peer", peerID)
			_ = conn.Close()
			continue
		}

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer r.store.Return(peer.ID)
			defer conn.Close()
			err = r.routePeer(peer, conn)
			if err != nil {
				r.logger.Error("peer failure", "peer", peer.ID, "err", err)
			}
		}()
	}
}

// dialPeers maintains outbound connections to peers.
func (r *Router) dialPeers() {
	for {
		select {
		case <-r.chClose:
			return
		default:
		}

		peer := r.store.Dispense()
		if peer == nil {
			// no dispensed peers, sleep for a while
			r.logger.Info("no eligible peers, sleeping")
			time.Sleep(time.Second)
			continue
		}

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer r.store.Return(peer.ID)
			conn, err := r.dialPeer(peer)
			if err != nil {
				r.logger.Error("failed to dial peer, will retry", "peer", peer.ID)
				return
			}
			defer conn.Close()
			err = r.routePeer(peer, conn)
			if err != nil {
				r.logger.Error("peer failure", "peer", peer.ID, "err", err)
			}
		}()
	}
}

// dialPeer attempts to connect to a peer.
func (r *Router) dialPeer(peer *sPeer) (Connection, error) {
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
		return nil, errors.New("unable to resolve any network endpoints")
	}

	for _, endpoint := range endpoints {
		t, ok := r.transports[endpoint.Protocol]
		if !ok {
			r.logger.Error("no transport found for protocol", "protocol", endpoint.Protocol)
			continue
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		conn, err := t.Dial(ctx, endpoint)
		if err != nil {
			r.logger.Error("failed to dial endpoint", "endpoint", endpoint)
		} else {
			r.logger.Info("connected to peer", "peer", peer.ID, "endpoint", endpoint)
			return conn, nil
		}
	}
	return nil, errors.New("failed to connect to peer")
}

// routePeer routes messages for a peer across a connection.
func (r *Router) routePeer(peer *sPeer, conn Connection) error {
	for {
		ch, msg, err := conn.ReceiveMessage()
		if err == io.EOF {
			r.logger.Info("disconnected from peer", "peer", peer.ID)
			return nil
		} else if err != nil {
			return err
		}
		r.logger.Info("received message", "peer", peer.ID, "channel", ch, "message", msg)
	}
}

// OnStart implements service.Service.
func (r *Router) OnStart() error {
	go r.dialPeers()
	for _, transport := range r.transports {
		go r.acceptPeers(transport)
	}
	return nil
}

// OnStop implements service.Service.
func (r *Router) OnStop() {
	close(r.chClose)
	r.wg.Wait()
}
