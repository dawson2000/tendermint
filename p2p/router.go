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

// rawEnvelope is an envelope used to route raw messages internally in the router.
type rawEnvelope struct {
	channel ChannelID
	peer    PeerID // either sender or recipient depending on direction
	message []byte
}

// Router manages peer connections and routes messages between peers and
// channels.
type Router struct {
	*service.BaseService
	logger     log.Logger
	transports map[Protocol]Transport
	store      *peerStore

	chClose    chan struct{}
	wgChannels sync.WaitGroup
	wgPeers    sync.WaitGroup

	// FIXME: should use a finer-grained mutex, e.g. one per resource type.
	mtx         sync.RWMutex
	channels    map[ChannelID]chan<- rawEnvelope
	peers       map[string]chan<- rawEnvelope
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
		channels:    map[ChannelID]chan<- rawEnvelope{},
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

	channel := NewChannel(id, messageType, make(chan Envelope), make(chan Envelope), nil) // FIXME: handle PeerError
	inScheduler := newFIFOScheduler(1000)
	r.channels[id] = inScheduler.enqueue()
	r.wgChannels.Add(1)
	go r.sendChannel(channel, inScheduler.dequeue())
	go r.receiveChannel(channel)
	go func() {
		<-channel.doneCh
		r.mtx.Lock()
		delete(r.channels, id)
		r.mtx.Unlock()
		r.wgChannels.Done()
	}()

	return channel, nil
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

		r.wgPeers.Add(1)
		go func() {
			defer r.wgPeers.Done()
			defer r.store.Return(peer.ID)
			defer conn.Close()
			err = r.receivePeer(peer, conn)
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

		r.wgPeers.Add(1)
		go func() {
			defer r.wgPeers.Done()
			defer r.store.Return(peer.ID)
			conn, err := r.dialPeer(peer)
			if err != nil {
				r.logger.Error("failed to dial peer, will retry", "peer", peer.ID)
				return
			}
			defer conn.Close()
			err = r.receivePeer(peer, conn)
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

// receivePeer receives inbound messages from a peer.
func (r *Router) receivePeer(peer *sPeer, conn Connection) error {
	for {
		chID, bz, err := conn.ReceiveMessage()
		if err == io.EOF {
			r.logger.Info("disconnected from peer", "peer", peer.ID)
			return nil
		} else if err != nil {
			return err
		}

		// FIXME: Avoid mutex here.
		r.mtx.RLock()
		ch, ok := r.channels[ChannelID(chID)]
		r.mtx.RUnlock()
		if !ok {
			r.logger.Error("dropping message for unknown channel", "peer", peer.ID, "channel", ch)
			continue
		}

		select {
		case ch <- rawEnvelope{channel: ChannelID(chID), peer: peer.ID, message: bz}:
		case <-r.chClose:
			return nil
		}
	}
}

// sendChannel sends inbound messages to a channel until either in or the
// destination channel is closed.
func (r *Router) sendChannel(channel *Channel, in <-chan rawEnvelope) {
	emptyMessage := proto.Clone(channel.messageType)
	emptyMessage.Reset()
	for {
		select {
		case raw, ok := <-in:
			if !ok {
				return
			}
			msg := proto.Clone(emptyMessage)
			if err := proto.Unmarshal(raw.message, msg); err != nil {
				r.logger.Error("message decoding failed", "peer", raw.peer, "err", err)
				continue
			}

			select {
			case channel.outCh <- Envelope{From: raw.peer, Message: msg}:
				r.logger.Info("received message", "peer", raw.peer, "channel", channel.ID, "message", msg)
			case <-channel.doneCh:
				return
			case <-r.chClose:
				return
			}
		case <-channel.doneCh:
			return
		case <-r.chClose:
			return
		}
	}
}

// receiveChannel receives outbound messages from a channel and sends them to the
// appropriate peer.
func (r *Router) receiveChannel(channel *Channel) {
	for {
		select {
		case envelope, ok := <-channel.outCh:
			if !ok {
				return
			}
			r.mtx.Lock()
			ch, ok := r.peers[envelope.To.String()]
			r.mtx.Unlock()
			if !ok {
				r.logger.Error("dropping message for disconnected peer", "peer", envelope.To.String())
				continue
			}

			bz, err := proto.Marshal(envelope.Message)
			if err != nil {
				r.logger.Error("failed to marshal message", "peer", envelope.To.String(), "err", err)
				continue
			}
			select {
			case ch <- rawEnvelope{peer: envelope.To, message: bz}:
			case <-channel.doneCh:
				return
			case <-r.chClose:
				return
			}
		case <-channel.doneCh:
			return
		case <-r.chClose:
			return
		}
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
	r.wgPeers.Wait()
}
