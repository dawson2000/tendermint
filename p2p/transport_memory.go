package p2p

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/tendermint/tendermint/crypto"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p/conn"
)

const (
	MemoryProtocol Protocol = "memory"
)

// MemoryNetwork creates an in-memory network, and is used to allocate
// transports with specific IDs and connect them.
type MemoryNetwork struct {
	logger log.Logger

	mtx        sync.RWMutex
	transports map[ID]*MemoryTransport
}

// NewMemoryNetwork creates a new in-memory network.
func NewMemoryNetwork(logger log.Logger) *MemoryNetwork {
	return &MemoryNetwork{
		logger:     logger,
		transports: map[ID]*MemoryTransport{},
	}
}

// CreateTransport creates a new transport (i.e. node) for the given NodeInfo
// and private key. Use GenerateTransport() to autogenerate key and info.
//
// The transport immediately begins listening on the address "memory:<id>", and
// can be accessed from other transports in the same memory network.
func (n *MemoryNetwork) CreateTransport(nodeInfo NodeInfo, privKey crypto.PrivKey) (*MemoryTransport, error) {
	nodeID := nodeInfo.DefaultNodeID
	if nodeID == "" {
		return nil, errors.New("no node ID")
	}
	n.mtx.Lock()
	defer n.mtx.Unlock()
	if _, ok := n.transports[nodeID]; ok {
		return nil, fmt.Errorf("peer with ID %q already exists", nodeID)
	}
	t := NewMemoryTransport(n, nodeInfo, privKey)
	n.transports[nodeID] = t
	return t, nil
}

// GenerateTransport generates a new transport.
func (n *MemoryNetwork) GenerateTransport() (*MemoryTransport, error) {
	privKey := ed25519.GenPrivKey()
	peerID := PubKeyToID(privKey.PubKey())
	nodeInfo := NodeInfo{
		DefaultNodeID: peerID,
		ListenAddr:    fmt.Sprintf("%v:%v", MemoryProtocol, peerID),
	}
	return n.CreateTransport(nodeInfo, privKey)
}

// GetTransport looks up a transport in the network, returning nil
// if not found.
func (n *MemoryNetwork) GetTransport(id ID) *MemoryTransport {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	return n.transports[id]
}

// RemoveTransport removes a transport from the network, closing it if not
// already closed.
func (n *MemoryNetwork) RemoveTransport(id ID) error {
	n.mtx.Lock()
	t, ok := n.transports[id]
	delete(n.transports, id)
	n.mtx.Unlock()

	var err error
	if ok {
		// It is safe to call Close here, even though that may recursively call
		// RemoveTransport() again, because we've already removed the transport from
		// the map above.
		err = t.Close()
	}
	return err
}

// MemoryTransport is an in-memory transport that's primarily meant for testing.
type MemoryTransport struct {
	network  *MemoryNetwork
	nodeInfo NodeInfo
	privKey  crypto.PrivKey
	logger   log.Logger

	closeOnce sync.Once
	chAccept  chan *MemoryConnection
	chClose   chan struct{}
}

// NewMemoryTransport creates a new in-memory transport.
func NewMemoryTransport(
	network *MemoryNetwork,
	nodeInfo NodeInfo,
	privKey crypto.PrivKey,
) *MemoryTransport {
	return &MemoryTransport{
		network:  network,
		nodeInfo: nodeInfo,
		privKey:  privKey,
		logger:   network.logger,

		chAccept: make(chan *MemoryConnection),
		chClose:  make(chan struct{}),
	}
}

// Accept implements Transport.
func (t *MemoryTransport) Accept(ctx context.Context) (Connection, error) {
	select {
	case conn := <-t.chAccept:
		return conn, nil
	case <-t.chClose:
		return nil, ErrTransportClosed{}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Dial implements Transport.
func (t *MemoryTransport) Dial(ctx context.Context, endpoint Endpoint) (Connection, error) {
	if endpoint.Protocol != MemoryProtocol {
		return nil, fmt.Errorf("invalid protocol %q", endpoint.Protocol)
	}
	if endpoint.Path == "" {
		return nil, errors.New("no path")
	}
	if endpoint.PeerID == "" {
		return nil, errors.New("no peer ID")
	}
	t.logger.Info("dialing peer", "endpoint", endpoint)

	peerTransport := t.network.GetTransport(ID(endpoint.Path))
	if peerTransport == nil {
		return nil, fmt.Errorf("unknown peer %q", endpoint.Path)
	}
	chIn := make(chan memoryMessage, 1)
	chOut := make(chan memoryMessage, 1)
	chClose := make(chan struct{})
	closeOnce := sync.Once{}
	closer := func() {
		closeOnce.Do(func() { close(chClose) })
	}
	connOut := newMemoryConnection(t, peerTransport, chIn, chOut, chClose, closer)
	connIn := newMemoryConnection(peerTransport, t, chOut, chIn, chClose, closer)

	select {
	case peerTransport.chAccept <- connIn:
		return connOut, nil
	case <-peerTransport.chClose:
		return nil, ErrTransportClosed{}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// DialAccept is a convenience function that dials B from A, and returns both
// ends of the connection (A to B, and B to A).
func (t *MemoryTransport) DialAccept(ctx context.Context, peer *MemoryTransport) (Connection, Connection, error) {
	chAccept := make(chan Connection, 1)
	chErr := make(chan error, 1)
	go func() {
		conn, err := peer.Accept(ctx)
		chErr <- err
		chAccept <- conn
	}()

	endpoints := peer.Endpoints()
	if len(endpoints) == 0 {
		return nil, nil, fmt.Errorf("peer %q not listening on any endpoints", peer.nodeInfo.DefaultNodeID)
	}
	connOut, err := t.Dial(ctx, endpoints[0])
	if err != nil {
		return nil, nil, err
	}
	if err = <-chErr; err != nil {
		return nil, nil, err
	}
	connIn := <-chAccept

	return connOut, connIn, nil
}

// Endpoints implements Transport.
func (t *MemoryTransport) Endpoints() []Endpoint {
	select {
	case <-t.chClose:
		return []Endpoint{}
	default:
		return []Endpoint{{
			PeerID:   t.nodeInfo.DefaultNodeID,
			Protocol: MemoryProtocol,
			Path:     string(t.nodeInfo.DefaultNodeID),
		}}
	}
}

// Close implements Transport.
func (t *MemoryTransport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		close(t.chClose)
		err = t.network.RemoveTransport(t.nodeInfo.DefaultNodeID)
	})
	return err
}

// SetChannelDescriptors implements Transport.
func (t *MemoryTransport) SetChannelDescriptors(chDescs []*conn.ChannelDescriptor) {
}

// MemoryConnection is an in-memory connection between two transports (nodes).
type MemoryConnection struct {
	transport *MemoryTransport
	peer      *MemoryTransport
	chReceive <-chan memoryMessage
	chSend    chan<- memoryMessage
	chClose   <-chan struct{}
	close     func()
}

type memoryMessage struct {
	channel byte
	message []byte
}

func newMemoryConnection(
	transport *MemoryTransport,
	peer *MemoryTransport,
	chReceive <-chan memoryMessage,
	chSend chan<- memoryMessage,
	chClose <-chan struct{},
	close func(),
) *MemoryConnection {
	return &MemoryConnection{
		transport: transport,
		peer:      peer,
		chReceive: chReceive,
		chSend:    chSend,
		chClose:   chClose,
		close:     close,
	}
}

// ReceiveMessage implements Connection.
func (c *MemoryConnection) ReceiveMessage() (chID byte, msg []byte, err error) {
	select {
	case <-c.chClose: // check close first, since channels are buffered
		return 0, nil, io.EOF
	default:
	}
	select {
	case msg := <-c.chReceive:
		return msg.channel, msg.message, nil
	case <-c.chClose:
		return 0, nil, io.EOF
	}
}

// SendMessage implements Connection.
func (c *MemoryConnection) SendMessage(chID byte, msg []byte) (bool, error) {
	select {
	case <-c.chClose: // check close first, since channels are buffered
		return false, io.EOF
	default:
	}
	select {
	case c.chSend <- memoryMessage{channel: chID, message: msg}:
		return true, nil
	case <-c.chClose:
		return false, io.EOF
	}
}

// TrySendMessage implements Connection.
func (c *MemoryConnection) TrySendMessage(chID byte, msg []byte) (bool, error) {
	select {
	case <-c.chClose: // check close first, since channels are buffered
		return false, io.EOF
	default:
	}
	select {
	case c.chSend <- memoryMessage{channel: chID, message: msg}:
		return true, nil
	case <-c.chClose:
		return false, io.EOF
	default:
		return false, io.EOF
	}
}

// LocalEndpoint returns the local endpoint for the connection.
func (c *MemoryConnection) LocalEndpoint() Endpoint {
	return Endpoint{
		PeerID:   c.transport.nodeInfo.DefaultNodeID,
		Protocol: MemoryProtocol,
		Path:     string(c.transport.nodeInfo.DefaultNodeID),
	}
}

// RemoteEndpoint returns the remote endpoint for the connection.
func (c *MemoryConnection) RemoteEndpoint() Endpoint {
	return Endpoint{
		PeerID:   c.peer.nodeInfo.DefaultNodeID,
		Protocol: MemoryProtocol,
		Path:     string(c.peer.nodeInfo.DefaultNodeID),
	}
}

// PubKey returns the remote peer's public key.
func (c *MemoryConnection) PubKey() crypto.PubKey {
	return c.peer.privKey.PubKey()
}

// NodeInfo returns the remote peer's node info.
func (c *MemoryConnection) NodeInfo() NodeInfo {
	return c.peer.nodeInfo
}

// Close closes the connection.
func (c *MemoryConnection) Close() error {
	c.close()
	return nil
}

// FlushClose flushes all pending sends and then closes the connection.
func (c *MemoryConnection) FlushClose() error {
	return c.Close()
}

// Status returns the current connection status.
func (c *MemoryConnection) Status() conn.ConnectionStatus {
	return conn.ConnectionStatus{}
}
