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

// MemoryNetwork is an in-memory network that uses Go channels to communicate
// between endpoints. Transport endpoints are created with CreateTransport.
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

// CreateTransport creates a new memory transport for the given NodeInfo and
// private key. Use GenerateTransport() to autogenerate a random key and node
// info.
//
// The transport immediately begins listening on the endpoint "memory:<id>", and
// can be accessed by other transports in the same memory network.
func (n *MemoryNetwork) CreateTransport(
	nodeInfo NodeInfo,
	privKey crypto.PrivKey,
) (*MemoryTransport, error) {
	nodeID := nodeInfo.DefaultNodeID
	if nodeID == "" {
		return nil, errors.New("no node ID")
	}
	n.mtx.Lock()
	defer n.mtx.Unlock()
	if _, ok := n.transports[nodeID]; ok {
		return nil, fmt.Errorf("transport with ID %q already exists", nodeID)
	}
	t := NewMemoryTransport(n, nodeInfo, privKey)
	n.transports[nodeID] = t
	return t, nil
}

// GenerateTransport generates a new transport.
func (n *MemoryNetwork) GenerateTransport() *MemoryTransport {
	privKey := ed25519.GenPrivKey()
	nodeID := PubKeyToID(privKey.PubKey())
	nodeInfo := NodeInfo{
		DefaultNodeID: nodeID,
		ListenAddr:    fmt.Sprintf("%v:%v", MemoryProtocol, nodeID),
	}
	t, err := n.CreateTransport(nodeInfo, privKey)
	if err != nil {
		// GenerateTransport will only be used for testing, and the likelihood
		// of generating a duplicate node ID is very low, so we'll panic.
		panic(err)
	}
	return t
}

// GetTransport looks up a transport in the network, returning nil if not found.
func (n *MemoryNetwork) GetTransport(id ID) *MemoryTransport {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	return n.transports[id]
}

// RemoveTransport removes a transport from the network and closes it.
func (n *MemoryNetwork) RemoveTransport(id ID) error {
	n.mtx.Lock()
	t, ok := n.transports[id]
	delete(n.transports, id)
	n.mtx.Unlock()

	if ok {
		// Close may recursively call RemoveTransport() again, but this is safe
		// because we've already removed the transport from the map above.
		return t.Close()
	}
	return nil
}

// MemoryTransport is an in-memory transport that's primarily meant for testing.
// It communicates between endpoints using Go channels.
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
		logger: network.logger.With("local",
			fmt.Sprintf("%v:%v", MemoryProtocol, nodeInfo.DefaultNodeID)),

		chAccept: make(chan *MemoryConnection),
		chClose:  make(chan struct{}),
	}
}

// Accept implements Transport.
func (t *MemoryTransport) Accept(ctx context.Context) (Connection, error) {
	select {
	case conn := <-t.chAccept:
		t.logger.Info("accepted connection from peer", "remote", conn.RemoteEndpoint())
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

	t.logger.Info("dialing peer", "remote", endpoint)
	peerTransport := t.network.GetTransport(ID(endpoint.Path))
	if peerTransport == nil {
		return nil, fmt.Errorf("unknown peer %q", endpoint.Path)
	}
	chIn := make(chan memoryMessage, 1)
	chOut := make(chan memoryMessage, 1)
	chClose := make(chan struct{})
	closeOnce := sync.Once{}
	closer := func() bool {
		closed := false
		closeOnce.Do(func() {
			close(chClose)
			closed = true
		})
		return closed
	}
	connOut := NewMemoryConnection(t, peerTransport, chIn, chOut, chClose, closer)
	connIn := NewMemoryConnection(peerTransport, t, chOut, chIn, chClose, closer)

	select {
	case peerTransport.chAccept <- connIn:
		return connOut, nil
	case <-peerTransport.chClose:
		return nil, ErrTransportClosed{}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// DialAccept is a convenience function that dials a peer and returns both ends
// of the connection (A to B and B to A).
func (t *MemoryTransport) DialAccept(
	ctx context.Context,
	peer *MemoryTransport,
) (Connection, Connection, error) {
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

// Close implements Transport.
func (t *MemoryTransport) Close() error {
	err := t.network.RemoveTransport(t.nodeInfo.DefaultNodeID)
	t.closeOnce.Do(func() {
		close(t.chClose)
	})
	t.logger.Info("stopped accepting connections")
	return err
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

// SetChannelDescriptors implements Transport.
func (t *MemoryTransport) SetChannelDescriptors(chDescs []*conn.ChannelDescriptor) {
}

// MemoryConnection is an in-memory connection between two transports (nodes).
type MemoryConnection struct {
	logger    log.Logger
	local     *MemoryTransport
	remote    *MemoryTransport
	chReceive <-chan memoryMessage
	chSend    chan<- memoryMessage
	chClose   <-chan struct{}
	close     func() bool
}

type memoryMessage struct {
	channel byte
	message []byte
}

// NewMemoryConnection creates a new MemoryConnection.
func NewMemoryConnection(
	local *MemoryTransport,
	remote *MemoryTransport,
	chReceive <-chan memoryMessage,
	chSend chan<- memoryMessage,
	chClose <-chan struct{},
	close func() bool,
) *MemoryConnection {
	c := &MemoryConnection{
		local:     local,
		remote:    remote,
		chReceive: chReceive,
		chSend:    chSend,
		chClose:   chClose,
		close:     close,
	}
	c.logger = c.local.logger.With("remote", c.RemoteEndpoint())
	return c
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
		c.logger.Debug("received message", "channel", msg.channel, "message", msg.message)
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
		c.logger.Debug("sent message", "channel", chID, "message", msg)
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
		c.logger.Debug("sent message", "channel", chID, "message", msg)
		return true, nil
	case <-c.chClose:
		return false, io.EOF
	default:
		return false, nil
	}
}

// Close closes the connection.
func (c *MemoryConnection) Close() error {
	if c.close() {
		c.logger.Info("closed connection")
	}
	return nil
}

// FlushClose flushes all pending sends and then closes the connection.
func (c *MemoryConnection) FlushClose() error {
	return c.Close()
}

// LocalEndpoint returns the local endpoint for the connection.
func (c *MemoryConnection) LocalEndpoint() Endpoint {
	return Endpoint{
		PeerID:   c.local.nodeInfo.DefaultNodeID,
		Protocol: MemoryProtocol,
		Path:     string(c.local.nodeInfo.DefaultNodeID),
	}
}

// RemoteEndpoint returns the remote endpoint for the connection.
func (c *MemoryConnection) RemoteEndpoint() Endpoint {
	return Endpoint{
		PeerID:   c.remote.nodeInfo.DefaultNodeID,
		Protocol: MemoryProtocol,
		Path:     string(c.remote.nodeInfo.DefaultNodeID),
	}
}

// PubKey returns the remote peer's public key.
func (c *MemoryConnection) PubKey() crypto.PubKey {
	return c.remote.privKey.PubKey()
}

// NodeInfo returns the remote peer's node info.
func (c *MemoryConnection) NodeInfo() NodeInfo {
	return c.remote.nodeInfo
}

// Status returns the current connection status.
func (c *MemoryConnection) Status() conn.ConnectionStatus {
	return conn.ConnectionStatus{}
}
