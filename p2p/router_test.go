package p2p_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
)

func TestRouter(t *testing.T) {
	logger := log.TestingLogger()
	network := p2p.NewMemoryNetwork(logger)
	transport := network.GenerateTransport()
	a := network.GenerateTransport()
	b := network.GenerateTransport()
	c := network.GenerateTransport()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runPeer := func(transport p2p.Transport) {
		for {
			conn, err := transport.Accept(ctx)
			if err == context.Canceled {
				return
			}
			require.NoError(t, err)
			sent, err := conn.SendMessage(1, []byte("hi!"))
			require.True(t, sent)
			require.NoError(t, err)
			conn.Close()
		}
	}
	go runPeer(a)
	go runPeer(b)
	go runPeer(c)

	router := p2p.NewRouter(logger, map[p2p.Protocol]p2p.Transport{
		p2p.MemoryProtocol: transport,
	}, []p2p.PeerAddress{
		a.Endpoints()[0].PeerAddress(),
		b.Endpoints()[0].PeerAddress(),
		c.Endpoints()[0].PeerAddress(),
	})
	err := router.Start()
	require.NoError(t, err)
	defer func() { require.NoError(t, router.Stop()) }()

	time.Sleep(3 * time.Second)
}
