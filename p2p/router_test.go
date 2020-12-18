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

	go func() { _, _ = a.Accept(context.Background()) }()
	go func() { _, _ = b.Accept(context.Background()) }()
	go func() { _, _ = c.Accept(context.Background()) }()

	router := p2p.NewRouter(logger, map[p2p.Protocol]p2p.Transport{
		p2p.MemoryProtocol: transport,
	}, []p2p.PeerAddress{
		a.Endpoints()[0].PeerAddress(),
		b.Endpoints()[0].PeerAddress(),
		c.Endpoints()[0].PeerAddress(),
	})
	err := router.Start()
	require.NoError(t, err)
	defer require.NoError(t, router.Stop())

	time.Sleep(3 * time.Second)
}
