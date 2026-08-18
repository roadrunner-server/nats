package natsjobs

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	c := &config{}
	c.InitDefaults()

	require.Equal(t, nats.DefaultURL, c.Addr)
	require.Equal(t, 30*time.Second, c.AckWait)
	require.Equal(t, uint64(1000), c.RateLimit)
	require.Equal(t, int64(10), c.Priority)
	require.Equal(t, "default-stream", c.StreamID)
	require.Equal(t, "default", c.Subject)
	require.Equal(t, 10, c.Prefetch)
}

func TestConfigKeepsExplicitValues(t *testing.T) {
	c := &config{
		Addr:               "nats://nats:4222",
		AckWait:            time.Second * 5,
		RateLimit:          77,
		Priority:           3,
		StreamID:           "stream-1",
		Subject:            "default-1.*",
		Prefetch:           64,
		DeleteAfterAck:     true,
		DeliverNew:         true,
		DeleteStreamOnStop: true,
	}
	c.InitDefaults()

	require.Equal(t, "nats://nats:4222", c.Addr)
	require.Equal(t, time.Second*5, c.AckWait)
	require.Equal(t, uint64(77), c.RateLimit)
	require.Equal(t, int64(3), c.Priority)
	require.Equal(t, "stream-1", c.StreamID)
	require.Equal(t, "default-1.*", c.Subject)
	require.Equal(t, 64, c.Prefetch)
	require.True(t, c.DeleteAfterAck)
	require.True(t, c.DeliverNew)
	require.True(t, c.DeleteStreamOnStop)
}
