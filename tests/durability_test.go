package tests

import (
	"testing"

	"tests/helpers"
)

const (
	durabilityAddr = "127.0.0.1:6001"
	// proxyName fronts the nats server on 19224, which the durability config
	// dials. Both addresses are inside the compose network; 19224 is published.
	proxyName     = "redial"
	proxyListen   = "0.0.0.0:19224"
	proxyUpstream = "nats:4222"
)

// TestRedialAfterOutage cuts the connection to the nats server underneath a
// running pipeline and checks the driver recovers once it comes back. The old
// test made the same calls behind 30 seconds of sleeps and asserted nothing.
func TestRedialAfterOutage(t *testing.T) {
	t.Cleanup(func() { _ = helpers.CleanupNats("foo", "foo2") })

	helpers.CreateProxy(t, proxyName, proxyListen, proxyUpstream)

	rr, _ := helpers.Start(t, "configs/.rr-nats-durability-redial.yaml", jobsPlugins(),
		helpers.WithObservedLogger(),
		helpers.WithTCPProbe(durabilityAddr),
	)

	rr.RequireLogCount(t, "pipeline was started", 2)

	helpers.PushToPipe("test-1", false, durabilityAddr)(t)
	helpers.PushToPipe("test-2", false, durabilityAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.SetProxyEnabled(t, proxyName, false)
	helpers.SetProxyEnabled(t, proxyName, true)

	// the driver has to reconnect before these land
	helpers.PushEventually(t, durabilityAddr, "test-1")
	helpers.PushEventually(t, durabilityAddr, "test-2")

	rr.WaitLog(t, "job was processed successfully", 4)

	helpers.DestroyPipelines(durabilityAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "pipeline was stopped", 2)
}
