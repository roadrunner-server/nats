package helpers

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	// NatsAddr is the url the compose file publishes the nats server on.
	NatsAddr = "nats://127.0.0.1:4222"
	// toxiproxyAddr is the toxiproxy api used by the durability test.
	toxiproxyAddr = "127.0.0.1:8474"
	// redialTimeout bounds PushEventually, which retries across an outage.
	redialTimeout = time.Second * 60
	redialTick    = time.Second
)

func NewJobsClient(t *testing.T, address string) *rpc.Client {
	t.Helper()

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Resume",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.JobsHandlerResponse{}))
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Pause",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.JobsHandlerResponse{}))
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Destroy",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.Pipelines{}))
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Push",
			&jobsProto.PushRequest{Job: dummyJob(pipeline, autoAck)},
			&jobsProto.JobsHandlerResponse{}))
	}
}

// PushEventually keeps retrying a push until it lands. Used after a broker
// outage, where the driver needs a few attempts to reconnect.
func PushEventually(t *testing.T, address string, pipeline string) {
	t.Helper()

	require.Eventually(t, func() bool {
		client := NewJobsClient(t, address)

		return client.Call("jobs.Push",
			&jobsProto.PushRequest{Job: dummyJob(pipeline, false)},
			&jobsProto.JobsHandlerResponse{}) == nil
	}, redialTimeout, redialTick, "the driver never recovered after the outage")
}

func dummyJob(pipeline string, autoAck bool) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
		},
	}
}

// DeclarePipe declares a pipeline over rpc and requires the call to succeed.
// Each call gets its own stream, so a test never inherits messages from another.
func DeclarePipe(address string, pipeline string, subject string, stream string) func(t *testing.T) {
	return func(t *testing.T) {
		require.NoError(t, Declare(t, address, map[string]string{
			"driver":      "nats",
			"name":        pipeline,
			"subject":     subject,
			"stream":      stream,
			"deliver_new": "true",
			"prefetch":    "100",
			"priority":    "3",
		}))

		t.Cleanup(func() { _ = CleanupNats(stream) })
	}
}

// Declare issues a raw declare call and returns its error, so negative tests
// can assert on a rejected pipeline configuration.
func Declare(t *testing.T, address string, pipeline map[string]string) error {
	t.Helper()

	client := NewJobsClient(t, address)

	return client.Call("jobs.Declare",
		&jobsProto.DeclareRequest{Pipeline: pipeline},
		&jobsProto.JobsHandlerResponse{})
}

// StatsFor returns the state the jobs plugin reports for one pipeline. Picking
// it by name keeps the assertion stable when several are registered.
func StatsFor(t *testing.T, address string, pipeline string) *jobState.State {
	t.Helper()

	resp := &jobsProto.Stats{}
	require.NoError(t, NewJobsClient(t, address).Call("jobs.GetStats", &emptypb.Empty{}, resp))

	for _, st := range resp.GetStats() {
		if st.GetPipeline() != pipeline {
			continue
		}

		return &jobState.State{
			Queue:    st.GetQueue(),
			Pipeline: st.GetPipeline(),
			Driver:   st.GetDriver(),
			Active:   st.GetActive(),
			Delayed:  st.GetDelayed(),
			Reserved: st.GetReserved(),
			Ready:    st.GetReady(),
			Priority: st.GetPriority(),
		}
	}

	require.FailNowf(t, "pipeline not reported", "no stats for %q", pipeline)

	return nil
}

// connect opens a direct connection to the nats server, bypassing RoadRunner.
func connect(t *testing.T) jetstream.JetStream {
	t.Helper()

	conn, err := nats.Connect(NatsAddr, nats.NoEcho(), nats.Timeout(time.Minute))
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	js, err := jetstream.New(conn)
	require.NoError(t, err)

	return js
}

// PublishRaw creates the stream if needed and publishes a message the driver
// did not produce, so a test can hand the listener an unparseable payload.
func PublishRaw(t *testing.T, stream string, subject string, data []byte, headers nats.Header) {
	t.Helper()

	js := connect(t)

	_, err := js.CreateOrUpdateStream(t.Context(), jetstream.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = CleanupNats(stream) })

	_, err = js.PublishMsg(t.Context(), &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  headers,
	})
	require.NoError(t, err)
}

// PublishTo puts a message on an existing stream's subject, without creating
// the stream: the pipeline owns it.
func PublishTo(t *testing.T, subject string, data []byte, headers nats.Header) {
	t.Helper()

	_, err := connect(t).PublishMsg(t.Context(), &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  headers,
	})
	require.NoError(t, err)
}

// CleanupNats drops the streams a test created. Called from t.Cleanup, where the
// test context is already canceled.
func CleanupNats(streams ...string) error {
	conn, err := nats.Connect(NatsAddr, nats.NoEcho(), nats.Timeout(time.Minute))
	if err != nil {
		return err
	}
	defer conn.Close()

	js, err := jetstream.New(conn)
	if err != nil {
		return err
	}

	for _, s := range streams {
		if err := js.DeleteStream(context.Background(), s); err != nil {
			return err
		}
	}

	return nil
}

// CreateProxy fronts the nats server with a toxiproxy the durability test can
// cut. Both addresses are resolved inside the compose network, not on the host.
func CreateProxy(t *testing.T, name string, listen string, upstream string) {
	t.Helper()

	// a proxy left behind by an interrupted run would make the create conflict
	deleteProxy(t, name)

	body := fmt.Sprintf(`{"name":%q,"listen":%q,"upstream":%q,"enabled":true}`, name, listen, upstream)
	post(t, "http://"+toxiproxyAddr+"/proxies", []byte(body), http.StatusCreated)
	t.Cleanup(func() { deleteProxy(t, name) })
}

// SetProxyEnabled cuts or restores the connection to the nats server.
func SetProxyEnabled(t *testing.T, name string, enabled bool) {
	t.Helper()

	post(t, "http://"+toxiproxyAddr+"/proxies/"+name, fmt.Appendf(nil, `{"enabled":%t}`, enabled), http.StatusOK)
}

func deleteProxy(t *testing.T, name string) {
	t.Helper()

	// runs from t.Cleanup, where the test context is already canceled
	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, "http://"+toxiproxyAddr+"/proxies/"+name, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Contains(t, []int{http.StatusNoContent, http.StatusNotFound}, resp.StatusCode)
}

func post(t *testing.T, addr string, body []byte, wantStatus int) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, addr, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, wantStatus, resp.StatusCode, "POST %s", addr)
}
