package helpers

import (
	"context"
	"net"
	"net/http"
	"net/rpc"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
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
		er := &jobsProto.JobsHandlerResponse{}
		err := client.Call("jobs.Resume", &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, er)
		require.NoError(t, err)
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		er := &jobsProto.JobsHandlerResponse{}
		err := client.Call("jobs.Push", &jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck)}, er)
		require.NoError(t, err)
	}
}

func createDummyJob(pipeline string, autoAck bool) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
			Topic:    pipeline,
		},
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		er := &jobsProto.JobsHandlerResponse{}
		err := client.Call("jobs.Pause", &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, er)
		assert.NoError(t, err)
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		req := &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}

		// Retry the destroy 10× with 1s gaps; if all attempts fail, return
		// without asserting. Some negative tests intentionally destroy
		// non-existent pipelines and rely on this silent-after-retry pattern.
		for range 10 {
			out := &jobsProto.Pipelines{}
			err := client.Call("jobs.Destroy", req, out)
			if err == nil {
				return
			}
			time.Sleep(time.Second)
		}
	}
}

func Stats(address string, state *jobState.State) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)

		resp := &jobsProto.Stats{}
		err := client.Call("jobs.GetStats", &emptypb.Empty{}, resp)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotEmpty(t, resp.GetStats())

		st := resp.GetStats()[0]
		state.Queue = st.GetQueue()
		state.Pipeline = st.GetPipeline()
		state.Driver = st.GetDriver()
		state.Active = st.GetActive()
		state.Delayed = st.GetDelayed()
		state.Reserved = st.GetReserved()
		state.Ready = st.GetReady()
		state.Priority = st.GetPriority()
	}
}

func setProxy(name string, enabled bool, t *testing.T) {
	t.Helper()
	body := strings.NewReader(`{"enabled":` + boolStr(enabled) + `}`)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://127.0.0.1:8474/proxies/"+name, body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func EnableProxy(name string, t *testing.T) {
	setProxy(name, true, t)
}

func DisableProxy(name string, t *testing.T) {
	setProxy(name, false, t)
}

func DeleteProxy(name string, t *testing.T) {
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, "http://127.0.0.1:8474/proxies/"+name, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 204, resp.StatusCode)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func CleanupNats(address string, stream ...string) error {
	conn, err := nats.Connect(address,
		nats.NoEcho(),
		nats.Timeout(time.Minute),
		nats.MaxReconnects(-1),
		nats.PingInterval(time.Second*10),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return err
	}

	js, err := jetstream.New(conn)
	if err != nil {
		return err
	}

	for _, s := range stream {
		err = js.DeleteStream(context.Background(), s)
		if err != nil {
			return err
		}
	}

	return nil
}
