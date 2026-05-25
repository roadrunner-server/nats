package helpers

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	"github.com/roadrunner-server/api-go/v6/jobs/v2/jobsV2connect"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newHTTPClient(t *testing.T) *http.Client {
	t.Helper()
	httpc := &http.Client{Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, network, addr)
		},
	}}
	t.Cleanup(httpc.CloseIdleConnections)
	return httpc
}

func NewJobsClient(t *testing.T, address string) jobsV2connect.JobsServiceClient {
	t.Helper()
	return jobsV2connect.NewJobsServiceClient(newHTTPClient(t), "http://"+address)
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		_, err := client.Resume(t.Context(), connect.NewRequest(&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}))
		require.NoError(t, err)
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		_, err := client.Push(t.Context(), connect.NewRequest(&jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck)}))
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
		_, err := client.Pause(t.Context(), connect.NewRequest(&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}))
		assert.NoError(t, err)
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		req := &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}

		var lastErr error
		for range 10 {
			_, err := client.Destroy(t.Context(), connect.NewRequest(req))
			if err == nil {
				return
			}
			lastErr = err
			time.Sleep(time.Second)
		}
		assert.NoError(t, lastErr)
	}
}

func Stats(address string, state *jobState.State) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)

		resp, err := client.GetStats(t.Context(), connect.NewRequest(&emptypb.Empty{}))
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotEmpty(t, resp.Msg.GetStats())

		st := resp.Msg.GetStats()[0]
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
