package tests

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"tests/helpers"

	"github.com/nats-io/nats.go"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	natsPlugin "github.com/roadrunner-server/nats/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const (
	initAddr    = "127.0.0.1:6001"
	headersAddr = "127.0.0.1:6464"
	pqAddr      = "127.0.0.1:6601"
	statsAddr   = "127.0.0.1:13001"
	subjectAddr = "127.0.0.1:6222"
	nackAddr    = "127.0.0.1:6223"
	otelAddr    = "127.0.0.1:6121"
	// declared is the pipeline the declare configs create over rpc.
	declared = "test-3"
)

func jobsPlugins() []any {
	return []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&natsPlugin.Plugin{},
	}
}

// boot starts the container with the observed logger and waits for the rpc
// listener, which is the readiness signal the fixed sleeps used to stand in for.
func boot(t *testing.T, cfgPath string, addr string, opts ...helpers.Option) (*helpers.RR, func()) {
	t.Helper()

	return helpers.Start(t, cfgPath, jobsPlugins(),
		append([]helpers.Option{
			helpers.WithObservedLogger(),
			helpers.WithTCPProbe(addr),
		}, opts...)...)
}

// bootInit starts the two pipeline init config and drops its streams afterwards.
func bootInit(t *testing.T) (*helpers.RR, func()) {
	t.Helper()

	t.Cleanup(func() { _ = helpers.CleanupNats("foo", "foo-2") })

	return boot(t, "configs/.rr-nats-init.yaml", initAddr)
}

// TestBoots covers the config-declared pipelines: both come up at startup and
// both come down on destroy.
func TestBoots(t *testing.T) {
	rr, _ := bootInit(t)

	rr.RequireLogCount(t, "pipeline was started", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "pipeline was stopped", 2)
}

// TestPushAndProcess follows two jobs from the rpc call to the worker ack. Both
// pipelines carry delete_after_ack, so the messages leave the stream as well.
func TestPushAndProcess(t *testing.T) {
	rr, _ := bootInit(t)

	helpers.PushToPipe("test-1", false, initAddr)(t)
	helpers.PushToPipe("test-2", false, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "job was pushed successfully", 2)
	rr.RequireLogCount(t, "job processing was started", 2)
	rr.RequireLogCount(t, "job was processed successfully", 2)
}

// TestAutoAck checks the listener acknowledges the message itself, before the
// worker ever sees it, when the job carries the auto ack option.
func TestAutoAck(t *testing.T) {
	rr, _ := bootInit(t)

	helpers.PushToPipe("test-1", true, initAddr)(t)
	helpers.PushToPipe("test-2", true, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "auto_ack option enabled", 2)
	rr.RequireLogCount(t, "job was processed successfully", 2)
}

// TestProcessWithoutDeleteAfterAck covers the pipelines that leave the message
// in the stream once it is acknowledged. The old test made the same calls and
// asserted nothing.
func TestProcessWithoutDeleteAfterAck(t *testing.T) {
	t.Cleanup(func() { _ = helpers.CleanupNats("foo-3", "foo-4") })

	rr, _ := boot(t, "configs/.rr-nats-init-v27.yaml", initAddr)

	helpers.PushToPipe("test-1", false, initAddr)(t)
	helpers.PushToPipe("test-2", false, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "job was pushed successfully", 2)
	rr.RequireLogCount(t, "job was processed successfully", 2)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
}

// TestHeadersReachTheWorker pushes a job carrying a header the worker asserts
// on: it throws when the value does not survive the round trip.
func TestHeadersReachTheWorker(t *testing.T) {
	t.Cleanup(func() { _ = helpers.CleanupNats("headers-test") })

	rr, _ := boot(t, "configs/.rr-nats-headers.yaml", headersAddr)

	helpers.PushToPipe("test-1", false, headersAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(headersAddr, "test-1")(t)

	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
	require.Zero(t, rr.CountLog("jobs protocol error"))
}

// TestPriorityQueueBacklog pushes far more jobs than the slow workers can take,
// so most of them sit in the priority queue until the pipelines are destroyed
// under them.
func TestPriorityQueueBacklog(t *testing.T) {
	const rounds = 100

	t.Cleanup(func() { _ = helpers.CleanupNats("foo-pq", "foo-2-pq") })

	rr, _ := boot(t, "configs/.rr-nats-pq.yaml", pqAddr)

	for range rounds {
		helpers.PushToPipe("test-1-pq", false, pqAddr)(t)
		helpers.PushToPipe("test-2-pq", false, pqAddr)(t)
	}

	rr.RequireLogCount(t, "job was pushed successfully", 2*rounds)

	// both workers have to be busy before the destroy, otherwise the backlog
	// would never form
	rr.WaitLog(t, "job processing was started", 2)

	helpers.DestroyPipelines(pqAddr, "test-1-pq", "test-2-pq")(t)

	rr.RequireLogCount(t, "pipeline was started", 2)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
	rr.RequireLogCount(t, "nats disconnected", 2)
}

// TestDeclareAndConsume declares a pipeline over rpc, runs a job through it and
// pauses it again. The old test made the same calls and asserted nothing.
func TestDeclareAndConsume(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nats-declare.yaml", initAddr)

	helpers.DeclarePipe(initAddr, declared, "default-10.*", "stream-10")(t)
	helpers.ResumePipes(initAddr, declared)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PushToPipe(declared, false, initAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(initAddr, declared)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.DestroyPipelines(initAddr, declared)(t)

	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestPauseStopsConsuming checks a paused pipeline still accepts pushes but
// leaves them in the stream until it is resumed.
func TestPauseStopsConsuming(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nats-declare.yaml", initAddr)

	helpers.DeclarePipe(initAddr, declared, "default-12.*", "stream-12")(t)
	helpers.ResumePipes(initAddr, declared)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PausePipelines(initAddr, declared)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.PushToPipe(declared, false, initAddr)(t)
	rr.WaitLog(t, "job was pushed successfully", 1)
	rr.NeverLog(t, "job was processed successfully")

	helpers.ResumePipes(initAddr, declared)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(initAddr, declared)(t)
}

// TestStatsReportReadiness covers the state report. NATS keeps the counters on
// the server, so the driver reports identity and whether it is consuming.
func TestStatsReportReadiness(t *testing.T) {
	boot(t, "configs/.rr-nats-stat.yaml", statsAddr)

	helpers.DeclarePipe(statsAddr, declared, "default-13.*", "stream-13")(t)

	paused := helpers.StatsFor(t, statsAddr, declared)
	require.Equal(t, "nats", paused.Driver)
	require.Equal(t, "default-13.*", paused.Queue)
	require.Equal(t, uint64(3), paused.Priority)
	require.False(t, paused.Ready)

	helpers.ResumePipes(statsAddr, declared)(t)

	ready := helpers.WaitStats(t, statsAddr, declared, func(s *jobState.State) bool {
		return s.Ready
	})

	require.Equal(t, "default-13.*", ready.Queue)
	require.Zero(t, ready.Active)
	require.Zero(t, ready.Delayed)
	require.Zero(t, ready.Reserved)

	helpers.PausePipelines(statsAddr, declared)(t)

	helpers.WaitStats(t, statsAddr, declared, func(s *jobState.State) bool {
		return !s.Ready
	})

	helpers.DestroyPipelines(statsAddr, declared)(t)
}

// TestRequeueRetriesUntilAck covers the worker that fails a job with a growing
// attempts header and only acknowledges it on the fourth delivery.
//
// The header only survives because the worker asks for a requeue, which
// republishes the job. A native nack redelivers the original message, so the
// counter would never move and the job would loop forever.
func TestRequeueRetriesUntilAck(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nats-jobs-err.yaml", initAddr)

	helpers.DeclarePipe(initAddr, declared, "default-11.*", "stream-11")(t)
	helpers.ResumePipes(initAddr, declared)(t)
	helpers.PushToPipe(declared, false, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(initAddr, declared)(t)
	helpers.DestroyPipelines(initAddr, declared)(t)

	// one original delivery plus the three the worker asked to redeliver
	rr.RequireLogCount(t, "job processing was started", 4)
	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestNackDropsTheMessage covers a worker rejecting a job outright. The native
// nack terminates the message, so it is never redelivered.
func TestNackDropsTheMessage(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nats-nack.yaml", nackAddr)

	helpers.DeclarePipe(nackAddr, declared, "default-14.*", "stream-14")(t)
	helpers.ResumePipes(nackAddr, declared)(t)
	helpers.PushToPipe(declared, false, nackAddr)(t)

	rr.WaitLog(t, "jobs nack request", 1)

	// a terminated message does not come back
	rr.NeverLog(t, "job was processed successfully")
	rr.RequireLogCount(t, "job processing was started", 1)

	helpers.PausePipelines(nackAddr, declared)(t)
	helpers.DestroyPipelines(nackAddr, declared)(t)
}

// TestRawPayload covers a message published by something other than RoadRunner.
// The payload is not an item, so the driver has to wrap it rather than drop it.
func TestRawPayload(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nats-raw.yaml", initAddr)

	helpers.PublishRaw(t, "foo-raw", "default-raw.*", []byte("foo-barrrrrr-bazzzzz"), nil)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(initAddr, "test-raw")(t)

	rr.RequireLogCount(t, "raw payload", 1)
	rr.RequireLogCount(t, "pipeline was started", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
	rr.RequireLogCount(t, "job processing was started", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
}

// TestMessageSubjectAsHeader checks the subject a message arrived on reaches the
// worker as a header, which is the only way a wildcard pipeline can tell which
// subject it came from. The worker throws when the value is wrong.
func TestMessageSubjectAsHeader(t *testing.T) {
	const (
		stream  = "foo-nats-message-subject-as-header"
		subject = "default-nats-message-subject-as-header.current-subject"
	)

	t.Cleanup(func() { _ = helpers.CleanupNats(stream) })

	rr, _ := boot(t, "configs/.rr-nats-message-subject-as-header.yaml", subjectAddr)

	// the pipeline created the stream at boot, this only puts a message on it
	helpers.PublishTo(t, subject, []byte("foo-barrrrrr-bazzzzz"), nats.Header{})

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(subjectAddr, "test-nats-message-subject-as-header")(t)

	rr.RequireLogCount(t, "pipeline was started", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
	rr.RequireLogCount(t, "job processing was started", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
	require.Zero(t, rr.CountLog("jobs protocol error"))
}

// TestBadResponseIsReported covers a worker answering with a payload the jobs
// response handler cannot parse.
func TestBadResponseIsReported(t *testing.T) {
	t.Cleanup(func() { _ = helpers.CleanupNats("foo-15", "foo-6") })

	rr, _ := boot(t, "configs/.rr-nats-init-v27-br.yaml", initAddr)

	helpers.PushToPipe("test-1", false, initAddr)(t)
	helpers.PushToPipe("test-2", false, initAddr)(t)

	rr.WaitLog(t, "response handler error", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "response handler error", 2)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
}

// TestNoGlobalSection covers a config with pipelines but no nats section. The
// plugin disables itself and the container still serves.
func TestNoGlobalSection(t *testing.T) {
	boot(t, "configs/.rr-no-global.yaml", initAddr, helpers.WithLogLevel(slog.LevelError))
}

// TestOTELSpans checks the spans the driver emits around a push and a destroy.
func TestOTELSpans(t *testing.T) {
	t.Cleanup(func() { _ = helpers.CleanupNats("foo-otel") })

	tracer := newInMemoryTracer(t)

	rr, _ := boot(t, "configs/.rr-nats-otel.yaml", otelAddr, helpers.WithPlugin(tracer))

	helpers.PushToPipe("test-1", false, otelAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(otelAddr, "test-1")(t)

	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)

	names := make(map[string]struct{})
	for _, s := range tracer.exp.GetSpans() {
		names[s.Name] = struct{}{}
	}

	got := make([]string, 0, len(names))
	for name := range names {
		got = append(got, name)
	}
	slices.Sort(got)

	for _, want := range []string{
		"destroy_pipeline",
		"jobs_listener",
		"nats_listener",
		"nats_push",
		"push",
	} {
		require.Contains(t, got, want, "collected spans: %v", got)
	}
}

// inMemoryTracer stands in for the otel plugin, keeping the spans in process.
type inMemoryTracer struct {
	tp  *sdktrace.TracerProvider
	exp *tracetest.InMemoryExporter
}

func newInMemoryTracer(t *testing.T) *inMemoryTracer {
	t.Helper()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	return &inMemoryTracer{tp: tp, exp: exp}
}

func (*inMemoryTracer) Init() error                        { return nil }
func (*inMemoryTracer) Name() string                       { return "inMemoryTracer" }
func (m *inMemoryTracer) Tracer() *sdktrace.TracerProvider { return m.tp }
