package natsjobs

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/require"
)

// testPipeline is the jobs.Pipeline the jobs plugin hands to the driver.
type testPipeline struct {
	name     string
	priority int64
}

func (p *testPipeline) Name() string                      { return p.name }
func (*testPipeline) Driver() string                      { return pluginName }
func (p *testPipeline) Priority() int64                   { return p.priority }
func (*testPipeline) With(string, any)                    {}
func (*testPipeline) Has(string) bool                     { return false }
func (*testPipeline) String(_ string, d string) string    { return d }
func (*testPipeline) Int(_ string, d int) int             { return d }
func (*testPipeline) Bool(_ string, d bool) bool          { return d }
func (*testPipeline) Map(string, map[string]string) error { return nil }
func (*testPipeline) Get(string) any                      { return nil }

var _ jobs.Pipeline = (*testPipeline)(nil)

// recorder is a slog handler keeping the rendered messages.
type recorder struct {
	records []string
}

func (*recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.records = append(r.records, rec.Message)
	return nil
}

func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }

// newTestDriver builds a driver with everything unpack touches and nothing else,
// so the payload decoding can be covered without a nats server.
func newTestDriver() (*Driver, *recorder) {
	rec := &recorder{}
	d := &Driver{log: slog.New(rec), streamID: "stream-1"}

	var pipe jobs.Pipeline = &testPipeline{name: "test-1", priority: 11}
	d.pipeline.Store(&pipe)

	return d, rec
}

// TestUnpackRoundTrip covers a payload this driver produced.
func TestUnpackRoundTrip(t *testing.T) {
	d, _ := newTestDriver()

	data, err := json.Marshal(&Item{
		Job:     "some/php/namespace",
		Ident:   "job-id",
		Payload: []byte(`{"hello":"world"}`),
		Options: &Options{Priority: 3, Pipeline: "test-1"},
	})
	require.NoError(t, err)

	headers := map[string][]string{"test": {"test2"}}
	item := &Item{}
	d.unpack(data, headers, item)

	require.Equal(t, "job-id", item.ID())
	require.Equal(t, int64(3), item.Priority())
	require.Equal(t, "test-1", item.GroupID())
	require.Equal(t, headers, item.Headers())
}

// TestUnpackRawPayload covers a message published by something other than
// RoadRunner. The raw bytes become the payload instead of being dropped, and
// the pipeline lends the job its identity.
func TestUnpackRawPayload(t *testing.T) {
	d, rec := newTestDriver()

	data := []byte("foo-barrrrrr-bazzzzz")
	headers := map[string][]string{"x-nats-subject": {"default-raw.1"}}
	item := &Item{}
	d.unpack(data, headers, item)

	require.Equal(t, auto, item.Job)
	require.NotEmpty(t, item.ID())
	require.Equal(t, data, item.Body())
	require.Equal(t, int64(11), item.Priority())
	require.Equal(t, "test-1", item.GroupID())
	require.Equal(t, "stream-1", item.Options.Queue)
	require.Equal(t, headers, item.Headers())
	require.Contains(t, rec.records, "raw payload")
}

func TestItemContext(t *testing.T) {
	item := &Item{
		Job:     "some/php/namespace",
		Ident:   "job-id",
		headers: map[string][]string{"test": {"test2"}},
		Options: &Options{Pipeline: "test-1", Queue: "stream-1"},
	}

	data, err := item.Context()
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, "job-id", got["id"])
	require.Equal(t, "nats", got["driver"])
	require.Equal(t, "stream-1", got["queue"])
	require.Equal(t, "test-1", got["pipeline"])
}

// calls records which of the broker callbacks an item reply reached.
type calls struct {
	ack     int
	nak     int
	term    int
	delay   []time.Duration
	requeue int
	err     error
}

// newItem wires an item to the recorder instead of a real jetstream message.
func newItem(c *calls, autoAck bool) *Item {
	return &Item{
		headers: map[string][]string{},
		Options: &Options{
			AutoAck: autoAck,
			stopped: &atomic.Uint64{},
			ack:     func() error { c.ack++; return nil },
			nak:     func() error { c.nak++; return nil },
			term:    func() error { c.term++; return nil },
			nakWithDelay: func(d time.Duration) error {
				c.delay = append(c.delay, d)
				return nil
			},
			requeueFn: func(*Item) error { c.requeue++; return c.err },
		},
	}
}

// newStoppedItem returns an item whose pipeline has already been stopped.
func newStoppedItem() *Item {
	item := newItem(&calls{}, false)
	item.Options.stopped.Store(1)

	return item
}

// TestStoppedPipelineRejectsReply covers the guard that keeps a late worker
// reply from touching a consumer the driver has already torn down.
func TestStoppedPipelineRejectsReply(t *testing.T) {
	require.ErrorContains(t, newStoppedItem().Ack(), "the pipeline is probably stopped")
	require.ErrorContains(t, newStoppedItem().Nack(), "the pipeline is probably stopped")
	require.ErrorContains(t, newStoppedItem().NackWithOptions(true, 0), "the pipeline is probably stopped")
	require.ErrorContains(t, newStoppedItem().Requeue(nil, 0), "the pipeline is probably stopped")
}

func TestAckAcknowledgesOnce(t *testing.T) {
	c := &calls{}

	require.NoError(t, newItem(c, false).Ack())
	require.Equal(t, 1, c.ack)
}

// TestAutoAckItemSkipsBroker checks the worker reply is a no-op once the
// listener acknowledged the message.
func TestAutoAckItemSkipsBroker(t *testing.T) {
	c := &calls{}

	require.NoError(t, newItem(c, true).Ack())
	require.NoError(t, newItem(c, true).Nack())

	require.Zero(t, c.ack)
	require.Zero(t, c.nak)
}

func TestNackReleasesTheMessage(t *testing.T) {
	c := &calls{}

	require.NoError(t, newItem(c, false).Nack())
	require.Equal(t, 1, c.nak)
}

// TestNackWithOptions covers the two ways a worker can reject a job: ask for a
// redelivery after a delay, or drop it for good.
func TestNackWithOptions(t *testing.T) {
	t.Run("requeue", func(t *testing.T) {
		c := &calls{}

		require.NoError(t, newItem(c, false).NackWithOptions(true, 5))
		require.Equal(t, []time.Duration{5 * time.Second}, c.delay)
		require.Zero(t, c.term)
	})

	t.Run("drop", func(t *testing.T) {
		c := &calls{}

		require.NoError(t, newItem(c, false).NackWithOptions(false, 5))
		require.Equal(t, 1, c.term)
		require.Empty(t, c.delay)
	})
}

// TestRequeuePublishesThenAcks covers the non-native requeue: a fresh copy goes
// out first, and only then is the original acknowledged.
func TestRequeuePublishesThenAcks(t *testing.T) {
	c := &calls{}
	item := newItem(c, false)

	require.NoError(t, item.Requeue(map[string][]string{"attempts": {"1"}}, 0))
	require.Equal(t, 1, c.requeue)
	require.Equal(t, 1, c.ack)
	require.Equal(t, map[string][]string{"attempts": {"1"}}, item.Headers())
}

// TestRequeueNaksOnPublishFailure checks a failed republish leaves the original
// message for redelivery instead of acknowledging it away.
func TestRequeueNaksOnPublishFailure(t *testing.T) {
	boom := errors.New("boom")
	c := &calls{err: boom}

	err := newItem(c, false).Requeue(nil, 0)

	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, c.nak)
	require.Zero(t, c.ack)
}

// TestRequeueAutoAckSkipsNak covers the auto acknowledged message: it is already
// gone from the stream, so a failed republish has nothing to release.
func TestRequeueAutoAckSkipsNak(t *testing.T) {
	boom := errors.New("boom")
	c := &calls{err: boom}

	err := newItem(c, true).Requeue(nil, 0)

	require.ErrorIs(t, err, boom)
	require.Zero(t, c.nak)
	require.Zero(t, c.ack)
}
