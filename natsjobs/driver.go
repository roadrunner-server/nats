package natsjobs

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/roadrunner-server/api/v4/plugins/v3/jobs"
	"github.com/roadrunner-server/errors"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	pluginName      string = "nats"
	reconnectBuffer int    = 20 * 1024 * 1024
	tracerName      string = "jobs"
)

var _ jobs.Driver = (*Driver)(nil)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Driver struct {
	// system
	log       *zap.Logger
	queue     jobs.Queue
	tracer    *sdktrace.TracerProvider
	prop      propagation.TextMapPropagator
	listeners uint32
	pipeline  atomic.Pointer[jobs.Pipeline]
	stopCh    chan struct{}
	stopped   uint64

	// nats
	consumer  *consumer
	conn      *nats.Conn
	stream    jetstream.Stream
	jetstream jetstream.JetStream
	msgCh     chan jetstream.Msg

	// config
	priority           int64
	subject            string
	streamID           string
	prefetch           int
	rateLimit          uint64
	deleteAfterAck     bool
	deliverNew         bool
	deleteStreamOnStop bool
}

type consumer struct {
	id      string
	jsc     jetstream.Consumer
	context jetstream.ConsumeContext
}

func FromConfig(tracer *sdktrace.TracerProvider, configKey string, log *zap.Logger, cfg Configurer, pipe jobs.Pipeline, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_nats_consumer")

	if !cfg.Has(configKey) {
		return nil, errors.E(op, errors.Errorf("no configuration by provided key: %s", configKey))
	}

	// if no global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global nats configuration, global configuration should contain NATS URL"))
	}

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	var conf *config
	err := cfg.UnmarshalKey(configKey, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	conf.InitDefaults()

	conn, err := nats.Connect(conf.Addr,
		nats.NoEcho(),
		nats.Timeout(time.Minute),
		nats.MaxReconnects(-1),
		nats.PingInterval(time.Second*10),
		nats.ReconnectWait(time.Second),
		nats.ReconnectBufSize(reconnectBuffer),
		nats.ReconnectHandler(reconnectHandler(log)),
		nats.DisconnectErrHandler(disconnectHandler(log)),
	)
	if err != nil {
		return nil, errors.E(op, err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		return nil, errors.E(op, err)
	}

	stream, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     conf.StreamId,
		Subjects: []string{conf.Subject},
	})
	if err != nil {
		return nil, err
	}

	cs := &Driver{
		tracer:  tracer,
		prop:    prop,
		log:     log,
		stopCh:  make(chan struct{}),
		stopped: 0,
		queue:   pq,

		conn:      conn,
		stream:    stream,
		jetstream: js,

		priority:           conf.Priority,
		subject:            conf.Subject,
		streamID:           conf.StreamId,
		deleteAfterAck:     conf.DeleteAfterAck,
		deleteStreamOnStop: conf.DeleteStreamOnStop,
		prefetch:           conf.Prefetch,
		deliverNew:         conf.DeliverNew,
		rateLimit:          conf.RateLimit,
		msgCh:              make(chan jetstream.Msg, conf.Prefetch),
	}

	cs.pipeline.Store(&pipe)

	return cs, nil
}

func FromPipeline(tracer *sdktrace.TracerProvider, pipe jobs.Pipeline, log *zap.Logger, cfg Configurer, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_nats_pipeline_consumer")

	// if no global section -- error
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global nats configuration, global configuration should contain NATS URL"))
	}

	var conf *config
	err := cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	conf.InitDefaults()

	conn, err := nats.Connect(conf.Addr,
		nats.NoEcho(),
		nats.Timeout(time.Minute),
		nats.MaxReconnects(-1),
		nats.PingInterval(time.Second*10),
		nats.ReconnectWait(time.Second),
		nats.ReconnectBufSize(reconnectBuffer),
		nats.ReconnectHandler(reconnectHandler(log)),
		nats.DisconnectErrHandler(disconnectHandler(log)),
	)
	if err != nil {
		return nil, errors.E(op, err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		return nil, errors.E(op, err)
	}

	defStream := pipe.String(pipeStream, "default-streamID")
	defSubject := pipe.String(pipeSubject, "default.*")

	stream, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     defStream,
		Subjects: []string{defSubject},
	})
	if err != nil {
		return nil, err
	}

	cs := &Driver{
		tracer:  tracer,
		prop:    prop,
		log:     log,
		queue:   pq,
		stopCh:  make(chan struct{}),
		stopped: 0,

		conn:      conn,
		stream:    stream,
		jetstream: js,

		priority:           pipe.Priority(),
		subject:            defSubject,
		streamID:           defStream,
		prefetch:           pipe.Int(pipePrefetch, 100),
		deleteAfterAck:     pipe.Bool(pipeDeleteAfterAck, false),
		deliverNew:         pipe.Bool(pipeDeliverNew, false),
		deleteStreamOnStop: pipe.Bool(pipeDeleteStreamOnStop, false),
		rateLimit:          uint64(pipe.Int(pipeRateLimit, 1000)),
		msgCh:              make(chan jetstream.Msg, pipe.Int(pipePrefetch, 100)),
	}

	cs.pipeline.Store(&pipe)

	return cs, nil
}

func (c *Driver) Push(ctx context.Context, job jobs.Message) error {
	const op = errors.Op("nats_consumer_push")

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nats_push")
	defer span.End()

	if job.Delay() > 0 {
		return errors.E(op, errors.Str("nats doesn't support delayed messages, see: https://github.com/nats-io/nats-streaming-server/issues/324"))
	}

	j := fromJob(job)
	c.prop.Inject(ctx, propagation.HeaderCarrier(j.headers))

	data, err := json.Marshal(j)
	if err != nil {
		return errors.E(op, err)
	}

	_, err = c.jetstream.PublishMsg(ctx, &nats.Msg{
		Data:    data,
		Subject: c.subject,
	})
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (c *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nats_run")
	defer span.End()

	const op = errors.Op("nats_run")

	pipe := *c.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	l := atomic.LoadUint32(&c.listeners)
	// listener already active
	if l == 1 {
		c.log.Warn("nats listener is already in the active state")
		return nil
	}

	atomic.AddUint32(&c.listeners, 1)
	err := c.listenerInit(c.streamID, c.rateLimit)
	if err != nil {
		return errors.E(op, err)
	}

	c.listenerStart()

	c.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (c *Driver) Pause(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nats_pause")
	defer span.End()

	pipe := *c.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&c.listeners)
	// no active listeners
	if l == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	// remove listener
	atomic.AddUint32(&c.listeners, ^uint32(0))

	c.consumer.context.Stop()
	c.stopCh <- struct{}{}

	c.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (c *Driver) Resume(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nats_resume")
	defer span.End()

	pipe := *c.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&c.listeners)
	// listener already active
	if l == 1 {
		return errors.Str("nats listener is already in the active state")
	}

	err := c.listenerInit(c.streamID, c.rateLimit)
	if err != nil {
		return err
	}

	c.listenerStart()

	atomic.AddUint32(&c.listeners, 1)

	c.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (c *Driver) State(ctx context.Context) (*jobs.State, error) {
	pipe := *c.pipeline.Load()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nats_state")
	defer span.End()

	st := &jobs.State{
		Pipeline: pipe.Name(),
		Priority: uint64(pipe.Priority()),
		Driver:   pipe.Driver(),
		Queue:    c.subject,
		Ready:    ready(atomic.LoadUint32(&c.listeners)),
	}

	if c.consumer != nil && c.consumer.jsc != nil {
		ci, err := c.consumer.jsc.Info(ctx)
		if err != nil {
			return nil, err
		}

		if ci != nil {
			st.Active = int64(ci.NumAckPending)
			st.Reserved = int64(ci.NumWaiting)
			st.Delayed = 0
		}
	}

	return st, nil
}

func (c *Driver) Stop(ctx context.Context) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nats_stop")
	defer span.End()

	atomic.StoreUint64(&c.stopped, 1)
	pipe := *c.pipeline.Load()
	// remove all items associated with the current pipeline
	_ = c.queue.Remove(pipe.Name())

	if atomic.LoadUint32(&c.listeners) > 0 {
		err := c.stream.Purge(ctx)
		if err != nil {
			c.log.Error("drain error", zap.Error(err))
		}
		c.stopCh <- struct{}{}
	}

	if c.deleteStreamOnStop {
		err := c.jetstream.DeleteStream(ctx, c.streamID)
		if err != nil {
			return err
		}
	}

	err := c.conn.Drain()
	if err != nil {
		return err
	}

	c.conn.Close()
	c.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

// private

func (c *Driver) requeue(item *Item) error {
	const op = errors.Op("nats_requeue")
	if item.Options.Delay > 0 {
		return errors.E(op, errors.Str("nats doesn't support delayed messages, see: https://github.com/nats-io/nats-streaming-server/issues/324"))
	}

	data, err := json.Marshal(item)
	if err != nil {
		return errors.E(op, err)
	}

	_, err = c.jetstream.PublishMsg(context.Background(), &nats.Msg{
		Subject: c.subject,
		Data:    data,
	})
	if err != nil {
		return errors.E(op, err)
	}

	// delete the old message
	_ = c.stream.DeleteMsg(context.Background(), item.Options.seq)

	item = nil
	return nil
}

func reconnectHandler(log *zap.Logger) func(*nats.Conn) {
	return func(conn *nats.Conn) {
		log.Warn("connection lost, reconnecting", zap.String("url", conn.ConnectedUrl()))
	}
}

func disconnectHandler(log *zap.Logger) func(*nats.Conn, error) {
	return func(_ *nats.Conn, err error) {
		if err != nil {
			log.Error("nats disconnected", zap.Error(err))
			return
		}

		log.Info("nats disconnected")
	}
}

func ready(r uint32) bool {
	return r > 0
}

func fromJob(job jobs.Message) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: job.Payload(),
		headers: job.Headers(),
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.GroupID(),
			Delay:    job.Delay(),
			AutoAck:  job.AutoAck(),
		},
	}
}
