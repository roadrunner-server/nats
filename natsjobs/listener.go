package natsjobs

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// blocking
func (c *Driver) listenerInit() error {
	id := uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// default AckWait is 30s, it means the server will resend the message if the client does not send inprogress state within 30s
	// see https://docs.nats.io/nats-concepts/jetstream/consumers#general
	cons, err := c.jetstream.CreateConsumer(ctx, c.streamID, jetstream.ConsumerConfig{
		Name:          id,
		MaxAckPending: c.prefetch,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}

	consume, err := cons.Consume(func(msg jetstream.Msg) {
		c.msgCh <- msg
	})

	if err != nil {
		return err
	}
	c.consumerLock.Lock()
	c.consumer = &consumer{
		id:      id,
		jsc:     cons,
		context: consume,
	}
	c.consumerLock.Unlock()

	return nil
}

// wrapCleanupFn wrap the cleanup function to ensure the inProgressItems map is updated correctly
func (c *Driver) wrapCleanupFn(id string, fn func() error) func() error {
	return func() error {
		err := fn()
		if err == nil {
			c.inProgressItems.Delete(id)
		}
		return err
	}
}

func (c *Driver) listenerStart() { //nolint:gocognit
	go func() {
		for {
			select {
			case m, ok := <-c.msgCh:
				if !ok {
					c.log.Warn("nats consume channel was closed")
					c.consumer = nil
					return
				}

				meta, err := m.Metadata()
				if err != nil {
					errn := m.Nak()
					if errn != nil {
						c.log.Error("failed to send Nak state", zap.Error(errn), zap.Error(err))
						continue
					}
					c.log.Info("can't get message metadata", zap.Error(err))
					continue
				}

				item := &Item{}
				c.unpack(m.Data(), m.Headers(), item)

				ctx := c.prop.Extract(context.Background(), propagation.HeaderCarrier(item.headers))
				ctx, span := c.tracer.Tracer(tracerName).Start(ctx, "nats_listener")

				if _, loaded := c.inProgressItems.LoadOrStore(item.ID(), struct{}{}); loaded {
					err = m.InProgress()
					if err != nil {
						errn := m.Nak()
						if errn != nil {
							c.log.Error("failed to send Nak state", zap.Error(errn), zap.Error(err))
							continue
						}
						c.log.Error("failed to send InProgress state", zap.Error(err))
						continue
					}

					c.log.Info("job already in progress", zap.String("id", item.ID()))
					span.End()
					continue
				}

				// set queue and pipeline
				item.Options.Queue = c.streamID
				item.Options.Pipeline = (*c.pipeline.Load()).Name()
				item.Options.stopped = &c.stopped

				// wrap the ack, nak, term, nakWithDelay and requeue functions
				item.Options.ack = c.wrapCleanupFn(item.ID(), m.Ack)
				item.Options.nak = c.wrapCleanupFn(item.ID(), m.Nak)
				item.Options.term = c.wrapCleanupFn(item.ID(), m.Term)
				item.Options.nakWithDelay = func(delay time.Duration) error {
					return c.wrapCleanupFn(item.ID(), func() error {
						return m.NakWithDelay(delay)
					})()
				}
				item.Options.requeueFn = func(item *Item) error {
					return c.wrapCleanupFn(item.ID(), func() error {
						return c.requeue(item)
					})()
				}
				// sequence needed for the requeue
				item.Options.seq = meta.Sequence.Stream

				// needed only if delete after ack is true
				if c.deleteAfterAck {
					item.Options.sub = c.stream
					item.Options.stream = c.streamID
					item.Options.deleteAfterAck = c.deleteAfterAck
				}

				if item.Priority() == 0 {
					item.Options.Priority = c.priority
				}

				// if auto ack is enabled, immediately execute ack
				if item.Options.AutoAck {
					c.log.Debug("auto_ack option enabled")
					err := item.Options.ack()
					if err != nil {
						item = nil
						c.log.Error("message acknowledge", zap.Error(err))
						span.RecordError(err)
						span.End()
						continue
					}

					if item.Options.deleteAfterAck {
						err = c.stream.DeleteMsg(context.Background(), meta.Sequence.Stream)
						if err != nil {
							c.log.Error("delete message", zap.Error(err))
							item = nil
							span.RecordError(err)
							span.End()
							continue
						}
					}

					item.Options.ack = nil
					item.Options.nak = nil
				}

				if item.headers == nil {
					item.headers = make(map[string][]string, 1)
				}
				item.headers[subjectHeaderKey] = []string{m.Subject()}

				c.prop.Inject(ctx, propagation.HeaderCarrier(item.headers))
				c.queue.Insert(item)
				span.End()

			case <-c.stopCh:
				c.consumerLock.Lock()
				c.consumer = nil
				c.consumerLock.Unlock()
				return
			}
		}
	}()
}
