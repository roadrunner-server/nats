package natsjobs

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// blocking
func (c *Driver) listenerInit() error {
	var err error

	opts := make([]nats.SubOpt, 0)
	if c.deliverNew {
		opts = append(opts, nats.DeliverNew())
	}

	opts = append(opts, nats.RateLimit(c.rateLimit))
	opts = append(opts, nats.AckExplicit())
	c.sub, err = c.js.ChanSubscribe(c.subject, c.msgCh, opts...)
	if err != nil {
		return err
	}

	return nil
}

func (c *Driver) listenerStart() { //nolint:gocognit
	go func() {
		for {
			select {
			case m, ok := <-c.msgCh:
				if !ok {
					c.log.Warn("nats consume channel was closed")
					return
				}

				// only JS messages
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

				item := &Item{}
				err = c.unpack(m.Data, item)
				if err != nil {
					errn := m.Term()
					if errn != nil {
						c.log.Error("failed to send Term state", zap.Error(errn), zap.Error(err))
						continue
					}
					c.log.Error("unmarshal nats payload, if you're using non RR send, consider using the `consume_all: true` option, message terminated and won't be redelivered", zap.Error(err))
					continue
				}

				ctx := c.prop.Extract(context.Background(), propagation.HeaderCarrier(item.headers))
				ctx, span := c.tracer.Tracer(tracerName).Start(ctx, "nats_listener")

				// set queue and pipeline
				item.Options.Queue = c.stream
				item.Options.Pipeline = (*c.pipeline.Load()).Name()
				item.Options.stopped = &c.stopped

				// save the ack, nak and requeue functions
				item.Options.ack = m.Ack
				item.Options.nak = m.Nak
				item.Options.requeueFn = c.requeue
				// sequence needed for the requeue
				item.Options.seq = meta.Sequence.Stream

				// needed only if delete after ack is true
				if c.deleteAfterAck {
					item.Options.stream = c.stream
					item.Options.sub = c.js
					item.Options.deleteAfterAck = c.deleteAfterAck
				}

				if item.Priority() == 0 {
					item.Options.Priority = c.priority
				}

				if item.Options.AutoAck {
					c.log.Debug("auto_ack option enabled")
					err = m.Ack()
					if err != nil {
						item = nil
						c.log.Error("message acknowledge", zap.Error(err))
						span.SetAttributes(attribute.KeyValue{
							Key:   "error",
							Value: attribute.StringValue(err.Error()),
						})
						span.End()
						continue
					}

					if item.Options.deleteAfterAck {
						err = c.js.DeleteMsg(c.stream, meta.Sequence.Stream)
						if err != nil {
							c.log.Error("delete message", zap.Error(err))
							item = nil
							span.SetAttributes(attribute.KeyValue{
								Key:   "error",
								Value: attribute.StringValue(err.Error()),
							})
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

				c.prop.Inject(ctx, propagation.HeaderCarrier(item.headers))
				c.queue.Insert(item)
				span.End()
			case <-c.stopCh:
				return
			}
		}
	}()
}
