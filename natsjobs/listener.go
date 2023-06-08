package natsjobs

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// blocking
func (c *Driver) listenerInit() (jetstream.MessagesContext, error) {
	conf := jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy,
		RateLimit: c.rateLimit,
	}

	if c.deliverNew {
		conf.DeliverPolicy = jetstream.DeliverNewPolicy
	}

	cons, err := c.sh.AddConsumer(context.Background(), conf)
	if err != nil {
		return nil, err
	}

	iter, err := cons.Messages()
	if err != nil {
		return nil, err
	}

	return iter, nil
}

func (c *Driver) listenerStart(iter jetstream.MessagesContext) { //nolint:gocognit
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
					c.log.Info("can't get message metadata", zap.Error(err))
					continue
				}

				err = m.InProgress()
				if err != nil {
					c.log.Error("failed to send InProgress state", zap.Error(err))
					continue
				}

				item := &Item{}
				err = c.unpack(m.Data, item)

				// set queue and pipeline
				item.Options.Queue = c.stream
				item.Options.Pipeline = (*c.pipeline.Load()).Name()

				ctx := c.prop.Extract(context.Background(), propagation.HeaderCarrier(item.Headers))
				ctx, span := c.tracer.Tracer(tracerName).Start(ctx, "nats_listener")

				if err != nil {
					c.log.Error("unmarshal nats payload", zap.Error(err))
					span.SetAttributes(attribute.KeyValue{
						Key:   "error",
						Value: attribute.StringValue(err.Error()),
					})
					span.End()
					continue
				}

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

				c.prop.Inject(ctx, propagation.HeaderCarrier(item.Headers))
				c.queue.Insert(item)
				span.End()
			case <-c.stopCh:
				return
			}
		}
	}()
}
