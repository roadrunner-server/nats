package natsjobs

import (
	"context"
	stderr "errors"
	"go.uber.org/zap"
	"maps"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
)

var _ jobs.Job = (*Item)(nil)

type Item struct {
	// Job contains the name of job broker (usually PHP class).
	Job string `json:"job"`
	// Ident is a unique identifier of the job, should be provided from outside
	Ident string `json:"id"`
	// Payload is string data (usually JSON) passed to Job broker.
	Payload []byte `json:"payload"`
	// Headers with key-values pairs
	headers map[string][]string
	// Options contain a set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle a given job.
type Options struct {
	// Priority is job priority, default - 10
	Priority int64 `json:"priority"`
	// Pipeline manually specified pipeline.
	Pipeline string `json:"pipeline,omitempty"`
	// Delay defines time duration to delay execution for. Defaults to none.
	Delay int64 `json:"delay,omitempty"`
	// AutoAck option
	AutoAck bool `json:"auto_ack"`
	// Nats JET-streamID name
	Queue string

	// private
	deleteAfterAck bool
	stopped        *uint64
	requeueFn      func(*Item) error
	ack            func() error
	nak            func() error
	term           func() error
	nakWithDelay   func(time.Duration) error
	stream         string
	seq            uint64
	sub            jetstream.Stream
	heartbeat      context.CancelFunc
	inProgressFunc func() error
}

// DelayDuration returns delay duration in the form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

func (i *Item) GroupID() string {
	return i.Options.Pipeline
}

func (i *Item) Headers() map[string][]string {
	return i.headers
}

// Body packs job payload into binary payload.
func (i *Item) Body() []byte {
	return i.Payload
}

// Context packs job context (job, id) into binary payload.
func (i *Item) Context() ([]byte, error) {
	ctx, err := json.Marshal(
		struct {
			ID       string              `json:"id"`
			Job      string              `json:"job"`
			Driver   string              `json:"driver"`
			Headers  map[string][]string `json:"headers"`
			Pipeline string              `json:"pipeline"`
			Queue    string              `json:"queue,omitempty"`
		}{
			ID:       i.Ident,
			Job:      i.Job,
			Driver:   pluginName,
			Headers:  i.headers,
			Queue:    i.Options.Queue,
			Pipeline: i.Options.Pipeline,
		},
	)

	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func (i *Item) startHeartbeat(log *zap.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	i.Options.heartbeat = cancel

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				log.Error("heartbeat")
				if err := i.Options.inProgressFunc(); err != nil {
					log.Warn("failed to send InProgress heartbeat, will retry",
						zap.Error(err),
						zap.String("job_id", i.Ident),
						zap.String("pipeline", i.Options.Pipeline))
					continue
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (i *Item) stopHeartbeat() {
	if i.Options.heartbeat != nil {
		i.Options.heartbeat()
		i.Options.heartbeat = nil
	}
}

func (i *Item) Ack() error {
	i.stopHeartbeat()
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}
	// the message already acknowledged
	if i.Options.AutoAck {
		return nil
	}

	err := i.Options.ack()
	if err != nil {
		return err
	}

	if i.Options.deleteAfterAck {
		err = i.Options.sub.DeleteMsg(context.Background(), i.Options.seq)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *Item) Nack() error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}
	if i.Options.AutoAck {
		return nil
	}
	i.stopHeartbeat()
	return i.Options.nak()
}

func (i *Item) NackWithOptions(requeue bool, delay int) error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to NACK the JOB, the pipeline is probably stopped")
	}

	// if the user requested to requeue the message
	if requeue {
		return i.Options.nakWithDelay(time.Second * time.Duration(delay))
	}

	// if the user requested to delete the message
	return i.Options.term()
}

func (i *Item) Requeue(headers map[string][]string, _ int) error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}

	maps.Copy(i.headers, headers)

	err := i.Options.requeueFn(i)
	if err != nil {
		// do not nak the message if it was auto acknowledged
		if !i.Options.AutoAck {
			errNak := i.Options.nak()
			if errNak != nil {
				return stderr.Join(err, errNak)
			}
		}

		return err
	}

	// ack message
	if i.Options.AutoAck {
		return nil
	}

	err = i.Options.ack()
	if err != nil {
		return err
	}

	if i.Options.deleteAfterAck {
		err = i.Options.sub.DeleteMsg(context.Background(), i.Options.seq)
		if err != nil {
			return err
		}
	}

	return nil
}
