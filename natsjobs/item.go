package natsjobs

import (
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go"
	"github.com/roadrunner-server/sdk/v4/utils"
)

type Item struct {
	// Job contains name of job broker (usually PHP class).
	Job string `json:"job"`
	// Ident is unique identifier of the job, should be provided from outside
	Ident string `json:"id"`
	// Payload is string data (usually JSON) passed to Job broker.
	Payload string `json:"payload"`
	// Headers with key-values pairs
	Headers map[string][]string `json:"headers"`
	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle given job.
type Options struct {
	// Priority is job priority, default - 10
	// pointer to distinguish 0 as a priority and nil as priority not set
	Priority int64 `json:"priority"`
	// Pipeline manually specified pipeline.
	Pipeline string `json:"pipeline,omitempty"`
	// Delay defines time duration to delay execution for. Defaults to none.
	Delay int64 `json:"delay,omitempty"`
	// AutoAck option
	AutoAck bool `json:"auto_ack"`

	// private
	deleteAfterAck bool
	requeueFn      func(*Item) error
	ack            func(...nats.AckOpt) error
	nak            func(...nats.AckOpt) error
	stream         string
	seq            uint64
	sub            nats.JetStreamContext
}

// DelayDuration returns delay duration in a form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

// Body packs job payload into binary payload.
func (i *Item) Body() []byte {
	return utils.AsBytes(i.Payload)
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
		}{
			ID:       i.Ident,
			Job:      i.Job,
			Driver:   pluginName,
			Headers:  i.Headers,
			Pipeline: i.Options.Pipeline,
		},
	)

	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func (i *Item) Ack() error {
	// the message already acknowledged
	if i.Options.AutoAck {
		return nil
	}

	err := i.Options.ack()
	if err != nil {
		return err
	}

	if i.Options.deleteAfterAck {
		err = i.Options.sub.DeleteMsg(i.Options.stream, i.Options.seq)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *Item) Nack() error {
	if i.Options.AutoAck {
		return nil
	}
	return i.Options.nak()
}

func (i *Item) Requeue(headers map[string][]string, _ int64) error {
	// overwrite the delay
	i.Headers = headers

	err := i.Options.requeueFn(i)
	if err != nil {
		// do not nak the message if it was auto acknowledged
		if !i.Options.AutoAck {
			errNak := i.Options.nak()
			if errNak != nil {
				return fmt.Errorf("requeue error: %w\n nak error: %v", err, errNak)
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
		err = i.Options.sub.DeleteMsg(i.Options.stream, i.Options.seq)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}
