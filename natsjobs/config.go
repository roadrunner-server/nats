package natsjobs

import (
	"time"

	"github.com/nats-io/nats.go"
)

const (
	subjectHeaderKey string = "x-nats-subject"

	pipeSubject            string = "subject"
	pipeStream             string = "stream"
	pipePrefetch           string = "prefetch"
	pipeDeleteAfterAck     string = "delete_after_ack"
	pipeDeliverNew         string = "deliver_new"
	pipeRateLimit          string = "rate_limit"
	pipeDeleteStreamOnStop string = "delete_stream_on_stop"
	pipeAckWait            string = "ack_wait"
)

type config struct {
	// global
	// NATS URL
	Addr string `mapstructure:"addr"`

	Priority           int64         `mapstructure:"priority"`
	Subject            string        `mapstructure:"subject"`
	StreamID           string        `mapstructure:"stream"`
	Prefetch           int           `mapstructure:"prefetch"`
	RateLimit          uint64        `mapstructure:"rate_limit"`
	DeleteAfterAck     bool          `mapstructure:"delete_after_ack"`
	DeliverNew         bool          `mapstructure:"deliver_new"`
	DeleteStreamOnStop bool          `mapstructure:"delete_stream_on_stop"`
	AckWait            time.Duration `mapstructure:"ack_wait"`
}

func (c *config) InitDefaults() {
	if c.Addr == "" {
		c.Addr = nats.DefaultURL
	}
	if c.AckWait == 0 {
		c.AckWait = 30 * time.Second
	}

	if c.RateLimit == 0 {
		c.RateLimit = 1000
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.StreamID == "" {
		c.StreamID = "default-stream"
	}

	if c.Subject == "" {
		c.Subject = "default"
	}

	if c.Prefetch == 0 {
		c.Prefetch = 10
	}
}
