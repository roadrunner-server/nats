package nats

import (
	"github.com/roadrunner-server/api/v2/plugins/config"
	"github.com/roadrunner-server/api/v2/plugins/jobs"
	"github.com/roadrunner-server/api/v2/plugins/jobs/pipeline"
	priorityqueue "github.com/roadrunner-server/api/v2/pq"
	"github.com/roadrunner-server/nats/v2/natsjobs"
	"go.uber.org/zap"
)

const (
	pluginName string = "nats"
)

type Plugin struct {
	log *zap.Logger
	cfg config.Configurer
}

func (p *Plugin) Init(log *zap.Logger, cfg config.Configurer) error {
	p.log = new(zap.Logger)
	*p.log = *log
	p.cfg = cfg
	return nil
}

func (p *Plugin) Serve() chan error {
	return make(chan error, 1)
}

func (p *Plugin) Stop() error {
	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) ConsumerFromConfig(configKey string, queue priorityqueue.Queue) (jobs.Consumer, error) {
	return natsjobs.FromConfig(configKey, p.log, p.cfg, queue)
}

func (p *Plugin) ConsumerFromPipeline(pipe *pipeline.Pipeline, queue priorityqueue.Queue) (jobs.Consumer, error) {
	return natsjobs.FromPipeline(pipe, p.log, p.cfg, queue)
}
