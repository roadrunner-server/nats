package natsjobs

import (
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	// consume all
	auto string = "deduced_by_rr"
)

func (c *Driver) unpack(data []byte, item *Item) {
	err := json.Unmarshal(data, item)
	if err != nil {
		*item = Item{
			Job:     auto,
			Ident:   uuid.NewString(),
			Payload: data,
			headers: make(map[string][]string, 2),
			Options: &Options{
				Priority: (*c.pipeline.Load()).Priority(),
				Pipeline: (*c.pipeline.Load()).Name(),
				Queue:    c.streamID,
			},
		}
		c.log.Debug("raw payload", zap.String("assigned ID", item.Ident))
	}
}
