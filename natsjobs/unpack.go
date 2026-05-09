package natsjobs

import (
	"encoding/json"

	"github.com/google/uuid"
)

const (
	// consume all
	auto string = "deduced_by_rr"
)

func (c *Driver) unpack(data []byte, headers map[string][]string, item *Item) {
	err := json.Unmarshal(data, item)
	item.headers = headers
	if err != nil {
		*item = Item{
			Job:     auto,
			Ident:   uuid.NewString(),
			Payload: data,
			headers: headers,
			Options: &Options{
				Priority: (*c.pipeline.Load()).Priority(),
				Pipeline: (*c.pipeline.Load()).Name(),
				Queue:    c.streamID,
			},
		}
		c.log.Debug("raw payload", "assigned ID", item.Ident)
	}
}
