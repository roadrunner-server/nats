package natsjobs

import (
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/roadrunner-server/sdk/v2/utils"
	"go.uber.org/zap"
)

const (
	// consume all
	auto string = "deduced_by_rr"
)

func (c *Consumer) unpack(data []byte, item *Item) error {
	err := json.Unmarshal(data, item)
	if err != nil {
		if c.consumeAll {
			c.log.Debug("unmarshal error", zap.Error(err))

			uid := uuid.NewString()
			c.log.Debug("get raw payload", zap.String("assigned ID", uid))

			*item = Item{
				Job:     auto,
				Ident:   uid,
				Payload: utils.AsString(data),
				Headers: nil,
				Options: &Options{
					Priority: 10,
					Pipeline: auto,
				},
			}

			return nil
		}

		return err
	}

	return nil
}
