package meta

import "github.com/influxdata/influxdb/models"

type (
	Series struct {
		Key  []byte      `json:"key"`
		Name []byte      `json:"name"`
		Tags models.Tags `json:"tags"`
	}
)
