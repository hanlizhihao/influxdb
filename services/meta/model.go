package meta

import (
	"github.com/influxdata/influxdb/models"
)

type (
	Series struct {
		Key  []byte      `json:"key"`
		Name []byte      `json:"name"`
		Tags models.Tags `json:"tags"`
	}
	MeasurementSeries struct {
		Deleted bool `json:"deleted"`

		// immutable
		ID          uint64       `json:"id"`
		Measurement *Measurement `json:"measurement"`
		Key         string       `json:"key"`
		Tags        models.Tags  `json:"tags"`
	}
	TagKeyValueEntry struct {
		M map[uint64]struct{} `json:"m"` // series id set
		A []uint64            `json:"a"` // lazily sorted list of series.
	}
	Measurement struct {
		Database  string `json:"database"`
		Name      string `json:"name,omitempty"`
		NameBytes []byte `json:"name_bytes"` // cached version as []byte

		FieldNames map[string]struct{} `json:"field_names"`

		// in-memory index fields
		SeriesByID          map[uint64]*MeasurementSeries           `json:"series_by_id"`            // lookup table for series by their id
		SeriesByTagKeyValue map[string]map[string]*TagKeyValueEntry `json:"series_by_tag_key_value"` // map from tag key to value to sorted set of series ids

		// lazyily created sorted series IDs
		SortedSeriesIDs []uint64 `json:"sorted_series_i_ds"` // sorted list of series IDs in this measurement

		// Indicates whether the seriesByTagKeyValueMap needs to be rebuilt as it contains deleted series
		// that waste memory.
		Dirty bool `json:"dirty"`
	}
)
