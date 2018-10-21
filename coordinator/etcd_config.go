package coordinator

import (
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultEtcdIp = "127.0.0.1:2379"
	// DefaultBindAddress is the default binding interface if none is specified.
	DefaultBindAddress = ":8089"

	// DefaultDatabase is the default database for UDP traffic.
	DefaultDatabase = "udp"

	// DefaultRetentionPolicy is the default retention policy used for writes.
	DefaultRetentionPolicy = ""

	// DefaultBatchSize is the default UDP batch size.
	DefaultBatchSize = 5000

	// DefaultBatchPending is the default number of pending UDP batches.
	DefaultBatchPending = 10

	// DefaultBatchTimeout is the default UDP batch timeout.
	DefaultBatchTimeout = time.Second

	// DefaultPrecision is the default time precision used for UDP services.
	DefaultPrecision = "n"

	// DefaultReadBuffer is the default buffer size for the UDP listener.
	// Sets the size of the operating system's receive buffer associated with
	// the UDP traffic. Keep in mind that the OS must be able
	// to handle the number set here or the UDP listener will error and exit.
	//
	// DefaultReadBuffer = 0 means to use the OS default, which is usually too
	// small for high UDP performance.
	//
	// Increasing OS buffer limits:
	//     Linux:      sudo sysctl -w net.core.rmem_max=<read-buffer>
	//     BSD/Darwin: sudo sysctl -w kern.ipc.maxsockbuf=<read-buffer>
	DefaultReadBuffer = 0
)

// EtcdConfig holds various configuration settings for the UDP listener.
type EtcdConfig struct {
	Enabled     bool   `toml:"enabled"`
	EtcdAddress string `toml:"address"`

	Database        string        `toml:"database"`
	RetentionPolicy string        `toml:"retention-policy"`
	BatchSize       int           `toml:"batch-size"`
	BatchPending    int           `toml:"batch-pending"`
	ReadBuffer      int           `toml:"read-buffer"`
	BatchTimeout    toml.Duration `toml:"batch-timeout"`
	Precision       string        `toml:"precision"`
}

// NewEtcdConfig returns a new instance of EtcdConfig with defaults.
func NewEtcdConfig() EtcdConfig {
	return EtcdConfig{
		Enabled:         true,
		EtcdAddress:     DefaultEtcdIp,
		Database:        DefaultDatabase,
		RetentionPolicy: DefaultRetentionPolicy,
		BatchSize:       DefaultBatchSize,
		BatchPending:    DefaultBatchPending,
		BatchTimeout:    toml.Duration(DefaultBatchTimeout),
	}
}

// WithDefaults takes the given config and returns a new config with any required
// default values set.
func (c *EtcdConfig) WithDefaults() *EtcdConfig {
	d := *c
	if d.EtcdAddress == "" {
		d.EtcdAddress = "127.0.0.1:2379"
	}
	return &d
}

// Configs wraps a slice of EtcdConfig to aggregate diagnostics.
type Configs []EtcdConfig

// Diagnostics returns one set of diagnostics for all of the Configs.
func (c Configs) Diagnostics() (*diagnostics.Diagnostics, error) {
	d := &diagnostics.Diagnostics{
		Columns: []string{"enabled", "bind-address", "database", "retention-policy", "batch-size", "batch-pending", "batch-timeout", "precision"},
	}

	for _, cc := range c {
		if !cc.Enabled {
			d.AddRow([]interface{}{false})
			continue
		}

		r := []interface{}{true, cc.EtcdAddress, cc.Database, cc.RetentionPolicy, cc.BatchSize, cc.BatchPending, cc.BatchTimeout, cc.Precision}
		d.AddRow(r)
	}

	return d, nil
}

// Enabled returns true if any underlying EtcdConfig is Enabled.
func (c Configs) Enabled() bool {
	for _, cc := range c {
		if cc.Enabled {
			return true
		}
	}
	return false
}
