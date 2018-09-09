package etcd_test

import (
	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/services/etcd"
	"testing"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c etcd.Config
	if _, err := toml.Decode(`
enabled = true
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if !c.Enabled {
		t.Fatalf("unexpected enabled: %v", c.Enabled)
	}
}
