package coordinator_test

import (
	"github.com/influxdata/influxql"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/coordinator"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c coordinator.Config
	if _, err := toml.Decode(`
write-timeout = "20s"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if time.Duration(c.WriteTimeout) != 20*time.Second {
		t.Fatalf("unexpected write timeout s: %s", c.WriteTimeout)
	}
}

func TestJson(t *testing.T) {
	statements := make([]influxql.Statement, 0)
	statement := influxql.AlterRetentionPolicyStatement{
		Name: "ss",
	}
	statements = append(statements, &statement)
	query := influxql.Query{
		Statements: statements,
	}

	jsonString := coordinator.ToJsonByte(query)
	var jsonQuery influxql.Query
	coordinator.ParseJson(jsonString, &jsonQuery)

	var state influxql.Statement
	coordinator.ParseJson(jsonString, &state)
	var alter influxql.AlterRetentionPolicyStatement

	state = &statement
	interString := coordinator.ToJsonByte(state)
	var result influxql.Statement
	coordinator.ParseJson(interString, &result)
	coordinator.ParseJson(interString, &alter)

}
