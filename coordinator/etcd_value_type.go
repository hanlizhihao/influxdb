package coordinator

import (
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"time"
)

type (
	RecruitClusters struct {
		Number     int32    `json:"number"`
		ClusterIds []uint64 `json:"clusterIds"`
	}
	Node struct {
		Id      uint64 `json:"id"`
		Host    string `json:"host"`
		UdpHost string `json:"udpHost"`
		// consistent hash weight
		Weight    int    `json:"weight"`
		Ip        string `json:"ip"`
		ClusterId uint64 `json:"cluster_id"`
	}
	CommonNodes []Node
	//[{id:1, nodes:[{id:1,host:,udpHost:}]}]
	Series struct {
		Key string `json:"key"`
	}

	WorkClusterInfo struct {
		ClusterId    uint64   `json:"cluster_id"`
		Limit        int      `json:"limit"`
		Number       int      `json:"number"`
		Series       []string `json:"series"`
		ClassId      uint64   `json:"class_id"`
		MasterUsable bool     `json:"master"`
		MasterId     uint64   `json:"master_id"`
		MasterHost   string   `json:"master_host"`
		MasterIp     string   `json:"master_ip"`
	}
	// tsdb-available-clusters
	AvailableClusterInfo struct {
		Clusters []WorkClusterInfo `json:"clusters"`
	}
	// TSDB-Database key: database name value: key: rp name value: RP
	Databases map[string]map[string]Rp
	// key: database name, value Cq Array
	Cqs map[string][]meta.ContinuousQueryInfo
	Rp  struct {
		Name               string        `json:"name"`
		Replica            int           `json:"replica"`
		Duration           time.Duration `json:"duration"`
		ShardGroupDuration time.Duration `json:"shard_group_duration"`
		NeedUpdate         bool          `json:"need_update"`
	}
	// TSDB-Class
	Classes []Class
	// every update need clear last newMeasurement and deleteMeasurement
	Class struct {
		ClassId    uint64   `json:"class_id"`
		Limit      int      `json:"limit"`
		ClusterIds []uint64 `json:"cluster_ids"`
		// latest measurement
		Measurements []string `json:"measurements"`
		// Incremental measurement
		NewMeasurement []string `json:"new_measurement"`
		// Incremental delete measurement
		DeleteMeasurement []string `json:"delete_measurement"`
	}
	// clusters:[{id,masterNode:{id,host,weight}}], measurements: [name]

	ClassDetail struct {
		Clusters     []WorkClusterInfo `json:"clusters"`
		Measurements []string          `json:"measurements"`
	}
	Users map[string]User
	User  struct {
		Name       string                        `json:"name"`
		Password   string                        `json:"password"`
		Admin      bool                          `json:"admin"`
		Privileges map[string]influxql.Privilege `json:"privileges"`
	}
)
