package coordinator

import "time"

type (
	RecruitClusters struct {
		number     int32    `json:"number"`
		clusterIds []uint64 `json:"clusterIds"`
	}
	Node struct {
		id      uint64 `json:"id"`
		host    string `json:"host"`
		udpHost string `json:"udpHost"`
	}
	CommonNodes struct {
		nodes []Node `json:"nodes"`
	}
	RecruitClusterInfo struct {
		clusterId uint64 `json:"cluster_id"`
		limit     int    `json:"limit"`
		number    int    `json:"number"`
		master    Node   `json:"master"`
		nodes     []Node `json:"nodes"`
	}
	//[{id:1, nodes:[{id:1,host:,udpHost:}]}]
	Series struct {
		key string `json:"key"`
	}
	WorkClusterInfo struct {
		RecruitClusterInfo
		series       []string `json:"series"`
		measurements []string `json:"measurements"`
	}
	// tsdb-available-clusters
	AvailableClusterInfo struct {
		clusters []WorkClusterInfo `json:"clusters"`
	}
	// TSDB-Database key: database name value: key: rp name value: RP
	Databases struct {
		database map[string]map[string]Rp `json:"database"`
	}
	Rp struct {
		name               string        `json:"name"`
		replica            int           `json:"replica"`
		duration           time.Duration `json:"duration"`
		shardGroupDuration time.Duration `json:"shard_group_duration"`
		needUpdate         bool          `json:"need_update"`
	}
)
