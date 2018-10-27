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
		limit  int32  `json:"limit"`
		number int32  `json:"number"`
		master Node   `json:"master"`
		nodes  []Node `json:"nodes"`
	}
	//[{id:1, nodes:[{id:1,host:,udpHost:}]}]
	SingleClusterInfo struct {
		clusterId uint64 `json:"clusterId"`
		master    Node   `json:"master"`
		nodes     []Node `json:"nodes"`
	}
	AllClusterInfo struct {
		cluster []SingleClusterInfo `json:"cluster"`
	}
	Series struct {
		key string `json:"key"`
	}
	WorkClusterInfo struct {
		RecruitClusterInfo
		series []Series `json:"series"`
	}
	// TSDB-Database
	Databases struct {
		database []Database `json:"database"`
	}
	Database struct {
		name string `json:"name"`
		rp   []Rp   `json:"rp"`
	}
	Rp struct {
		name               string        `json:"name"`
		replica            int           `json:"replica"`
		duration           time.Duration `json:"duration"`
		shardGroupDuration time.Duration `json:"shard_group_duration"`
		needUpdate         bool          `json:"need_update"`
	}
)
