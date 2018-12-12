package coordinator

import "time"

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
		Weight int `json:"weight"`
	}
	CommonNodes    []Node
	RecruitCluster struct {
		ClusterId uint64 `json:"cluster_id"`
		Limit     int    `json:"limit"`
		Number    int    `json:"number"`
		Master    Node   `json:"master"`
		Nodes     []Node `json:"nodes"`
	}
	//[{id:1, nodes:[{id:1,host:,udpHost:}]}]
	Series struct {
		Key string `json:"key"`
	}
	WorkClusterInfo struct {
		RecruitCluster
		Series  []string `json:"series"`
		ClassId uint64   `json:"class_id"`
	}
	// tsdb-available-clusters
	AvailableClusterInfo struct {
		Clusters []WorkClusterInfo `json:"clusters"`
	}
	// TSDB-Database key: database name value: key: rp name value: RP
	Databases struct {
		Database map[string]map[string]Rp `json:"database"`
	}
	Rp struct {
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
)
