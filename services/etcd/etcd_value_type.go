package etcd

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
)
