package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
	"net/url"
	"strconv"
	"sync"
)

type ReplicationService struct {
	MetaClient interface {
		Data() meta.Data
		SetData(data *meta.Data) error
		Databases() []meta.DatabaseInfo
		WaitForDataChanged() chan struct{}
		CreateSubscription(database, rp, name, mode string, destinations []string) error
		DropSubscription(database, rp, name string) error
	}
	update          chan struct{}
	stats           *Statistics
	points          chan *coordinator.WritePointsRequest
	wg              sync.WaitGroup
	closed          bool
	closing         chan struct{}
	mu              sync.Mutex
	subs            map[subEntry]chanWriter
	subMu           sync.RWMutex
	Logger          *zap.Logger
	NewPointsWriter func(u url.URL) (subscriber.PointsWriter, error)
	store           *tsdb.Store
	etcdConfig      Config
	httpConfig      httpd.Config
	udpConfig       udp.Config
}

func NewReplicationService(store *tsdb.Store, etcdConfig Config, httpConfig httpd.Config, udpConfig udp.Config) *ReplicationService {
	s := &ReplicationService{
		Logger:     zap.NewNop(),
		closed:     true,
		stats:      &Statistics{},
		store:      store,
		etcdConfig: etcdConfig,
		httpConfig: httpConfig,
		udpConfig:  udpConfig,
	}
	return s
}

func (rs *ReplicationService) Open() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.Logger.Info("Self checking measurements...")
	metaData := rs.MetaClient.Data()
	// Try to connect the original cluster.
	cli, err := GetEtcdClient(rs.etcdConfig)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	originClusterKey := TSDBWorkKey + strconv.FormatUint(metaData.ClusterID, 10)
	workClusterResp, err := cli.Get(ctx, originClusterKey)
	cancel()
	dbs := rs.MetaClient.Databases()
	for _, db := range dbs {
		measures, _ := rs.store.MeasurementNames(nil, db.Name, nil)
		rs.Logger.Info(string(measures[0]))
	}
	if workClusterResp == nil || err != nil {
		rs.Logger.Info("origin cluster is null by meta data clusterId")
		rs.joinClusterOrCreateCluster()
	} else {
		if workClusterResp.Count != 0 {
			var workCluster WorkClusterInfo
			ParseJson(workClusterResp.Kvs[0].Value, &workCluster)
			if workCluster.number >= workCluster.limit {
				rs.clearHistoryData()
				err := rs.joinClusterOrCreateCluster()
				return err
			}
			err = rs.joinRecruitClusterByClusterId(metaData.ClusterID)
		}
		rs.joinClusterOrCreateCluster()
	}
	rs.closed = false

	rs.closing = make(chan struct{})
	rs.update = make(chan struct{})
	rs.points = make(chan *coordinator.WritePointsRequest, 100)

	rs.wg.Add(2)
	go func() {
		defer rs.wg.Done()
	}()
	go func() {
		defer rs.wg.Done()
	}()

	rs.Logger.Info("Opened service")
	return nil
}
func (rs *ReplicationService) clearHistoryData() {
	for _, db := range rs.MetaClient.Databases() {
		rs.store.DeleteDatabase(db.Name)
	}
}
func (rs *ReplicationService) joinClusterOrCreateCluster() error {
	rs.Logger.Info("Database will join cluster or create new")
	cli, err := GetEtcdClient(rs.etcdConfig)
	if err != nil {
		return err
	}
	recruit, err := cli.Get(context.Background(), TSDBRecruitClustersKey)
	if err != nil {
		return err
	}
	var recruitCluster RecruitClusters
	// if recruitCluster key non-existent
	if recruit.Count == 0 {
		recruitCluster = RecruitClusters{
			number:     0,
			clusterIds: make([]uint64, 1),
		}
		cli.Put(context.Background(), TSDBRecruitClustersKey, ToJson(recruitCluster))
		// create cluster
		rs.createCluster()
		return nil
	}
	for _, kv := range recruit.Kvs {
		err := json.Unmarshal(kv.Value, &recruitCluster)
		if err != nil {
			return err
		}
		// Logical nodes need nodes to join it.
		if recruitCluster.number > 0 {
			for _, id := range recruitCluster.clusterIds {
				err = rs.joinRecruitClusterByClusterId(id)
				if err == nil {
					return nil
				}
			}
			err = rs.createCluster()
			return err
		}
		err = rs.createCluster()
		if err != nil {
			return err
		}
	}
	return nil
}

// Join cluster failed, create new cluster
func (rs *ReplicationService) createCluster() error {
	cli, err := GetEtcdClient(rs.etcdConfig)
	recruit, err := cli.Get(context.Background(), TSDBRecruitClustersKey)
	if err != nil {
		return err
	}
	var recruitCluster RecruitClusters
	err = json.Unmarshal(recruit.Kvs[0].Value, &recruitCluster)
	if err != nil {
		return err
	}
	clusterId, err := GetLatestClusterID(rs.etcdConfig)
	var allClusterInfo AllClusterInfo
RetryCreate:
	allClusterInfoResp, err := cli.Get(context.Background(), TSDBClustersKey)
	if err != nil {
		return err
	}
	if len(allClusterInfoResp.Kvs) == 0 {
		allClusterInfo = rs.initAllClusterInfo(allClusterInfo, cli)
	} else {
		ParseJson(allClusterInfoResp.Kvs[0].Value, &allClusterInfo)
	}
	if allClusterInfo.cluster == nil {
		rs.initAllClusterInfo(allClusterInfo, cli)
	}
	var nodes []Node
	ip, err := GetLocalHostIp()
	nodes = append(nodes, Node{
		host:    ip + rs.httpConfig.BindAddress,
		udpHost: ip + rs.udpConfig.BindAddress,
	})
	allClusterInfo.cluster = append(allClusterInfo.cluster, SingleClusterInfo{
		clusterId: clusterId,
		nodes:     nodes,
	})
	recruitCluster.clusterIds = append(recruitCluster.clusterIds, clusterId)
	recruitCluster.number++
	recruitClusterInfo := RecruitClusterInfo{
		number: 1,
		limit:  3,
		nodes:  nodes,
		master: nodes[0],
	}
	// Transaction creation cluster
	cmpAllCluster := clientv3.Compare(clientv3.Value(TSDBClustersKey), "=", string(allClusterInfoResp.Kvs[0].Value))
	cmpRecruit := clientv3.Compare(clientv3.Value(TSDBRecruitClustersKey), "=", string(recruit.Kvs[0].Value))
	putAllCluster := clientv3.OpPut(TSDBClustersKey, ToJson(allClusterInfo))
	putAllRecruit := clientv3.OpPut(TSDBRecruitClustersKey, ToJson(recruitCluster))
	putRecruit := clientv3.OpPut(TSDBRecruitClusterKey+strconv.FormatUint(clusterId, 10), ToJson(recruitClusterInfo))
	resp, err := cli.Txn(context.Background()).If(cmpAllCluster, cmpRecruit).Then(putAllCluster, putAllRecruit, putRecruit).Commit()
	if !resp.Succeeded {
		goto RetryCreate
	}
	metaData := rs.MetaClient.Data()
	metaData.ClusterID = clusterId
	rs.MetaClient.SetData(&metaData)
	go rs.watchClusterNodeChange(clusterId)
	return nil
}

// Watch node of the cluster, change cluster information and change subscriber
func (rs *ReplicationService) watchClusterNodeChange(clusterId uint64) {
	cli, err := GetEtcdClient(rs.etcdConfig)
	if err != nil {
		rs.Logger.Error("replication service connected failed")
	}
	var prevRecruitClusterInfo RecruitClusterInfo
	var recruitClusterInfo RecruitClusterInfo
	dbNodes := cli.Watch(context.Background(), TSDBRecruitClusterKey+strconv.FormatUint(clusterId, 10), clientv3.WithPrefix())
	for dbNode := range dbNodes {
		for _, ev := range dbNode.Events {
			if bytes.Equal(ev.PrevKv.Value, ev.Kv.Value) {
				continue
			}
			ParseJson(ev.Kv.Value, &recruitClusterInfo)
			ParseJson(ev.PrevKv.Value, &prevRecruitClusterInfo)
			for _, db := range rs.MetaClient.Databases() {
				for _, rp := range db.RetentionPolicies {
					for _, node := range recruitClusterInfo.nodes {
						subscriberName := db.Name + rp.Name + node.host
						destination := make([]string, 1)
						destination = append(destination, "http://"+node.host)
						rs.MetaClient.CreateSubscription(db.Name, rp.Name, subscriberName, "All", destination)
					}
				}
			}
			if recruitClusterInfo.number >= 2 {
				workClusterKey := TSDBWorkKey + strconv.FormatUint(clusterId, 10)
				resp, err := cli.Get(context.Background(), workClusterKey)
				if resp == nil || err != nil || resp.Count == 0 {
					series := make([]Series, 10)
					for _, s := range rs.store.Indexes() {
						series = append(series, Series{
							key: s,
						})
					}
					workClusterInfo := WorkClusterInfo{
						RecruitClusterInfo: recruitClusterInfo,
						series:             series,
					}
					cli.Put(context.Background(), workClusterKey, ToJson(workClusterInfo))
				} else {
					// todo 当集群节点变化时，也应该改变series
				}
			}
		}
	}
}

func (rs *ReplicationService) initAllClusterInfo(allClusterInfo AllClusterInfo, cli *clientv3.Client) AllClusterInfo {
	var singleClusterInfo []SingleClusterInfo
	allClusterInfo = AllClusterInfo{
		cluster: singleClusterInfo,
	}
	cli.Put(context.Background(), TSDBClustersKey, ToJson(allClusterInfo))
	return allClusterInfo
}
func (rs *ReplicationService) joinRecruitClusterByClusterId(clusterId uint64) error {
	cli, err := GetEtcdClient(rs.etcdConfig)
	if err != nil {
		return err
	}
	recruitClusterKey := TSDBRecruitClusterKey
	recruitClusterKey += strconv.FormatUint(clusterId, 10)
Loop:
	recruitClusterNodes, err := cli.Get(context.Background(), recruitClusterKey)
	if err != nil {
		return err
	}
	for _, kv := range recruitClusterNodes.Kvs {
		var recruitClusterInfo RecruitClusterInfo
		err := json.Unmarshal(kv.Value, &recruitClusterInfo)
		if err != nil {
			return err
		}
		if recruitClusterInfo.number >= recruitClusterInfo.limit {
			continue
		}
		cmp := clientv3.Compare(clientv3.Value(recruitClusterKey), "=", string(kv.Value))
		ip, err := GetLocalHostIp()
		if err != nil {
			return err
		}
		recruitClusterInfo.nodes = append(recruitClusterInfo.nodes, Node{
			host:    ip + rs.httpConfig.BindAddress,
			udpHost: ip + rs.udpConfig.BindAddress,
		})
		infoByte, err := json.Marshal(recruitClusterInfo)
		if err != nil {
			return err
		}
		put := clientv3.OpPut(recruitClusterKey, string(infoByte))
		get := clientv3.OpGet(recruitClusterKey)
		resp, err := cli.Txn(context.Background()).If(cmp).Then(put).Else(get).Commit()
		if !resp.Succeeded {
			// retry
			goto Loop
		}
	}
	metaData := rs.MetaClient.Data()
	metaData.ClusterID = clusterId
	go rs.watchClusterNodeChange(clusterId)
	return nil
}

// close closes the existing channel writers.
func (rs *ReplicationService) close(wg *sync.WaitGroup) {
	rs.subMu.Lock()
	defer rs.subMu.Unlock()

	// Wait for them to finish
	wg.Wait()
	rs.subs = nil
}
func (rs *ReplicationService) Close() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.closed {
		return nil // Already closed.
	}

	rs.closed = true

	close(rs.points)
	close(rs.closing)

	rs.wg.Wait()
	rs.Logger.Info("Closed service")
	return nil
}

// WithLogger sets the logger on the service.
func (rs *ReplicationService) WithLogger(log *zap.Logger) {
	rs.Logger = log.With(zap.String("replicationService", "replication"))
}

func (rs *ReplicationService) updateSubs(wg *sync.WaitGroup) {
	rs.subMu.Lock()
	defer rs.subMu.Unlock()

	if rs.subs == nil {
		rs.subs = make(map[subEntry]chanWriter)
	}

}

// subEntry is a unique set that identifies a given subscription.
type subEntry struct {
	db   string
	rp   string
	name string
}

// chanWriter sends WritePointsRequest to a PointsWriter received over a channel.
type chanWriter struct {
	writeRequests chan *coordinator.WritePointsRequest
	pw            subscriber.PointsWriter
	pointsWritten *int64
	failures      *int64
	logger        *zap.Logger
}
