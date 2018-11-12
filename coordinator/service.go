package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"strconv"
	"sync"
)

const (
	// Value is a collection of all database instances.
	TSDBCommonNodeKey = "tsdb-common-node"
	// Value is a collection of all available cluster, every item is key of cluster
	TSDBClustersKey = "tsdb-available-clusters"
	// Value is the cluster that tries to connect
	TSDBWorkKey                = "tsdb-work-cluster-"
	TSDBRecruitClustersKey     = "tsdb-recruit-clusters"
	TSDBRecruitClusterKey      = "tsdb-recruit-cluster"
	TSDBClusterAutoIncrementId = "tsdb-cluster-auto-increment-id"
	TSDBNodeAutoIncrementId    = "tsdb-node-auto-increment-id"
	TSDBDatabase               = "tsdb-databases"
)

type Service struct {
	MetaClient interface {
		Data() meta.Data
		SetData(data *meta.Data) error
		Databases() []meta.DatabaseInfo
		WaitForDataChanged() chan struct{}
		CreateSubscription(database, rp, name, mode string, destinations []string) error
		DropSubscription(database, rp, name string) error
		CreateContinuousQuery(database, name, query string) error
		CreateDatabase(name string) (*meta.DatabaseInfo, error)
		DropDatabase(name string) error
		UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
		CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
		DropRetentionPolicy(database, name string) error
	}
	wg            sync.WaitGroup
	closed        bool
	closing       chan struct{}
	mu            sync.Mutex
	subMu         sync.RWMutex
	Logger        *zap.Logger
	subscriptions []meta.SubscriptionInfo

	httpd      *httpd.Service
	store      *tsdb.Store
	etcdConfig EtcdConfig
	httpConfig httpd.Config
	udpConfig  udp.Config
	cli        *clientv3.Client
	// other cluster's key:MeasurementName value: ip Array
	measurements map[string][]string
	// local cluster's measurements
	localMeasurement map[string][]string
}

func NewService(store *tsdb.Store, etcdConfig EtcdConfig, httpConfig httpd.Config, udpConfig udp.Config) *Service {
	s := &Service{
		Logger:     zap.NewNop(),
		closed:     true,
		store:      store,
		etcdConfig: etcdConfig,
		httpConfig: httpConfig,
		udpConfig:  udpConfig,
	}
	return s
}
func (s *Service) SetHttpdService(httpd *httpd.Service) {
	s.httpd = httpd
}

func (s *Service) Open() error {
	var err error
	s.cli, err = GetEtcdClient(s.etcdConfig)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Logger.Info("Starting register for ETCD Service...")
	err = s.registerToCommonNode()
	if err != nil {
		return err
	}
	metaData := s.MetaClient.Data()
	// Try to connect the original cluster.
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	originClusterKey := TSDBWorkKey + strconv.FormatUint(metaData.ClusterID, 10)
	workClusterResp, err := s.cli.Get(ctx, originClusterKey)
	cancel()
	if workClusterResp == nil || err != nil {
		s.Logger.Info("origin cluster is null by meta data clusterId")
		s.joinClusterOrCreateCluster()
	} else {
		if workClusterResp.Count != 0 {
			var workCluster WorkClusterInfo
			ParseJson(workClusterResp.Kvs[0].Value, &workCluster)
			if workCluster.number >= workCluster.limit {
				s.clearHistoryData()
				err := s.joinClusterOrCreateCluster()
				return err
			}
			err = s.joinRecruitClusterByClusterId(metaData.ClusterID)
		}
		s.joinClusterOrCreateCluster()
	}
	go s.watchClusterDatabaseInfo()
	go s.watchWorkClusterInfo()
	s.closed = false

	s.closing = make(chan struct{})

	s.Logger.Info("Opened Service")
	return nil
}

// Every node of cluster may create database and retention policy, So focus on and respond to changes in database
// information in metadata. So that each node's database and reservation policy data are consistent.
func (s *Service) watchClusterDatabaseInfo() {
	cli, err := GetEtcdClient(s.etcdConfig)
	if err != nil {
		s.Logger.Error("replication Service connected failed")
	}
	databaseInfoResp, err := cli.Get(context.Background(), TSDBDatabase)
	if databaseInfoResp == nil || err != nil || databaseInfoResp.Count == 0 {
		databases := s.toDatabaseInfo(s.MetaClient.Databases())
		cli.Put(context.Background(), TSDBDatabase, ToJson(databases))
	}
	databaseInfo := cli.Watch(context.Background(), TSDBDatabase, clientv3.WithPrefix())
	for database := range databaseInfo {
		for _, db := range database.Events {
			if bytes.Equal(db.PrevKv.Value, db.Kv.Value) {
				continue
			}
			var databases Databases
			localDBInfo := make(map[string]map[string]Rp, len(s.MetaClient.Databases()))
			ParseJson(db.Kv.Value, &databases)
			// Add new database and retention policy
			for _, localDB := range s.MetaClient.Databases() {
				rps := databases.database[localDB.Name]
				if rps == nil {
					s.MetaClient.DropDatabase(localDB.Name)
					continue
				}
				// Local information is up to date.
				//localDBInfo[localDB.Name] = make(map[string]Rp, len(localDB.RetentionPolicies))
				rpInfo := make(map[string]Rp, len(localDB.RetentionPolicies))
				for _, localRP := range localDB.RetentionPolicies {
					if s.subscriptions == nil || len(s.subscriptions) == 0 {
						s.subscriptions = localRP.Subscriptions
					}
					latestRP := rps[localRP.Name]
					if &latestRP == nil {
						s.MetaClient.DropRetentionPolicy(localDB.Name, localRP.Name)
						continue
					}
					if latestRP.needUpdate {
						s.MetaClient.UpdateRetentionPolicy(localDB.Name, localRP.Name, &meta.RetentionPolicyUpdate{
							Duration:           &latestRP.duration,
							ReplicaN:           &latestRP.replica,
							ShardGroupDuration: &latestRP.shardGroupDuration,
						}, false)
					}
					// Save RP that has been completed.
					rpInfo[localRP.Name] = latestRP
					// Delete RP that has been completed
					delete(rps, localRP.Name)
				}
				// rps will only save new rp
				for _, value := range rps {
					s.MetaClient.CreateRetentionPolicy(localDB.Name, &meta.RetentionPolicySpec{
						Name:               value.name,
						ReplicaN:           &value.replica,
						Duration:           &value.duration,
						ShardGroupDuration: value.shardGroupDuration,
					}, false)
					for _, sub := range s.subscriptions {
						s.MetaClient.CreateSubscription(localDB.Name, value.name, sub.Name, sub.Mode, sub.Destinations)
					}
				}
				// save DB that has been completed
				localDBInfo[localDB.Name] = rpInfo
				delete(databases.database, localDB.Name)
			}
			// add new database
			for key, value := range databases.database {
				s.MetaClient.CreateDatabase(key)
				for _, rpValue := range value {
					s.MetaClient.CreateRetentionPolicy(key, &meta.RetentionPolicySpec{
						Name:               rpValue.name,
						ReplicaN:           &rpValue.replica,
						Duration:           &rpValue.duration,
						ShardGroupDuration: rpValue.shardGroupDuration,
					}, false)
					for _, sub := range s.subscriptions {
						s.MetaClient.CreateSubscription(key, rpValue.name, sub.Name, sub.Mode, sub.Destinations)
					}
				}
			}
		}
	}
}

func (s *Service) GetLatestDatabaseInfo() (*Databases, error) {
	cli, err := GetEtcdClient(s.etcdConfig)
	if err != nil {
		return nil, err
	}
	var databases Databases
	databasesInfoResp, err := cli.Get(context.Background(), TSDBDatabase)
	if err != nil {
		return nil, err
	}
	ParseJson(databasesInfoResp.Kvs[0].Value, &databases)
	return &databases, nil
}
func (s *Service) PutDatabaseInfo(database *Databases) error {
	cli, err := GetEtcdClient(s.etcdConfig)
	if err != nil {
		return err
	}
	_, errs := cli.Put(context.Background(), TSDBDatabase, ToJson(*database))
	return errs
}

func (s *Service) toDatabaseInfo(databaseInfo []meta.DatabaseInfo) Databases {
	var databases = make(map[string]map[string]Rp, len(databaseInfo))
	for _, db := range databaseInfo {
		var rps = make(map[string]Rp, len(db.RetentionPolicies))
		for _, rp := range db.RetentionPolicies {
			rps[rp.Name] = Rp{
				name:               rp.Name,
				replica:            rp.ReplicaN,
				duration:           rp.Duration,
				shardGroupDuration: rp.ShardGroupDuration,
			}
		}
		databases[db.Name] = rps
	}
	return Databases{
		database: databases,
	}
}
func (s *Service) clearHistoryData() {
	for _, db := range s.MetaClient.Databases() {
		s.store.DeleteDatabase(db.Name)
	}
}
func (s *Service) joinClusterOrCreateCluster() error {
	s.Logger.Info("Database will join cluster or create new")
	cli, err := GetEtcdClient(s.etcdConfig)
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
		s.createCluster()
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
				err = s.joinRecruitClusterByClusterId(id)
				if err == nil {
					return nil
				}
			}
			err = s.createCluster()
			return err
		}
		err = s.createCluster()
		if err != nil {
			return err
		}
	}
	return nil
}

// Join cluster failed, create new cluster
func (s *Service) createCluster() error {
	recruit, err := s.cli.Get(context.Background(), TSDBRecruitClustersKey)
	if err != nil {
		return err
	}
	var recruitCluster RecruitClusters
	err = json.Unmarshal(recruit.Kvs[0].Value, &recruitCluster)
	if err != nil {
		return err
	}
	var workClusterInfo AvailableClusterInfo
RetryCreate:
	allClusterInfoResp, err := s.cli.Get(context.Background(), TSDBClustersKey)
	if err != nil {
		return err
	}
	if allClusterInfoResp == nil || allClusterInfoResp.Count == 0 {
		s.initAllClusterInfo(&workClusterInfo)
	} else {
		ParseJson(allClusterInfoResp.Kvs[0].Value, &workClusterInfo)
		if workClusterInfo.clusters == nil {
			s.initAllClusterInfo(&workClusterInfo)
		}
	}
	latestClusterInfo := workClusterInfo.clusters[len(workClusterInfo.clusters)-1]
	recruitCluster.clusterIds = append(recruitCluster.clusterIds, latestClusterInfo.clusterId)
	recruitCluster.number++
	recruitClusterInfo := RecruitClusterInfo{
		number: 1,
		limit:  3,
		nodes:  latestClusterInfo.nodes,
		master: latestClusterInfo.nodes[0],
	}
	// Transaction creation cluster
	cmpAllCluster := clientv3.Compare(clientv3.Value(TSDBClustersKey), "=", string(allClusterInfoResp.Kvs[0].Value))
	cmpRecruit := clientv3.Compare(clientv3.Value(TSDBRecruitClustersKey), "=", string(recruit.Kvs[0].Value))
	putAllCluster := clientv3.OpPut(TSDBClustersKey, ToJson(workClusterInfo))
	putAllRecruit := clientv3.OpPut(TSDBRecruitClustersKey, ToJson(recruitCluster))
	putRecruit := clientv3.OpPut(TSDBRecruitClusterKey+strconv.FormatUint(latestClusterInfo.clusterId, 10), ToJson(recruitClusterInfo))
	resp, err := s.cli.Txn(context.Background()).If(cmpAllCluster, cmpRecruit).Then(putAllCluster, putAllRecruit, putRecruit).Commit()
	if !resp.Succeeded {
		goto RetryCreate
	}
	metaData := s.MetaClient.Data()
	metaData.ClusterID = latestClusterInfo.clusterId
	s.MetaClient.SetData(&metaData)
	go s.watchRecruitCluster(latestClusterInfo.clusterId)
	return nil
}

// Watch node of the cluster, change cluster information and change subscriber
func (s *Service) watchRecruitCluster(clusterId uint64) {
	cli, err := GetEtcdClient(s.etcdConfig)
	if err != nil {
		s.Logger.Error("replication Service connected failed")
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
			for _, db := range s.MetaClient.Databases() {
				for _, rp := range db.RetentionPolicies {
					for _, node := range recruitClusterInfo.nodes {
						subscriberName := db.Name + rp.Name + node.host
						destination := make([]string, 1)
						destination = append(destination, "http://"+node.host)
						s.MetaClient.CreateSubscription(db.Name, rp.Name, subscriberName, "All", destination)
					}
				}
			}
			if recruitClusterInfo.number >= 2 {
				workClusterKey := TSDBWorkKey + strconv.FormatUint(clusterId, 10)
				resp, err := cli.Get(context.Background(), workClusterKey)
				if resp == nil || err != nil || resp.Count == 0 {
					series := make([]string, 10)
					seriesMap := make(map[string]interface{})
					for _, index := range s.store.GetInmemIndexOfShards() {
						var inmemIndex = index.(inmem.Index)
						for _, seriesKey := range inmemIndex.SeriesKeys() {
							seriesMap[seriesKey] = nil
						}
					}
					for s := range seriesMap {
						series = append(series, s)
					}
					workClusterInfo := WorkClusterInfo{
						RecruitClusterInfo: recruitClusterInfo,
						series:             series,
					}
					cli.Put(context.Background(), workClusterKey, ToJson(workClusterInfo))
				}
			}
		}
	}
}

func (s *Service) registerToCommonNode() error {
RegisterNode:
	// Register with etcd
	commonNodesResp, err := s.cli.Get(context.Background(), TSDBCommonNodeKey)
	if err != nil {
		return err
	}
	// TSDBCommonNodeKey is not exist
	if commonNodesResp == nil || commonNodesResp.Count == 0 {
		nodeId, err := GetLatestID(s.etcdConfig, TSDBNodeAutoIncrementId)
		if err != nil {
			return errors.New("Get node id failed ")
		}
		var nodes []Node
		ip, err := GetLocalHostIp()
		nodes = append(nodes, Node{
			id:      nodeId,
			host:    ip + s.httpConfig.BindAddress,
			udpHost: ip + s.udpConfig.BindAddress,
		})
		commonNodes := CommonNodes{
			nodes: nodes,
		}
		s.cli.Put(context.Background(), TSDBCommonNodeKey, ToJson(commonNodes))
	}
	var commonNodes CommonNodes
	ParseJson(commonNodesResp.Kvs[0].Value, &commonNodes)
	nodeId, err := GetLatestID(s.etcdConfig, TSDBNodeAutoIncrementId)
	ip, err := GetLocalHostIp()
	commonNodes.nodes = append(commonNodes.nodes, Node{
		id:      nodeId,
		host:    ip + s.httpConfig.BindAddress,
		udpHost: ip + s.udpConfig.BindAddress,
	})
	cmp := clientv3.Compare(clientv3.Value(TSDBCommonNodeKey), "=", string(commonNodesResp.Kvs[0].Value))
	put := clientv3.OpPut(TSDBCommonNodeKey, ToJson(commonNodes))
	resp, err := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if !resp.Succeeded {
		goto RegisterNode
	}
	return nil
}

// Watch node of the work cluster，update series Index
func (s *Service) watchWorkClusterInfo() {
	availableResp, err := s.cli.Get(context.Background(), TSDBClustersKey)
	if err != nil {
		s.Logger.Error("Get information of cluster from etcd error")
	} else {
		s.buildMeasurementIndex(availableResp.Kvs[0].Value)
	}
	workClustersResp := s.cli.Watch(context.Background(), TSDBClustersKey, clientv3.WithPrefix())
	for event := range workClustersResp {
		for _, value := range event.Events {
			s.buildMeasurementIndex(value.Kv.Value)
		}
		s.Logger.Info("WorkClusterChanged" + string(event.Events[0].Kv.Value))
	}
}

func (s *Service) initAllClusterInfo(allClusterInfo *AvailableClusterInfo) error {
	clusterId, err := GetLatestID(s.etcdConfig, TSDBClusterAutoIncrementId)
	if err != nil {
		return err
	}
	var nodes []Node
	ip, err := GetLocalHostIp()
	nodes = append(nodes, Node{
		host:    ip + s.httpConfig.BindAddress,
		udpHost: ip + s.udpConfig.BindAddress,
	})
	var workClusterInfo = make([]WorkClusterInfo, 0)
	workClusterInfo = append(workClusterInfo, WorkClusterInfo{
		RecruitClusterInfo: RecruitClusterInfo{clusterId: clusterId, nodes: nodes, limit: 3, number: len(nodes), master: nodes[0]},
		series:             make([]string, 0),
		measurements:       make([]string, 0),
	})
	*allClusterInfo = AvailableClusterInfo{
		clusters: workClusterInfo,
	}
	s.cli.Put(context.Background(), TSDBClustersKey, ToJson(allClusterInfo))
	return nil
}
func (s *Service) joinRecruitClusterByClusterId(clusterId uint64) error {
	cli, err := GetEtcdClient(s.etcdConfig)
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
			host:    ip + s.httpConfig.BindAddress,
			udpHost: ip + s.udpConfig.BindAddress,
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
	metaData := s.MetaClient.Data()
	metaData.ClusterID = clusterId
	go s.watchRecruitCluster(clusterId)
	return nil
}

// close closes the existing channel writers.
func (s *Service) close(wg *sync.WaitGroup) {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	// Wait for them to finish
	wg.Wait()
}
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil // Already closed.
	}

	s.closed = true

	close(s.closing)

	s.wg.Wait()
	s.Logger.Info("Closed Service")
	return nil
}

// WithLogger sets the logger on the Service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("replicationService", "replication"))
}

// Build Index for balance
func (s *Service) buildMeasurementIndex(data []byte) {
	var availableClusterInfo AvailableClusterInfo
	ParseJson(data, &availableClusterInfo)
	if s.measurements == nil {
		s.measurements = make(map[string][]string, len(availableClusterInfo.clusters))
	}
	// measurement may be save in cluster(a) and cluster(b), every cluster will be forwarded to the request
	for _, cluster := range availableClusterInfo.clusters {
		var ips []string
		for _, node := range cluster.nodes {
			ips = append(ips, node.host)
		}
		if cluster.clusterId == s.MetaClient.Data().ClusterID {
			for _, m := range cluster.measurements {
				s.localMeasurement[m] = ips
			}
			continue
		}
		for _, measurement := range cluster.measurements {
			s.measurements[measurement] = ips
		}
	}
	s.httpd.Handler.Balancing.SetMeasurementMapIndex(s.measurements, s.localMeasurement)
}
func (s *Service) clusterSelect(wg *sync.WaitGroup, stmt *influxql.SelectStatement, ctx *query.ExecutionContext) {

}
