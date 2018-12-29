package coordinator

import (
	"bytes"
	"context"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/httpd/consistent"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Value is a collection of all database instances.
	TSDBCommonNodeIdKey = "tsdb-common-node-"
	// Value is a collection of all available Cluster, every item is key of Cluster
	TSDBClustersKey = "tsdb-available-clusters"
	// master key: -master; common node: -node
	TSDBWorkKey = "tsdb-work-Cluster-"
	//common key node-id, master key node-master-id
	TSDBWorkNode               = "tsdb-Cluster-"
	TSDBRecruitClustersKey     = "tsdb-recruit-clusters"
	TSDBClusterAutoIncrementId = "tsdb-Cluster-auto-increment-id"
	TSDBNodeAutoIncrementId    = "tsdb-node-auto-increment-id"
	TSDBClassAutoIncrementId   = "tsdb-class-auto-increment-id"
	TSDBClassesInfo            = "tsdb-classes-info"
	TSDBClassId                = "tsdb-class-"
	// Cluster-id
	TSDBClassNode = "tsdb-cla-"
	TSDBDatabase  = "tsdb-databases"
	TSDBcq        = "tsdb-cq"
	TSDBUsers     = "tsdb-users"
	// default class limit
	DefaultClassLimit       = 3
	DefaultClusterNodeLimit = 3
)

var Once sync.Once

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
		DropUser(name string) error
		DropDatabase(name string) error
		UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
		CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
		DropRetentionPolicy(database, name string) error
		DropContinuousQuery(database, name string) error
		CreateUser(name, password string, admin bool) (meta.User, error)
	}
	closed        bool
	mu            sync.Mutex
	Logger        *zap.Logger
	subscriptions []meta.SubscriptionInfo

	httpd      *httpd.Service
	store      *tsdb.Store
	etcdConfig EtcdConfig
	httpConfig httpd.Config
	udpConfig  udp.Config
	cli        *clientv3.Client
	lease      *clientv3.LeaseGrantResponse
	// local class's measurements
	measurement map[string]interface{}
	// other class's measurement
	otherMeasurement map[string]uint64
	// if there is point transfer to class, will need ip
	classIpMap map[uint64][]string
	// latest classes info
	classes     *Classes
	ch          *consistent.Consistent
	classDetail *ClassDetail

	masterNode   *Node
	clusterNodes *[]Node
	rpcQuery     *RpcService
	ip           string
	Cluster      bool
	dbsMu        sync.RWMutex
	databases    Databases
	latestUsers  Users
}

func NewService(store *tsdb.Store, etcdConfig EtcdConfig, httpConfig httpd.Config, udpConfig udp.Config,
	mapper *query.ShardMapper) *Service {
	nodes := make([]Node, 0)
	s := &Service{
		Logger:       zap.NewNop(),
		closed:       true,
		store:        store,
		etcdConfig:   etcdConfig,
		httpConfig:   httpConfig,
		udpConfig:    udpConfig,
		classDetail:  &ClassDetail{},
		clusterNodes: &nodes,
		rpcQuery:     NewRpcService(mapper, &nodes),
		Cluster:      false,
	}
	return s
}
func (s *Service) SetHttpdService(httpd *httpd.Service) {
	s.httpd = httpd
}

// 获取集群及物理节点的自增id
func (s *Service) GetLatestID(key string) (uint64, error) {
	cli := s.cli
	latestClusterId, err := cli.Get(context.Background(), key)
	if err != nil {
		return 0, err
	}
	if latestClusterId.Count == 0 {
		_, err = cli.Put(context.Background(), key, "0")
	}
getClusterId:
	latestClusterId, err = cli.Get(context.Background(), key)
	cmp := clientv3.Compare(clientv3.Value(key), "=", string(latestClusterId.Kvs[0].Value))
	clusterId, err := ByteToUint64(latestClusterId.Kvs[0].Value)
	if err != nil {
		return 0, err
	}
	clusterId++
	put := clientv3.OpPut(key, strconv.FormatUint(clusterId, 10))
	resp, err := cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if !resp.Succeeded {
		goto getClusterId
	}
	return clusterId, nil
}

func (s *Service) Open() error {
	var err error
	ip, err := GetLocalHostIp()
	if err != nil {
		return err
	}
	s.ip = ip
	s.cli, err = clientv3.New(clientv3.Config{
		Endpoints:            []string{s.etcdConfig.EtcdAddress},
		DialTimeout:          10 * time.Second,
		DialKeepAliveTime:    20 * time.Second,
		DialKeepAliveTimeout: 10 * time.Second,
	})
	if err != nil || s.cli == nil {
		s.Logger.Error("Get etcd client failed, error message " + err.Error())
		return err
	}
	lease, err := s.cli.Grant(context.Background(), 30)
	ch, err := s.cli.KeepAlive(context.Background(), lease.ID)
	s.lease = lease
	go func() {
		for resp := range ch {
			s.lease.ID = resp.ID
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.measurement = make(map[string]interface{})
	s.otherMeasurement = make(map[string]uint64)
	s.classIpMap = make(map[uint64][]string)
	s.Logger.Info("Starting register for ETCD Service...")
	err = s.registerToCommonNode()
	if err != nil {
		return err
	}
	err = s.joinClusterOrCreateCluster()
	if err != nil {
		return err
	}
	err = s.joinClass()
	if err != nil {
		return err
	}
	go s.watchDatabasesInfo()
	go s.watchContinuesQuery()
	go s.processNewMeasurement()
	go s.watchUsers()
	s.closed = false
	s.Logger.Info("Opened Service")
	// Cluster query rpc process
	s.rpcQuery.WithLogger(s.Logger)
	s.rpcQuery.MetaClient = s.MetaClient
	err = s.rpcQuery.Open()
	return err
}

func (s *Service) processNewMeasurement() {
	newPointChan := s.httpd.Handler.Balancing.GetNewPointChan()
	var filterPoint = func(point models.Point, measurementPoint *httpd.NewMeasurementPoint) error {
		if s.measurement[string(point.Name())] != nil {
			node := s.ch.Get(string(point.Key()))
			if node.Id == s.masterNode.Id {
				if err := s.httpd.Handler.PointsWriter.WritePoints(measurementPoint.DB, measurementPoint.Rp,
					1, measurementPoint.User, []models.Point{point}); err != nil {
					s.Logger.Error(err.Error())
				}
				return nil
			}
			httpClientP, err := s.httpd.Handler.Balancing.GetClient(node.Ip, "")
			var httpClient = *httpClientP
			// Cluster's master node crash
			if err = s.httpd.Handler.Balancing.ForwardPoint(httpClient, []models.Point{point}); err != nil {
				s.Logger.Error(err.Error())
			}
			if httpClient != nil {
				httpClient.Close()
			}
			return nil
		}
		if classId := s.otherMeasurement[string(point.Name())]; &classId != nil {
			httpClientP, err := s.httpd.Handler.Balancing.GetClientByClassId("InfluxForwardClient", classId)
			var httpClient = *httpClientP
			if err = s.httpd.Handler.Balancing.ForwardPoint(httpClient, []models.Point{point}); err != nil {
				s.Logger.Error(err.Error())
			}
			if httpClient != nil {
				httpClient.Close()
			}
		}
		return errors.New("")
	}
	for {
		newMeasurementPoint, ok := <-newPointChan
		if !ok {
			s.Logger.Warn("Http Service handler balance new measurement point channel closed")
		}
		// create new measurement
		failedPoints := make([]models.Point, 0)
	ProcessFailedPoint:
		for _, point := range newMeasurementPoint.Points {
			err := filterPoint(point, newMeasurementPoint)
			if err == nil {
				continue
			}
			if len(*s.classes) == 0 {
				s.Logger.Error("Meta Data is nil, please restart database")
				failedPoints = append(failedPoints, point)
				continue
			}
			resp, err := s.cli.Get(context.Background(), TSDBClassesInfo)
			if err != nil && resp.Count == 0 {
				ParseJson(resp.Kvs[0].Value, s.classes)
				var classes = *s.classes
				classes[0].Measurements = append(classes[0].Measurements, string(point.Name()))
				updateClass := classes[0]
				classes = append(classes[:1], classes[2:]...)
				classes = append(classes, updateClass)
				cmpClasses := clientv3.Compare(clientv3.Value(TSDBClassesInfo), "=", string(resp.Kvs[0].Value))
				putClasses := clientv3.OpPut(TSDBClassesInfo, ToJson(*s.classes))
				resp, err := s.cli.Txn(context.Background()).If(cmpClasses).Then(putClasses).Commit()
				if resp.Succeeded && err == nil {
					continue
				}
			}
			s.Logger.Error("Add measurement to class failed")
			failedPoints = append(failedPoints, point)
		}
		if len(failedPoints) != 0 {
			newMeasurementPoint.Points = failedPoints
			failedPoints = make([]models.Point, 0)
			goto ProcessFailedPoint
		}
	}
}

// watch classes info, update index
func (s *Service) watchClassesInfo() {
	classesResp, err := s.cli.Get(context.Background(), TSDBClassesInfo)
	s.CheckErrorExit("Get classes from etcd failed", err)
	if classesResp == nil || classesResp.Count == 0 {
		resp, err := s.cli.Get(context.Background(), TSDBWorkNode+strconv.FormatUint(s.MetaClient.Data().ClusterID, 10))
		var workClusterInfo WorkClusterInfo
		ParseJson(resp.Kvs[0].Value, &workClusterInfo)
		err = s.initClasses(&workClusterInfo)
		s.CheckErrorExit("Watch Classes info, init classes meta data failed", err)
	}
	classesWatch := s.cli.Watch(context.Background(), TSDBClassesInfo)
	for classesInfo := range classesWatch {
		for _, event := range classesInfo.Events {
			if bytes.Equal(event.PrevKv.Value, event.Kv.Value) {
				continue
			}
			s.buildMeasurementIndex(event.Kv.Value)
		}
	}
}

func (s *Service) watchUsers() {
	resp, err := s.cli.Get(context.Background(), TSDBUsers)
	s.CheckErrorExit("Get users info from etcd failed, stop watch users", err)
	if resp == nil || resp.Count == 0 {
		users := make(Users)
		_, err = s.cli.Put(context.Background(), TSDBUsers, ToJson(users))
		s.CheckErrorExit("Update databases failed, stop watch databases, error message", err)
	}
	usersCh := s.cli.Watch(context.Background(), TSDBUsers)
	for userResp := range usersCh {
		for _, event := range userResp.Events {
			if bytes.Equal(event.PrevKv.Value, event.Kv.Value) {
				continue
			}
			s.mu.Lock()
			ParseJson(event.Kv.Value, &s.latestUsers)
			s.mu.Unlock()
			noProcessed := s.latestUsers
			for _, user := range s.MetaClient.Data().Users {
				delete(noProcessed, user.Name)
				if user := s.latestUsers[user.Name]; &user != nil {
					continue
				}
				s.MetaClient.DropUser(user.Name)
			}
			for _, user := range noProcessed {
				s.MetaClient.CreateUser(user.Name, user.Password, user.Admin)
			}
		}
	}
}

// Every node of Cluster may create database and retention policy, So focus on and respond to changes in database
// information in metadata. So that each node's database and reservation policy data are consistent.
func (s *Service) watchDatabasesInfo() {
	databaseInfoResp, err := s.cli.Get(context.Background(), TSDBDatabase)
	s.CheckErrorExit("Get databases from etcd failed, error message", err)
	if databaseInfoResp == nil || databaseInfoResp.Count == 0 {
		databases := s.convertToDatabases(s.MetaClient.Databases())
		_, err := s.cli.Put(context.Background(), TSDBDatabase, ToJson(databases))
		s.CheckErrorExit("Update databases failed, stop watch databases, error message", err)
		s.dbsMu.Lock()
		s.databases = databases
		s.dbsMu.Unlock()
	}
	databaseInfo := s.cli.Watch(context.Background(), TSDBDatabase, clientv3.WithPrefix())
	for database := range databaseInfo {
		for _, db := range database.Events {
			if bytes.Equal(db.PrevKv.Value, db.Kv.Value) {
				continue
			}
			localDBInfo := make(map[string]map[string]Rp, len(s.MetaClient.Databases()))
			s.dbsMu.Lock()
			ParseJson(db.Kv.Value, &s.databases)
			s.dbsMu.Unlock()
			var noProcessedData = s.databases.Database
			// Add new database and retention policy
			for _, localDB := range s.MetaClient.Databases() {
				rps := s.databases.Database[localDB.Name]
				if rps == nil {
					_ = s.MetaClient.DropDatabase(localDB.Name)
					continue
				}
				// Local information is up to date.
				rpInfo := make(map[string]Rp, len(localDB.RetentionPolicies))
				for _, localRP := range localDB.RetentionPolicies {
					if s.subscriptions == nil || len(s.subscriptions) == 0 {
						s.subscriptions = localRP.Subscriptions
					}
					latestRP := rps[localRP.Name]
					if &latestRP == nil {
						_ = s.MetaClient.DropRetentionPolicy(localDB.Name, localRP.Name)
						continue
					}
					if latestRP.NeedUpdate {
						_ = s.MetaClient.UpdateRetentionPolicy(localDB.Name, localRP.Name, &meta.RetentionPolicyUpdate{
							Duration:           &latestRP.Duration,
							ReplicaN:           &latestRP.Replica,
							ShardGroupDuration: &latestRP.ShardGroupDuration,
						}, false)
					}
					// Save RP that has been completed.
					rpInfo[localRP.Name] = latestRP
					// Delete RP that has been completed
					delete(rps, localRP.Name)
				}
				// rps will only save new rp
				for _, value := range rps {
					_, _ = s.MetaClient.CreateRetentionPolicy(localDB.Name, &meta.RetentionPolicySpec{
						Name:               value.Name,
						ReplicaN:           &value.Replica,
						Duration:           &value.Duration,
						ShardGroupDuration: value.ShardGroupDuration,
					}, false)
					for _, sub := range s.subscriptions {
						_ = s.MetaClient.CreateSubscription(localDB.Name, value.Name, sub.Name, sub.Mode, sub.Destinations)
					}
				}
				// save DB that has been completed
				localDBInfo[localDB.Name] = rpInfo
				delete(noProcessedData, localDB.Name)
			}
			// add new database
			for key, value := range noProcessedData {
				_, err = s.MetaClient.CreateDatabase(key)
				for _, rpValue := range value {
					_, err = s.MetaClient.CreateRetentionPolicy(key, &meta.RetentionPolicySpec{
						Name:               rpValue.Name,
						ReplicaN:           &rpValue.Replica,
						Duration:           &rpValue.Duration,
						ShardGroupDuration: rpValue.ShardGroupDuration,
					}, false)
					for _, sub := range s.subscriptions {
						err = s.MetaClient.CreateSubscription(key, rpValue.Name, sub.Name, sub.Mode, sub.Destinations)
					}
				}
			}
			if err != nil {
				s.Logger.Error("create new database error !")
				s.Logger.Error(err.Error())
			}
		}
	}
}

func (s *Service) CheckErrorExit(msg string, err error) {
	if err != nil {
		s.Logger.Error(msg, zap.Error(err))
		os.Exit(1)
		return
	}
}
func (s *Service) CheckErrPrintLog(mag string, err error) {
	if err != nil {
		s.Logger.Error(mag, zap.Error(err))
	}
}

func (s *Service) watchContinuesQuery() {
	cqResp, err := s.cli.Get(context.Background(), TSDBcq)
	s.CheckErrorExit("Get continues query from etcd failed, stop watch cq, error message", err)
	if cqResp == nil || cqResp.Count == 0 {
		cqs := make(map[string][]meta.ContinuousQueryInfo, len(s.MetaClient.Databases()))
		for _, database := range s.MetaClient.Databases() {
			cqs[database.Name] = database.ContinuousQueries
		}
		_, err := s.cli.Put(context.Background(), TSDBcq, ToJson(cqs))
		s.CheckErrorExit("Initial Meta Data Continues Query failed, error message: ", err)
	}
	cqInfo := s.cli.Watch(context.Background(), TSDBcq, clientv3.WithPrefix())
	for cq := range cqInfo {
		for _, ev := range cq.Events {
			if bytes.Equal(ev.PrevKv.Value, ev.Kv.Value) {
				continue
			}
			var latestCqs Cqs
			ParseJson(ev.Kv.Value, &latestCqs)
			noProcessedCq := latestCqs
			for _, db := range s.MetaClient.Databases() {
				cqs := latestCqs[db.Name]
				if cqs == nil {
					for _, tempCq := range db.ContinuousQueries {
						s.MetaClient.DropContinuousQuery(db.Name, tempCq.Name)
					}
					delete(noProcessedCq, db.Name)
					continue
				}
			LocalCQ:
				for _, localCq := range db.ContinuousQueries {
					for i, latestCq := range cqs {
						if latestCq.Name == localCq.Name {
							noProcessedCq[db.Name] = append(noProcessedCq[db.Name][0:i], noProcessedCq[db.Name][i+1:]...)
							continue LocalCQ
						}
					}
					// If local cq don't belong latest, will delete local cq
					s.MetaClient.DropContinuousQuery(db.Name, localCq.Name)
				}
			}
			// noProcessedCq is new cq
			for newDB, newCqs := range noProcessedCq {
				for _, newCq := range newCqs {
					s.MetaClient.CreateContinuousQuery(newDB, newCq.Name, newCq.Query)
				}
			}
		}
	}
}

func (s *Service) GetLatestDatabaseInfo() (*Databases, error) {
	var databases Databases
	databasesInfoResp, err := s.cli.Get(context.Background(), TSDBDatabase)
	if err != nil {
		return nil, err
	}
	ParseJson(databasesInfoResp.Kvs[0].Value, &databases)
	return &databases, nil
}
func (s *Service) PutMetaDataForEtcd(data interface{}, key string) error {
	_, err := s.cli.Put(context.Background(), key, ToJson(data))
	return err
}

func (s *Service) convertToDatabases(databaseInfo []meta.DatabaseInfo) Databases {
	var databases = make(map[string]map[string]Rp, len(databaseInfo))
	for _, db := range databaseInfo {
		var rps = make(map[string]Rp, len(db.RetentionPolicies))
		for _, rp := range db.RetentionPolicies {
			rps[rp.Name] = Rp{
				Name:               rp.Name,
				Replica:            rp.ReplicaN,
				Duration:           rp.Duration,
				ShardGroupDuration: rp.ShardGroupDuration,
			}
		}
		databases[db.Name] = rps
	}
	return Databases{
		Database: databases,
	}
}
func (s *Service) clearHistoryData() {
	for _, db := range s.MetaClient.Databases() {
		s.store.DeleteDatabase(db.Name)
	}
}
func (s *Service) setMetaDataNodeId(nodeId uint64) error {
	originMetaData := s.MetaClient.Data()
	originMetaData.NodeID = nodeId
	return s.MetaClient.SetData(&originMetaData)
}
func (s *Service) putClusterNode(node Node, cluster WorkClusterInfo, clusterKey string, cmp clientv3.Cmp) bool {
	nodeKey := clusterKey + "-node-"
	putNode := clientv3.OpPut(nodeKey+strconv.FormatUint(node.Id, 10), ToJson(node), clientv3.WithLease(s.lease.ID))
	putWorkCluster := clientv3.OpPut(clusterKey, ToJson(cluster))
	var resp *clientv3.TxnResponse
	var err error
	originMasterNodeKey := clusterKey + "-master"
	if cluster.MasterUsable {
		resp, err = s.cli.Txn(context.Background()).If(cmp).Then(putWorkCluster, putNode).Commit()
	} else {
		putMasterNode := clientv3.OpPut(originMasterNodeKey, ToJson(node), clientv3.WithLease(s.lease.ID))
		resp, err = s.cli.Txn(context.Background()).If(cmp).Then(putWorkCluster, putNode, putMasterNode).Commit()
	}
	if resp.Succeeded && err == nil {
		var metaData = s.MetaClient.Data()
		metaData.ClassID = cluster.ClassId
		metaData.ClusterID = cluster.ClusterId
		// if class id is null, it is exception
		err = s.MetaClient.SetData(&metaData)
		var masterNode Node
		masterNodeResp, err := s.cli.Get(context.Background(), originMasterNodeKey)
		s.CheckErrorExit("Join Cluster Success, but master node dont't exist", err)
		ParseJson(masterNodeResp.Kvs[0].Value, &masterNode)
		masterNode.ClusterId = cluster.ClusterId
		s.masterNode = &masterNode
		nodeResp, err := s.cli.Get(context.Background(), nodeKey)
		s.CheckErrorExit("Join Cluster Success, but common node get failed", err)
		nodes := make([]Node, nodeResp.Count)
		for _, kv := range nodeResp.Kvs {
			var node Node
			ParseJson(kv.Value, &node)
			nodes = append(nodes, node)
		}
		s.clusterNodes = &nodes
		return true
	}
	return false
}
func (s *Service) joinClusterOrCreateCluster() error {
	s.Logger.Info("System will join origin Cluster or create new Cluster")
RetryJoinOriginalCluster:
	// Try to connect the original Cluster.
	originClusterKey := TSDBWorkKey + strconv.FormatUint(s.MetaClient.Data().ClusterID, 10)
	workClusterResp, err := s.cli.Get(context.Background(), originClusterKey)
	if err != nil {
		return err
	}
	if workClusterResp.Count != 0 && err == nil {
		var originWorkCluster WorkClusterInfo
		ParseJson(workClusterResp.Kvs[0].Value, &originWorkCluster)
		clusterNodeResp, err := s.cli.Get(context.Background(), originClusterKey+"-node")
		if err != nil {
			return err
		}
		if int(clusterNodeResp.Count) < originWorkCluster.Limit {
			originWorkCluster.Number = int(clusterNodeResp.Count)
			originWorkCluster.Number++
			node := Node{
				Id:      s.MetaClient.Data().NodeID,
				Host:    s.ip + s.httpConfig.BindAddress,
				UdpHost: s.ip + s.udpConfig.BindAddress,
				Ip:      s.ip,
			}
			cmpWorkCluster := clientv3.Compare(clientv3.Value(originClusterKey), "=", string(workClusterResp.Kvs[0].Value))
			joinSuccess := s.putClusterNode(node, originWorkCluster, originClusterKey, cmpWorkCluster)
			if !joinSuccess {
				goto RetryJoinOriginalCluster
			}
		}
	} else {
		recruitResp, err := s.cli.Get(context.Background(), TSDBRecruitClustersKey)
		if err != nil {
			return err
		}
		var recruitCluster RecruitClusters
		// if recruitCluster key non-existent
		if recruitResp.Count == 0 {
			recruitCluster = RecruitClusters{
				Number:     0,
				ClusterIds: make([]uint64, 1),
			}
			_, err = s.cli.Put(context.Background(), TSDBRecruitClustersKey, ToJson(recruitCluster))
			// create Cluster
			err = s.createCluster()
			s.CheckErrorExit("Try create Cluster failed, error message is", err)
			return nil
		}
		ParseJson(recruitResp.Kvs[0].Value, recruitCluster)
		if recruitCluster.Number > 0 {
			for _, id := range recruitCluster.ClusterIds {
				err = s.joinRecruitClusterByClusterId(id)
				if err == nil {
					return nil
				}
			}
			return s.createCluster()
		}
		for _, kv := range recruitResp.Kvs {
			ParseJson(kv.Value, &recruitCluster)
			// Logical nodes need nodes to join it.

			err = s.createCluster()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Join Cluster failed, create new Cluster
func (s *Service) createCluster() error {
	recruit, err := s.cli.Get(context.Background(), TSDBRecruitClustersKey)
	if err != nil {
		return err
	}
	var recruitCluster RecruitClusters
	ParseJson(recruit.Kvs[0].Value, &recruitCluster)
	allClustersInfo := AvailableClusterInfo{
		Clusters: make([]WorkClusterInfo, 0),
	}
	clusterInitial := false
RetryCreate:
	allClusterInfoResp, err := s.cli.Get(context.Background(), TSDBClustersKey)
	var nodes []Node
	ops := make([]clientv3.Op, 0)
	if allClusterInfoResp.Count > 0 {
		ParseJson(allClusterInfoResp.Kvs[0].Value, &allClustersInfo)
		err, allClustersInfo.Clusters, nodes = s.appendNewWorkCluster(allClustersInfo.Clusters)
	} else {
		err, nodes = s.initAllClusterInfo(&allClustersInfo)
		clusterInitial = true
	}
	if err != nil {
		return err
	}
	latestClusterInfo := allClustersInfo.Clusters[len(allClustersInfo.Clusters)-1]
	recruitCluster.ClusterIds = append(recruitCluster.ClusterIds, latestClusterInfo.ClusterId)
	recruitCluster.Number++

	workClusterInfo := allClustersInfo.Clusters[len(allClustersInfo.Clusters)-1]
	clusterKey := TSDBWorkKey + strconv.FormatUint(latestClusterInfo.ClusterId, 10)

	ops = append(ops, clientv3.OpPut(clusterKey+"-master", ToJson(nodes[0])))
	ops = append(ops, clientv3.OpPut(TSDBClustersKey, ToJson(allClustersInfo)))
	ops = append(ops, clientv3.OpPut(TSDBRecruitClustersKey, ToJson(recruitCluster)))
	ops = append(ops, clientv3.OpPut(clusterKey, ToJson(workClusterInfo)))
	clusterNodeKey := clusterKey + "-node"
	for _, node := range nodes {
		ops = append(ops, clientv3.OpPut(clusterNodeKey, ToJson(node)))
	}

	// Transaction creation Cluster
	cmpAllCluster := clientv3.Compare(clientv3.CreateRevision(TSDBClustersKey), "=", 0)
	cmpRecruit := clientv3.Compare(clientv3.Value(TSDBRecruitClustersKey), "=", string(recruit.Kvs[0].Value))
	var resp *clientv3.TxnResponse
	if clusterInitial {
		resp, err = s.cli.Txn(context.Background()).If(cmpAllCluster, cmpRecruit).Then(ops...).Commit()
	} else {
		resp, err = s.cli.Txn(context.Background()).If(cmpRecruit).Then(ops...).Commit()
	}
	if !resp.Succeeded || err != nil {
		goto RetryCreate
	}
	go s.watchWorkCluster(latestClusterInfo.ClusterId)
	// if class is nil, create class
	return s.initClasses(&workClusterInfo)
}

func (s *Service) initClasses(workClusterInfo *WorkClusterInfo) error {
RetryCreateClass:
	classResp, err := s.cli.Get(context.Background(), TSDBClassesInfo)
	if err != nil || classResp == nil || classResp.Count == 0 {
		classIdResp, err := s.GetLatestID(TSDBClassAutoIncrementId)
		if err != nil {
			return err
		}
		classes := []Class{{
			ClassId:           classIdResp,
			Limit:             DefaultClassLimit,
			ClusterIds:        []uint64{workClusterInfo.ClusterId},
			Measurements:      make([]string, 0),
			DeleteMeasurement: make([]string, 0),
			NewMeasurement:    make([]string, 0),
		}}
		class := ClassDetail{
			Clusters:     []WorkClusterInfo{*workClusterInfo},
			Measurements: make([]string, 0),
		}
		metaData := s.MetaClient.Data()
		metaData.ClassID = classIdResp
		err = s.MetaClient.SetData(&metaData)
		if err != nil {
			s.Logger.Error("When create Cluster and create class, set meta data failed")
			s.Logger.Error(err.Error())
		}
		classesCamp := clientv3.Compare(clientv3.CreateRevision(TSDBClassesInfo), "=", 0)
		classDetailCamp := clientv3.Compare(clientv3.CreateRevision(TSDBClassId+strconv.FormatUint(classIdResp,
			10)), "=", 0)
		classesOp := clientv3.OpPut(TSDBClassesInfo, ToJson(classes))
		classDetailOp := clientv3.OpPut(TSDBClassId+strconv.FormatUint(classIdResp, 10), ToJson(class))
		resp, err := s.cli.Txn(context.Background()).If(classesCamp, classDetailCamp).Then(classesOp, classDetailOp).Commit()
		if !resp.Succeeded || err != nil {
			goto RetryCreateClass
		}
		// put alive Cluster key with lease
		_, err = s.cli.Put(context.Background(), TSDBClassNode+strconv.FormatUint(classIdResp, 10)+
			"-Cluster-"+strconv.FormatUint(workClusterInfo.ClusterId, 10), ToJson(class), clientv3.WithLease(s.lease.ID))
		return err
	}
	return nil
}

func (s *Service) appendNewWorkCluster(workClusterInfo []WorkClusterInfo) (error, []WorkClusterInfo, []Node) {
	clusterId, err := s.GetLatestID(TSDBClusterAutoIncrementId)
	s.CheckErrorExit("Get cluster auto increment id faied ", err)
	nodes := []Node{{
		Id:      s.MetaClient.Data().NodeID,
		Host:    s.ip + s.httpConfig.BindAddress,
		UdpHost: s.ip + s.udpConfig.BindAddress,
	}}
	workClusterInfo = append(workClusterInfo, WorkClusterInfo{
		Series:     make([]string, 0),
		ClusterId:  clusterId,
		Limit:      3,
		Number:     len(nodes),
		MasterId:   nodes[0].Id,
		MasterHost: nodes[0].Host,
	})
	return nil, workClusterInfo, nodes
}

func (s *Service) initAllClusterInfo(allClusterInfo *AvailableClusterInfo) (error, []Node) {
	var workClusterInfo = make([]WorkClusterInfo, 0, 1)
	err, workClusterInfo, nodes := s.appendNewWorkCluster(workClusterInfo)
	if err != nil {
		return err, nil
	}
	*allClusterInfo = AvailableClusterInfo{
		Clusters: workClusterInfo,
	}
	return nil, nodes
}

// Watch node of the Cluster, change Cluster information and change subscriber
func (s *Service) watchWorkCluster(clusterId uint64) {
	clusterIdStr := strconv.FormatUint(clusterId, 10)
	clusterKey := TSDBWorkNode + clusterIdStr
	workAllNode := s.cli.Watch(context.Background(), clusterKey, clientv3.WithPrefix())
	var node Node
	for nodeEvent := range workAllNode {
		for _, ev := range nodeEvent.Events {
			ParseJson(ev.PrevKv.Value, &node)
			if mvccpb.DELETE == ev.Type {
				for _, db := range s.MetaClient.Databases() {
					for _, rp := range db.RetentionPolicies {
						subscriberName := db.Name + rp.Name + node.Host
						err := s.MetaClient.DropSubscription(db.Name, rp.Name, subscriberName)
						s.CheckErrPrintLog("Watch cluster "+clusterIdStr+"deleted, Drop subscription failed", err)
					}
				}
				// election master node
				if strings.Contains(string(ev.Kv.Key), "master") {
					s.masterNode = nil
					masterKey := clusterKey + "-master"
					// choice master
				ChoiceMaster:
					masterResp, err := s.cli.Get(context.Background(), masterKey, clientv3.WithPrefix())
					s.CheckErrPrintLog("Watch work cluster, get master node failed", err)
					if masterResp.Count == 0 {
						cmp := clientv3.Compare(clientv3.CreateRevision(masterKey), "=", 0)
						node := Node{
							Id:        s.MetaClient.Data().NodeID,
							Host:      s.ip + s.httpConfig.BindAddress,
							UdpHost:   s.ip + s.udpConfig.BindAddress,
							ClusterId: clusterId,
						}
						opPut := clientv3.OpPut(masterKey, ToJson(node), clientv3.WithLease(s.lease.ID))
						opMasterResp, err := s.cli.Txn(context.Background()).If(cmp).Then(opPut).Commit()
						if opMasterResp.Succeeded && err != nil {
							s.masterNode = &node
							// If cluster's master node crash, class's class detail be created by the node will be delete
							// New master node need create class detail again
							// class detail is array, express the class's cluster member
							metaData := s.MetaClient.Data()
							node.ClusterId = clusterId
							_, err = s.cli.Put(context.Background(), TSDBClassNode+strconv.FormatUint(metaData.ClassID, 10)+
								"-Cluster-"+strconv.FormatUint(metaData.ClusterID, 10), ToJson(node),
								clientv3.WithLease(s.lease.ID))
							continue
						}
						goto ChoiceMaster
					}
				}
				continue
			}
			if mvccpb.PUT == ev.Type {
				for _, db := range s.MetaClient.Databases() {
					for _, rp := range db.RetentionPolicies {
						subscriberName := db.Name + rp.Name + node.Host
						destination := []string{"http://" + node.Host}
						err := s.MetaClient.CreateSubscription(db.Name, rp.Name, subscriberName, "All", destination)
						s.CheckErrPrintLog("Watch adding node event, create subscription failed ", err)
					}
				}
				if strings.Contains(string(ev.Kv.Key), "master") {
					port, _ := strconv.Atoi(string([]rune(s.httpConfig.BindAddress)[1 : len(s.httpConfig.BindAddress)-1]))
					s.httpd.Handler.Balancing.SetMasterNode(consistent.NewNode(node.Id, node.Host, port,
						"host_"+strconv.FormatUint(clusterId, 10), 1))
				}
			}
		}
	}
}

// Cluster is logical node and min uint, they made up class, working Cluster must be class member
// Become a Cluster of worker, add to classes info, create or join the original class.
func (s *Service) joinClass() error {
RetryAddClasses:
	classesResp, err := s.cli.Get(context.Background(), TSDBClassesInfo)
	var classes []Class
	// etcd don't contain TSDBClassesInfo
	if err == nil && classesResp.Count > 0 {
		ParseJson(classesResp.Kvs[0].Value, &classes)
	}
	var addNewClass = func() error {
		class := Class{
			Limit: DefaultClassLimit,
		}
		class.ClassId, err = s.GetLatestID(TSDBClassAutoIncrementId)
		if err != nil {
			return err
		}
		class.ClusterIds = make([]uint64, 1)
		class.ClusterIds = append(class.ClusterIds, s.MetaClient.Data().ClusterID)
		classes = append(classes, class)
		metaData := s.MetaClient.Data()
		metaData.ClassID = class.ClassId
		err = s.MetaClient.SetData(&metaData)
		return err
	}
	// create new classes info
	if classes == nil {
		classes = make([]Class, 0)
		err = addNewClass()
		if err != nil {
			return err
		}
		cmpClasses := clientv3.Compare(clientv3.CreateRevision(TSDBClassesInfo), "=", 0)
		opPutClasses := clientv3.OpPut(TSDBClassesInfo, ToJson(classes))
		putResp, _ := s.cli.Txn(context.Background()).If(cmpClasses).Then(opPutClasses).Commit()
		if !putResp.Succeeded {
			return errors.New("Put Classes failed ")
		}
	} else {
		index := -1
		clusterProcessed := false
		// join exist classes or create new class
		for classIndex, class := range classes {
			for _, id := range class.ClusterIds {
				if id == s.MetaClient.Data().ClusterID {
					clusterProcessed = true
					break
				}
			}
			if clusterProcessed {
				break
			}
			if index == -1 {
				if class.Limit > len(class.ClusterIds) {
					index = classIndex
					break
				}
			}
		}
		// Class's clusters don't have local Cluster,
		if !clusterProcessed && index == -1 {
			err = addNewClass()
			err = s.putClasses(classesResp, classes)
		}
		if !clusterProcessed && index != -1 {
			classes[index].ClusterIds = append(classes[index].ClusterIds, s.MetaClient.Data().ClusterID)
			err = s.putClasses(classesResp, classes)
			if err == nil {
				metaData := s.MetaClient.Data()
				metaData.ClassID = classes[index].ClassId
				err = s.MetaClient.SetData(&metaData)
			}
		}
	}
	if err != nil {
		s.Logger.Error("Create Class for etcd error, error message" + err.Error())
		goto RetryAddClasses
	}
	// Process Class Detail
	var workClusterInfo WorkClusterInfo
	clusterInfoResp, err := s.cli.Get(context.Background(), TSDBWorkKey+
		strconv.FormatUint(s.MetaClient.Data().ClusterID, 10))
	if err == nil && clusterInfoResp.Count > 0 {
		ParseJson(clusterInfoResp.Kvs[0].Value, &workClusterInfo)
	} else {
		return errors.New("The MetaData Service don't contain the Cluster, Cluster id is " +
			strconv.FormatUint(s.MetaClient.Data().ClusterID, 10))
	}
	classResp, err := s.cli.Get(context.Background(), TSDBClassId+
		strconv.FormatUint(s.MetaClient.Data().ClassID, 10))
	if err != nil || classResp.Count == 0 {
		s.classDetail = &ClassDetail{
			Measurements: make([]string, 0),
			Clusters:     []WorkClusterInfo{workClusterInfo},
		}

		if err = s.putClassDetail(); err == nil {
			go s.watchClassNode()
		}
		return err
	}
	ParseJson(classResp.Kvs[0].Value, s.classDetail)
	s.classDetail.Clusters = append(s.classDetail.Clusters, workClusterInfo)
	err = s.putClassDetail()
	s.buildConsistentHash()
	go s.watchClassNode()
	return err
}

func (s *Service) watchClassNode() {
	classNodeEventChan := s.cli.Watch(context.Background(), TSDBClassNode+strconv.
		FormatUint(s.MetaClient.Data().ClassID, 10)+"-Cluster", clientv3.WithPrefix())
	for classNodeInfo := range classNodeEventChan {
		for _, event := range classNodeInfo.Events {
			if event.Type == mvccpb.DELETE {
				// every node need update consistent hash
				var deletedClusterMasterNode Node
				ParseJson(event.PrevKv.Value, &deletedClusterMasterNode)
				for i, c := range s.classDetail.Clusters {
					if c.ClusterId == deletedClusterMasterNode.ClusterId {
						s.classDetail.Clusters = append(s.classDetail.Clusters[0:i], s.classDetail.Clusters[i+1:]...)
						break
					}
				}
				s.buildConsistentHash()
				// Class's Cluster[0] master node update class detail
				metaData := s.MetaClient.Data()
				if s.classDetail.Clusters[0].ClusterId == metaData.ClusterID && s.masterNode.Id == metaData.NodeID {
					_, err := s.cli.Put(context.Background(), TSDBClassId+strconv.FormatUint(s.MetaClient.Data().ClassID, 10),
						ToJson(*s.classDetail))
					s.Logger.Error("Get class detail update event, update etcd failed, error message: " + err.Error())
					var classes = *s.classes
					classIndex := -1
					clusterIndex := -1
				Class:
					for ci, c := range classes {
						if c.ClassId == s.MetaClient.Data().ClassID {
							for i, cluster := range c.ClusterIds {
								if cluster == deletedClusterMasterNode.ClusterId {
									clusterIndex = i
									classIndex = ci
									break Class
								}
							}

						}
					}
					classes[classIndex].ClusterIds = append(classes[classIndex].ClusterIds[0:clusterIndex],
						classes[classIndex].ClusterIds[clusterIndex+1:]...)
					_, err = s.cli.Put(context.Background(), TSDBClassesInfo, ToJson(classes))
					if err != nil {
						s.Logger.Error("Get class detail update event, update meta data classes failed, error message: " +
							err.Error())
					}
				}
			}
		}
	}
}

func (s *Service) putClasses(classesResp *clientv3.GetResponse, classes []Class) error {
	cmpClasses := clientv3.Compare(clientv3.Value(TSDBClassesInfo), "=", string(classesResp.Kvs[0].Value))
	opPutClasses := clientv3.OpPut(TSDBClassesInfo, ToJson(classes))
	putResp, _ := s.cli.Txn(context.Background()).If(cmpClasses).Then(opPutClasses).Commit()
	if !putResp.Succeeded {
		return errors.New("Put Classes failed ")
	}
	return nil
}

func (s *Service) putClassDetail() error {
	cmpClassDetail := clientv3.Compare(clientv3.Value(TSDBClassId+strconv.FormatUint(s.MetaClient.Data().ClassID,
		10)), "=", string(ToJson(*s.classDetail)))
	opPutClassDetail := clientv3.OpPut(TSDBClassId+strconv.FormatUint(s.MetaClient.Data().ClassID, 10),
		ToJson(*s.classDetail))
	resp, err := s.cli.Txn(context.Background()).If(cmpClassDetail).Then(opPutClassDetail).Commit()
	if resp.Succeeded {
		// If local node is local Cluster's master node, local node will put Cluster key of the class
		if s.masterNode.Id == s.MetaClient.Data().NodeID {
			_, err = s.cli.Put(context.Background(), TSDBClassNode+strconv.FormatUint(s.MetaClient.Data().ClassID, 10)+
				"-Cluster-"+strconv.FormatUint(s.classDetail.Clusters[0].ClusterId, 10), ToJson(s.masterNode),
				clientv3.WithLease(s.lease.ID))
		}
		return nil
	}
	return err
}

func (s *Service) registerToCommonNode() error {
	nodeId, err := s.GetLatestID(TSDBNodeAutoIncrementId)
	s.CheckErrorExit("Get node id failed", err)
	nodeKey := TSDBCommonNodeIdKey + strconv.FormatUint(nodeId, 10)
	node := Node{
		Id:      nodeId,
		Host:    s.ip + s.httpConfig.BindAddress,
		UdpHost: s.ip + s.udpConfig.BindAddress,
	}
	cmp := clientv3.Compare(clientv3.CreateRevision(nodeKey), "=", 0)
	opPut := clientv3.OpPut(nodeKey, ToJson(node))
RegisterNode:
	// Register with etcd
	resp, err := s.cli.Txn(context.Background()).If(cmp).Then(opPut).Commit()
	s.CheckErrorExit("Register node failed ", err)
	if !resp.Succeeded {
		goto RegisterNode
	}
	return nil
}

func (s *Service) joinRecruitClusterByClusterId(clusterId uint64) error {
	// 不使用RecruitClusterKey，计划通过worker来判断是否允许加入，
	workClusterKey := TSDBWorkKey + strconv.FormatUint(clusterId, 10)
RetryJoinTarget:
	var joinSuccess = false
	clusterMetaResp, metaErr := s.cli.Get(context.Background(), workClusterKey)
	clusterNodeResp, err := s.cli.Get(context.Background(), workClusterKey+"-node")
	if err != nil || metaErr != nil {
		return errors.New("Get cluster info failed, meta error message:" + metaErr.Error() + "err message :" + err.Error())
	}
	var workClusterInfo WorkClusterInfo
	if clusterNodeResp.Count > 0 {
		ParseJson(clusterNodeResp.Kvs[0].Value, &workClusterInfo)
		if workClusterInfo.Number < workClusterInfo.Limit {
			cmp := clientv3.Compare(clientv3.Value(workClusterKey), "=", string(clusterMetaResp.Kvs[0].Value))
			node := Node{
				Id:      s.MetaClient.Data().NodeID,
				Host:    s.ip + s.httpConfig.BindAddress,
				UdpHost: s.ip + s.udpConfig.BindAddress,
			}
			joinSuccess = s.putClusterNode(node, workClusterInfo, workClusterKey, cmp)
			if !joinSuccess {
				goto RetryJoinTarget
			}
		}
	} else {
		s.cli.Delete(context.Background(), workClusterKey)
	}
	if joinSuccess {
		metaData := s.MetaClient.Data()
		metaData.ClusterID = clusterId
		err = s.MetaClient.SetData(&metaData)
		go s.watchWorkCluster(clusterId)
		return err
	}
	return errors.New("Join Cluster" + strconv.FormatUint(clusterId, 10) + "failed")
}

// close closes the existing channel writers.
func (s *Service) close(wg *sync.WaitGroup) {
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()

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

	s.Logger.Info("Closed Service")
	return nil
}

// WithLogger sets the logger on the Service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("CoordinatorService", "Cluster"))
}

// Build Index for balance
// @param data is classes
func (s *Service) buildMeasurementIndex(data []byte) {
	ParseJson(data, s.classes)
	if s.otherMeasurement == nil {
		s.otherMeasurement = make(map[string]uint64, len(*s.classes))
	}
	for _, class := range *s.classes {
		if len(class.NewMeasurement) == 0 && len(class.DeleteMeasurement) == 0 {
			continue
		}
		// process local class
		if class.ClassId == s.MetaClient.Data().ClassID {
			if s.measurement == nil {
				s.measurement = make(map[string]interface{})
			}
			for _, measurementName := range class.Measurements {
				s.measurement[measurementName] = ""
			}
			if len(class.DeleteMeasurement) != 0 {
				for _, deleteMeasurement := range class.DeleteMeasurement {
					delete(s.measurement, deleteMeasurement)
				}
			}
			continue
		}
		// process other class
		for _, measurement := range class.Measurements {
			s.otherMeasurement[measurement] = class.ClassId
		}
		for _, deleteMeasurement := range class.DeleteMeasurement {
			delete(s.otherMeasurement, deleteMeasurement)
		}
		// process ip map index
		if classIp := s.classIpMap[class.ClassId]; classIp == nil || len(classIp) <= 1 {
			clusterNodeResp, err := s.cli.Get(context.Background(), TSDBWorkNode+strconv.FormatUint(class.ClusterIds[0], 10)+
				"-node", clientv3.WithPrefix())
			s.CheckErrPrintLog("Build Measurement Index failed", err)
			ipArray := make([]string, clusterNodeResp.Count)
			for _, kv := range clusterNodeResp.Kvs {
				var node Node
				ParseJson(kv.Value, &node)
				ipArray = append(ipArray, node.Host)
			}
			s.classIpMap[class.ClassId] = ipArray
		}
	}
	Once.Do(func() {
		s.httpd.Handler.Balancing.SetMeasurementMapIndex(s.measurement, s.otherMeasurement, s.classIpMap)
	})
}

func (s *Service) buildConsistentHash() {
	if s.ch == nil {
		s.ch = consistent.NewConsistent()
	}
	for _, classItem := range s.classDetail.Clusters {
		port, _ := strconv.Atoi(string([]rune(s.httpConfig.BindAddress)[1 : len(s.httpConfig.BindAddress)-1]))
		weight := 1
		s.ch.Add(consistent.NewNode(classItem.MasterId, classItem.MasterHost, port,
			"host_"+strconv.FormatUint(classItem.ClusterId, 10), weight))
	}
	Once.Do(func() {
		s.httpd.Handler.Balancing.SetConsistent(s.ch)
	})
}
