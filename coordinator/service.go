package coordinator

import (
	"context"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/promise"
	"github.com/influxdata/influxdb/pkg/slices"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/httpd/consistent"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"io"
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
	TSDBWorkKey                = "tsdb-work-cluster-"
	TSDBRecruitClustersKey     = "tsdb-recruit-clusters"
	TSDBClusterAutoIncrementId = "tsdb-cluster-auto-increment-id"
	TSDBNodeAutoIncrementId    = "tsdb-node-auto-increment-id"
	TSDBClassAutoIncrementId   = "tsdb-class-auto-increment-id"
	TSDBClassesInfo            = "tsdb-classes"
	TSDBClassId                = "tsdb-class-"
	// example tsdb-cla-1-cluster-1, describe class 1 has cluster 1
	TSDBClassNode = "tsdb-cla-"
	TSDBDatabase  = "tsdb-databases"
	TSDBcq        = "tsdb-cq"
	TSDBUsers     = "tsdb-users"
	// default class limit
	DefaultClassLimit = 3

	TSDBStatement                = "tsdb-statement-"
	TSDBStatementAutoIncrementId = "tsdb-auto-increment-statement-id"
)

var IgnoreDBS = []string{"_internal", "database"}

var one sync.Once

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
	// FirstKey: db, SecondKey: measurement local class's measurements
	measurement map[string]map[string]interface{}
	// FirstKey: db, SecondKey: measurement other class's measurement
	otherMeasurement map[string]map[string]uint64
	// if there is point transfer to class, will need ip
	classIpMap map[uint64][]string
	// latest classes info
	classes *Classes
	ch      *consistent.Consistent
	// key: master node ip of cluster
	classDetail map[string]Node

	// master node
	masterNode  *Node
	rpcQuery    *RpcService
	ip          string
	Cluster     bool
	dbsMu       sync.RWMutex
	databases   Databases
	latestUsers Users
}

func NewService(store *tsdb.Store, etcdConfig EtcdConfig, httpConfig httpd.Config, udpConfig udp.Config,
	mapper *query.ShardMapper) *Service {
	nodes := make([]Node, 0)
	s := &Service{
		Logger:           zap.NewNop(),
		closed:           true,
		store:            store,
		etcdConfig:       etcdConfig,
		httpConfig:       httpConfig,
		udpConfig:        udpConfig,
		rpcQuery:         NewRpcService(mapper, nodes),
		classDetail:      make(map[string]Node),
		Cluster:          false,
		classes:          new(Classes),
		measurement:      make(map[string]map[string]interface{}),
		otherMeasurement: make(map[string]map[string]uint64),
		classIpMap:       make(map[uint64][]string),
		masterNode:       new(Node),
		ch:               consistent.NewConsistent(),
	}
	return s
}
func (s *Service) SetHttpdService(httpd *httpd.Service) {
	s.httpd = httpd
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
	_, err = s.cli.KeepAlive(context.Background(), lease.ID)
	s.lease = lease
	s.mu.Lock()
	defer s.mu.Unlock()
	s.httpd.Handler.Balancing.SetMeasurementMapIndex(s.measurement, s.otherMeasurement, s.classIpMap)
	s.Logger.Info("Starting register for ETCD Service...")
	err = s.registerToCommonNode()
	if err != nil {
		return err
	}
	s.checkAvailable()
	s.checkRecruit()
	err = s.joinClusterOrCreateCluster()
	if err != nil {
		return err
	}
	err = s.joinClass()
	if err != nil {
		return err
	}
	go s.watchClassesInfo()
	go s.watchClassCluster()
	go s.watchDatabasesInfo()
	go s.watchContinuesQuery()
	go s.processNewMeasurement()
	go s.watchUsers()
	s.closed = false
	s.Logger.Info("Opened Coordinator Service")
	// Cluster query rpc process
	s.rpcQuery.WithLogger(s.Logger)
	s.rpcQuery.MetaClient = s.MetaClient
	err = s.rpcQuery.Open()
	return err
}
func (s *Service) isLive(clusterId uint64) bool {
	resp, err := s.cli.Get(context.Background(), TSDBWorkKey+strconv.FormatUint(clusterId, 10)+"-node",
		clientv3.WithPrefix())
	if err == nil && resp.Count > 0 {
		return true
	}
	return false
}
func (s *Service) checkRecruit() {
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBRecruitClustersKey)
	if err == nil && resp.Count > 0 {
		var recruit RecruitClusters
		ParseJson(resp.Kvs[0].Value, &recruit)
		clusterMap := make(map[uint64]interface{})
		for _, id := range recruit.ClusterIds {
			clusterMap[id] = ""
		}
		ids := make([]uint64, 0, len(clusterMap))
		for key := range clusterMap {
			if s.isLive(key) {
				ids = append(ids, key)
				continue
			}
			s.cli.Delete(context.Background(), TSDBWorkKey+strconv.FormatUint(key, 10))
		}
		recruit.Number = int32(len(ids))
		recruit.ClusterIds = ids
		cmp := clientv3.Compare(clientv3.Value(TSDBRecruitClustersKey), "=", string(resp.Kvs[0].Value))
		put := clientv3.OpPut(TSDBRecruitClustersKey, ToJson(recruit))
		txnResp, _ := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
		if txnResp == nil || !txnResp.Succeeded {
			goto Retry
		}
	}
}

func (s *Service) processNewMeasurement() {
	newPointChan := s.httpd.Handler.Balancing.GetNewPointChan()
	for {
		newMeasurementPoint, ok := <-newPointChan
		if !ok {
			s.Logger.Warn("Http Service handler balance new measurement point channel closed")
		}
		s.Logger.Info("Global create measurement ", zap.ByteString("name",
			newMeasurementPoint.Points[0].Name()))
		// create new measurement
		failedPoints := make([]models.Point, 0)
		for _, point := range newMeasurementPoint.Points {
			measurement := string(point.Name())
			mapValue := s.measurement[newMeasurementPoint.DB][measurement]
			classId := s.otherMeasurement[newMeasurementPoint.DB][measurement]
			// the point belong to new measurement
			if mapValue == nil && classId == 0 {
				if len(*s.classes) == 0 {
					s.Logger.Error("Meta Data is nil, please restart database")
					failedPoints = append(failedPoints, point)
					continue
				}
				resp, err := s.cli.Get(context.Background(), TSDBClassesInfo)
				if err == nil && resp.Count > 0 {
					ParseJson(resp.Kvs[0].Value, s.classes)
					var classes = *s.classes
					// balance measurement algorithm
					class := classes[0]
					ms := class.DBMeasurements[newMeasurementPoint.DB]
					if ms == nil {
						ms := []string{measurement}
						class.DBMeasurements[newMeasurementPoint.DB] = ms
					} else {
						ms = append(ms, measurement)
						class.DBMeasurements[newMeasurementPoint.DB] = ms
					}
					class.DBNewMeasure[newMeasurementPoint.DB] = []string{measurement}
					if len(classes) > 1 {
						classes = classes[1:]
						classes = append(classes, class)
					}
					cmpClasses := clientv3.Compare(clientv3.Value(TSDBClassesInfo), "=", string(resp.Kvs[0].Value))
					putClasses := clientv3.OpPut(TSDBClassesInfo, ToJson(*s.classes))
					resp, err := s.cli.Txn(context.Background()).If(cmpClasses).Then(putClasses).Commit()
					if resp != nil && resp.Succeeded && err == nil {
						// dispatcher point to target class node
						s.distributeWritePoint(point, classes[0].ClassId, newMeasurementPoint)
						continue
					}
				}
			}
			s.Logger.Error("Point is not create measurement point, will forward the point")
			if classId == 0 {
				classId = s.MetaClient.Data().ClassID
			}
			s.distributeWritePoint(point, classId, newMeasurementPoint)
		}
	}
}

func (s *Service) distributeWritePoint(point models.Point, classId uint64, newMeasurementPoint *httpd.NewMeasurementPoint) {
	if classId == s.MetaClient.Data().ClassID {
		targetClusterMasterNode := s.ch.Get(string(point.Key()))
		// if target node is self node
		if targetClusterMasterNode.Id == s.MetaClient.Data().NodeID {
			if err := s.httpd.Handler.PointsWriter.WritePoints(newMeasurementPoint.DB,
				newMeasurementPoint.Rp, 1, newMeasurementPoint.User,
				[]models.Point{point}); err != nil {
				s.Logger.Error(err.Error())
			}
		} else {
			promise.ExecuteNeedRetryAction(func() error {
				targetHttpClientPoint, err := s.httpd.Handler.Balancing.GetClient(
					targetClusterMasterNode.Ip, "")
				if err != nil {
					return err
				}
				var client = *targetHttpClientPoint
				return s.httpd.Handler.Balancing.ForwardPoint(client, []models.Point{point})
			}, func() {
				// success
			}, func() {
				// error
			}, s.Logger)
		}
		return
	}
	promise.ExecuteNeedRetryAction(func() error {
		httpClientP, err := s.httpd.Handler.Balancing.GetClientByClassId("InfluxForwardClient", classId)
		if err != nil {
			return err
		}
		var httpClient = *httpClientP
		return s.httpd.Handler.Balancing.ForwardPoint(httpClient, []models.Point{point})
	}, func() {

	}, func() {

	}, s.Logger)
}

// watch classes info, update index
func (s *Service) watchClassesInfo() {
	classesResp, err := s.cli.Get(context.Background(), TSDBClassesInfo)
	s.CheckErrorExit("Get classes from etcd failed", err)
	if classesResp == nil || classesResp.Count == 0 {
		resp, err := s.cli.Get(context.Background(), TSDBWorkKey+strconv.FormatUint(s.MetaClient.Data().ClusterID, 10))
		var workClusterInfo WorkClusterInfo
		ParseJson(resp.Kvs[0].Value, &workClusterInfo)
		err = s.initClasses(&workClusterInfo)
		s.CheckErrorExit("Watch Classes info, init classes meta data failed", err)
		classesResp, err = s.cli.Get(context.Background(), TSDBClassesInfo)
		s.CheckErrorExit("Watch Classes info, init classes meta data failed", err)
		s.buildMeasurementIndex(classesResp.Kvs[0].Value)
	} else {
		s.buildMeasurementIndex(classesResp.Kvs[0].Value)
	}
	classesWatch := s.cli.Watch(context.Background(), TSDBClassesInfo)
	for classesInfo := range classesWatch {
		for _, event := range classesInfo.Events {
			s.Logger.Info("tsdb-classes changed")
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
	} else {
		ParseJson(databaseInfoResp.Kvs[0].Value, &s.databases)
		err := s.addNewDatabase(s.databases)
		s.CheckErrorExit("synchronize database failed, error message: ", err)
	}
	databaseInfo := s.cli.Watch(context.Background(), TSDBDatabase, clientv3.WithPrefix())
	for database := range databaseInfo {
		s.Logger.Info("The database has changed,db")
		for _, db := range database.Events {
			localDBInfo := make(map[string]map[string]Rp, len(s.MetaClient.Databases()))
			s.dbsMu.Lock()
			ParseJson(db.Kv.Value, &s.databases)
			s.dbsMu.Unlock()
			var additionalDB = s.databases
			// Add new database and retention policy
			for _, localDB := range s.MetaClient.Databases() {
				rps := s.databases[localDB.Name]
				if rps == nil {
					s.store.DeleteDatabase(localDB.Name)
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
				delete(additionalDB, localDB.Name)
			}
			err = s.addNewDatabase(additionalDB)
			if err != nil {
				s.Logger.Error("create new database error !")
				s.Logger.Error(err.Error())
			}
		}
	}
}

func (s *Service) addNewDatabase(additionalDB Databases) error {
	var err error
	// add new database
	for key, value := range additionalDB {
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
	return err
}
func (s *Service) dropDB(name string) error {
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBDatabase)
	if err != nil || resp.Count == 0 {
		return errors.New("DeleteDB Get latest dbs error")
	}
	ParseJson(resp.Kvs[0].Value, &s.databases)
	delete(s.databases, name)
	cmp := clientv3.Compare(clientv3.Value(TSDBDatabase), "=", string(resp.Kvs[0].Value))
	put := clientv3.OpPut(TSDBDatabase, ToJson(s.databases))
	txResp, err := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if txResp.Succeeded {
		return nil
	}
	goto Retry
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
	} else {
		var cqs Cqs
		ParseJson(cqResp.Kvs[0].Value, &cqs)
		for k, v := range cqs {
			for _, cq := range v {
				err = s.MetaClient.CreateContinuousQuery(k, cq.Name, cq.Query)
				s.CheckErrorExit("Synchronize continue query failed", err)
			}
		}
	}
	cqInfo := s.cli.Watch(context.Background(), TSDBcq, clientv3.WithPrefix())
	for cq := range cqInfo {
		s.Logger.Info("Continues query changed")
		for _, ev := range cq.Events {
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
	return databases
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

func (s *Service) putClusterNode(node Node, cluster WorkClusterInfo, clusterKey string, cmp *clientv3.Cmp) bool {
	nodeKey := clusterKey + "-node-"
	ops := make([]clientv3.Op, 0)
	ops = append(ops, clientv3.OpPut(nodeKey+strconv.FormatUint(node.Id, 10), ToJson(node), clientv3.WithLease(s.lease.ID)))
	var resp *clientv3.TxnResponse
	var err error
	originMasterNodeKey := clusterKey + "-master"
	if cluster.MasterUsable {
		ops = append(ops, clientv3.OpPut(clusterKey, ToJson(cluster)))
		resp, err = s.cli.Txn(context.Background()).If(*cmp).Then(ops...).Commit()
	} else {
		cluster.MasterUsable = true
		ops = append(ops, clientv3.OpPut(clusterKey, ToJson(cluster)))
		ops := append(ops, clientv3.OpPut(originMasterNodeKey, ToJson(node), clientv3.WithLease(s.lease.ID)))
		resp, err = s.cli.Txn(context.Background()).If(*cmp).Then(ops...).Commit()
	}
	if resp != nil && resp.Succeeded && err == nil {
		// change meta data
		var metaData = s.MetaClient.Data()
		metaData.ClassID = cluster.ClassId
		metaData.ClusterID = cluster.ClusterId
		// if class id is null, it is exception
		err = s.MetaClient.SetData(&metaData)
		// change master node
		var masterNode Node
		masterNodeResp, err := s.cli.Get(context.Background(), originMasterNodeKey, clientv3.WithPrefix())
		s.CheckErrorExit("Join Cluster Success, but master node dont't exist", err)
		if masterNodeResp.Count == 0 {
			s.Logger.Error("Join Cluster Success, but the cluster master node crash")
			os.Exit(1)
		}
		ParseJson(masterNodeResp.Kvs[0].Value, &masterNode)
		masterNode.ClusterId = cluster.ClusterId
		s.masterNode = &masterNode
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
		clusterNodeResp, err := s.cli.Get(context.Background(), originClusterKey+"-node", clientv3.WithPrefix())
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
			joinSuccess := s.putClusterNode(node, originWorkCluster, originClusterKey, &cmpWorkCluster)
			if !joinSuccess {
				goto RetryJoinOriginalCluster
			}
		}
	} else {
	RetryJoin:
		recruitResp, err := s.cli.Get(context.Background(), TSDBRecruitClustersKey)
		if err != nil {
			return err
		}
		var recruitCluster RecruitClusters
		// if recruitCluster key non-existent, First Start machine
		if recruitResp.Count == 0 {
			recruitCluster = RecruitClusters{
				Number:     0,
				ClusterIds: make([]uint64, 0, 1),
			}
			_, err = s.cli.Put(context.Background(), TSDBRecruitClustersKey, ToJson(recruitCluster))
			// create Cluster
			err = s.createCluster()
			s.CheckErrorExit("Try create Cluster failed, error message is", err)
			return nil
		}
		ParseJson(recruitResp.Kvs[0].Value, &recruitCluster)
		if recruitCluster.Number > 0 {
			recruitChanged := false
			for i := 0; i < len(recruitCluster.ClusterIds); {
				if err := s.joinRecruitClusterByClusterId(recruitCluster.ClusterIds[i]); err == nil {
					break
				} else {
					recruitChanged = true
					recruitCluster.ClusterIds = append(recruitCluster.ClusterIds[0:i], recruitCluster.ClusterIds[i+1:]...)
				}
			}
			if recruitChanged {
				cmp := clientv3.Compare(clientv3.Value(TSDBRecruitClustersKey), "=", string(recruitResp.Kvs[0].Value))
				opPut := clientv3.OpPut(TSDBRecruitClustersKey, ToJson(recruitCluster))
				resp, _ := s.cli.Txn(context.Background()).If(cmp).Then(opPut).Commit()
				if resp == nil || !resp.Succeeded {
					goto RetryJoin
				}
			}
		} else {
			err = s.createCluster()
		}
	}
	masterResp, err := s.cli.Get(context.Background(), TSDBWorkKey+strconv.FormatUint(s.MetaClient.Data().ClusterID,
		10)+"-master", clientv3.WithPrefix())
	s.CheckErrorExit("Get cluster master node failed", err)
	if masterResp.Count == 0 {
		return errors.New("Cluster master node is nil ")
	}
	ParseJson(masterResp.Kvs[0].Value, s.masterNode)
	port, _ := strconv.Atoi(string([]rune(s.httpConfig.BindAddress)[1 : len(s.httpConfig.BindAddress)-1]))
	s.httpd.Handler.Balancing.SetMasterNode(consistent.NewNode(s.masterNode.Id, s.masterNode.Host, port,
		"host_"+strconv.FormatUint(s.MetaClient.Data().ClusterID, 10), 1))
	return err
}

func (s *Service) containCluster(clusterId uint64, clusters []uint64) bool {
	for _, id := range clusters {
		if id == clusterId {
			return true
		}
	}
	return false
}

func (s *Service) checkAvailable() {
Retry:
	allClusterInfoResp, err := s.cli.Get(context.Background(), TSDBClustersKey)
	var cluster AvailableClusterInfo
	if err == nil && allClusterInfoResp.Count > 0 {
		ParseJson(allClusterInfoResp.Kvs[0].Value, &cluster)
		liveClusterMap := make(map[uint64]interface{})
		liveCluster := make([]WorkClusterInfo, 0)
		for _, c := range cluster.Clusters {
			masterResp, err := s.cli.Get(context.Background(), TSDBWorkKey+strconv.FormatUint(c.ClusterId,
				10)+"-master", clientv3.WithPrefix())
			if masterResp.Count > 0 && err == nil {
				if value := liveClusterMap[c.ClusterId]; value == nil {
					liveCluster = append(liveCluster, c)
				}
			}
		}
		cluster.Clusters = liveCluster
		cmp := clientv3.Compare(clientv3.Value(TSDBClustersKey), "=", string(allClusterInfoResp.Kvs[0].Value))
		put := clientv3.OpPut(TSDBClustersKey, ToJson(cluster))
		resp, _ := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
		if resp == nil || !resp.Succeeded {
			goto Retry
		}
	}
	s.CheckErrorExit("Get TSDBCluster failed ", err)
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
	if !s.containCluster(latestClusterInfo.ClusterId, recruitCluster.ClusterIds) {
		recruitCluster.ClusterIds = append(recruitCluster.ClusterIds, latestClusterInfo.ClusterId)
		recruitCluster.Number++
		ops = append(ops, clientv3.OpPut(TSDBRecruitClustersKey, ToJson(recruitCluster)))
	}

	workClusterInfo := allClustersInfo.Clusters[len(allClustersInfo.Clusters)-1]
	clusterKey := TSDBWorkKey + strconv.FormatUint(latestClusterInfo.ClusterId, 10)

	ops = append(ops, clientv3.OpPut(clusterKey+"-master", ToJson(nodes[0]), clientv3.WithLease(s.lease.ID)))
	ops = append(ops, clientv3.OpPut(TSDBClustersKey, ToJson(allClustersInfo)))
	ops = append(ops, clientv3.OpPut(clusterKey, ToJson(workClusterInfo)))
	clusterNodeKey := clusterKey + "-node-"
	for _, node := range nodes {
		ops = append(ops, clientv3.OpPut(clusterNodeKey+strconv.FormatUint(node.Id, 10), ToJson(node),
			clientv3.WithLease(s.lease.ID)))
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
	if resp == nil || !resp.Succeeded || err != nil {
		goto RetryCreate
	}
	var metaData = s.MetaClient.Data()
	metaData.ClusterID = workClusterInfo.ClusterId
	s.MetaClient.SetData(&metaData)
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
			ClassId:        classIdResp,
			Limit:          DefaultClassLimit,
			ClusterIds:     []uint64{workClusterInfo.ClusterId},
			DBMeasurements: make(map[string][]string),
			DBNewMeasure:   make(map[string][]string),
			DBDelMeasure:   make(map[string][]string),
		}}
		metaData := s.MetaClient.Data()
		metaData.ClassID = classIdResp
		err = s.MetaClient.SetData(&metaData)
		if err != nil {
			s.Logger.Error("When create Cluster and create class, set meta data failed")
			s.Logger.Error(err.Error())
		}
		classesCamp := clientv3.Compare(clientv3.CreateRevision(TSDBClassesInfo), "=", 0)
		classesOp := clientv3.OpPut(TSDBClassesInfo, ToJson(classes))
		resp, err := s.cli.Txn(context.Background()).If(classesCamp).Then(classesOp).Commit()
		if resp == nil && !resp.Succeeded || err != nil {
			goto RetryCreateClass
		}
		// put alive Cluster key with lease
		_, err = s.cli.Put(context.Background(), TSDBClassNode+strconv.FormatUint(classIdResp, 10)+
			"-cluster-"+strconv.FormatUint(workClusterInfo.ClusterId, 10), ToJson(Node{
			Id:        workClusterInfo.MasterId,
			Host:      workClusterInfo.MasterHost,
			Ip:        workClusterInfo.MasterIp,
			ClusterId: workClusterInfo.ClusterId,
		}), clientv3.WithLease(s.lease.ID))
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
		Series:       make([]string, 0),
		ClusterId:    clusterId,
		Limit:        3,
		Number:       len(nodes),
		MasterId:     nodes[0].Id,
		MasterHost:   nodes[0].Host,
		MasterUsable: true,
		MasterIp:     nodes[0].Ip,
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
func (s *Service) addRpcServiceNodes(node Node) {
	s.rpcQuery.nodes = append(s.rpcQuery.nodes, node)
}
func (s *Service) removeRpcServiceNode(node Node) {
	index := -1
	for i, n := range s.rpcQuery.nodes {
		if n.Id == node.Id {
			index = i
			break
		}
	}
	if index != -1 {
		if index+1 == len(s.rpcQuery.nodes) {
			s.rpcQuery.nodes = s.rpcQuery.nodes[0:index]
			return
		}
		s.rpcQuery.nodes = append(s.rpcQuery.nodes[0:index], s.rpcQuery.nodes[index+1:]...)
	}
}
func (s *Service) initRpcServiceNodes() {
	metaData := s.MetaClient.Data()
	commonNodeKey := TSDBWorkKey + strconv.FormatUint(metaData.ClusterID, 10) + "-node-"
	resp, err := s.cli.Get(context.Background(), commonNodeKey, clientv3.WithPrefix())
	s.CheckErrorExit("Init Rpc Service nodes failed", err)
	if resp != nil && resp.Count > 0 {
		for _, v := range resp.Kvs {
			var node Node
			ParseJson(v.Value, &node)
			if node.Id != metaData.NodeID {
				s.rpcQuery.nodes = append(s.rpcQuery.nodes, node)
			}
		}
	}
}

// Watch node of the Cluster, change Cluster information and change subscriber
func (s *Service) watchWorkCluster(clusterId uint64) {
	clusterIdStr := strconv.FormatUint(clusterId, 10)
	clusterKey := TSDBWorkKey + clusterIdStr
	s.initRpcServiceNodes()
	workAllNode := s.cli.Watch(context.Background(), clusterKey+"-", clientv3.WithPrefix())
	var node Node
	for nodeEvent := range workAllNode {
		for _, ev := range nodeEvent.Events {
			ParseJson(ev.Kv.Value, &node)
			if node.Id == s.MetaClient.Data().NodeID {
				continue
			}
			if mvccpb.DELETE == ev.Type {
				s.removeRpcServiceNode(node)
				for _, db := range s.MetaClient.Databases() {
					if ContainsStr(IgnoreDBS, db.Name) {
						continue
					}
					for _, rp := range db.RetentionPolicies {
						subscriberName := db.Name + rp.Name + node.Host
						err := s.MetaClient.DropSubscription(db.Name, rp.Name, subscriberName)
						s.CheckErrPrintLog("Watch cluster "+clusterIdStr+"deleted, Drop subscription failed", err)
					}
				}
				// election master node
				if strings.Contains(string(ev.Kv.Key), strconv.FormatUint(s.masterNode.Id, 10)+"-master") {
					s.masterNode = nil
					masterKey := clusterKey + "-master"
					// choice master
				ChoiceMaster:
					masterResp, err := s.cli.Get(context.Background(), masterKey, clientv3.WithPrefix())
					s.CheckErrPrintLog("Watch work cluster, get master node failed", err)
					if masterResp.Count == 0 {
						node := Node{
							Id:        s.MetaClient.Data().NodeID,
							Host:      s.ip + s.httpConfig.BindAddress,
							UdpHost:   s.ip + s.udpConfig.BindAddress,
							Ip:        s.ip,
							ClusterId: clusterId,
						}
						_, err = s.cli.Put(context.Background(), masterKey, ToJson(node), clientv3.WithLease(s.lease.ID))
						if err != nil {
							s.Logger.Error(err.Error())
							goto ChoiceMaster
						}
						s.masterNode = &node
						// If cluster's master node crash, class's class detail be created by the node will be delete
						// New master node need create class detail again
						// class detail is array, express the class's cluster member
						metaData := s.MetaClient.Data()
						node.ClusterId = clusterId
						_, err = s.cli.Put(context.Background(), TSDBClassNode+strconv.FormatUint(metaData.ClassID, 10)+
							"-cluster-"+strconv.FormatUint(metaData.ClusterID, 10), ToJson(node),
							clientv3.WithLease(s.lease.ID))
					}
				}
				continue
			}
			if mvccpb.PUT == ev.Type {
				s.addRpcServiceNodes(node)
				for _, db := range s.MetaClient.Databases() {
					if ContainsStr(IgnoreDBS, db.Name) {
						continue
					}
					for _, rp := range db.RetentionPolicies {
						subscriberName := db.Name + rp.Name + node.Host
						destination := []string{"http://" + node.Host}
						err := s.MetaClient.CreateSubscription(db.Name, rp.Name, subscriberName, "ALL", destination)
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

func (s *Service) checkClassesMetaData(classesP *[]Class) {
	if classesP == nil {
		s.Logger.Error("CheckClassesMetaData Classes is nil")
	}
	classes := *classesP
	for i := 0; i < len(classes); i++ {
		clusterIds := make([]uint64, 0)
		for j := 0; j < len(classes[i].ClusterIds); j++ {
			resp, err := s.cli.Get(context.Background(), TSDBWorkKey+strconv.FormatUint(classes[i].ClusterIds[j],
				10)+"-", clientv3.WithPrefix())
			if resp.Count > 1 && err == nil {
				clusterIds = append(clusterIds, classes[i].ClusterIds[j])
			}
		}
		classes[i].ClusterIds = clusterIds
	}
	_, err := s.cli.Put(context.Background(), TSDBClassesInfo, ToJson(classes))
	if err != nil {
		s.Logger.Error(err.Error())
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
		s.checkClassesMetaData(&classes)
	}
	var addNewClass = func() error {
		class := Class{
			Limit: DefaultClassLimit,
		}
		class.ClassId, err = s.GetLatestID(TSDBClassAutoIncrementId)
		if err != nil {
			return err
		}
		class.ClusterIds = make([]uint64, 0, 1)
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
		if putResp == nil || !putResp.Succeeded {
			return errors.New("Put Classes failed ")
		}
	} else {
		index := -1
		clusterProcessed := false
		// join exist classes or create new class
		for classIndex, class := range classes {
			for _, id := range class.ClusterIds {
				if id == s.MetaClient.Data().ClusterID {
					// ensure meta class id is same as etcd's meta class id
					clusterProcessed = true
					metaData := s.MetaClient.Data()
					metaData.ClassID = class.ClassId
					s.MetaClient.SetData(&metaData)
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
	metaData := s.MetaClient.Data()
	// Process Class Detail
	var workClusterInfo WorkClusterInfo
	clusterInfoResp, err := s.cli.Get(context.Background(), TSDBWorkKey+strconv.FormatUint(metaData.ClusterID, 10))
	if err == nil && clusterInfoResp.Count > 0 {
		ParseJson(clusterInfoResp.Kvs[0].Value, &workClusterInfo)
	} else {
		return errors.New("The MetaData Service don't contain the Cluster, Cluster id is " +
			strconv.FormatUint(metaData.ClusterID, 10))
	}
	// If local node is local Cluster's master node, local node will put Cluster key of the class
	if s.masterNode.Id == metaData.NodeID {
		_, err = s.cli.Put(context.Background(), TSDBClassNode+strconv.FormatUint(metaData.ClassID, 10)+
			"-cluster-"+strconv.FormatUint(s.masterNode.ClusterId, 10), ToJson(*s.masterNode),
			clientv3.WithLease(s.lease.ID))
	}
	return err
}

func (s *Service) watchClassCluster() {
	// init class detail
	clusterOfClassKey := TSDBClassNode + strconv.FormatUint(s.MetaClient.Data().ClassID, 10) + "-cluster"
	resp, err := s.cli.Get(context.Background(), clusterOfClassKey, clientv3.WithPrefix())
	s.CheckErrorExit("Get Cluster of Class failed", err)
	if resp.Count > 0 {
		for _, v := range resp.Kvs {
			var node Node
			ParseJson(v.Value, &node)
			s.classDetail[node.Ip] = node
		}
	}
	s.buildConsistentHash()
	// watch class detail
	classNodeEventChan := s.cli.Watch(context.Background(), clusterOfClassKey, clientv3.WithPrefix())
	for classNodeInfo := range classNodeEventChan {
		for _, event := range classNodeInfo.Events {
			// every node need update consistent hash
			var changedNode Node
			ParseJson(event.Kv.Value, &changedNode)
			s.Logger.Info("Get " + clusterOfClassKey + "change event, cluster id" +
				strconv.FormatUint(changedNode.ClusterId, 10))
			if event.Type == mvccpb.DELETE {
				if event.Kv.Value == nil {
					continue
				}
				delete(s.classDetail, changedNode.Ip)
				metaData := s.MetaClient.Data()
				if s.masterNode.Id == metaData.NodeID {
					promise.ExecuteNeedRetryAction(func() error {
						resp, err := s.cli.Get(context.Background(), TSDBClassesInfo)
						s.CheckErrPrintLog("WatchClassCluster get classes failed", err)
						if resp.Count > 0 {
							ParseJson(resp.Kvs[0].Value, s.classes)
						}
						var classes = *s.classes
						classIndex := -1
						clusterIndex := -1
					Class:
						for ci, c := range classes {
							if c.ClassId == metaData.ClassID {
								for i, cluster := range c.ClusterIds {
									if cluster == changedNode.ClusterId {
										clusterIndex = i
										classIndex = ci
										break Class
									}
								}

							}
						}
						if classIndex != -1 && clusterIndex != -1 {
							classes[classIndex].ClusterIds = append(classes[classIndex].ClusterIds[0:clusterIndex],
								classes[classIndex].ClusterIds[clusterIndex+1:]...)
							cmp := clientv3.Compare(clientv3.Value(TSDBClassesInfo), "=", string(resp.Kvs[0].Value))
							opPut := clientv3.OpPut(TSDBClassesInfo, ToJson(classes))
							resp, err := s.cli.Txn(context.Background()).If(cmp).Then(opPut).Commit()
							if resp != nil && resp.Succeeded {
								return nil
							}
							return err
						}
						return nil
					}, func() {

					}, func() {

					}, s.Logger)
				}
				continue
			}
			// New Cluster join
			s.classDetail[changedNode.Ip] = changedNode
			s.Logger.Error("Get New cluster join the class, but TSDBWorkKey dont exist")
		}
	}
}

func (s *Service) putClasses(classesResp *clientv3.GetResponse, classes []Class) error {
	cmpClasses := clientv3.Compare(clientv3.Value(TSDBClassesInfo), "=", string(classesResp.Kvs[0].Value))
	opPutClasses := clientv3.OpPut(TSDBClassesInfo, ToJson(classes))
	putResp, _ := s.cli.Txn(context.Background()).If(cmpClasses).Then(opPutClasses).Commit()
	if putResp == nil || !putResp.Succeeded {
		return errors.New("Put Classes failed ")
	}
	return nil
}

func (s *Service) registerToCommonNode() error {
RegisterNode:
	var err error
	metaData := s.MetaClient.Data()
	if metaData.NodeID == 0 {
		metaData.NodeID, err = s.GetLatestID(TSDBNodeAutoIncrementId)
		s.CheckErrorExit(" Get auto increment id failed", err)
	}
	nodeKey := TSDBCommonNodeIdKey + strconv.FormatUint(metaData.NodeID, 10)
	node := Node{
		Id:      metaData.NodeID,
		Host:    s.ip + s.httpConfig.BindAddress,
		UdpHost: s.ip + s.udpConfig.BindAddress,
	}
	cmp := clientv3.Compare(clientv3.CreateRevision(nodeKey), "=", 0)
	opPut := clientv3.OpPut(nodeKey, ToJson(node), clientv3.WithLease(s.lease.ID))
	// Register with etcd
	resp, err := s.cli.Txn(context.Background()).If(cmp).Then(opPut).Commit()
	s.CheckErrorExit("Register node failed ", err)
	if resp == nil || !resp.Succeeded {
		goto RegisterNode
	}
	s.MetaClient.SetData(&metaData)
	return nil
}

func (s *Service) joinRecruitClusterByClusterId(clusterId uint64) error {
	// 不使用RecruitClusterKey，计划通过worker来判断是否允许加入，
	workClusterKey := TSDBWorkKey + strconv.FormatUint(clusterId, 10)
RetryJoinTarget:
	var joinSuccess = false
	clusterMetaResp, metaErr := s.cli.Get(context.Background(), workClusterKey)
	clusterNodeResp, err := s.cli.Get(context.Background(), workClusterKey+"-node", clientv3.WithPrefix())
	if err != nil || metaErr != nil {
		return errors.New("Get cluster info failed, meta error message:" + metaErr.Error() + "err message :" + err.Error())
	}
	var workClusterInfo WorkClusterInfo
	if clusterNodeResp.Count > 0 {
		ParseJson(clusterMetaResp.Kvs[0].Value, &workClusterInfo)
		if workClusterInfo.Number < workClusterInfo.Limit {
			cmp := clientv3.Compare(clientv3.Value(workClusterKey), "=", string(clusterMetaResp.Kvs[0].Value))
			node := Node{
				Id:        s.MetaClient.Data().NodeID,
				Host:      s.ip + s.httpConfig.BindAddress,
				UdpHost:   s.ip + s.udpConfig.BindAddress,
				Ip:        s.ip,
				ClusterId: clusterId,
			}
			workClusterInfo.Number++
			if joinSuccess = s.putClusterNode(node, workClusterInfo, workClusterKey, &cmp); !joinSuccess {
				goto RetryJoinTarget
			}
		}
	} else {
		s.cli.Delete(context.Background(), workClusterKey)
	}
	if joinSuccess {
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
	var classes = *s.classes
	for _, class := range classes {
		if len(class.DBMeasurements) == 0 && len(class.DBDelMeasure) == 0 {
			continue
		}
		// process local class
		if class.ClassId == s.MetaClient.Data().ClassID {
			for db, ms := range class.DBMeasurements {
				for _, m := range ms {
					s.measurement[db][m] = ""
				}
			}
			if len(class.DBDelMeasure) != 0 {
				for db, ms := range class.DBDelMeasure {
					for _, m := range ms {
						// local class delete measurement need clear local data
						delete(s.measurement[db], m)
						s.store.DeleteMeasurement(db, m)
					}
				}
			}
			continue
		}
		// process other class
		for db, ms := range class.DBMeasurements {
			for _, m := range ms {
				s.otherMeasurement[db][m] = class.ClassId
			}
		}
		for db, ms := range class.DBMeasurements {
			for _, m := range ms {
				delete(s.otherMeasurement[db], m)
			}
		}
		// process ip map index
		if classIp := s.classIpMap[class.ClassId]; classIp == nil || len(classIp) <= 1 {
			clusterNodeResp, err := s.cli.Get(context.Background(), TSDBWorkKey+strconv.FormatUint(class.ClusterIds[0], 10)+
				"-node", clientv3.WithPrefix())
			s.CheckErrPrintLog("Build Measurement Index failed", err)
			ips := make([]string, 0, clusterNodeResp.Count)
			for _, kv := range clusterNodeResp.Kvs {
				var node Node
				ParseJson(kv.Value, &node)
				ips = append(ips, node.Host)
			}
			s.classIpMap[class.ClassId] = ips
		}
	}
}

func (s *Service) buildConsistentHash() {
	for _, node := range s.classDetail {
		port, _ := strconv.Atoi(string([]rune(s.httpConfig.BindAddress)[1 : len(s.httpConfig.BindAddress)-1]))
		weight := 1
		s.ch.Add(consistent.NewNode(node.Id, node.Ip, port, "host_"+strconv.FormatUint(node.ClusterId, 10), weight))
	}
	one.Do(func() {
		s.httpd.Handler.Balancing.SetConsistent(s.ch)
	})
}
func (s *Service) putSql(state *Statement) error {
	if state == nil {
		return errors.New("Statement is nil error ")
	}
PutSql:
	id, err := s.GetLatestID(TSDBStatementAutoIncrementId)
	if err != nil {
		return err
	}
	idStr := TSDBStatement + strconv.FormatUint(id, 10)
	cmp := clientv3.Compare(clientv3.CreateRevision(idStr), "=", 0)
	put := clientv3.OpPut(idStr, ToJson(state))
	resp, err := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if !resp.Succeeded || err != nil {
		goto PutSql
	}
	return nil
}
func (s *Service) watchStatement() {
	stateCh := s.cli.Watch(context.Background(), TSDBStatement, clientv3.WithPrefix())
	for stateInfo := range stateCh {
		for _, event := range stateInfo.Events {
			if event.Type == mvccpb.PUT {
				var state Statement
				ParseJson(event.Kv.Value, &state)
				var qr io.Reader
				qr = strings.NewReader(state.Sql)
				if qr == nil {
					continue
				}
				p := influxql.NewParser(qr)
				q, err := p.ParseQuery()
				if err != nil {
					continue
				}
				executionCont := &query.ExecutionContext{
					ExecutionOptions: state.ExecOpt,
				}
				for i, statement := range q.Statements {
					if executor, ok := s.httpd.Handler.QueryExecutor.StatementExecutor.(*ClusterStatementExecutor); ok {
						executionCont.QueryID = uint64(i)
						err = executor.StatementExecutor.ExecuteStatement(statement, executionCont)
						s.CheckErrPrintLog("WatchStatemet StatementExecutor execute statement failed, sql is"+state.Sql+
							" error message is ", err)
					}
				}
			}
		}
	}
}

func (s *Service) dropMeasurement(db string, name string) error {
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBClassesInfo)
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return errors.New("Classes meta data don't exist!!! ,Etcd Service may be crash or occur error ")
	}
	ParseJson(resp.Kvs[0].Value, s.classes)
	var classes = *s.classes
	update := false
	for i, class := range classes {
		if ms := class.DBMeasurements[db]; ms != nil {
			ms = slices.DeleteStr(ms, name)
			update = true
			classes[i] = class
			break
		}
	}
	if update {
		cmp := clientv3.Compare(clientv3.Value(TSDBClassesInfo), "=", string(resp.Kvs[0].Value))
		put := clientv3.OpPut(TSDBClassesInfo, ToJson(classes))
		txnResp, err := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
		if txnResp.Succeeded && err == nil {
			return nil
		}
		goto Retry
	}

	return nil
}

func (s *Service) dropCqs(db string, name string) error {
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBcq)
	if resp.Count == 0 || err != nil {
		return errors.New("Get meta data from etcd failed ")
	}
	var cqs Cqs
	ParseJson(resp.Kvs[0].Value, &cqs)
	cqInfo := cqs[db]
	index := -1
	for i, cq := range cqInfo {
		if cq.Name == name {
			index = i
			break
		}
	}
	if index == -1 {
		return errors.New("Continues Query don't exist ")
	}
	if index >= len(cqInfo)-1 {
		cqInfo = cqInfo[0 : len(cqInfo)-1]
	} else {
		cqInfo = append(cqInfo[0:index], cqInfo[index+1:]...)
	}
	cqs[db] = cqInfo
	cmp := clientv3.Compare(clientv3.Value(TSDBcq), "=", string(resp.Kvs[0].Value))
	put := clientv3.OpPut(TSDBcq, ToJson(cqs))
	txnResp, err := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if txnResp.Succeeded && err == nil {
		return nil
	}
	goto Retry
}
