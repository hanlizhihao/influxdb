package coordinator

import (
	"bytes"
	"context"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/httpd/consistent"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/tsdb"
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
	TSDBClusterAutoIncrementId = "tsdb-cluster-auto-increment-id"
	TSDBNodeAutoIncrementId    = "tsdb-node-auto-increment-id"
	TSDBClassAutoIncrementId   = "tsdb-class-auto-increment-id"
	TSDBClassesInfo            = "tsdb-classes-info"
	TSDBClassId                = "tsdb-class-"
	TSDBDatabase               = "tsdb-databases"
	// default class limit
	DefaultClassLimit       = 3
	DefaultClusterNodeLimit = 3
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
	// local class's measurements
	measurement map[string]interface{}
	// other class's measurement
	otherMeasurement map[string]uint64
	// if there is point transfer to class, will need ip
	classIpMap map[uint64][]string
	// latest classes info
	classes *Classes
	ch      *consistent.Consistent

	masterNode *consistent.Node
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
	s.measurement = make(map[string]interface{})
	s.otherMeasurement = make(map[string]uint64)
	s.classIpMap = make(map[uint64][]string)
	s.Logger.Info("Starting register for ETCD Service...")
	err = s.registerToCommonNode()
	if err != nil {
		return err
	}
	err = s.joinClusterOrCreateCluster()
	go s.watchClusterDatabaseInfo()
	go s.watchClassesInfo()
	go s.processNewMeasurement()
	s.closed = false
	s.Logger.Info("Opened Service")
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
			httpClient, err := s.httpd.Handler.Balancing.GetClient(node.Ip, "")
			// cluster's master node crash
			if err = s.httpd.Handler.Balancing.ForwardPoint(httpClient, []models.Point{point}); err != nil {
				s.Logger.Error(err.Error())
			}
			if httpClient != nil {
				httpClient.Close()
			}
			return nil
		}
		if classId := s.otherMeasurement[string(point.Name())]; &classId != nil {
			httpClient, err := s.httpd.Handler.Balancing.GetClientByClassId("InfluxForwardClient", classId)
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
				classes = append(classes[:1], classes[1:]...)
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
	if classesResp.Count == 0 || err != nil {
		s.Logger.Error("Etcd don't contain classes info key")
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

//明天和后天全天培训，都没时间玩游戏，你打算来之前，提前说，我把时间腾出来给你
//
// Every node of cluster may create database and retention policy, So focus on and respond to changes in database
// information in metadata. So that each node's database and reservation policy data are consistent.
func (s *Service) watchClusterDatabaseInfo() {
	databaseInfoResp, err := s.cli.Get(context.Background(), TSDBDatabase)
	if databaseInfoResp == nil || err != nil || databaseInfoResp.Count == 0 {
		databases := s.toDatabaseInfo(s.MetaClient.Databases())
		_, err := s.cli.Put(context.Background(), TSDBDatabase, ToJson(databases))
		if err != nil {
			s.Logger.Error(err.Error())
		}
	}
	databaseInfo := s.cli.Watch(context.Background(), TSDBDatabase, clientv3.WithPrefix())
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
				rps := databases.Database[localDB.Name]
				if rps == nil {
					_ = s.MetaClient.DropDatabase(localDB.Name)
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
				delete(databases.Database, localDB.Name)
			}
			// add new database
			for key, value := range databases.Database {
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
			s.Logger.Error("create new database error !")
			s.Logger.Error(err.Error())
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
func (s *Service) joinClusterOrCreateCluster() error {
	s.Logger.Info("System will join origin cluster or create new cluster")
RetryJoinOriginalCluster:
	// Try to connect the original cluster.
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	originClusterKey := TSDBWorkKey + strconv.FormatUint(s.MetaClient.Data().ClusterID, 10)
	workClusterResp, err := s.cli.Get(ctx, originClusterKey)
	cancel()
	if workClusterResp.Count != 0 && err == nil {
		var originWorkCluster WorkClusterInfo
		ParseJson(workClusterResp.Kvs[0].Value, &originWorkCluster)
		if originWorkCluster.Number < originWorkCluster.Limit {
			originWorkCluster.Number++
			nodeId, err := GetLatestID(s.etcdConfig, TSDBNodeAutoIncrementId)
			ip, err := GetLocalHostIp()
			if err != nil {
				return err
			}
			originWorkCluster.Nodes = append(originWorkCluster.Nodes, Node{
				Id:      nodeId,
				Host:    ip + s.httpConfig.BindAddress,
				UdpHost: ip + s.udpConfig.BindAddress,
			})
			cmpWorkCluster := clientv3.Compare(clientv3.Value(originClusterKey), "=", string(workClusterResp.Kvs[0].Value))
			putWorkCluster := clientv3.OpPut(originClusterKey, ToJson(originWorkCluster))
			resp, err := s.cli.Txn(context.Background()).If(cmpWorkCluster).Then(putWorkCluster).Commit()
			if resp.Succeeded && err != nil {
				var metaData = s.MetaClient.Data()
				metaData.ClassID = originWorkCluster.ClassId
				// if class id is null, it is exception
				s.MetaClient.SetData(&metaData)
			} else {
				goto RetryJoinOriginalCluster
			}
		}
	} else {
		recruit, err := s.cli.Get(context.Background(), TSDBRecruitClustersKey)
		if err != nil {
			return err
		}
		var recruitCluster RecruitClusters
		// if recruitCluster key non-existent
		if recruit.Count == 0 {
			recruitCluster = RecruitClusters{
				Number:     0,
				ClusterIds: make([]uint64, 1),
			}
			s.cli.Put(context.Background(), TSDBRecruitClustersKey, ToJson(recruitCluster))
			// create cluster
			s.createCluster()
			return nil
		}
		for _, kv := range recruit.Kvs {
			ParseJson(kv.Value, &recruitCluster)
			// Logical nodes need nodes to join it.
			if recruitCluster.Number > 0 {
				for _, id := range recruitCluster.ClusterIds {
					err = s.joinRecruitClusterByClusterId(id)
					if err == nil {
						return nil
					}
				}
				return s.createCluster()
			}
			err = s.createCluster()
			if err != nil {
				return err
			}
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
	ParseJson(recruit.Kvs[0].Value, &recruitCluster)
	var allClustersInfo AvailableClusterInfo
RetryCreate:
	allClusterInfoResp, err := s.cli.Get(context.Background(), TSDBClustersKey)
	if allClusterInfoResp == nil || allClusterInfoResp.Count == 0 {
		err = s.initAllClusterInfo(&allClustersInfo)
	} else {
		ParseJson(allClusterInfoResp.Kvs[0].Value, &allClustersInfo)
		if allClustersInfo.Clusters == nil {
			err = s.initAllClusterInfo(&allClustersInfo)
		}
	}
	if err != nil {
		return err
	}
	latestClusterInfo := allClustersInfo.Clusters[len(allClustersInfo.Clusters)-1]
	recruitCluster.ClusterIds = append(recruitCluster.ClusterIds, latestClusterInfo.ClusterId)
	recruitCluster.Number++
	workClusterInfo := WorkClusterInfo{}
	workClusterInfo.Number = 1
	workClusterInfo.Limit = DefaultClusterNodeLimit
	workClusterInfo.Nodes = latestClusterInfo.Nodes
	workClusterInfo.Master = latestClusterInfo.Nodes[0]
	// Transaction creation cluster
	cmpAllCluster := clientv3.Compare(clientv3.Value(TSDBClustersKey), "=", string(allClusterInfoResp.Kvs[0].Value))
	cmpRecruit := clientv3.Compare(clientv3.Value(TSDBRecruitClustersKey), "=", string(recruit.Kvs[0].Value))
	putAllCluster := clientv3.OpPut(TSDBClustersKey, ToJson(allClustersInfo))
	putAllRecruit := clientv3.OpPut(TSDBRecruitClustersKey, ToJson(recruitCluster))
	putWorkerCluster := clientv3.OpPut(TSDBWorkKey+strconv.FormatUint(latestClusterInfo.ClusterId, 10), ToJson(workClusterInfo))
	resp, err := s.cli.Txn(context.Background()).If(cmpAllCluster, cmpRecruit).Then(putAllCluster, putAllRecruit, putWorkerCluster).Commit()
	if !resp.Succeeded && err != nil {
		goto RetryCreate
	}
	metaData := s.MetaClient.Data()
	metaData.ClusterID = latestClusterInfo.ClusterId
	err = s.MetaClient.SetData(&metaData)
	if err != nil {
		return err
	}
	go s.watchWorkCluster(latestClusterInfo.ClusterId)
	// if class is nil, create class
RetryCreateClass:
	classResp, err := s.cli.Get(context.Background(), TSDBClassesInfo)
	if err != nil || classResp == nil || classResp.Count == 0 {
		classIdResp, err := GetLatestID(s.etcdConfig, TSDBClassAutoIncrementId)
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
			Clusters:     []WorkClusterInfo{workClusterInfo},
			Measurements: make([]string, 0),
		}
		metaData := s.MetaClient.Data()
		metaData.ClassID = classIdResp
		err = s.MetaClient.SetData(&metaData)
		if err != nil {
			s.Logger.Error("when Create cluster and create class, set meta data failed")
			s.Logger.Error(err.Error())
		}
		classesCamp := clientv3.Compare(clientv3.Value(TSDBClassesInfo), "=", nil)
		classDetailCamp := clientv3.Compare(clientv3.Value(TSDBClassId+strconv.FormatUint(classIdResp, 10)), "=", nil)
		classesOp := clientv3.OpPut(TSDBClassesInfo, ToJson(classes))
		classDetailOp := clientv3.OpPut(TSDBClassId+strconv.FormatUint(classIdResp, 10), ToJson(class))
		resp, err := s.cli.Txn(context.Background()).If(classesCamp, classDetailCamp).Then(classesOp, classDetailOp).Commit()
		if !resp.Succeeded && err != nil {
			goto RetryCreateClass
		}
	}
	return nil
}

// Watch node of the cluster, change cluster information and change subscriber
func (s *Service) watchWorkCluster(clusterId uint64) {
	cli, err := GetEtcdClient(s.etcdConfig)
	if err != nil {
		s.Logger.Error("replication Service connected failed")
	}
	var prevWorkClusterInfo WorkClusterInfo
	var workClusterInfo WorkClusterInfo
	dbNodes := cli.Watch(context.Background(), TSDBWorkKey+strconv.FormatUint(clusterId, 10), clientv3.WithPrefix())
	for dbNode := range dbNodes {
		for _, ev := range dbNode.Events {
			if bytes.Equal(ev.PrevKv.Value, ev.Kv.Value) {
				continue
			}
			ParseJson(ev.Kv.Value, &workClusterInfo)
			ParseJson(ev.PrevKv.Value, &prevWorkClusterInfo)
			for _, db := range s.MetaClient.Databases() {
				for _, rp := range db.RetentionPolicies {
					for _, node := range workClusterInfo.Nodes {
						subscriberName := db.Name + rp.Name + node.Host
						destination := make([]string, 1)
						destination = append(destination, "http://"+node.Host)
						s.MetaClient.CreateSubscription(db.Name, rp.Name, subscriberName, "All", destination)
					}
				}
			}
			port, _ := strconv.Atoi(string([]rune(s.httpConfig.BindAddress)[1 : len(s.httpConfig.BindAddress)-1]))
			if s.masterNode == nil || s.masterNode.Id != workClusterInfo.Master.Id {
				s.masterNode = consistent.NewNode(workClusterInfo.Master.Id, workClusterInfo.Master.Host, port,
					"host_"+strconv.FormatUint(workClusterInfo.ClusterId, 10), 1)
				s.httpd.Handler.Balancing.SetMasterNode(s.masterNode)
			}
			if workClusterInfo.Number >= 2 {
				err = s.joinClass()
				if err != nil {
					s.Logger.Error("Join class failed, error message is " + err.Error())
				}
			}
		}
	}
}

// cluster is logical node and min uint, they made up class, working cluster must be class member
// Become a cluster of worker, add to classes info, create or join the original class.
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
		class.ClassId, err = GetLatestID(s.etcdConfig, TSDBClassAutoIncrementId)
		if err != nil {
			return err
		}
		class.ClusterIds = make([]uint64, 1)
		class.ClusterIds = append(class.ClusterIds, s.MetaClient.Data().ClusterID)
		classes = append(classes, class)
		metaData := s.MetaClient.Data()
		metaData.ClassID = class.ClassId
		s.MetaClient.SetData(&metaData)
		return nil
	}
	// create new classes info
	if classes == nil {
		classes = make([]Class, 0)
		err = addNewClass()
		if err != nil {
			return err
		}
		err = s.putClasses(classesResp, classes)
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
			if index == -1 && class.Limit > len(class.ClusterIds) {
				index = classIndex
			}
		}
		// join class
		if !clusterProcessed && index != -1 {
			classes[index].ClusterIds = append(classes[index].ClusterIds, s.MetaClient.Data().ClusterID)
			metaData := s.MetaClient.Data()
			metaData.ClassID = classes[index].ClassId
			s.MetaClient.SetData(&metaData)
			err = s.putClasses(classesResp, classes)
		} else {
			err = addNewClass()
			err = s.putClasses(classesResp, classes)
		}
	}
	if err != nil {
		goto RetryAddClasses
	}
	// Process Class Detail
	var workClusterInfo WorkClusterInfo
	clusterInfoResp, err := s.cli.Get(context.Background(), TSDBWorkKey+
		strconv.FormatUint(s.MetaClient.Data().ClusterID, 10))
	if err == nil && clusterInfoResp.Count > 0 {
		ParseJson(clusterInfoResp.Kvs[0].Value, &workClusterInfo)
	} else {
		return errors.New("The MetaData Service don't contain the cluster, cluster id is " +
			strconv.FormatUint(s.MetaClient.Data().ClusterID, 10))
	}
	classInfoResp, err := s.cli.Get(context.Background(), TSDBClassId+
		strconv.FormatUint(s.MetaClient.Data().ClassID, 10))
	var classDetail ClassDetail
	if err == nil || classInfoResp.Count == 0 {
		var classDetailWorkClusterInfo WorkClusterInfo
		classDetailWorkClusterInfo.ClusterId = s.MetaClient.Data().ClusterID
		classDetailWorkClusterInfo.Master = workClusterInfo.Master
		classDetail = ClassDetail{
			Measurements: make([]string, 0),
			Clusters:     []WorkClusterInfo{classDetailWorkClusterInfo},
		}
		err = s.putClassDetail(&classDetail)
		go s.watchClassDetail()
		return err
	}
	ParseJson(classInfoResp.Kvs[0].Value, &classDetail)
	classDetail.Clusters = append(classDetail.Clusters, workClusterInfo)
	err = s.putClassDetail(&classDetail)
	s.buildConsistentHash(&classDetail)
	go s.watchClassDetail()
	return err
}

func (s *Service) watchClassDetail() {
	var processChange = func(data []byte) {
		var classDetail ClassDetail
		ParseJson(data, &classDetail)
		tempMeasurement := make(map[string]interface{}, len(classDetail.Measurements))
		for _, measurement := range classDetail.Measurements {
			tempMeasurement[measurement] = ""
		}
		s.measurement = tempMeasurement
		s.buildConsistentHash(&classDetail)
	}
	classResp, err := s.cli.Get(context.Background(), TSDBClassId+strconv.FormatUint(s.MetaClient.Data().ClassID, 10))
	if classResp.Count == 0 || err != nil {
		s.Logger.Error("Etcd Service get" + TSDBClassId + strconv.FormatUint(s.MetaClient.Data().ClassID, 10) + "failed")
	} else {
		processChange(classResp.Kvs[0].Value)
	}
	watchChan := s.cli.Watch(context.Background(), TSDBClassId+strconv.FormatUint(s.MetaClient.Data().ClassID, 10),
		clientv3.WithPrefix())
	for changeInfo := range watchChan {
		for _, event := range changeInfo.Events {
			if bytes.Equal(event.PrevKv.Value, event.Kv.Value) {
				continue
			}
			processChange(event.Kv.Value)
		}
	}
}

func (s *Service) putClasses(classesResp *clientv3.GetResponse, classes []Class) error {
	cmpClasses := clientv3.Compare(clientv3.Value(TSDBClassesInfo), "=", string(classesResp.Kvs[0].Value))
	opPutClasses := clientv3.OpPut(TSDBClassesInfo, ToJson(classes))
	transactionOpResp, _ := s.cli.Txn(context.Background()).If(cmpClasses).Then(opPutClasses).Commit()
	if !transactionOpResp.Succeeded {
		return errors.New("Put Classes failed ")
	}
	return nil
}

func (s *Service) putClassDetail(classDetail *ClassDetail) error {
	cmpClassDetail := clientv3.Compare(clientv3.Value(TSDBClassId+strconv.FormatUint(s.MetaClient.Data().ClassID,
		10)), "=", string(ToJson(*classDetail)))
	opPutClassDetail := clientv3.OpPut(TSDBClassId+strconv.FormatUint(s.MetaClient.Data().ClassID, 10),
		ToJson(*classDetail))
	resp, err := s.cli.Txn(context.Background()).If(cmpClassDetail).Then(opPutClassDetail).Commit()
	if resp.Succeeded {
		return nil
	}
	return err
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
			Id:      nodeId,
			Host:    ip + s.httpConfig.BindAddress,
			UdpHost: ip + s.udpConfig.BindAddress,
		})
		commonNodes := CommonNodes{
			Nodes: nodes,
		}
		s.cli.Put(context.Background(), TSDBCommonNodeKey, ToJson(commonNodes))
	}
	var commonNodes CommonNodes
	ParseJson(commonNodesResp.Kvs[0].Value, &commonNodes)
	nodeId, err := GetLatestID(s.etcdConfig, TSDBNodeAutoIncrementId)
	ip, err := GetLocalHostIp()
	commonNodes.Nodes = append(commonNodes.Nodes, Node{
		Id:      nodeId,
		Host:    ip + s.httpConfig.BindAddress,
		UdpHost: ip + s.udpConfig.BindAddress,
	})
	cmp := clientv3.Compare(clientv3.Value(TSDBCommonNodeKey), "=", string(commonNodesResp.Kvs[0].Value))
	put := clientv3.OpPut(TSDBCommonNodeKey, ToJson(commonNodes))
	resp, err := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if !resp.Succeeded {
		goto RegisterNode
	}
	return nil
}

func (s *Service) initAllClusterInfo(allClusterInfo *AvailableClusterInfo) error {
	clusterId, err := GetLatestID(s.etcdConfig, TSDBClusterAutoIncrementId)
	if err != nil {
		return err
	}
	var nodes []Node
	ip, err := GetLocalHostIp()
	nodes = append(nodes, Node{
		Host:    ip + s.httpConfig.BindAddress,
		UdpHost: ip + s.udpConfig.BindAddress,
	})
	var workClusterInfo = make([]WorkClusterInfo, 0)
	workClusterInfo = append(workClusterInfo, WorkClusterInfo{
		RecruitCluster: RecruitCluster{ClusterId: clusterId, Nodes: nodes, Limit: 3, Number: len(nodes), Master: nodes[0]},
		Series:         make([]string, 0),
	})
	*allClusterInfo = AvailableClusterInfo{
		Clusters: workClusterInfo,
	}
	_, err = s.cli.Put(context.Background(), TSDBClustersKey, ToJson(allClusterInfo))
	return err
}
func (s *Service) joinRecruitClusterByClusterId(clusterId uint64) error {
	// 不使用RecruitClusterKey，计划通过worker来判断是否允许加入，
	workClusterKey := TSDBWorkKey + strconv.FormatUint(clusterId, 10)
RetryJoinTarget:
	workClusterNodes, err := s.cli.Get(context.Background(), workClusterKey)
	if err != nil {
		return err
	}
	var joinSuccess = false
	for _, kv := range workClusterNodes.Kvs {
		var workClusterInfo WorkClusterInfo
		ParseJson(kv.Value, &workClusterInfo)
		if workClusterInfo.Number >= workClusterInfo.Limit {
			continue
		}
		cmp := clientv3.Compare(clientv3.Value(workClusterKey), "=", string(kv.Value))
		ip, err := GetLocalHostIp()
		if err != nil {
			return err
		}
		workClusterInfo.Nodes = append(workClusterInfo.Nodes, Node{
			Host:    ip + s.httpConfig.BindAddress,
			UdpHost: ip + s.udpConfig.BindAddress,
		})
		put := clientv3.OpPut(workClusterKey, ToJson(workClusterInfo))
		resp, err := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
		if !resp.Succeeded {
			// retry
			goto RetryJoinTarget
		}
		joinSuccess = true
		break
	}
	if joinSuccess {
		metaData := s.MetaClient.Data()
		metaData.ClusterID = clusterId
		err = s.MetaClient.SetData(&metaData)
		go s.watchWorkCluster(clusterId)
		return err
	}
	return errors.New("Join cluster" + strconv.FormatUint(clusterId, 10) + "failed")
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

	s.wg.Wait()
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
		// process local class
		if class.ClassId == s.MetaClient.Data().ClassID {
			if s.measurement == nil {

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
			clusterResp, err := s.cli.Get(context.Background(), TSDBWorkKey+strconv.FormatUint(class.ClusterIds[0], 10))
			if err != nil || clusterResp.Count == 0 {
				s.Logger.Error("Get cluster info by " + strconv.FormatUint(class.ClusterIds[0], 10) + "failed")
				continue
			}
			var cluster WorkClusterInfo
			ParseJson(clusterResp.Kvs[0].Value, &cluster)
			ipArray := make([]string, len(cluster.Nodes))
			for _, node := range cluster.Nodes {
				ipArray = append(ipArray, node.Host)
			}
			s.classIpMap[class.ClassId] = ipArray
		}
	}
	Once.Do(func() {
		s.httpd.Handler.Balancing.SetMeasurementMapIndex(s.measurement, s.otherMeasurement, s.classIpMap)
	})
}
func (s *Service) buildConsistentHash(classDetail *ClassDetail) {
	if s.ch == nil {
		s.ch = consistent.NewConsistent()
	}
	for _, classItem := range classDetail.Clusters {
		port, _ := strconv.Atoi(string([]rune(s.httpConfig.BindAddress)[1 : len(s.httpConfig.BindAddress)-1]))
		weight := 0
		if classItem.Master.Weight == 0 {
			weight = 1
		} else {
			weight = classItem.Master.Weight
		}
		s.ch.Add(consistent.NewNode(classItem.Master.Id, classItem.Master.Host, port,
			"host_"+strconv.FormatUint(classItem.ClusterId, 10), weight))
	}
	Once.Do(func() {
		s.httpd.Handler.Balancing.SetConsistent(s.ch)
	})
}

func (s *Service) clusterSelect(wg *sync.WaitGroup, stmt *influxql.SelectStatement, ctx *query.ExecutionContext) {

}
