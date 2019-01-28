package coordinator

import (
	"bytes"
	"context"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/influxdata/influxdb/pkg/slices"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"io"
	"strconv"
	"strings"
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
	// TSDBMeasurement            = "tsdb-db-name"
	TSDBTag                    = "tsdb-tag-db-name-tag"
	// example tsdb-cla-1-cluster-1, describe class 1 has cluster 1
	TSDBClassNode = "tsdb-cla-"

	TSDBDatabase  = "tsdb-databases"
	// map[string]map[string]rp if first value rp exist, only delete rp
	TSDBDatabaseDel    = "tsdb-databases-del"
	TSDBDatabaseNew    = "tsdb-databases-new"
	TSDBDatabaseUpdate = "tsdb-databases-update"

	TSDBcq        = "tsdb-cq"
	TSDBUsers     = "tsdb-users"
	TSDBUserNew   = "tsdb-users-new"
	TSDBUserDel   = "tsdb-users-del"
	TSDBUserAdmin = "tsdb-users-admin"
	TSDBUserGrant = "tsdb-users-grant"
	TSDBUserUpdate = "tsdb-users-update"

	TSDBStatement                = "tsdb-statement-"
	TSDBStatementAutoIncrementId = "tsdb-auto-increment-statement-id"

	// subscription
	TSDBSubscription    = "tsdb-subscription"
	TSDBSubscriptionDel = "tsdb-subscription-del"
	TSDBSubscriptionNew = "tsdb-subscription-new"
	// default class limit
	DefaultClassLimit = 3
)

func (s *Service) getTimeoutLease() (*clientv3.LeaseGrantResponse, error) {
	lease, err := s.cli.Grant(context.Background(), 3)
	if err != nil {
		return nil, err
	}
	_, err = s.cli.KeepAliveOnce(context.Background(), lease.ID)
	if err != nil {
		return nil, err
	}
	return lease, nil
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
	lease, err := s.getTimeoutLease()
	if err != nil {
		return err
	}
	put := clientv3.OpPut(idStr, ToJson(state), clientv3.WithLease(lease.ID))
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
			result, ok := slices.DeleteStr(ms, name)
			if !ok {
				continue
			}
			class.DBMeasurements[db] = result
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

	return ErrMeasurementNotExist(name)
}

func (s *Service) dropCqs(db string, name string) error {
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBcq)
	if resp.Count == 0 || err != nil {
		return ErrMetaDataDisappear
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
		return meta.ErrContinuousQueryNotFound
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
func (s *Service) watchUsers() {
	resp, err := s.cli.Get(context.Background(), TSDBUsers)
	s.CheckErrorExit("Get users info from etcd failed, stop watch users", err)
	if resp == nil || resp.Count == 0 {
		users := make(Users)
		s.latestUsers = users
		_, err = s.cli.Put(context.Background(), TSDBUsers, ToJson(users))
		s.CheckErrorExit("Update databases failed, stop watch databases, error message", err)
	} else {
		var users Users
		ParseJson(resp.Kvs[0].Value, &users)
		for _, u := range users {
			s.MetaClient.CreateUser(u.Name, u.Password, u.Admin)
			if u.Privileges == nil {
				continue
			}
			for d, p := range u.Privileges {
				s.MetaClient.SetPrivilege(u.Name, d, p)
			}
		}
		s.latestUsers = users
		users = nil
	}
	usersCh := s.cli.Watch(context.Background(), TSDBUsers+"-", clientv3.WithPrefix())
	for userResp := range usersCh {
		for _, event := range userResp.Events {
			if event.Type == mvccpb.PUT {
				var user User
				if bytes.Equal(event.Kv.Key, []byte(TSDBUserNew)) {
					ParseJson(event.Kv.Value, &user)
					s.MetaClient.CreateUser(user.Name, user.Password, user.Admin)
					&user = nil
					continue
				}
				if bytes.Equal(event.Kv.Key, []byte(TSDBUserAdmin)) {
					ParseJson(event.Kv.Value, &user)
					s.MetaClient.SetAdminPrivilege(user.Name, user.Admin)
					&user = nil
					continue
				}
				if bytes.Equal(event.Kv.Key, []byte(TSDBUserDel)) {
					ParseJson(event.Kv.Value, &user)
					s.MetaClient.DropUser(user.Name)
					continue
				}
				if bytes.Equal(event.Kv.Key, []byte(TSDBUserGrant)) {
					ParseJson(event.Kv.Value, &user)
					if user.Privileges == nil {
						continue
					}
					for db, p := range user.Privileges {
						s.MetaClient.SetPrivilege(user.Name, db, p)
					}
					continue
				}
				if bytes.Equal(event.Kv.Key, []byte(TSDBUserUpdate)) {
					ParseJson(event.Kv.Value, &user)
					s.MetaClient.UpdateUser(user.Name, user.Password)
					continue
				}
			}
		}
	}
}

func (s *Service) createDatabase(stmt *influxql.CreateDatabaseStatement) error {
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateDatabase`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBDatabase)
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return ErrMetaDataDisappear
	}
	ParseJson(resp.Kvs[0].Value, &s.databases)
	if s.databases[stmt.Name] != nil {
		return errors.New("Database " + stmt.Name + "exist")
	}
	if stmt.RetentionPolicyCreate {
		if stmt.RetentionPolicyName != "" && !meta.ValidName(stmt.RetentionPolicyName) {
			return meta.ErrInvalidName
		}
		rp := make(map[string]Rp, 0)
		rp[stmt.RetentionPolicyName] = Rp{
			Name:               stmt.RetentionPolicyName,
			Duration:           *stmt.RetentionPolicyDuration,
			Replica:            *stmt.RetentionPolicyReplication,
			ShardGroupDuration: stmt.RetentionPolicyShardGroupDuration,
		}
		s.databases[stmt.Name] = rp
	} else {
		s.databases[stmt.Name] = make(map[string]Rp)
	}
	databaseNew := make(Databases)
	databaseNew[stmt.Name] = s.databases[stmt.Name]
	cmp := clientv3.Compare(clientv3.Value(TSDBDatabase), "=", string(resp.Kvs[0].Value))
	opPut := clientv3.OpPut(TSDBDatabase, ToJson(s.databases))
	opPutNew := clientv3.OpPut(TSDBDatabaseNew, ToJson(databaseNew))
	txnResp, err := s.cli.Txn(context.Background()).If(cmp).Then(opPut, opPutNew).Commit()
	if txnResp.Succeeded && err == nil {
		return nil
	}
	goto Retry
}

func (s *Service) createRetentionPolicy(stmt *influxql.CreateRetentionPolicyStatement) error {
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateRetentionPolicy`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}
	if &stmt.Duration != nil && stmt.Duration < meta.MinRetentionPolicyDuration && stmt.Duration != 0 {
		return meta.ErrRetentionPolicyDurationTooLow
	}
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBDatabase)
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return ErrMetaDataDisappear
	}
	ParseJson(resp.Kvs[0].Value, &s.databases)
	rpsNew := make(map[string]Rp)
	rps := s.databases[stmt.Database]
	if rps == nil {
		return meta.ErrRetentionPolicyExists
	}
	if rp := rps[stmt.Name]; &rp != nil {
		return meta.ErrContinuousQueryExists
	}
	rp := Rp{
		Name:               stmt.Name,
		Duration:           stmt.Duration,
		ShardGroupDuration: stmt.ShardGroupDuration,
		Replica:            stmt.Replication,
	}
	rps[stmt.Name] = rp
	s.databases[stmt.Database] = rps
	databases := make(Databases)
	rpsNew[stmt.Name] = rp
	databases[stmt.Database] = rpsNew
	cmp := clientv3.Compare(clientv3.Value(TSDBDatabase), "=", string(resp.Kvs[0].Value))
	opPut := clientv3.OpPut(TSDBDatabase, ToJson(s.databases))
	opPutNew := clientv3.OpPut(TSDBDatabaseNew, ToJson(databases))
	txnResp, err := s.cli.Txn(context.Background()).If(cmp).Then(opPutNew, opPut).Commit()
	if txnResp.Succeeded && err == nil {
		return nil
	}
	goto Retry
}

func (s *Service) dropRetentionPolicy(db string, name string) error {
	var opSub clientv3.Op
	var subscriptions Subscriptions
	subResp, err := s.cli.Get(context.Background(), TSDBSubscription)
	if subResp.Count == 0 {
		s.Logger.Error("DropDB get subscription failed, TSDBSubscription meta data disappear")
		subscriptions = make(Subscriptions)
		s.cli.Put(context.Background(), TSDBSubscription, ToJson(subscriptions))
	}
	if subscriptions == nil {
		ParseJson(subResp.Kvs[0].Value, &subscriptions)
	}
	if rp := subscriptions[db]; rp != nil {
		if sub := rp[name]; sub != nil {
			delete(rp, name)
			subscriptions[db] = rp
			opSub = clientv3.OpPut(TSDBSubscription, ToJson(subscriptions))
		}
	}
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBDatabase)
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return ErrMetaDataDisappear
	}
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	ParseJson(resp.Kvs[0].Value, &s.databases)
	if rp := s.databases[db][name]; &rp == nil {
		return meta.ErrRetentionPolicyNotFound
	}
	// put delete data for TSDBDatabaseDel
	delDatabases := make(Databases)
	rp := make(map[string]Rp)
	rp[name] = s.databases[db][name]
	delDatabases[db] = rp
	opPutDelDB := clientv3.OpPut(TSDBDatabaseDel, ToJson(delDatabases))
	delete(s.databases[db], name)
	cmp := clientv3.Compare(clientv3.Value(TSDBDatabase), "=", string(resp.Kvs[0].Value))
	opPut := clientv3.OpPut(TSDBDatabase, ToJson(s.databases))
	var txnResp *clientv3.TxnResponse
	if &opSub == nil {
		txnResp, err = s.cli.Txn(context.Background()).If(cmp).Then(opPut, opPutDelDB).Commit()
	} else {
		txnResp, err = s.cli.Txn(context.Background()).If(cmp).Then(opSub, opPut, opPutDelDB).Commit()
	}
	if txnResp.Succeeded && err == nil {
		return nil
	}
	goto Retry
}

func (s *Service) dropDB(name string) error {
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	var opSub clientv3.Op
	var subscriptions Subscriptions
	subResp, err := s.cli.Get(context.Background(), TSDBSubscription)
	if subResp.Count == 0 {
		s.Logger.Error("DropDB get subscription failed, TSDBSubscription meta data disappear")
		subscriptions = make(Subscriptions)
		s.cli.Put(context.Background(), TSDBSubscription, ToJson(subscriptions))
	}
	if subscriptions == nil {
		ParseJson(subResp.Kvs[0].Value, &subscriptions)
	}
	if rp := subscriptions[name]; rp != nil {
		delete(subscriptions, name)
		opSub = clientv3.OpPut(TSDBSubscription, ToJson(subscriptions))
	}
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBDatabase)
	if err != nil || resp.Count == 0 {
		return errors.New("DeleteDB Get latest dbs error")
	}
	ParseJson(resp.Kvs[0].Value, &s.databases)
	if ok := s.databases[name]; ok == nil {
		return meta.ErrDatabaseExists
	}
	delDB := make(Databases)
	delDB[name] = make(map[string]Rp)
	opPutDel := clientv3.OpPut(TSDBDatabaseDel, ToJson(delDB))
	delete(s.databases, name)
	cmp := clientv3.Compare(clientv3.Value(TSDBDatabase), "=", string(resp.Kvs[0].Value))
	put := clientv3.OpPut(TSDBDatabase, ToJson(s.databases))
	var txResp *clientv3.TxnResponse
	if &opSub == nil {
		txResp, err = s.cli.Txn(context.Background()).If(cmp).Then(put, opPutDel).Commit()
	} else {
		txResp, err = s.cli.Txn(context.Background()).If(cmp).Then(put, opSub, opPutDel).Commit()
	}
	if txResp.Succeeded && err == nil {
		return nil
	}
	goto Retry
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
	databaseInfo := s.cli.Watch(context.Background(), TSDBDatabase+"-", clientv3.WithPrefix())
	for database := range databaseInfo {
		s.Logger.Info("WatchDatabaseInfo get db change event")
		for _, event := range database.Events {
			s.dbsMu.Lock()
			var dbs Databases
			ParseJson(event.Kv.Value, &dbs)
			if bytes.Equal(event.Kv.Key, []byte(TSDBDatabaseNew)) {
				for db, rp := range dbs {
					oldDB := s.databases[db]
					if oldDB == nil {
						s.databases[db] = rp
						_, err = s.MetaClient.CreateDatabase(db)
						s.CheckErrPrintLog("WatchDatabasesInfo MetaClient CreateDatabase failed", err)
						continue
					}
					for rpName, rpInfo := range rp {
						if oldRp := oldDB[rpName]; &oldRp == nil {
							oldDB[rpName] = rpInfo
							_, err = s.MetaClient.CreateRetentionPolicy(db, &meta.RetentionPolicySpec{
								Duration:           &rpInfo.Duration,
								ReplicaN:           &rpInfo.Replica,
								ShardGroupDuration: rpInfo.ShardGroupDuration,
							}, false)
							s.CheckErrPrintLog("WatchDatabasesInfo MetaClient CreateRetentionPolicy failed", err)
							for _, sub := range s.subscriptions {
								err = s.MetaClient.CreateSubscription(db, rpName, sub.Name, sub.Mode, sub.Destinations)
								s.CheckErrPrintLog("WatchDatabasesInfo MetaClient CreateSubscription failed", err)
							}
						}
					}
					s.databases[db] = oldDB
				}
				continue
			}
			if bytes.Equal(event.Kv.Key, []byte(TSDBDatabaseUpdate)) {
				for db, rps := range dbs {
					oldDB := s.databases[db]
					for _, rp := range rps {
						oldDB[rp.Name] = rp
						err = s.MetaClient.UpdateRetentionPolicy(db, rp.Name, &meta.RetentionPolicyUpdate{
							Duration:           &rp.Duration,
							ReplicaN:           &rp.Replica,
							ShardGroupDuration: &rp.ShardGroupDuration,
						}, false)
						s.CheckErrPrintLog("WatchDatabasesInfo MetaClient UpdateRetentionPolicy failed", err)
					}
					s.databases[db] = oldDB
				}
				continue
			}
			if bytes.Equal(event.Kv.Key, []byte(TSDBDatabaseDel)) {
				for db, rps := range dbs {
					oldDB := s.databases[db]
					if len(rps) > 0 {
						for rp := range rps {
							delete(oldDB, rp)
							err = s.store.DeleteRetentionPolicy(db, rp)
							s.CheckErrPrintLog("WatchDatabasesInfo Store DeleteRetentionPolicy failed", err)
							err = s.MetaClient.DropRetentionPolicy(db, rp)
							s.CheckErrPrintLog("WatchDatabasesInfo MetaClient DropRetentionPolicy failed", err)
						}
						s.databases[db] = oldDB
						continue
					}
					delete(s.databases, db)
					err = s.store.DeleteDatabase(db)
					s.CheckErrPrintLog("WatchDatabasesInfo Store DeleteDatabase failed", err)
					err = s.MetaClient.DropDatabase(db)
					s.CheckErrPrintLog("WatchDatabasesInfo MetaClient DropDatabase  failed", err)
				}
			}
			s.dbsMu.Unlock()
		}
	}
}

func (s *Service) createClusterSubscription(q *influxql.CreateSubscriptionStatement) error {
	s.dbsMu.RLock()
	defer s.dbsMu.RUnlock()
	if rp := s.databases[q.Database]; rp != nil {
		if rpInfo := rp[q.RetentionPolicy]; &rpInfo != nil {
			for _, destination := range q.Destinations {
				err := meta.ValidateURL(destination)
				if err != nil {
					return err
				}
			}
			// validate pass
			resp, err := s.cli.Get(context.Background(), TSDBSubscription)
			if err != nil {
				return err
			}
			if resp.Count == 0 {
				return ErrMetaDataDisappear
			}
			var subscriptions Subscriptions
			ParseJson(resp.Kvs[0].Value, &subscriptions)
			newSubs := make([]Subscription, 0)
			if sub := subscriptions[q.Database][q.RetentionPolicy][q.Name]; &sub == nil {
				sub = Subscription{
					DB:           q.Database,
					RP:           q.RetentionPolicy,
					Name:         q.Name,
					Mode:         q.Mode,
					Destinations: q.Destinations,
				}
				subscriptions[q.Database][q.RetentionPolicy][q.Name] = sub
				newSubs = append(newSubs, sub)
			} else {
				return meta.ErrSubscriptionExists
			}
			cmp := clientv3.Compare(clientv3.Value(TSDBSubscription), "=", string(resp.Kvs[0].Value))
			opPut := clientv3.OpPut(TSDBSubscription, ToJson(subscriptions))
			opPutNew := clientv3.OpPut(TSDBSubscriptionNew, ToJson(newSubs))
			txnResp, err := s.cli.Txn(context.Background()).If(cmp).Then(opPut, opPutNew).Commit()
			if txnResp.Succeeded && err == nil {
				return nil
			}
		}
		return meta.ErrRetentionPolicyNotFound
	}
	return meta.ErrDatabaseNotExists
}

func (s *Service) dropClusterSubscription(q *influxql.DropSubscriptionStatement) error {
	s.dbsMu.RLock()
	defer s.dbsMu.Unlock()
Retry:
	resp, err := s.cli.Get(context.Background(), TSDBSubscription)
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return ErrMetaDataDisappear
	}
	var subscriptions Subscriptions
	ParseJson(resp.Kvs[0].Value, &subscriptions)
	originalRpsMap := subscriptions[q.Database]
	originalSubsMap := originalRpsMap[q.RetentionPolicy]
	originalSub := originalSubsMap[q.Name]
	// the sub has bean delete
	if &originalSub == nil {
		return meta.ErrSubscriptionNotFound
	}
	// create delete subscription
	subDel := make(Subscriptions)
	subDelMap := make(map[string]Subscription)
	subDelMap[q.Name] = originalSub
	rpMap := make(map[string]map[string]Subscription)
	rpMap[q.RetentionPolicy] = subDelMap
	subDel[q.Database] = rpMap

	delete(originalSubsMap, q.Name)
	originalRpsMap[q.RetentionPolicy] = originalSubsMap
	subscriptions[q.Database] = originalRpsMap
	cmp := clientv3.Compare(clientv3.Value(TSDBSubscription), "=", string(resp.Kvs[0].Value))
	opPut := clientv3.OpPut(TSDBSubscription, ToJson(subscriptions))
	opPutDel := clientv3.OpPut(TSDBDatabaseDel, ToJson(subDel))
	txnResp, err := s.cli.Txn(context.Background()).If(cmp).Then(opPut, opPutDel).Commit()
	if txnResp.Succeeded && err == nil {
		return nil
	}
	goto Retry
}

func (s *Service) watchSubscriptions() {
	resp, err := s.cli.Get(context.Background(), TSDBSubscription)
	s.CheckErrorExit("WatchSubscriptions get subscriptions failed, err message", err)
	var subscriptions Subscriptions
	if resp.Count == 0 {
		subscriptions = make(Subscriptions)
		s.cli.Put(context.Background(), TSDBSubscription, ToJson(subscriptions))
	}
	if subscriptions == nil {
		ParseJson(resp.Kvs[0].Value, &subscriptions)
	}
	// Every cluster's master node of the class need transfer data to cluster subscription
	if s.masterNode.Id == s.MetaClient.Data().NodeID {
		for _, rps := range subscriptions {
			for _, subs := range rps {
				for _, sub := range subs {
					err = s.MetaClient.CreateSubscription(sub.DB, sub.RP, sub.Name, sub.Mode, sub.Destinations)
					s.CheckErrorExit("sync remote meta data ", err)
				}
			}
		}
	}
	subChan := s.cli.Watch(context.Background(), TSDBSubscription, clientv3.WithPrefix())
	for subInfo := range subChan {
		for _, event := range subInfo.Events {
			if s.masterNode.Id == s.MetaClient.Data().NodeID {
				if bytes.Equal(event.Kv.Key, []byte(TSDBSubscriptionNew)) {
					var subs []Subscription
					ParseJson(event.Kv.Value, &subs)
					for _, sub := range subs {
						err = s.MetaClient.CreateSubscription(sub.DB, sub.RP, sub.Name, sub.Mode, sub.Destinations)
						s.CheckErrPrintLog("WatchSubscriptions get create sub event, MetaClient CreateSub failed", err)
					}
					continue
				}
				if bytes.Equal(event.Kv.Key, []byte(TSDBSubscriptionDel)) {
					var subs []Subscription
					ParseJson(event.Kv.Value, &subs)
					for _, subs := range subs {
						err = s.MetaClient.DropSubscription(subs.DB, subs.RP, subs.Name)
						s.CheckErrPrintLog("WatchSubscriptions get del sub event, MetaClient del sub failed", err)
					}
				}
			}
		}
	}
}
