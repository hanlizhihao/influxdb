package coordinator

import (
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
		return ErrMetaDataDisappear()
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
		return ErrMetaDataDisappear()
	}
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	ParseJson(resp.Kvs[0].Value, &s.databases)
	if rp := s.databases[db][name]; &rp == nil {
		return nil
	}
	delete(s.databases[db], name)
	cmp := clientv3.Compare(clientv3.Value(TSDBDatabase), "=", string(resp.Kvs[0].Value))
	opPut := clientv3.OpPut(TSDBDatabase, ToJson(s.databases))
	var txnResp *clientv3.TxnResponse
	if &opSub == nil {
		txnResp, err = s.cli.Txn(context.Background()).If(cmp).Then(opPut).Commit()
	} else {
		txnResp, err = s.cli.Txn(context.Background()).If(cmp).Then(opSub, opPut).Commit()
	}
	if txnResp.Succeeded && err == nil {
		return nil
	}
	goto Retry
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
				// validate pass
				resp, err := s.cli.Get(context.Background(), TSDBSubscription)
				var subscriptions Subscriptions
				ParseJson(resp.Kvs[0].Value, &subscriptions)
				if sub := subscriptions[q.Database][q.RetentionPolicy][q.Name]; &sub == nil {
					subscriptions[q.Database][q.RetentionPolicy][q.Name] = Subscription{
						DB:           q.Database,
						RP:           q.RetentionPolicy,
						Name:         q.Name,
						Mode:         q.Mode,
						Destinations: q.Destinations,
					}
				}
				cmp := clientv3.Compare(clientv3.Value(TSDBSubscription), "=", string(resp.Kvs[0].Value))
				opPut := clientv3.OpPut(TSDBSubscription, ToJson(subscriptions))
				txnResp, err := s.cli.Txn(context.Background()).If(cmp).Then(opPut).Commit()
				if txnResp.Succeeded && err == nil {
					return nil
				}
			}
		}
		return meta.ErrRetentionPolicyNotFound
	}
	return meta.ErrDatabaseNotExists
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
	delete(s.databases, name)
	cmp := clientv3.Compare(clientv3.Value(TSDBDatabase), "=", string(resp.Kvs[0].Value))
	put := clientv3.OpPut(TSDBDatabase, ToJson(s.databases))
	var txResp *clientv3.TxnResponse
	if &opSub == nil {
		txResp, err = s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	} else {
		txResp, err = s.cli.Txn(context.Background()).If(cmp).Then(put, opSub).Commit()
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
	databaseInfo := s.cli.Watch(context.Background(), TSDBDatabase, clientv3.WithPrefix())
	for database := range databaseInfo {
		s.Logger.Info("The database has changed,db")
		for _, db := range database.Events {
			localDBInfo := make(map[string]map[string]Rp, len(s.MetaClient.Databases()))
			s.dbsMu.Lock()
			ParseJson(db.Kv.Value, &s.databases)
			s.dbsMu.Unlock()

			// Correlation deletion cluster subscription
			var additionalDB = s.databases
			// Add new database and retention policy
			for _, localDB := range s.MetaClient.Databases() {
				rps := s.databases[localDB.Name]
				if rps == nil {
					s.store.DeleteDatabase(localDB.Name)
					s.MetaClient.DropDatabase(localDB.Name)
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
						s.store.DeleteRetentionPolicy(localDB.Name, localRP.Name)
						s.MetaClient.DropRetentionPolicy(localDB.Name, localRP.Name)
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
					s.databases[localDB.Name] = rps
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

func (s Service) watchSubscriptions() {
	resp, err := s.cli.Get(context.Background(), TSDBSubscription)
	if err != nil || resp.Count == 0 {
		// todo initial

	}
}
