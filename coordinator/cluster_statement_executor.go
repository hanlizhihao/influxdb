package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/pkg/tracing/fields"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// ErrDatabaseNameRequired is returned when executing statements that require a database,
// when a database has not been provided.

// StatementExecutor executes a statement in the query.
type ClusterStatementExecutor struct {
	StatementExecutor
}

// ExecuteStatement executes the given statement with the given execution context.
func (e *ClusterStatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx *query.ExecutionContext) error {
	// Select statements are handled separately so that they can be streamed.
	if stmt, ok := stmt.(*influxql.SelectStatement); ok {
		return e.executeSelectStatement(stmt, ctx)
	}

	var rows models.Rows
	var messages []*query.Message
	var err error
	switch stmt := stmt.(type) {
	case *influxql.AlterRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeAlterRetentionPolicyStatement(stmt)
	case *influxql.CreateContinuousQueryStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateContinuousQueryStatement(stmt)
	case *influxql.CreateDatabaseStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateDatabaseStatement(stmt)
	case *influxql.CreateRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateRetentionPolicyStatement(stmt)
	case *influxql.CreateSubscriptionStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateSubscriptionStatement(stmt)
	case *influxql.CreateUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateUserStatement(stmt)
	case *influxql.DeleteSeriesStatement:
		messages, err = e.executeDistributeSql(ctx, messages, stmt)
		// err = e.executeDeleteSeriesStatement(stmt, ctx.Database)
	case *influxql.DropContinuousQueryStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropContinuousQueryStatement(stmt)
	case *influxql.DropDatabaseStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropDatabaseStatement(stmt)
	case *influxql.DropMeasurementStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropMeasurementStatement(stmt, ctx.Database)
	case *influxql.DropSeriesStatement:
		messages, err = e.executeDistributeSql(ctx, messages, stmt)
		// err = e.executeDropSeriesStatement(stmt, ctx.Database)
	case *influxql.DropRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropRetentionPolicyStatement(stmt)
	case *influxql.DropShardStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropShardStatement(stmt)
	case *influxql.DropSubscriptionStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropSubscriptionStatement(stmt)
	case *influxql.DropUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropUserStatement(stmt)
	case *influxql.ExplainStatement:
		if stmt.Analyze {
			rows, err = e.executeExplainAnalyzeStatement(stmt, ctx)
		} else {
			rows, err = e.executeExplainStatement(stmt, ctx)
		}
	case *influxql.GrantStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeGrantStatement(stmt)
	case *influxql.GrantAdminStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeGrantAdminStatement(stmt)
	case *influxql.RevokeStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeRevokeStatement(stmt)
	case *influxql.RevokeAdminStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeRevokeAdminStatement(stmt)
	case *influxql.ShowContinuousQueriesStatement:
		rows, err = e.executeShowContinuousQueriesStatement(stmt)
	case *influxql.ShowDatabasesStatement:
		rows, err = e.executeShowDatabasesStatement(stmt, ctx)
	case *influxql.ShowDiagnosticsStatement:
		rows, err = e.executeShowDiagnosticsStatement(stmt)
	case *influxql.ShowGrantsForUserStatement:
		rows, err = e.executeShowGrantsForUserStatement(stmt)
	case *influxql.ShowMeasurementsStatement:
		return e.executeShowMeasurementsStatement(stmt, ctx)
	case *influxql.ShowMeasurementCardinalityStatement:
		rows, err = e.executeShowMeasurementCardinalityStatement(stmt)
	case *influxql.ShowRetentionPoliciesStatement:
		rows, err = e.executeShowRetentionPoliciesStatement(stmt)
	case *influxql.ShowSeriesCardinalityStatement:
		rows, err = e.executeShowSeriesCardinalityStatement(stmt)
	case *influxql.ShowShardsStatement:
		rows, err = e.executeShowShardsStatement(stmt)
	case *influxql.ShowShardGroupsStatement:
		rows, err = e.executeShowShardGroupsStatement(stmt)
	case *influxql.ShowStatsStatement:
		rows, err = e.executeShowStatsStatement(stmt)
	case *influxql.ShowSubscriptionsStatement:
		rows, err = e.executeShowSubscriptionsStatement(stmt)
	case *influxql.ShowTagKeysStatement:
		return e.executeShowTagKeys(stmt, ctx)
	case *influxql.ShowTagValuesStatement:
		return e.executeShowTagValues(stmt, ctx)
	case *influxql.ShowUsersStatement:
		rows, err = e.executeShowUsersStatement(stmt)
	case *influxql.SetPasswordUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeSetPasswordUserStatement(stmt)
	case *influxql.ShowQueriesStatement, *influxql.KillQueryStatement:
		// Send query related statements to the task manager.
		return e.TaskManager.ExecuteStatement(stmt, ctx)
	default:
		return query.ErrInvalidQuery
	}

	if err != nil {
		return err
	}

	return ctx.Send(&query.Result{
		Series:   rows,
		Messages: messages,
	})
}

func (e *ClusterStatementExecutor) executeDistributeSql(ctx *query.ExecutionContext, messages []*query.Message,
	stmt influxql.Statement) ([]*query.Message, error) {
	if ctx.ReadOnly {
		messages = append(messages, query.ReadOnlyWarning(stmt.String()))
	}
	statement := &Statement{
		Sql:     stmt.String(),
		ExecOpt: ctx.ExecutionOptions,
	}
	return messages, e.EtcdService.putSql(statement)
}

func (e *ClusterStatementExecutor) executeAlterRetentionPolicyStatement(stmt *influxql.AlterRetentionPolicyStatement) error {
	e.EtcdService.dbsMu.Lock()
	defer e.EtcdService.dbsMu.Unlock()
	rps := e.EtcdService.databases[stmt.Database]
	if rps == nil {
		return influxdb.ErrRetentionPolicyNotFound(stmt.Database)
	}
	rp := rps[stmt.Name]
	if &rp == nil {
		return influxdb.ErrRetentionPolicyNotFound(stmt.Name)
	}
	if stmt.Duration != nil && *stmt.Duration < time.Hour && *stmt.Duration != 0 {
		return meta.ErrRetentionPolicyDurationTooLow
	}
	if (stmt.Duration != nil && *stmt.Duration > 0 &&
		((stmt.ShardGroupDuration != nil && *stmt.Duration < *stmt.ShardGroupDuration) ||
			(stmt.ShardGroupDuration == nil && *stmt.Duration < rp.ShardGroupDuration))) ||
		(stmt.Duration == nil && rp.Duration > 0 &&
			stmt.ShardGroupDuration != nil && rp.Duration < *stmt.ShardGroupDuration) {
		return meta.ErrIncompatibleDurations
	}
	rp.Duration = *stmt.Duration
	rp.NeedUpdate = true
	rp.Replica = *stmt.Replication
	rp.ShardGroupDuration = *stmt.ShardGroupDuration
	rps[stmt.Name] = rp
	e.EtcdService.databases[stmt.Database] = rps
	return e.EtcdService.PutMetaDataForEtcd(e.EtcdService.databases, TSDBDatabase)
}

func (e *ClusterStatementExecutor) executeCreateContinuousQueryStatement(q *influxql.CreateContinuousQueryStatement) error {
	// Verify that retention policies exist.
	var err error
	verifyRPFn := func(n influxql.Node) {
		if err != nil {
			return
		}
		switch m := n.(type) {
		case *influxql.Measurement:
			var rp *meta.RetentionPolicyInfo
			if rp, err = e.MetaClient.RetentionPolicy(m.Database, m.RetentionPolicy); err != nil {
				return
			} else if rp == nil {
				err = fmt.Errorf("%s: %s.%s", meta.ErrRetentionPolicyNotFound, m.Database, m.RetentionPolicy)
			}
		default:
			return
		}
	}

	influxql.WalkFunc(q, verifyRPFn)

	if err != nil {
		return err
	}

	resp, err := e.EtcdService.cli.Get(context.Background(), TSDBcq)
	if resp.Count == 0 || err != nil {
		return errors.New("Get meta data from etcd failed ")
	}
	var cqs Cqs
	ParseJson(resp.Kvs[0].Value, &cqs)
	dbCqs := cqs[q.Database]
	if dbCqs == nil {
		return influxdb.ErrDatabaseNotFound(q.Database)
	}
	for _, cq := range dbCqs {
		if cq.Name == q.Database {
			return meta.ErrContinuousQueryExists
		}
	}
	dbCqs = append(dbCqs, meta.ContinuousQueryInfo{Name: q.Name, Query: q.String()})
	return e.EtcdService.PutMetaDataForEtcd(cqs, TSDBcq)
}

func (e *ClusterStatementExecutor) executeCreateDatabaseStatement(stmt *influxql.CreateDatabaseStatement) error {
	e.EtcdService.dbsMu.Lock()
	defer e.EtcdService.dbsMu.Unlock()
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateDatabase`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}
	if e.EtcdService.databases[stmt.Name] != nil {
		return errors.New("Database " + stmt.Name + "exist")
	}
	if stmt.RetentionPolicyCreate {
		if stmt.RetentionPolicyName != "" && !meta.ValidName(stmt.RetentionPolicyName) {
			return meta.ErrInvalidName
		}
		rp := make(map[string]Rp, 1)
		rp[stmt.RetentionPolicyName] = Rp{
			Name:               stmt.RetentionPolicyName,
			Duration:           *stmt.RetentionPolicyDuration,
			Replica:            *stmt.RetentionPolicyReplication,
			ShardGroupDuration: stmt.RetentionPolicyShardGroupDuration,
		}
	} else {
		e.EtcdService.databases[stmt.Name] = make(map[string]Rp)
	}
	return e.EtcdService.PutMetaDataForEtcd(e.EtcdService.databases, TSDBDatabase)
}
func (e *ClusterStatementExecutor) executeCreateRetentionPolicyStatement(stmt *influxql.CreateRetentionPolicyStatement) error {
	e.EtcdService.dbsMu.Lock()
	defer e.EtcdService.dbsMu.Unlock()
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateRetentionPolicy`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}
	if &stmt.Duration != nil && stmt.Duration < meta.MinRetentionPolicyDuration && stmt.Duration != 0 {
		return meta.ErrRetentionPolicyDurationTooLow
	}
	rps := e.EtcdService.databases[stmt.Database]
	if rp := rps[stmt.Name]; &rp != nil {
		return meta.ErrContinuousQueryExists
	}
	rps[stmt.Database] = Rp{
		Name:               stmt.Name,
		Duration:           stmt.Duration,
		ShardGroupDuration: stmt.ShardGroupDuration,
		Replica:            stmt.Replication,
	}
	return e.EtcdService.PutMetaDataForEtcd(e.EtcdService.databases, TSDBDatabase)
}

func (e *ClusterStatementExecutor) executeCreateSubscriptionStatement(q *influxql.CreateSubscriptionStatement) error {
	return e.MetaClient.CreateSubscription(q.Database, q.RetentionPolicy, q.Name, q.Mode, q.Destinations)
}

func (e *ClusterStatementExecutor) executeCreateUserStatement(q *influxql.CreateUserStatement) error {
	e.EtcdService.mu.Lock()
	defer e.EtcdService.mu.Unlock()
	if user := e.EtcdService.latestUsers[q.Name]; &user != nil {
		return meta.ErrUserExists
	}
	e.EtcdService.latestUsers[q.Name] = User{
		Name:     q.Name,
		Password: q.Password,
		Admin:    q.Admin,
	}
	return e.EtcdService.PutMetaDataForEtcd(e.EtcdService.latestUsers, TSDBUsers)
}

func (e *ClusterStatementExecutor) executeDeleteSeriesStatement(stmt *influxql.DeleteSeriesStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return query.ErrDatabaseNotFound(database)
	}

	// Convert "now()" to current time.
	stmt.Condition = influxql.Reduce(stmt.Condition, &influxql.NowValuer{Now: time.Now().UTC()})

	// Locally delete the series.
	return e.TSDBStore.DeleteSeries(database, stmt.Sources, stmt.Condition)
}

func (e *ClusterStatementExecutor) executeDropContinuousQueryStatement(q *influxql.DropContinuousQueryStatement) error {
	return e.EtcdService.dropCqs(q.Database, q.Name)
}

// executeDropDatabaseStatement drops a database from the Cluster.
// It does not return an error if the database was not found on any of
// the nodes, or in the Meta store.
func (e *ClusterStatementExecutor) executeDropDatabaseStatement(stmt *influxql.DropDatabaseStatement) error {
	if e.MetaClient.Database(stmt.Name) == nil {
		return nil
	}

	// Locally delete the datababse.
	if err := e.TSDBStore.DeleteDatabase(stmt.Name); err != nil {
		return err
	}

	return e.EtcdService.dropDB(stmt.Name)
	//// Remove the database from the Meta Store.
	//return e.MetaClient.DropDatabase(stmt.Name)
}

func (e *ClusterStatementExecutor) executeDropMeasurementStatement(stmt *influxql.DropMeasurementStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return query.ErrDatabaseNotFound(database)
	}

	return e.EtcdService.dropMeasurement(database, stmt.Name)
	// Locally drop the measurement
	//return e.TSDBStore.DeleteMeasurement(database, stmt.Name)
}

func (e *ClusterStatementExecutor) executeDropSeriesStatement(stmt *influxql.DropSeriesStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return query.ErrDatabaseNotFound(database)
	}

	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return errors.New("DROP SERIES doesn't support time in WHERE clause")
	}

	// Locally drop the series.
	return e.TSDBStore.DeleteSeries(database, stmt.Sources, stmt.Condition)
}

func (e *ClusterStatementExecutor) executeDropShardStatement(stmt *influxql.DropShardStatement) error {
	// Locally delete the shard.
	if err := e.TSDBStore.DeleteShard(stmt.ID); err != nil {
		return err
	}

	// Remove the shard reference from the Meta Store.
	return e.MetaClient.DropShard(stmt.ID)
}

func (e *ClusterStatementExecutor) executeDropRetentionPolicyStatement(stmt *influxql.DropRetentionPolicyStatement) error {
	dbi := e.MetaClient.Database(stmt.Database)
	if dbi == nil {
		return nil
	}

	if dbi.RetentionPolicy(stmt.Name) == nil {
		return nil
	}

	// Locally drop the retention policy.
	if err := e.TSDBStore.DeleteRetentionPolicy(stmt.Database, stmt.Name); err != nil {
		return err
	}

	return e.EtcdService.dropRetentionPolicy(stmt.Database, stmt.Name)
	//return e.MetaClient.DropRetentionPolicy(stmt.Database, stmt.Name)
}

func (e *ClusterStatementExecutor) executeDropSubscriptionStatement(q *influxql.DropSubscriptionStatement) error {
	return e.MetaClient.DropSubscription(q.Database, q.RetentionPolicy, q.Name)
}

func (e *ClusterStatementExecutor) executeDropUserStatement(q *influxql.DropUserStatement) error {
	return e.MetaClient.DropUser(q.Name)
}

func (e *ClusterStatementExecutor) executeExplainStatement(q *influxql.ExplainStatement, ctx *query.ExecutionContext) (models.Rows, error) {
	opt := query.SelectOptions{
		NodeID:      ctx.ExecutionOptions.NodeID,
		MaxSeriesN:  e.MaxSelectSeriesN,
		MaxBucketsN: e.MaxSelectBucketsN,
		Authorizer:  ctx.Authorizer,
	}

	// Prepare the query for execution, but do not actually execute it.
	// This should perform any needed substitutions.
	p, err := query.Prepare(q.Statement, e.ShardMapper, opt)
	if err != nil {
		return nil, err
	}
	defer p.Close()

	plan, err := p.Explain()
	if err != nil {
		return nil, err
	}
	plan = strings.TrimSpace(plan)

	row := &models.Row{
		Columns: []string{"QUERY PLAN"},
	}
	for _, s := range strings.Split(plan, "\n") {
		row.Values = append(row.Values, []interface{}{s})
	}
	return models.Rows{row}, nil
}

func (e *ClusterStatementExecutor) executeExplainAnalyzeStatement(q *influxql.ExplainStatement, ectx *query.ExecutionContext) (models.Rows, error) {
	stmt := q.Statement
	t, span := tracing.NewTrace("select")
	ctx := tracing.NewContextWithTrace(ectx, t)
	ctx = tracing.NewContextWithSpan(ctx, span)
	var aux query.Iterators
	ctx = query.NewContextWithIterators(ctx, &aux)
	start := time.Now()

	cur, err := e.createIterators(ctx, stmt, ectx.ExecutionOptions)
	if err != nil {
		return nil, err
	}

	iterTime := time.Since(start)

	// Generate a row emitter from the iterator set.
	em := query.NewEmitter(cur, ectx.ChunkSize)

	// Emit rows to the results channel.
	var writeN int64
	for {
		var row *models.Row
		row, _, err = em.Emit()
		if err != nil {
			goto CLEANUP
		} else if row == nil {
			// Check if the query was interrupted while emitting.
			select {
			case <-ectx.Done():
				err = ectx.Err()
				goto CLEANUP
			default:
			}
			break
		}

		writeN += int64(len(row.Values))
	}

CLEANUP:
	em.Close()
	if err != nil {
		return nil, err
	}

	// close auxiliary iterators deterministically to finalize any captured measurements
	aux.Close()

	totalTime := time.Since(start)
	span.MergeFields(
		fields.Duration("total_time", totalTime),
		fields.Duration("planning_time", iterTime),
		fields.Duration("execution_time", totalTime-iterTime),
	)
	span.Finish()

	row := &models.Row{
		Columns: []string{"EXPLAIN ANALYZE"},
	}
	for _, s := range strings.Split(t.Tree().String(), "\n") {
		row.Values = append(row.Values, []interface{}{s})
	}

	return models.Rows{row}, nil
}

func (e *ClusterStatementExecutor) executeGrantStatement(stmt *influxql.GrantStatement) error {
	return e.MetaClient.SetPrivilege(stmt.User, stmt.On, stmt.Privilege)
}

func (e *ClusterStatementExecutor) executeGrantAdminStatement(stmt *influxql.GrantAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, true)
}

func (e *ClusterStatementExecutor) executeRevokeStatement(stmt *influxql.RevokeStatement) error {
	priv := influxql.NoPrivileges

	// Revoking all privileges means there's no need to look at existing user privileges.
	if stmt.Privilege != influxql.AllPrivileges {
		p, err := e.MetaClient.UserPrivilege(stmt.User, stmt.On)
		if err != nil {
			return err
		}
		// Bit clear (AND NOT) the user's privilege with the revoked privilege.
		priv = *p &^ stmt.Privilege
	}

	return e.MetaClient.SetPrivilege(stmt.User, stmt.On, priv)
}

func (e *ClusterStatementExecutor) executeRevokeAdminStatement(stmt *influxql.RevokeAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, false)
}

func (e *ClusterStatementExecutor) executeSetPasswordUserStatement(q *influxql.SetPasswordUserStatement) error {
	return e.MetaClient.UpdateUser(q.Name, q.Password)
}

func (e *ClusterStatementExecutor) executeSelectStatement(stmt *influxql.SelectStatement, ctx *query.ExecutionContext) error {
	cur, err := e.createIterators(ctx, stmt, ctx.ExecutionOptions)
	if err != nil {
		return err
	}

	// Generate a row emitter from the iterator set.
	em := query.NewEmitter(cur, ctx.ChunkSize)
	defer em.Close()

	// Emit rows to the results channel.
	var writeN int64
	var emitted bool

	var pointsWriter *BufferedPointsWriter
	if stmt.Target != nil {
		pointsWriter = NewBufferedPointsWriter(e.PointsWriter, stmt.Target.Measurement.Database, stmt.Target.Measurement.RetentionPolicy, 10000)
	}

	for {
		row, partial, err := em.Emit()
		if err != nil {
			return err
		} else if row == nil {
			// Check if the query was interrupted while emitting.
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			break
		}

		// Write points back into system for INTO statements.
		if stmt.Target != nil {
			n, err := e.writeInto(pointsWriter, stmt, row)
			if err != nil {
				return err
			}
			writeN += n
			continue
		}

		result := &query.Result{
			Series:  []*models.Row{row},
			Partial: partial,
		}

		// Send results or exit if closing.
		if err := ctx.Send(result); err != nil {
			return err
		}

		emitted = true
	}

	// Flush remaining points and emit write count if an INTO statement.
	if stmt.Target != nil {
		if err := pointsWriter.Flush(); err != nil {
			return err
		}

		var messages []*query.Message
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}

		return ctx.Send(&query.Result{
			Messages: messages,
			Series: []*models.Row{{
				Name:    "result",
				Columns: []string{"time", "written"},
				Values:  [][]interface{}{{time.Unix(0, 0).UTC(), writeN}},
			}},
		})
	}

	// Always emit at least one result.
	if !emitted {
		return ctx.Send(&query.Result{
			Series: make([]*models.Row, 0),
		})
	}
	return nil
}

func (e *ClusterStatementExecutor) createIterators(ctx context.Context, stmt *influxql.SelectStatement, opt query.ExecutionOptions) (query.Cursor, error) {
	sopt := query.SelectOptions{
		NodeID:      opt.NodeID,
		MaxSeriesN:  e.MaxSelectSeriesN,
		MaxPointN:   e.MaxSelectPointN,
		MaxBucketsN: e.MaxSelectBucketsN,
		Authorizer:  opt.Authorizer,
	}

	// Create a set of iterators from a selection.
	cur, err := query.Select(ctx, stmt, e.ShardMapper, sopt)
	if err != nil {
		return nil, err
	}
	return cur, nil
}

func (e *ClusterStatementExecutor) executeShowContinuousQueriesStatement(stmt *influxql.ShowContinuousQueriesStatement) (models.Rows, error) {
	dis := e.MetaClient.Databases()

	rows := []*models.Row{}
	for _, di := range dis {
		row := &models.Row{Columns: []string{"name", "query"}, Name: di.Name}
		for _, cqi := range di.ContinuousQueries {
			row.Values = append(row.Values, []interface{}{cqi.Name, cqi.Query})
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *ClusterStatementExecutor) executeShowDatabasesStatement(q *influxql.ShowDatabasesStatement, ctx *query.ExecutionContext) (models.Rows, error) {
	dis := e.MetaClient.Databases()
	a := ctx.ExecutionOptions.Authorizer

	row := &models.Row{Name: "databases", Columns: []string{"name"}}
	for _, di := range dis {
		// Only include databases that the user is authorized to read or write.
		if a.AuthorizeDatabase(influxql.ReadPrivilege, di.Name) || a.AuthorizeDatabase(influxql.WritePrivilege, di.Name) {
			row.Values = append(row.Values, []interface{}{di.Name})
		}
	}
	return []*models.Row{row}, nil
}

func (e *ClusterStatementExecutor) executeShowDiagnosticsStatement(stmt *influxql.ShowDiagnosticsStatement) (models.Rows, error) {
	diags, err := e.Monitor.Diagnostics()
	if err != nil {
		return nil, err
	}

	// Get a sorted list of diagnostics keys.
	sortedKeys := make([]string, 0, len(diags))
	for k := range diags {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	rows := make([]*models.Row, 0, len(diags))
	for _, k := range sortedKeys {
		if stmt.Module != "" && k != stmt.Module {
			continue
		}

		row := &models.Row{Name: k}

		row.Columns = diags[k].Columns
		row.Values = diags[k].Rows
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *ClusterStatementExecutor) executeShowGrantsForUserStatement(q *influxql.ShowGrantsForUserStatement) (models.Rows, error) {
	priv, err := e.MetaClient.UserPrivileges(q.Name)
	if err != nil {
		return nil, err
	}

	row := &models.Row{Columns: []string{"database", "privilege"}}
	for d, p := range priv {
		row.Values = append(row.Values, []interface{}{d, p.String()})
	}
	return []*models.Row{row}, nil
}

func (e *ClusterStatementExecutor) executeShowMeasurementsStatement(q *influxql.ShowMeasurementsStatement, ctx *query.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	names, err := e.TSDBStore.MeasurementNames(ctx.Authorizer, q.Database, q.Condition)
	if err != nil || len(names) == 0 {
		return ctx.Send(&query.Result{
			Err: err,
		})
	}

	if q.Offset > 0 {
		if q.Offset >= len(names) {
			names = nil
		} else {
			names = names[q.Offset:]
		}
	}

	if q.Limit > 0 {
		if q.Limit < len(names) {
			names = names[:q.Limit]
		}
	}

	values := make([][]interface{}, len(names))
	for i, name := range names {
		values[i] = []interface{}{string(name)}
	}

	if len(values) == 0 {
		return ctx.Send(&query.Result{})
	}

	return ctx.Send(&query.Result{
		Series: []*models.Row{{
			Name:    "measurements",
			Columns: []string{"name"},
			Values:  values,
		}},
	})
}

func (e *ClusterStatementExecutor) executeShowMeasurementCardinalityStatement(stmt *influxql.ShowMeasurementCardinalityStatement) (models.Rows, error) {
	if stmt.Database == "" {
		return nil, ErrDatabaseNameRequired
	}

	n, err := e.TSDBStore.MeasurementsCardinality(stmt.Database)
	if err != nil {
		return nil, err
	}

	return []*models.Row{&models.Row{
		Columns: []string{"cardinality estimation"},
		Values:  [][]interface{}{{n}},
	}}, nil
}

func (e *ClusterStatementExecutor) executeShowRetentionPoliciesStatement(q *influxql.ShowRetentionPoliciesStatement) (models.Rows, error) {
	if q.Database == "" {
		return nil, ErrDatabaseNameRequired
	}

	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return nil, influxdb.ErrDatabaseNotFound(q.Database)
	}

	row := &models.Row{Columns: []string{"name", "duration", "shardGroupDuration", "replicaN", "default"}}
	for _, rpi := range di.RetentionPolicies {
		row.Values = append(row.Values, []interface{}{rpi.Name, rpi.Duration.String(), rpi.ShardGroupDuration.String(), rpi.ReplicaN, di.DefaultRetentionPolicy == rpi.Name})
	}
	return []*models.Row{row}, nil
}

func (e *ClusterStatementExecutor) executeShowShardsStatement(stmt *influxql.ShowShardsStatement) (models.Rows, error) {
	dis := e.MetaClient.Databases()

	rows := []*models.Row{}
	for _, di := range dis {
		row := &models.Row{Columns: []string{"id", "database", "retention_policy", "shard_group", "start_time", "end_time", "expiry_time", "owners"}, Name: di.Name}
		for _, rpi := range di.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				// Shards associated with deleted shard groups are effectively deleted.
				// Don't list them.
				if sgi.Deleted() {
					continue
				}

				for _, si := range sgi.Shards {
					ownerIDs := make([]uint64, len(si.Owners))
					for i, owner := range si.Owners {
						ownerIDs[i] = owner.NodeID
					}

					row.Values = append(row.Values, []interface{}{
						si.ID,
						di.Name,
						rpi.Name,
						sgi.ID,
						sgi.StartTime.UTC().Format(time.RFC3339),
						sgi.EndTime.UTC().Format(time.RFC3339),
						sgi.EndTime.Add(rpi.Duration).UTC().Format(time.RFC3339),
						joinUint64(ownerIDs),
					})
				}
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *ClusterStatementExecutor) executeShowSeriesCardinalityStatement(stmt *influxql.ShowSeriesCardinalityStatement) (models.Rows, error) {
	if stmt.Database == "" {
		return nil, ErrDatabaseNameRequired
	}

	n, err := e.TSDBStore.SeriesCardinality(stmt.Database)
	if err != nil {
		return nil, err
	}

	return []*models.Row{&models.Row{
		Columns: []string{"cardinality estimation"},
		Values:  [][]interface{}{{n}},
	}}, nil
}

func (e *ClusterStatementExecutor) executeShowShardGroupsStatement(stmt *influxql.ShowShardGroupsStatement) (models.Rows, error) {
	dis := e.MetaClient.Databases()

	row := &models.Row{Columns: []string{"id", "database", "retention_policy", "start_time", "end_time", "expiry_time"}, Name: "shard groups"}
	for _, di := range dis {
		for _, rpi := range di.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				// Shards associated with deleted shard groups are effectively deleted.
				// Don't list them.
				if sgi.Deleted() {
					continue
				}

				row.Values = append(row.Values, []interface{}{
					sgi.ID,
					di.Name,
					rpi.Name,
					sgi.StartTime.UTC().Format(time.RFC3339),
					sgi.EndTime.UTC().Format(time.RFC3339),
					sgi.EndTime.Add(rpi.Duration).UTC().Format(time.RFC3339),
				})
			}
		}
	}

	return []*models.Row{row}, nil
}

func (e *ClusterStatementExecutor) executeShowStatsStatement(stmt *influxql.ShowStatsStatement) (models.Rows, error) {
	var rows []*models.Row

	if _, ok := e.TSDBStore.(*tsdb.Store); stmt.Module == "indexes" && ok {
		// The cost of collecting indexes metrics grows with the size of the indexes, so only collect this
		// stat when explicitly requested.
		b := e.TSDBStore.(*tsdb.Store).IndexBytes()
		row := &models.Row{
			Name:    "indexes",
			Columns: []string{"memoryBytes"},
			Values:  [][]interface{}{{b}},
		}
		rows = append(rows, row)

	} else {
		stats, err := e.Monitor.Statistics(nil)
		if err != nil {
			return nil, err
		}

		for _, stat := range stats {
			if stmt.Module != "" && stat.Name != stmt.Module {
				continue
			}
			row := &models.Row{Name: stat.Name, Tags: stat.Tags}

			values := make([]interface{}, 0, len(stat.Values))
			for _, k := range stat.ValueNames() {
				row.Columns = append(row.Columns, k)
				values = append(values, stat.Values[k])
			}
			row.Values = [][]interface{}{values}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (e *ClusterStatementExecutor) executeShowSubscriptionsStatement(stmt *influxql.ShowSubscriptionsStatement) (models.Rows, error) {
	dis := e.MetaClient.Databases()

	rows := []*models.Row{}
	for _, di := range dis {
		row := &models.Row{Columns: []string{"retention_policy", "name", "mode", "destinations"}, Name: di.Name}
		for _, rpi := range di.RetentionPolicies {
			for _, si := range rpi.Subscriptions {
				row.Values = append(row.Values, []interface{}{rpi.Name, si.Name, si.Mode, si.Destinations})
			}
		}
		if len(row.Values) > 0 {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (e *ClusterStatementExecutor) executeShowTagKeys(q *influxql.ShowTagKeysStatement, ctx *query.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Determine shard set based on database and time range.
	// SHOW TAG KEYS returns all tag keys for the default retention policy.
	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return fmt.Errorf("database not found: %s", q.Database)
	}

	// Determine appropriate time range. If one or fewer time boundaries provided
	// then min/max possible time should be used instead.
	valuer := &influxql.NowValuer{Now: time.Now()}
	cond, timeRange, err := influxql.ConditionExpr(q.Condition, valuer)
	if err != nil {
		return err
	}

	// Get all shards for all retention policies.
	var allGroups []meta.ShardGroupInfo
	for _, rpi := range di.RetentionPolicies {
		sgis, err := e.MetaClient.ShardGroupsByTimeRange(q.Database, rpi.Name, timeRange.MinTime(), timeRange.MaxTime())
		if err != nil {
			return err
		}
		allGroups = append(allGroups, sgis...)
	}

	var shardIDs []uint64
	for _, sgi := range allGroups {
		for _, si := range sgi.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}

	tagKeys, err := e.TSDBStore.TagKeys(ctx.Authorizer, shardIDs, cond)
	if err != nil {
		return ctx.Send(&query.Result{
			Err: err,
		})
	}

	emitted := false
	for _, m := range tagKeys {
		keys := m.Keys

		if q.Offset > 0 {
			if q.Offset >= len(keys) {
				keys = nil
			} else {
				keys = keys[q.Offset:]
			}
		}
		if q.Limit > 0 && q.Limit < len(keys) {
			keys = keys[:q.Limit]
		}

		if len(keys) == 0 {
			continue
		}

		row := &models.Row{
			Name:    m.Measurement,
			Columns: []string{"tagKey"},
			Values:  make([][]interface{}, len(keys)),
		}
		for i, key := range keys {
			row.Values[i] = []interface{}{key}
		}

		if err := ctx.Send(&query.Result{
			Series: []*models.Row{row},
		}); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ctx.Send(&query.Result{})
	}
	return nil
}

func (e *ClusterStatementExecutor) executeShowTagValues(q *influxql.ShowTagValuesStatement, ctx *query.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Determine shard set based on database and time range.
	// SHOW TAG VALUES returns all tag values for the default retention policy.
	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return fmt.Errorf("database not found: %s", q.Database)
	}

	// Determine appropriate time range. If one or fewer time boundaries provided
	// then min/max possible time should be used instead.
	valuer := &influxql.NowValuer{Now: time.Now()}
	cond, timeRange, err := influxql.ConditionExpr(q.Condition, valuer)
	if err != nil {
		return err
	}

	// Get all shards for all retention policies.
	var allGroups []meta.ShardGroupInfo
	for _, rpi := range di.RetentionPolicies {
		sgis, err := e.MetaClient.ShardGroupsByTimeRange(q.Database, rpi.Name, timeRange.MinTime(), timeRange.MaxTime())
		if err != nil {
			return err
		}
		allGroups = append(allGroups, sgis...)
	}

	var shardIDs []uint64
	for _, sgi := range allGroups {
		for _, si := range sgi.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}

	tagValues, err := e.TSDBStore.TagValues(ctx.Authorizer, shardIDs, cond)
	if err != nil {
		return ctx.Send(&query.Result{Err: err})
	}

	emitted := false
	for _, m := range tagValues {
		values := m.Values

		if q.Offset > 0 {
			if q.Offset >= len(values) {
				values = nil
			} else {
				values = values[q.Offset:]
			}
		}

		if q.Limit > 0 {
			if q.Limit < len(values) {
				values = values[:q.Limit]
			}
		}

		if len(values) == 0 {
			continue
		}

		row := &models.Row{
			Name:    m.Measurement,
			Columns: []string{"key", "value"},
			Values:  make([][]interface{}, len(values)),
		}
		for i, v := range values {
			row.Values[i] = []interface{}{v.Key, v.Value}
		}

		if err := ctx.Send(&query.Result{
			Series: []*models.Row{row},
		}); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ctx.Send(&query.Result{})
	}
	return nil
}

func (e *ClusterStatementExecutor) executeShowUsersStatement(q *influxql.ShowUsersStatement) (models.Rows, error) {
	row := &models.Row{Columns: []string{"user", "admin"}}
	for _, ui := range e.MetaClient.Users() {
		row.Values = append(row.Values, []interface{}{ui.Name, ui.Admin})
	}
	return []*models.Row{row}, nil
}

// NormalizeStatement adds a default database and policy to the measurements in statement.
// Parameter defaultRetentionPolicy can be "".
func (e *ClusterStatementExecutor) NormalizeStatement(stmt influxql.Statement, defaultDatabase, defaultRetentionPolicy string) (err error) {
	influxql.WalkFunc(stmt, func(node influxql.Node) {
		if err != nil {
			return
		}
		switch node := node.(type) {
		case *influxql.ShowRetentionPoliciesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowMeasurementsStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowTagKeysStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowTagValuesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowMeasurementCardinalityStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowSeriesCardinalityStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.Measurement:
			switch stmt.(type) {
			case *influxql.DropSeriesStatement, *influxql.DeleteSeriesStatement:
				// DB and RP not supported by these statements so don't rewrite into invalid
				// statements
			default:
				err = e.normalizeMeasurement(node, defaultDatabase, defaultRetentionPolicy)
			}
		}
	})
	return
}

func (e *ClusterStatementExecutor) normalizeMeasurement(m *influxql.Measurement, defaultDatabase, defaultRetentionPolicy string) error {
	// Targets (measurements in an INTO clause) can have blank names, which means it will be
	// the same as the measurement name it came from in the FROM clause.
	if !m.IsTarget && m.Name == "" && m.SystemIterator == "" && m.Regex == nil {
		return errors.New("invalid measurement")
	}

	// Measurement does not have an explicit database? Insert default.
	if m.Database == "" {
		m.Database = defaultDatabase
	}

	// The database must now be specified by this point.
	if m.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Find database.
	di := e.MetaClient.Database(m.Database)
	if di == nil {
		return influxdb.ErrDatabaseNotFound(m.Database)
	}

	// If no retention policy was specified, use the default.
	if m.RetentionPolicy == "" {
		if defaultRetentionPolicy != "" {
			m.RetentionPolicy = defaultRetentionPolicy
		} else if di.DefaultRetentionPolicy != "" {
			m.RetentionPolicy = di.DefaultRetentionPolicy
		} else {
			return fmt.Errorf("default retention policy not set for: %s", di.Name)
		}
	}
	return nil
}
func (e *ClusterStatementExecutor) writeInto(w pointsWriter, stmt *influxql.SelectStatement, row *models.Row) (n int64, err error) {
	if stmt.Target.Measurement.Database == "" {
		return 0, errNoDatabaseInTarget
	}

	// It might seem a bit weird that this is where we do this, since we will have to
	// convert rows back to points. The Executors (both aggregate and raw) are complex
	// enough that changing them to write back to the DB is going to be clumsy
	//
	// it might seem weird to have the write be in the Executor, but the interweaving of
	// limitedRowWriter and ExecuteAggregate/Raw makes it ridiculously hard to make sure that the
	// results will be the same as when queried normally.
	name := stmt.Target.Measurement.Name
	if name == "" {
		name = row.Name
	}

	points, err := convertRowToPoints(name, row)
	if err != nil {
		return 0, err
	}

	if err := w.WritePointsInto(&IntoWriteRequest{
		Database:        stmt.Target.Measurement.Database,
		RetentionPolicy: stmt.Target.Measurement.RetentionPolicy,
		Points:          points,
	}); err != nil {
		return 0, err
	}

	return int64(len(points)), nil
}
