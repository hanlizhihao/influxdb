package coordinator

import (
	"context"
	"go.uber.org/zap"
	"io"
	"net/rpc"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// IteratorCreator is an interface that combines mapping fields and creating iterators.
type IteratorCreator interface {
	query.IteratorCreator
	influxql.FieldMapper
	io.Closer
}

// LocalShardMapper implements a ShardMapper for local shards.
type LocalShardMapper struct {
	MetaClient interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}

	TSDBStore interface {
		ShardGroup(ids []uint64) tsdb.ShardGroup
	}
	service *Service
}

// MapShards maps the sources to the appropriate shards into an IteratorCreator.
func (e *LocalShardMapper) MapShards(sources influxql.Sources, t influxql.TimeRange, opt query.SelectOptions) (query.ShardGroup, error) {
	a := &LocalShardMapping{
		ShardMap: make(map[Source]tsdb.ShardGroup),
		s:        e.service,
		qb: &DefaultQueryBooster{
			s: e.service,
		},
	}

	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	if err := e.mapShards(a, sources, tmin, tmax); err != nil {
		return nil, err
	}
	a.MinTime, a.MaxTime = tmin, tmax
	return a, nil
}

func (e *LocalShardMapper) mapShards(a *LocalShardMapping, sources influxql.Sources, tmin, tmax time.Time) error {
	for _, s := range sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			source := Source{
				Database:        s.Database,
				RetentionPolicy: s.RetentionPolicy,
			}
			// Retrieve the list of shards for this database. This list of
			// shards is always the same regardless of which measurement we are
			// using.
			if _, ok := a.ShardMap[source]; !ok {
				groups, err := e.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
				if err != nil {
					return err
				}

				if len(groups) == 0 {
					a.ShardMap[source] = nil
					continue
				}

				shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
				for _, g := range groups {
					for _, si := range g.Shards {
						shardIDs = append(shardIDs, si.ID)
					}
				}
				a.ShardMap[source] = e.TSDBStore.ShardGroup(shardIDs)
			}
		case *influxql.SubQuery:
			if err := e.mapShards(a, s.Statement.Sources, tmin, tmax); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *LocalShardMapper) SetEtcdService(s *Service) {
	e.service = s
}

// ShardMapper maps data sources to a list of shard information.
type LocalShardMapping struct {
	ShardMap map[Source]tsdb.ShardGroup

	// MinTime is the minimum time that this shard mapper will allow.
	// Any attempt to use a time before this one will automatically result in using
	// this time instead.
	MinTime time.Time

	// MaxTime is the maximum time that this shard mapper will allow.
	// Any attempt to use a time after this one will automatically result in using
	// this time instead.
	MaxTime time.Time

	s  *Service
	qb QueryBooster
}

func (a *LocalShardMapping) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	var measurements []string
	if m.Regex != nil {
		measurements = sg.MeasurementsByRegex(m.Regex.Val)
	} else {
		measurements = []string{m.Name}
	}

	f, d, err := sg.FieldDimensions(measurements)
	if err != nil {
		return nil, nil, err
	}
	for k, typ := range f {
		fields[k] = typ
	}
	for k := range d {
		dimensions[k] = struct{}{}
	}
	return
}

func (a *LocalShardMapping) MapType(m *influxql.Measurement, field string) influxql.DataType {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return influxql.Unknown
	}

	var names []string
	if m.Regex != nil {
		names = sg.MeasurementsByRegex(m.Regex.Val)
	} else {
		names = []string{m.Name}
	}

	var typ influxql.DataType
	for _, name := range names {
		if m.SystemIterator != "" {
			name = m.SystemIterator
		}
		t := sg.MapType(name, field)
		if typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a *LocalShardMapping) LocalCreateIterator(ctx context.Context, m *influxql.Measurement,
	opt query.IteratorOptions) (query.Iterator, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return nil, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	if m.Regex != nil {
		measurements := sg.MeasurementsByRegex(m.Regex.Val)
		inputs := make([]query.Iterator, 0, len(measurements))
		if err := func() error {
			// Create a Measurement for each returned matching measurement value
			// from the regex.
			for _, measurement := range measurements {
				mm := m.Clone()
				mm.Name = measurement // Set the name to this matching regex value.
				input, err := sg.CreateIterator(ctx, mm, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			}
			return nil
		}(); err != nil {
			query.Iterators(inputs).Close()
			return nil, err
		}

		return query.Iterators(inputs).Merge(opt)
	}
	return sg.CreateIterator(ctx, m, opt)
}

func (a *LocalShardMapping) BoosterCreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	itrs := make([]query.Iterator, 0)
	var err error
	// Booster query execute local CrateIterator, judge execute rpc
	ctxTimeOut, cancel := context.WithTimeout(ctx, time.Minute*1)
	resultCh := make(chan query.Iterator)
	defer close(resultCh)
	go func(itrs []query.Iterator) {
		it, err := a.LocalCreateIterator(ctxTimeOut, m, opt)
		if err != nil {
			a.s.Logger.Error(err.Error())
		}
		itrs = append(itrs, it)
	}(itrs)
	count := 0
	// juge execute rpc
	timeRange := influxql.TimeRange{Min: time.Unix(opt.StartTime, 0), Max: time.Unix(opt.EndTime, 0)}
	if t := time.Unix(opt.StartTime, 0).Add(DefaultTimeRangeLimit); t.Before(timeRange.Max) {
		// execute booster rpc
		count, err = a.qb.Query(ctxTimeOut, m, opt, resultCh)
	}
	count++
	a.s.CheckErrPrintLog("Local Booster Query failed", err)
	go func(itrs []query.Iterator) {
		for i := range resultCh {
			count--
			if i != nil {
				itrs = append(itrs, i)
			}
			if count == 0 {
				cancel()
				break
			}
		}
	}(itrs)
	select {
	case <-ctxTimeOut.Done():
		return query.Iterators(itrs).Merge(opt)
	}
}

func (a *LocalShardMapping) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	timeRange := influxql.TimeRange{Min: time.Unix(opt.StartTime, 0), Max: time.Unix(opt.EndTime, 0)}
	resultCh := make(chan query.Iterator)
	ctxTimeOut, cancel := context.WithTimeout(ctx, time.Minute*1)
	rpcParam := GetRpcParam(*m, timeRange, opt)
	count := 0
	defer cancel()
	for _, node := range a.s.ch.MasterNodes {
		if node.Id != a.s.masterNode.Id {
			count++
			client, err := rpc.Dial("tcp", node.Ip+DefaultRpcBindAddress)
			if err != nil {
				return nil, err
			}
			go func() {
				var it *query.Iterator
				err = client.Call("QueryExecutor.DistributeQuery", *rpcParam, it)
				resultCh <- *it
				if err != nil {
					a.s.Logger.Error("LocalShardMapper RPC Query Error ", zap.Error(err))
				}
			}()
		}
	}
	itrs := make([]query.Iterator, 0, count)
	localIterator, err := a.BoosterCreateIterator(ctxTimeOut, m, opt)
	if err != nil {
		return localIterator, err
	}

	itrs = append(itrs, localIterator)
	if count == 0 {
		return query.Iterators(itrs).Merge(opt)
	}
	go func(itrs []query.Iterator) {
		for i := range resultCh {
			count--
			if i != nil {
				itrs = append(itrs, i)
			}
			if count == 0 {
				cancel()
				break
			}
		}
	}(itrs)
	select {
	case <-ctxTimeOut.Done():
		return query.Iterators(itrs).Merge(opt)
	}
}

func (a *LocalShardMapping) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return query.IteratorCost{}, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	if m.Regex != nil {
		var costs query.IteratorCost
		measurements := sg.MeasurementsByRegex(m.Regex.Val)
		for _, measurement := range measurements {
			cost, err := sg.IteratorCost(measurement, opt)
			if err != nil {
				return query.IteratorCost{}, err
			}
			costs = costs.Combine(cost)
		}
		return costs, nil
	}
	return sg.IteratorCost(m.Name, opt)
}

// Close clears out the list of mapped shards.
func (a *LocalShardMapping) Close() error {
	a.ShardMap = nil
	return nil
}

// Source contains the database and retention policy Source for data.
type Source struct {
	Database        string
	RetentionPolicy string
}

type QueryBooster interface {
	Query(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, result chan query.Iterator) (int, error)
}

type DefaultQueryBooster struct {
	s *Service
}

func (qb *DefaultQueryBooster) Query(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, result chan query.Iterator) (int, error) {
	min := time.Unix(opt.StartTime, 0)
	max := time.Unix(opt.EndTime, 0)
	duration := (max.Second() - min.Second()) / len(qb.s.rpcQuery.nodes)
	for i, node := range qb.s.rpcQuery.nodes {
		rewriteParam := influxql.TimeRange{}
		rewriteParam.Max = min.Add(time.Duration(duration))
		rewriteParam.Min = min
		min = rewriteParam.Max
		if i == len(qb.s.rpcQuery.nodes)-1 {
			rewriteParam.Max = max
		}
		rpcParam := GetRpcParam(*m, rewriteParam, opt)
		client, err := rpc.Dial("tcp", node.Ip+DefaultRpcBindAddress)
		go func(client *rpc.Client, i chan query.Iterator) {
			it := new(query.Iterator)
			err = client.Call("RpcService.BoosterQuery", rpcParam, it)
			if err != nil {
				qb.s.Logger.Info("Distribute Query failed")
				return
			}
			if it != nil {
				i <- *it
			}
		}(client, result)
	}
	return len(qb.s.rpcQuery.nodes), nil
}
