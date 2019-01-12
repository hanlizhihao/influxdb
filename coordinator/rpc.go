package coordinator

import (
	"context"
	"errors"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"net"
	"net/rpc"
	"time"
)

const (
	DefaultEnabled        = true
	DefaultRpcBindAddress = ":8888"
	DefaultTimeRangeLimit = time.Hour * 24
)

type RpcConfig struct {
	Enabled     bool   `toml:"enabled"`
	BindAddress string `toml:"address"`
}

func NewRpcConfig() RpcConfig {
	return RpcConfig{
		BindAddress: DefaultRpcBindAddress,
	}
}

// shards, err := shardMapper.MapShards(c.stmt.Sources, TimeRange, sopt)
type RpcService struct {
	shardMapper *query.ShardMapper
	rpcConfig   RpcConfig
	Logger      *zap.Logger
	closed      bool

	nodes      []Node
	MetaClient interface {
		Data() meta.Data
	}
	ip            string
	queryExecutor *QueryExecutor
}

func NewRpcService(lsm *query.ShardMapper, n []Node) *RpcService {
	return &RpcService{
		rpcConfig:     NewRpcConfig(),
		shardMapper:   lsm,
		Logger:        zap.NewNop(),
		closed:        true,
		nodes:         n,
		queryExecutor: NewQuery(lsm, n),
	}
}
func (rs *RpcService) Close() error {

	if rs.closed {
		return nil // Already closed.
	}

	rs.closed = true

	rs.Logger.Info("Closed Rpc Service")
	return nil
}

// WithLogger sets the logger on the Service.
func (rs *RpcService) WithLogger(log *zap.Logger) {
	rs.Logger = log.With(zap.String("RpcQueryService", "Cluster"))
}
func (rs *RpcService) Open() error {
	rs.queryExecutor.MetaClient = rs.MetaClient
	err := rpc.Register(rs.queryExecutor)
	tcpAddr, err := net.ResolveTCPAddr("tcp", rs.rpcConfig.BindAddress)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	rs.closed = false
	if err != nil {
		return err
	}
	go func(listener *net.TCPListener) {
		rs.Logger.Info("Rpc Query Open success")
		for {
			if rs.closed {
				return
			}
			conn, err := listener.Accept()
			if err != nil {
				rs.Logger.Error("Rpc process connection error", zap.Error(err))
				continue
			}
			rs.Logger.Debug("Rpc Query Service processing connection, addr", zap.String("addr",
				conn.RemoteAddr().String()))
			go rpc.ServeConn(conn)
		}
	}(listener)
	ip, err := GetLocalHostIp()
	if err != nil {
		return err
	}
	rs.ip = ip
	return nil
}

type RpcParam struct {
	Source    []byte
	TimeRange []byte
	Opt       []byte
}

func GetRpcParam(s influxql.Measurement, t influxql.TimeRange, o query.IteratorOptions) *RpcParam {
	return &RpcParam{
		Source:    ToJsonByte(s),
		TimeRange: ToJsonByte(t),
		Opt:       ToJsonByte(o),
	}
}
func ParseRpcParam(p RpcParam) (influxql.Measurement, influxql.TimeRange, query.IteratorOptions) {
	var s influxql.Measurement
	var t influxql.TimeRange
	var opt query.IteratorOptions
	ParseJson(p.Source, &s)
	ParseJson(p.TimeRange, &t)
	ParseJson(p.Opt, &opt)
	return s, t, opt
}

type QueryExecutor struct {
	nodes      []Node
	MetaClient interface {
		Data() meta.Data
	}
	Logger      *zap.Logger
	shardMapper *query.ShardMapper
	ip          string
}

func NewQuery(lsm *query.ShardMapper, n []Node) *QueryExecutor {
	return &QueryExecutor{
		shardMapper: lsm,
		Logger:      zap.NewNop(),
		nodes:       n,
	}
}

func (rq *QueryExecutor) DistributeQuery(p RpcParam, iterator *query.Iterator) error {
	source, timeRange, opt := ParseRpcParam(p)
	rq.Logger.Debug("DistributeQuery start")
	if t := timeRange.Min.Add(DefaultTimeRangeLimit); t.After(timeRange.Max) {
		return rq.localQuery(source, timeRange, opt, iterator)
	}
	// The time interval of parameters exceeds the limit
	nodes := rq.nodes
	m := rq.MetaClient.Data()
	duration := (timeRange.Max.Second() - timeRange.Min.Second()) / len(nodes)
	itCh := make(chan query.Iterator)
	iterators := make([]query.Iterator, len(nodes)-1)
	defer close(itCh)
	c := context.Background()
	tc, cancel := context.WithTimeout(c, time.Second*2)
	defer cancel()
	startTime := timeRange.Min
	for i, node := range nodes {
		rewriteParam := influxql.TimeRange{}
		rewriteParam.Max = startTime.Add(time.Duration(duration))
		rewriteParam.Min = startTime
		startTime = rewriteParam.Max
		if i == len(nodes)-1 {
			rewriteParam.Max = timeRange.Max
		}
		var it *query.Iterator
		if node.Id == m.NodeID {
			err := rq.localQuery(source, timeRange, opt, it)
			if err != nil {
				return err
			}
			itCh <- *it
			continue
		}
		go func(i chan query.Iterator) {
			client, err := rpc.Dial("tcp", node.Ip+DefaultRpcBindAddress)
			err = client.Call("RpcService.BoosterQuery", rewriteParam, it)
			if err != nil {
				rq.Logger.Info("Distribute Query failed")
				return
			}
			i <- *it
		}(itCh)
	}
	go func() {
		for i := range itCh {
			iterators = append(iterators, i)
			if iterators[len(nodes)-2] != nil {
				cancel()
			}
		}
	}()
	select {
	case <-tc.Done():
		if len(iterators) < len(nodes) {
			return rq.localQuery(source, timeRange, opt, iterator)
		}
		result, err := query.Iterators(iterators).Merge(opt)
		if err != nil {
			return err
		}
		iterator = &result
		return nil
	}
}
func (rq *QueryExecutor) localQuery(s influxql.Measurement, t influxql.TimeRange, opt query.IteratorOptions,
	iterator *query.Iterator) error {
	rq.Logger.Debug("Booster query start")
	sm := *rq.shardMapper
	sources := make([]influxql.Source, 0)
	sources = append(sources, &s)
	sg, err := sm.MapShards(sources, t, query.SelectOptions{})
	if err != nil {
		return err
	}
	c := context.Background()
	tc, cancel := context.WithTimeout(c, time.Second*2)
	defer cancel()
	go func(i *query.Iterator, cancel func()) {
		it, err := sg.CreateIterator(tc, &s, opt)
		if err == nil {
			iterator = &it
		}
		cancel()
	}(iterator, cancel)
	select {
	case <-tc.Done():
		if iterator == nil {
			return errors.New("Get Data failed ")
		}
		return nil
	}
}
func (rq *QueryExecutor) BoosterQuery(param RpcParam, iterator *query.Iterator) error {
	source, timeRange, opt := ParseRpcParam(param)
	return rq.localQuery(source, timeRange, opt, iterator)
}
