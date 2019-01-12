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

// shards, err := shardMapper.MapShards(c.stmt.Sources, timeRange, sopt)
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
	source    influxql.Measurement
	timeRange influxql.TimeRange
	opt       query.IteratorOptions
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

func (rq *QueryExecutor) DistributeQuery(param RpcParam, iterator *query.Iterator) error {
	rq.Logger.Debug("DistributeQuery start")
	if t := param.timeRange.Min.Add(DefaultTimeRangeLimit); t.After(param.timeRange.Max) {
		return rq.BoosterQuery(param, iterator)
	}
	// The time interval of parameters exceeds the limit
	nodes := rq.nodes
	m := rq.MetaClient.Data()
	duration := (param.timeRange.Max.Second() - param.timeRange.Min.Second()) / len(nodes)
	itCh := make(chan query.Iterator)
	iterators := make([]query.Iterator, len(nodes)-1)
	defer close(itCh)
	c := context.Background()
	tc, cancel := context.WithTimeout(c, time.Second*2)
	defer cancel()
	startTime := param.timeRange.Min
	for i, node := range nodes {
		rewriteParam := param
		rewriteParam.timeRange.Max = startTime.Add(time.Duration(duration))
		rewriteParam.timeRange.Min = startTime
		startTime = rewriteParam.timeRange.Max
		if i == len(nodes)-1 {
			rewriteParam.timeRange.Max = param.timeRange.Max
		}
		var it *query.Iterator
		if node.Id == m.NodeID {
			err := rq.BoosterQuery(rewriteParam, it)
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
			return rq.BoosterQuery(param, iterator)
		}
		result, err := query.Iterators(iterators).Merge(param.opt)
		if err != nil {
			return err
		}
		iterator = &result
		return nil
	}
}
func (rq *QueryExecutor) BoosterQuery(param RpcParam, iterator *query.Iterator) error {
	rq.Logger.Debug("Booster query start")
	sm := *rq.shardMapper
	sources := make([]influxql.Source, 0)
	sources = append(sources, &param.source)
	sg, err := sm.MapShards(sources, param.timeRange, query.SelectOptions{})
	if err != nil {
		return err
	}
	c := context.Background()
	tc, cancel := context.WithTimeout(c, time.Second*2)
	defer cancel()
	go func(i *query.Iterator, cancel func()) {
		it, err := sg.CreateIterator(tc, &param.source, param.opt)
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
