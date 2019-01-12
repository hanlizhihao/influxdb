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
	Opt       *query.IteratorOptions
}
type RpcResponse struct {
	It query.Iterator
}

func GetRpcParam(s influxql.Measurement, o query.IteratorOptions) *RpcParam {
	return &RpcParam{
		Source: ToJsonByte(s),
		Opt:    &o,
	}
}
func ParseRpcParam(p RpcParam) (influxql.Measurement, *query.IteratorOptions) {
	var s influxql.Measurement
	ParseJson(p.Source, &s)
	return s, p.Opt
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

func (rq *QueryExecutor) DistributeQuery(p RpcParam, iterator *RpcResponse) error {
	source, opt := ParseRpcParam(p)
	rq.Logger.Debug("DistributeQuery start")
	sm := *rq.shardMapper
	sources := make([]influxql.Source, 0)
	sources = append(sources, &source)
	timeRange := influxql.TimeRange{
		Min: time.Unix(0, opt.StartTime),
		Max: time.Unix(0, opt.EndTime),
	}
	sg, err := sm.MapShards(sources, timeRange, query.SelectOptions{})
	if err != nil {
		return err
	}
	if localShardMapping, ok := sg.(*LocalShardMapping); ok {
		c := context.Background()
		tc, cancel := context.WithTimeout(c, time.Second*2)
		defer cancel()
		it, err := localShardMapping.BoosterCreateIterator(tc, &source, *opt)
		if err == nil {
			iterator.It = it
			return nil
		}
	}
	return errors.New("QueryExecutor Query failed")
}
func (rq *QueryExecutor) BoosterQuery(param RpcParam, iterator *RpcResponse) error {
	source, opt := ParseRpcParam(param)
	rq.Logger.Debug("Booster query start")
	sm := *rq.shardMapper
	sources := make([]influxql.Source, 0)
	sources = append(sources, &source)
	timeRange := influxql.TimeRange{
		Min: time.Unix(0, opt.StartTime),
		Max: time.Unix(0, opt.EndTime),
	}
	sg, err := sm.MapShards(sources, timeRange, query.SelectOptions{})
	if err != nil {
		return err
	}
	if localShardMapping, ok := sg.(*LocalShardMapping); ok {
		c := context.Background()
		tc, cancel := context.WithTimeout(c, time.Second*2)
		defer cancel()
		it, err := localShardMapping.LocalCreateIterator(tc, &source, *opt)
		if err == nil {
			iterator.It = it
			return nil
		}
	}
	return errors.New("QueryExecutor Query failed")
}
