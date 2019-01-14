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

type Measurement struct {
	Database        string
	RetentionPolicy string
	Name            string
	IsTarget        bool

	// This field indicates that the measurement should read be read from the
	// specified system iterator.
	SystemIterator string
}
type RpcRequest struct {
	Source *Measurement
	Opt    *query.IteratorOptions
}

func EncodeSource(measurement *influxql.Measurement) *Measurement {
	return &Measurement{
		Database:        measurement.Database,
		RetentionPolicy: measurement.RetentionPolicy,
		Name:            measurement.Name,
		IsTarget:        measurement.IsTarget,
		SystemIterator:  measurement.SystemIterator,
	}
}
func DecodeSource(measurement *Measurement) *influxql.Measurement {
	return &influxql.Measurement{
		Database:        measurement.Database,
		RetentionPolicy: measurement.RetentionPolicy,
		Name:            measurement.Name,
		IsTarget:        measurement.IsTarget,
		SystemIterator:  measurement.SystemIterator,
	}
}

type RpcResponse struct {
	Floats   FloatPoints
	Strings  StringPoints
	Booleans BooleanPoints
	Integers IntegerPoints
	Unsigned UnsignedPoints
}
type UnsignedPoints struct {
	UnsignedPoints []query.UnsignedPoint
	IteratorStats  query.IteratorStats
	Enable         bool
}

func (p *UnsignedPoints) Next() (*query.UnsignedPoint, error) {
	if len(p.UnsignedPoints) > 1 {
		point := p.UnsignedPoints[0]
		p.UnsignedPoints = p.UnsignedPoints[1:]
		return &point, nil
	}
	if len(p.UnsignedPoints) == 1 {
		point := p.UnsignedPoints[0]
		p.UnsignedPoints = make([]query.UnsignedPoint, 0)
		return &point, nil
	}
	return nil, nil
}
func (p UnsignedPoints) Close() error {
	return nil
}
func (p UnsignedPoints) Stats() query.IteratorStats {
	return p.IteratorStats
}

type IntegerPoints struct {
	IntegerPoints []query.IntegerPoint
	IteratorStats query.IteratorStats
	Enable        bool
}

func (p *IntegerPoints) Next() (*query.IntegerPoint, error) {
	if len(p.IntegerPoints) > 1 {
		point := p.IntegerPoints[0]
		p.IntegerPoints = p.IntegerPoints[1:]
		return &point, nil
	}
	if len(p.IntegerPoints) == 1 {
		point := p.IntegerPoints[0]
		p.IntegerPoints = make([]query.IntegerPoint, 0)
		return &point, nil
	}
	return nil, nil
}
func (p IntegerPoints) Close() error {
	return nil
}
func (p IntegerPoints) Stats() query.IteratorStats {
	return p.IteratorStats
}

type BooleanPoints struct {
	BooleanPoints []query.BooleanPoint
	IteratorStats query.IteratorStats
	Enable        bool
}

func (p *BooleanPoints) Next() (*query.BooleanPoint, error) {
	if len(p.BooleanPoints) > 1 {
		point := p.BooleanPoints[0]
		p.BooleanPoints = p.BooleanPoints[1:]
		return &point, nil
	}
	if len(p.BooleanPoints) == 1 {
		point := p.BooleanPoints[0]
		p.BooleanPoints = make([]query.BooleanPoint, 0)
		return &point, nil
	}
	return nil, nil
}
func (p BooleanPoints) Close() error {
	return nil
}
func (p BooleanPoints) Stats() query.IteratorStats {
	return p.IteratorStats
}

type StringPoints struct {
	StringPoints  []query.StringPoint
	IteratorStats query.IteratorStats
	Enable        bool
}

func (p *StringPoints) Next() (*query.StringPoint, error) {
	if len(p.StringPoints) > 1 {
		point := p.StringPoints[0]
		p.StringPoints = p.StringPoints[1:]
		return &point, nil
	}
	if len(p.StringPoints) == 1 {
		point := p.StringPoints[0]
		p.StringPoints = make([]query.StringPoint, 0)
		return &point, nil
	}
	return nil, nil
}
func (p StringPoints) Close() error {
	return nil
}
func (p StringPoints) Stats() query.IteratorStats {
	return p.IteratorStats
}

type FloatPoints struct {
	FloatPoints   []query.FloatPoint
	IteratorStats query.IteratorStats
	Enable        bool
}

func (p *FloatPoints) Next() (*query.FloatPoint, error) {
	if len(p.FloatPoints) > 1 {
		point := p.FloatPoints[0]
		p.FloatPoints = p.FloatPoints[1:]
		return &point, nil
	}
	if len(p.FloatPoints) == 1 {
		point := p.FloatPoints[0]
		p.FloatPoints = make([]query.FloatPoint, 0)
		return &point, nil
	}
	return nil, nil
}
func (p FloatPoints) Close() error {
	return nil
}
func (p FloatPoints) Stats() query.IteratorStats {
	return p.IteratorStats
}

func EncodeIterator(it query.Iterator) (*RpcResponse, error) {
	resp := &RpcResponse{}
	if f, ok := it.(query.FloatIterator); ok {
		for {
			// Retrieve the next point from the iterator.
			p, err := f.Next()
			if err != nil {
				return nil, err
			} else if p == nil {
				break
			}
			resp.Floats.FloatPoints = append(resp.Floats.FloatPoints, *p)
			resp.Floats.Enable = true
		}
		resp.Floats.IteratorStats = f.Stats()
		return resp, nil
	}
	if b, ok := it.(query.BooleanIterator); ok {
		for {
			// Retrieve the next point from the iterator.
			p, err := b.Next()
			if err != nil {
				return nil, err
			} else if p == nil {
				break
			}
			resp.Booleans.BooleanPoints = append(resp.Booleans.BooleanPoints, *p)
			resp.Booleans.Enable = true
		}
		resp.Booleans.IteratorStats = b.Stats()
		return resp, nil
	}
	if i, ok := it.(query.IntegerIterator); ok {
		for {
			// Retrieve the next point from the iterator.
			p, err := i.Next()
			if err != nil {
				return nil, err
			} else if p == nil {
				break
			}
			resp.Integers.IntegerPoints = append(resp.Integers.IntegerPoints, *p)
			resp.Integers.Enable = true
		}
		resp.Integers.IteratorStats = i.Stats()
		return resp, nil
	}
	if s, ok := it.(query.StringIterator); ok {
		for {
			// Retrieve the next point from the iterator.
			p, err := s.Next()
			if err != nil {
				return nil, err
			} else if p == nil {
				break
			}
			resp.Strings.StringPoints = append(resp.Strings.StringPoints, *p)
			resp.Strings.Enable = true
		}
		resp.Strings.IteratorStats = s.Stats()
		return resp, nil
	}
	if u, ok := it.(query.UnsignedIterator); ok {
		for {
			// Retrieve the next point from the iterator.
			p, err := u.Next()
			if err != nil {
				return nil, err
			} else if p == nil {
				break
			}
			resp.Unsigned.UnsignedPoints = append(resp.Unsigned.UnsignedPoints, *p)
			resp.Unsigned.Enable = true
		}
		resp.Unsigned.IteratorStats = u.Stats()
		return resp, nil
	}
	return nil, errors.New("Unknown Iterator Type ")
}
func DecodeIterator(opt query.IteratorOptions, r *RpcResponse) (query.Iterator, error) {
	itrs := make([]query.Iterator, 0)
	if r.Unsigned.Enable {
		itrs = append(itrs, r.Unsigned)
	}
	if r.Booleans.Enable {
		itrs = append(itrs, r.Booleans)
	}
	if r.Integers.Enable {
		itrs = append(itrs, r.Integers)
	}
	if r.Floats.Enable {
		itrs = append(itrs, r.Floats)
	}
	if r.Strings.Enable {
		itrs = append(itrs, r.Strings)
	}
	return query.Iterators(itrs).Merge(opt)
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

func (rq *QueryExecutor) DistributeQuery(r RpcRequest, iterator *RpcResponse) error {
	rq.Logger.Debug("DistributeQuery start")
	sm := *rq.shardMapper
	source := DecodeSource(r.Source)
	sources := make([]influxql.Source, 0)
	sources = append(sources, source)
	timeRange := influxql.TimeRange{
		Min: time.Unix(0, r.Opt.StartTime),
		Max: time.Unix(0, r.Opt.EndTime),
	}
	sg, err := sm.MapShards(sources, timeRange, query.SelectOptions{})
	if err != nil {
		return err
	}
	if localShardMapping, ok := sg.(*LocalShardMapping); ok {
		c := context.Background()
		tc, cancel := context.WithTimeout(c, time.Second*2)
		defer cancel()
		it, err := localShardMapping.BoosterCreateIterator(tc, source, *r.Opt)
		if err != nil {
			return err
		}
		resp, err := EncodeIterator(it)
		if err != nil {
			return err
		}
		*iterator = *resp
		return nil
	}
	return errors.New("QueryExecutor Query failed")
}
func (rq *QueryExecutor) BoosterQuery(r RpcRequest, iterator *RpcResponse) error {
	rq.Logger.Debug("Booster query start")
	sm := *rq.shardMapper
	source := DecodeSource(r.Source)
	sources := make([]influxql.Source, 0)
	sources = append(sources, source)
	timeRange := influxql.TimeRange{
		Min: time.Unix(0, r.Opt.StartTime),
		Max: time.Unix(0, r.Opt.EndTime),
	}
	sg, err := sm.MapShards(sources, timeRange, query.SelectOptions{})
	if err != nil {
		return err
	}
	if localShardMapping, ok := sg.(*LocalShardMapping); ok {
		c := context.Background()
		tc, cancel := context.WithTimeout(c, time.Second*2)
		defer cancel()
		it, err := localShardMapping.LocalCreateIterator(tc, source, *r.Opt)
		if err != nil {
			return err
		}
		resp, err := EncodeIterator(it)
		if err != nil {
			return err
		}
		*iterator = *resp
		return nil
	}
	return errors.New("QueryExecutor Query failed")
}
