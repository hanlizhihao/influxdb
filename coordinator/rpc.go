package coordinator

import (
	"bytes"
	"context"
	"errors"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"net"
	"net/rpc"
	"sort"
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
	UnsignedPoints []Point
	IteratorStats  query.IteratorStats
	Enable         bool
}

func (p UnsignedPoints) Next() (*query.UnsignedPoint, error) {
	if len(p.UnsignedPoints) > 1 {
		point := p.UnsignedPoints[0]
		p.UnsignedPoints = p.UnsignedPoints[1:]
		return point.DecodeUnsigned(), nil
	}
	if len(p.UnsignedPoints) == 1 {
		point := p.UnsignedPoints[0]
		p.UnsignedPoints = make([]Point, 0)
		return point.DecodeUnsigned(), nil
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
	IntegerPoints []Point
	IteratorStats query.IteratorStats
	Enable        bool
}

func (p IntegerPoints) Next() (*query.IntegerPoint, error) {
	if len(p.IntegerPoints) > 1 {
		point := p.IntegerPoints[0]
		p.IntegerPoints = p.IntegerPoints[1:]
		return point.DecodeInteger(), nil
	}
	if len(p.IntegerPoints) == 1 {
		point := p.IntegerPoints[0]
		p.IntegerPoints = make([]Point, 0)
		return point.DecodeInteger(), nil
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
	BooleanPoints []Point
	IteratorStats query.IteratorStats
	Enable        bool
}

func (p BooleanPoints) Next() (*query.BooleanPoint, error) {
	if len(p.BooleanPoints) > 1 {
		point := p.BooleanPoints[0]
		p.BooleanPoints = p.BooleanPoints[1:]
		return point.DecodeBool(), nil
	}
	if len(p.BooleanPoints) == 1 {
		point := p.BooleanPoints[0]
		p.BooleanPoints = make([]Point, 0)
		return point.DecodeBool(), nil
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
	StringPoints  []Point
	IteratorStats query.IteratorStats
	Enable        bool
}

func (p StringPoints) Next() (*query.StringPoint, error) {
	if len(p.StringPoints) > 1 {
		point := p.StringPoints[0]
		p.StringPoints = p.StringPoints[1:]
		return point.DecodeString(), nil
	}
	if len(p.StringPoints) == 1 {
		point := p.StringPoints[0]
		p.StringPoints = make([]Point, 0)
		return point.DecodeString(), nil
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
	FloatPoints   []Point
	IteratorStats query.IteratorStats
	Enable        bool
}

func (p FloatPoints) Next() (*query.FloatPoint, error) {
	if len(p.FloatPoints) > 1 {
		point := p.FloatPoints[0]
		p.FloatPoints = p.FloatPoints[1:]
		return point.DecodeFloat(), nil
	}
	if len(p.FloatPoints) == 1 {
		point := p.FloatPoints[0]
		p.FloatPoints = make([]Point, 0)
		return point.DecodeFloat(), nil
	}
	return nil, nil
}
func (p FloatPoints) Close() error {
	return nil
}
func (p FloatPoints) Stats() query.IteratorStats {
	return p.IteratorStats
}

type Point struct {
	Name string
	Tags string

	Time          int64
	FloatValue    float64
	IntegerValue  int64
	StringValue   string
	BooleanValue  bool
	UnsignedValue uint64
	Aux           []Aux

	// Total number of points that were combined into this point from an aggregate.
	// If this is zero, the point is not the result of an aggregate function.
	Aggregated uint32
	Nil        bool
}

func newTagsID(id string) query.Tags {
	m := decodeTags([]byte(id))
	if len(m) == 0 {
		return query.Tags{}
	}
	return query.NewTags(m)
}
func (p *Point) GetTags() string {
	if p != nil && &p.Tags != nil {
		return p.Tags
	}
	return ""
}

// encodeTags converts a map of strings to an identifier.
func encodeTags(m map[string]string) []byte {
	// Empty maps marshal to empty bytes.
	if len(m) == 0 {
		return nil
	}

	// Extract keys and determine final size.
	sz := (len(m) * 2) - 1 // separators
	keys := make([]string, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		sz += len(k) + len(v)
	}
	sort.Strings(keys)

	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	for _, k := range keys {
		copy(buf, k)
		buf[len(k)] = '\x00'
		buf = buf[len(k)+1:]
	}
	for i, k := range keys {
		v := m[k]
		copy(buf, v)
		if i < len(keys)-1 {
			buf[len(v)] = '\x00'
			buf = buf[len(v)+1:]
		}
	}
	return b
}

// decodeTags parses an identifier into a map of tags.
func decodeTags(id []byte) map[string]string {
	a := bytes.Split(id, []byte{'\x00'})

	// There must be an even number of segments.
	if len(a) > 0 && len(a)%2 == 1 {
		a = a[:len(a)-1]
	}

	// Return nil if there are no segments.
	if len(a) == 0 {
		return nil
	}
	mid := len(a) / 2

	// Decode key/value tags.
	m := make(map[string]string)
	for i := 0; i < mid; i++ {
		m[string(a[i])] = string(a[i+mid])
	}
	return m
}
func (p *Point) DecodeFloat() *query.FloatPoint {
	return &query.FloatPoint{
		Name:       p.Name,
		Aggregated: p.Aggregated,
		Aux:        decodeAux(p.Aux),
		Value:      p.FloatValue,
		Nil:        p.Nil,
		Tags:       newTagsID(p.Tags),
		Time:       p.Time,
	}
}
func (p *Point) EncodeFloat(float *query.FloatPoint) {
	p.Name = float.Name
	p.Aggregated = float.Aggregated
	p.Aux = encodeAux(float.Aux)
	p.FloatValue = float.Value
	p.Nil = float.Nil
	p.Tags = float.Tags.ID()
	p.Time = float.Time
}
func (p *Point) DecodeString() *query.StringPoint {
	return &query.StringPoint{
		Name:       p.Name,
		Aggregated: p.Aggregated,
		Aux:        decodeAux(p.Aux),
		Value:      p.StringValue,
		Nil:        p.Nil,
		Tags:       newTagsID(p.Tags),
		Time:       p.Time,
	}
}
func (p *Point) EncodeString(float *query.StringPoint) {
	p.Name = float.Name
	p.Aggregated = float.Aggregated
	p.Aux = encodeAux(float.Aux)
	p.StringValue = float.Value
	p.Nil = float.Nil
	p.Tags = float.Tags.ID()
	p.Time = float.Time
}
func (p *Point) DecodeBool() *query.BooleanPoint {
	return &query.BooleanPoint{
		Name:       p.Name,
		Aggregated: p.Aggregated,
		Aux:        decodeAux(p.Aux),
		Value:      p.BooleanValue,
		Nil:        p.Nil,
		Tags:       newTagsID(p.Tags),
		Time:       p.Time,
	}
}
func (p *Point) EncodeBool(float *query.BooleanPoint) {
	p.Name = float.Name
	p.Aggregated = float.Aggregated
	p.Aux = encodeAux(float.Aux)
	p.BooleanValue = float.Value
	p.Nil = float.Nil
	p.Tags = float.Tags.ID()
	p.Time = float.Time
}
func (p *Point) DecodeInteger() *query.IntegerPoint {
	return &query.IntegerPoint{
		Name:       p.Name,
		Aggregated: p.Aggregated,
		Aux:        decodeAux(p.Aux),
		Value:      p.IntegerValue,
		Nil:        p.Nil,
		Tags:       newTagsID(p.Tags),
		Time:       p.Time,
	}
}
func (p *Point) EncodeInteger(float *query.IntegerPoint) {
	p.Name = float.Name
	p.Aggregated = float.Aggregated
	p.Aux = encodeAux(float.Aux)
	p.IntegerValue = float.Value
	p.Nil = float.Nil
	p.Tags = float.Tags.ID()
	p.Time = float.Time
}
func (p *Point) DecodeUnsigned() *query.UnsignedPoint {
	return &query.UnsignedPoint{
		Name:       p.Name,
		Aggregated: p.Aggregated,
		Aux:        decodeAux(p.Aux),
		Value:      p.UnsignedValue,
		Nil:        p.Nil,
		Tags:       newTagsID(p.Tags),
		Time:       p.Time,
	}
}
func (p *Point) EncodeUnsigned(float *query.UnsignedPoint) {
	p.Name = float.Name
	p.Aggregated = float.Aggregated
	p.Aux = encodeAux(float.Aux)
	p.UnsignedValue = float.Value
	p.Nil = float.Nil
	p.Tags = float.Tags.ID()
	p.Time = float.Time
}

type Aux struct {
	DataType      int32
	FloatValue    float64
	IntegerValue  int64
	StringValue   string
	BooleanValue  bool
	UnsignedValue uint64
}

func encodeAux(aux []interface{}) []Aux {
	pb := make([]Aux, len(aux))
	for i := range aux {
		switch v := aux[i].(type) {
		case float64:
			pb[i] = Aux{DataType: int32(influxql.Float), FloatValue: v}
		case *float64:
			pb[i] = Aux{DataType: int32(influxql.Float)}
		case int64:
			pb[i] = Aux{DataType: int32(influxql.Integer), IntegerValue: v}
		case *int64:
			pb[i] = Aux{DataType: int32(influxql.Integer)}
		case uint64:
			pb[i] = Aux{DataType: int32(influxql.Unsigned), UnsignedValue: v}
		case *uint64:
			pb[i] = Aux{DataType: int32(influxql.Unsigned)}
		case string:
			pb[i] = Aux{DataType: int32(influxql.String), StringValue: v}
		case *string:
			pb[i] = Aux{DataType: int32(influxql.String)}
		case bool:
			pb[i] = Aux{DataType: int32(influxql.Boolean), BooleanValue: v}
		case *bool:
			pb[i] = Aux{DataType: int32(influxql.Boolean)}
		default:
			pb[i] = Aux{DataType: int32(influxql.Unknown)}
		}
	}
	return pb
}
func (m *Aux) GetDataType() int32 {
	if m != nil && &m.DataType != nil {
		return m.DataType
	}
	return 0
}
func decodeAux(pb []Aux) []interface{} {
	if len(pb) == 0 {
		return nil
	}

	aux := make([]interface{}, len(pb))
	for i := range pb {
		switch influxql.DataType(pb[i].GetDataType()) {
		case influxql.Float:
			if &pb[i].FloatValue != nil {
				aux[i] = pb[i].FloatValue
			} else {
				aux[i] = (*float64)(nil)
			}
		case influxql.Integer:
			if &pb[i].IntegerValue != nil {
				aux[i] = pb[i].IntegerValue
			} else {
				aux[i] = (*int64)(nil)
			}
		case influxql.Unsigned:
			if &pb[i].UnsignedValue != nil {
				aux[i] = pb[i].UnsignedValue
			} else {
				aux[i] = (*uint64)(nil)
			}
		case influxql.String:
			if &pb[i].StringValue != nil {
				aux[i] = pb[i].StringValue
			} else {
				aux[i] = (*string)(nil)
			}
		case influxql.Boolean:
			if &pb[i].BooleanValue != nil {
				aux[i] = pb[i].BooleanValue
			} else {
				aux[i] = (*bool)(nil)
			}
		default:
			aux[i] = nil
		}
	}
	return aux
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
			point := Point{}
			point.EncodeFloat(p)
			resp.Floats.FloatPoints = append(resp.Floats.FloatPoints, point)
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
			point := Point{}
			point.EncodeBool(p)
			resp.Booleans.BooleanPoints = append(resp.Booleans.BooleanPoints, point)
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
			point := Point{}
			point.EncodeInteger(p)
			resp.Integers.IntegerPoints = append(resp.Integers.IntegerPoints, point)
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
			point := Point{}
			point.EncodeString(p)
			resp.Strings.StringPoints = append(resp.Strings.StringPoints, point)
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
			point := Point{}
			point.EncodeUnsigned(p)
			resp.Unsigned.UnsignedPoints = append(resp.Unsigned.UnsignedPoints, point)
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
		it.Close()
		return nil
	}
	return errors.New("QueryExecutor Query failed")
}
