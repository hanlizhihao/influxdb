package inmem

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/etcd"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
	"strconv"
)

func (idx *ShardIndex) SyncIndexData() error {
	if idx.opt.Cli == nil {
		return nil
	}
	key := meta.TSDBShardIndex + strconv.FormatUint(idx.id, 10) + "-"
	resp, err := idx.opt.Cli.Get(context.Background(), key, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	// Init Etcd data
	if resp.Count == 0 {
		for _, s := range idx.series {
			if s.Key == "" {
				continue
			}
			idx.opt.Cli.Put(context.Background(), key+s.Key, etcd.ToJson(meta.Series{
				Key:  []byte(s.Key),
				Name: []byte(s.Measurement.Name),
				Tags: s.Tags,
			}))
		}
	}
	for _, kv := range resp.Kvs {
		var series meta.Series
		etcd.ParseJson(kv.Value, &series)
		if s := idx.Index.series[string(series.Key)]; s == nil {
			err = idx.CreateSeriesIfNotExists(series.Key, series.Name, series.Tags)
			if err != nil {
				return err
			}
		}
	}
	go idx.watchShardIndexData(key)
	measurementKey := key + meta.TSDBMeasurementIndex
	mResp, err := idx.opt.Cli.Get(context.Background(), measurementKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if mResp.Count == 0 {
		for _, m := range idx.measurements {
			if m.Name == "" {
				continue
			}
			idx.opt.Cli.Put(context.Background(), measurementKey+m.Name, etcd.ToJson(*m.ConvertToMetaData()))
		}
	}
	for _, kv := range mResp.Kvs {
		var measurement meta.Measurement
		etcd.ParseJson(kv.Value, &measurement)
		if m := idx.Index.measurements[measurement.Name]; m == nil {
			idx.measurements[measurement.Name] = parseMetaDataMeasurement(&measurement)
		}
	}
	go idx.watchIndexMeasurement(measurementKey)
	return nil
}

func (idx *ShardIndex) watchShardIndexData(key string) {
	logger := zap.NewNop()
	logger = logger.With(zap.String("ShardIndex", "Watch Index"))
	indexCh := idx.opt.Cli.Watch(context.Background(), key, clientv3.WithPrefix())
	for indexInfo := range indexCh {
		for _, event := range indexInfo.Events {
			if event.Type == clientv3.EventTypePut {
				var series meta.Series
				etcd.ParseJson(event.Kv.Value, &series)
				err := idx.Index.CreateSeriesListIfNotExists(idx.seriesIDSet, [][]byte{series.Key}, [][]byte{series.Name},
					[]models.Tags{series.Tags}, &idx.opt, false)
				if err != nil {
					logger.Error("Get put series event, but update local index data error", zap.Error(err))
				}
				continue
			}
			if event.Type == clientv3.EventTypeDelete {
				var series meta.Series
				etcd.ParseJson(event.Kv.Value, &series)
				err := idx.DropSeriesGlobal(series.Key)
				s, err := idx.Series(series.Key)
				if s != nil {
					err = idx.DropSeries(s.ID, nil, false)
				}
				if err != nil {
					logger.Error("Get delete series event, but delete local index data error", zap.Error(err))
				}
			}
		}
	}
}
func (idx *ShardIndex) createSeriesForEtcd(series *meta.Series) error {
	seriesData := *series
	_, err := idx.opt.Cli.Put(context.Background(), meta.TSDBShardIndex+strconv.
		FormatUint(idx.id, 10)+"-"+string(series.Key), etcd.ToJson(seriesData))
	return err
}

func (idx *ShardIndex) watchIndexMeasurement(key string) {
	indexCh := idx.opt.Cli.Watch(context.Background(), key, clientv3.WithPrefix())
	for indexInfo := range indexCh {
		for _, event := range indexInfo.Events {
			if event.Type == clientv3.EventTypePut {
				var measurement meta.Measurement
				etcd.ParseJson(event.Kv.Value, &measurement)
				idx.mu.Lock()
				if m := idx.measurements[measurement.Name]; m == nil {
					idx.measurements[measurement.Name] = parseMetaDataMeasurement(&measurement)
				}
				idx.mu.Unlock()
				continue
			}
			if event.Type == mvccpb.DELETE {
				var measurement meta.Measurement
				etcd.ParseJson(event.Kv.Value, &measurement)
				idx.mu.Lock()
				if m := idx.measurements[measurement.Name]; m != nil {
					delete(idx.measurements, m.Name)
				}
				idx.mu.Unlock()
			}
		}
	}
}
