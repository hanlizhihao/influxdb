package tsi1

import (
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

func (i *Index) SyncIndexData() error {
	if i.opt.Cli == nil {
		return nil
	}
	//key := meta.TSDBShardIndex + strconv.FormatUint(i.id, 10) + "-"
	//resp, err := i.opt.Cli.Get(context.Background(), key, clientv3.WithPrefix())
	//if err != nil {
	//	return err
	//}
	//// Init Etcd data
	//if resp.Count == 0 {
	//	for _, s := range idx.series {
	//		idx.opt.Cli.Put(context.Background(), key+s.Key, etcd.ToJson(meta.Series{
	//			Key:  []byte(s.Key),
	//			Name: []byte(s.Measurement.Name),
	//			Tags: s.Tags,
	//		}))
	//	}
	//}
	//for _, kv := range resp.Kvs {
	//	var series meta.Series
	//	etcd.ParseJson(kv.Value, &series)
	//	if s := idx.Index.series[string(series.Key)]; s == nil {
	//		err = idx.CreateSeriesIfNotExists(series.Key, series.Name, series.Tags)
	//		if err == nil {
	//			return err
	//		}
	//	}
	//}
	//go idx.watchIndexData(key)
	return nil
}

func (i *Index) watchIndexData(key string) {
	logger := zap.NewNop()
	logger = logger.With(zap.String("ShardIndex", "Watch Index"))
	//indexCh := i.opt.Cli.Watch(context.Background(), key, clientv3.WithPrefix())
	//for indexInfo := range indexCh {
	//	for _, event := range indexInfo.Events {
	//		if event.Type == clientv3.EventTypePut {
	//			var series meta.Series
	//			etcd.ParseJson(event.Kv.Value, &series)
	//			err := i.CreateSeriesListIfNotExists(idx.seriesIDSet, [][]byte{series.Key}, [][]byte{series.Name},
	//				[]models.Tags{series.Tags}, &idx.opt, false)
	//			if err != nil {
	//				logger.Error("Get put series event, but update local index data error", zap.Error(err))
	//			}
	//			continue
	//		}
	//		if event.Type == clientv3.EventTypeDelete {
	//			var series meta.Series
	//			etcd.ParseJson(event.Kv.Value, &series)
	//			err := i.DropSeriesGlobal(series.Key)
	//			if err != nil {
	//				logger.Error("Get delete series event, but delete local index data error", zap.Error(err))
	//			}
	//		}
	//	}
	//}
}
func (i *Index) createSeriesForEtcd(series *meta.Series) error {
	//seriesData := *series
	//_, err := i.opt.Cli.Put(context.Background(), meta.TSDBShardIndex+strconv.
	//	FormatUint(i.id, 10)+"-"+string(series.Key), etcd.ToJson(seriesData))
	//return err
	return nil
}
