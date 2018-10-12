package etcd

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"sync"
	"time"
)

const RequestTimeout = 10 * time.Second

var clientInstance *clientv3.Client
var once sync.Once

func GetEtcdClient(c Config) (*clientv3.Client, error) {
	var err error
	once.Do(func() {
		clientInstance, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{c.EtcdAddress},
			DialTimeout: 5 * time.Second,
		})
	})
	if err != nil {
		return nil, err
	}
	return clientInstance, nil
}
func GetLatestClusterID(c Config) (uint64, error) {
	cli, err := GetEtcdClient(c)
	newestCluster, err := cli.Get(context.Background(), TSDBClusterAutoIncrementId)
	if err != nil {
		return 0, err
	}
	if newestCluster.Count == 0 {
		cli.Put(context.Background(), TSDBClusterAutoIncrementId, "0")
	}
getClusterId:
	newestCluster, err = cli.Get(context.Background(), TSDBClusterAutoIncrementId)
	cmp := clientv3.Compare(clientv3.Value(TSDBClusterAutoIncrementId), "=", newestCluster.Kvs[0].String())
	clusterId, err := ByteToUint64(newestCluster.Kvs[0].Value)
	if err != nil {
		return 0, err
	}
	clusterId++
	put := clientv3.OpPut(TSDBClusterAutoIncrementId, string(clusterId))
	resp, err := cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if !resp.Succeeded {
		goto getClusterId
	}
	return clusterId, nil
}
