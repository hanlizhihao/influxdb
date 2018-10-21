package coordinator

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"strconv"
	"sync"
	"time"
)

const RequestTimeout = 10 * time.Second

var clientInstance *clientv3.Client
var once sync.Once

func GetEtcdClient(c EtcdConfig) (*clientv3.Client, error) {
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

// 获取集群及物理节点的自增id
func GetLatestID(c EtcdConfig, key string) (uint64, error) {
	cli, err := GetEtcdClient(c)
	latestClusterId, err := cli.Get(context.Background(), key)
	if err != nil {
		return 0, err
	}
	if latestClusterId.Count == 0 {
		cli.Put(context.Background(), key, "0")
	}
getClusterId:
	latestClusterId, err = cli.Get(context.Background(), key)
	cmp := clientv3.Compare(clientv3.Value(key), "=", string(latestClusterId.Kvs[0].Value))
	clusterId, err := ByteToUint64(latestClusterId.Kvs[0].Value)
	if err != nil {
		return 0, err
	}
	clusterId++
	put := clientv3.OpPut(key, strconv.FormatUint(clusterId, 10))
	resp, err := cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if !resp.Succeeded {
		goto getClusterId
	}
	return clusterId, nil
}
