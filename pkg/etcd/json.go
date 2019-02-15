package etcd

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"strconv"
)

func ToJson(value interface{}) string {
	dataByte, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(dataByte)
}
func ToJsonByte(value interface{}) []byte {
	dataByte, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return dataByte
}
func ParseJson(data []byte, target interface{}) {
	err := json.Unmarshal(data, target)
	if err != nil {
		target = nil
	}
}
func ContainsStr(all []string, sub string) bool {
	for _, s := range all {
		if s == sub {
			return true
		}
	}
	return false
}

// 获取集群及物理节点的自增id
func GetLatestID(key string, cli *clientv3.Client) (uint64, error) {
	response, err := cli.Get(context.Background(), key)
	if err != nil {
		return 0, err
	}
	if response.Count == 0 {
		_, err = cli.Put(context.Background(), key, "1")
	}
getClusterId:
	response, err = cli.Get(context.Background(), key)
	cmp := clientv3.Compare(clientv3.Value(key), "=", string(response.Kvs[0].Value))
	clusterId, err := strconv.ParseUint(string(response.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, err
	}
	if clusterId > uint64(18446744073709551610) {
		clusterId = 0
	}
	clusterId++
	put := clientv3.OpPut(key, strconv.FormatUint(clusterId, 10))
	resp, err := cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if !resp.Succeeded {
		goto getClusterId
	}
	return clusterId, nil
}
