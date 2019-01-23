package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"go.uber.org/zap"
	"net"
	"os"
	"strconv"
)

func GetLocalHostIp() (ip string, err error) {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		fmt.Println("net.Interfaces failed, err:", err.Error())
		return "", err
	}

	for i := 0; i < len(netInterfaces); i++ {
		if (netInterfaces[i].Flags & net.FlagUp) != 0 {
			addrs, _ := netInterfaces[i].Addrs()

			for _, address := range addrs {
				if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						return ipnet.IP.String(), nil
					}
				}
			}
		}
	}

	return "", errors.New("cannot get localhost ip")
}

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
func (s *Service) GetLatestID(key string) (uint64, error) {
	response, err := s.cli.Get(context.Background(), key)
	if err != nil {
		return 0, err
	}
	if response.Count == 0 {
		_, err = s.cli.Put(context.Background(), key, "1")
	}
getClusterId:
	response, err = s.cli.Get(context.Background(), key)
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
	resp, err := s.cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if !resp.Succeeded {
		goto getClusterId
	}
	return clusterId, nil
}
func (s *Service) CheckErrorExit(msg string, err error) {
	if err != nil {
		s.Logger.Error(msg, zap.Error(err))
		os.Exit(1)
		return
	}
}
func (s *Service) CheckErrPrintLog(mag string, err error) {
	if err != nil {
		s.Logger.Error(mag, zap.Error(err))
	}
}
