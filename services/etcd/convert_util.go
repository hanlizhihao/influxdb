package etcd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
)

func convert(value []byte, result interface{}) (interface{}, error) {
	if value == nil || len(value) == 0 {
		return 0, errors.New("value is nil")
	}
	buf := bytes.NewBuffer(value)
	binary.Read(buf, binary.BigEndian, &result)
	return result, nil
}
func ByteToInt32(value []byte) (int32, error) {
	var x int32
	_, err := convert(value, &x)
	return x, err
}
func ByteToInt64(value []byte) (int64, error) {
	var x int64
	_, err := convert(value, &x)
	return x, err
}
func ByteToUint64(value []byte) (uint64, error) {
	var x uint64
	_, err := convert(value, &x)
	return x, err
}
func ToJson(value interface{}) string {
	dataByte, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(dataByte)
}
func ParseJson(data []byte, target interface{}) {
	err := json.Unmarshal(data, target)
	if err != nil {
		target = nil
	}
}
