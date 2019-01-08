package coordinator

import (
	"encoding/json"
)

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
