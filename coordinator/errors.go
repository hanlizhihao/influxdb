package coordinator

import "errors"

var (
	ErrMetaDataDisappear = errors.New("Attempt to get data from etcd failed, meta data don't exist ")
)
