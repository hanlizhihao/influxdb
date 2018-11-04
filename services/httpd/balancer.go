package httpd

import (
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"math/rand"
	"net/http"
	"sync"
)

const (
	timeout = 3
)

type Balance interface {
	SetMeasurementMapIndex(remote map[string][]string, local map[string]interface{})
	balance(w *http.ResponseWriter, q *influxql.Query, r *http.Request) (bool, error)
}

type QueryBalance struct {
	// other cluster's key:MeasurementName value: ip Array
	measurements map[string][]string
	// local cluster's measurements
	localMeasurement map[string]interface{}
	rw               sync.RWMutex
	c                map[string]*client.Client
	clientConfig     client.HTTPConfig
	Logger           *zap.Logger
}

func NewBalance() *QueryBalance {
	return &QueryBalance{
		Logger:           zap.NewNop(),
		measurements:     make(map[string][]string, 0),
		localMeasurement: make(map[string]interface{}, 0),
		clientConfig: client.HTTPConfig{
			Timeout:            timeout,
			InsecureSkipVerify: false,
		},
		c: make(map[string]*client.Client, 0),
	}
}

func (qb *QueryBalance) SetMeasurementMapIndex(remote map[string][]string, local map[string]interface{}) {
	qb.rw.Lock()
	defer qb.rw.Unlock()
	qb.localMeasurement = local
	qb.measurements = remote
}

// return true, the request will be forward, return false, the request will be not forward
func (qb *QueryBalance) balance(w *http.ResponseWriter, q *influxql.Query, r *http.Request) (bool, error) {
	qb.rw.RLock()
	defer qb.rw.RUnlock()
	var i int
	for ; i < len(q.Statements); i++ {
		if qr, ok := q.Statements[i].(*influxql.SelectStatement); ok {
			for _, measurement := range qr.Sources.Measurements() {
				// If the measurement querying for any statement exists and is local, no load balancing will be performed.
				if qb.localMeasurement[measurement.Name] != nil {
					return false, nil
				}
			}
		}
	}
	// All request statements select data on other nodes.
	go qb.forwardRequest(q.Statements[0].(*influxql.SelectStatement).Sources.Measurements()[0].Name, w, r)
	return true, nil
}
func (qb *QueryBalance) forwardRequest(measurement string, response *http.ResponseWriter, r *http.Request) {
	qb.rw.RLock()
	defer qb.rw.RUnlock()
	ips := qb.measurements[measurement]
	if ips == nil {
		qb.Logger.Error("Balance error !!! Measurement does not exist in any node " +
			"in the cluster, the name of measurement is " + measurement)
		w := *response
		w.WriteHeader(404)
		w.Write([]byte("\n"))
		return
	}
	index := rand.Intn(len(ips) - 1)
	qb.clientConfig.Addr = "http://" + ips[index]
	c, err := client.NewHTTPClient(qb.clientConfig)
	if err != nil {
		qb.Logger.Error("Balance error !!! Http client open error ")
	}
	c.Balance(response, r)
	// todo高性能请求转发
}
