package httpd

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/httpd/consistent"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	timeout = 3
)

type Balance interface {
	SetMeasurementMapIndex(measurement map[string]interface{}, otherMeasurement map[string]uint64, classIpMap map[uint64][]string)
	SetConsistent(consistent *consistent.Consistent)
	SetMasterNode(node *consistent.Node)
	queryBalance(w *http.ResponseWriter, q string, r *http.Request) (bool, error)
	writeBalance(w *http.ResponseWriter, r *http.Request, points []models.Point, db string, rp string, user meta.User) ([]models.Point, error)
	GetNewPointChan() chan *NewMeasurementPoint
	// 数据库连接池
	GetClient(ip string, agent string) (*client.Client, error)
	GetClientByClassId(agent string, classId uint64) (*client.Client, error)
	ForwardPoint(c client.Client, points []models.Point) error
	SetMetaData(me *meta.Client)
}

type NewMeasurementPoint struct {
	Points []models.Point
	DB     string
	Rp     string
	User   meta.User
}

type HttpBalance struct {
	MetaClient interface {
		Data() meta.Data
	}
	// local class's measurements
	measurement map[string]interface{}
	// other class's measurement
	otherMeasurement map[string]uint64
	// if there is point transfer to class, will need ip
	classIpMap map[uint64][]string
	// local cluster's master node
	masterNode   *consistent.Node
	clients      map[string]client.Client
	clientConfig client.HTTPConfig
	Logger       *zap.Logger
	transport    http.Transport
	// Consistent Hash
	ConsistentHash *consistent.Consistent

	newPointChan chan *NewMeasurementPoint
}

func NewBalance() *HttpBalance {
	return &HttpBalance{
		Logger: zap.NewNop(),
		clientConfig: client.HTTPConfig{
			Timeout:            timeout,
			InsecureSkipVerify: false,
		},
		clients:      make(map[string]client.Client, 0),
		newPointChan: make(chan *NewMeasurementPoint),
	}
}

func (qb *HttpBalance) SetMetaData(me *meta.Client) {
	qb.MetaClient = me
}

func (qb *HttpBalance) GetNewPointChan() chan *NewMeasurementPoint {
	return qb.newPointChan
}

func (qb *HttpBalance) SetMeasurementMapIndex(measurement map[string]interface{}, otherMeasurement map[string]uint64, classIpMap map[uint64][]string) {
	if qb.measurement == nil {
		qb.measurement = measurement
	}
	if qb.otherMeasurement == nil {
		qb.otherMeasurement = otherMeasurement
	}
	if qb.classIpMap == nil {
		qb.classIpMap = classIpMap
	}
}

func (qb *HttpBalance) SetConsistent(ch *consistent.Consistent) {
	if ch != qb.ConsistentHash {
		qb.ConsistentHash = ch
	}
}

func (qb *HttpBalance) SetMasterNode(node *consistent.Node) {
	qb.masterNode = node
}

func (qb *HttpBalance) query(c *int32, r *http.Request, resultChan chan *query.Result, ip string) {
	var err error
	r.URL, err = url.Parse("http://" + ip + "/query?" + r.Form.Encode())
	r.Header.Add("distribute", "")
	r.Header.Add("balance", "")
	resp, err := qb.transport.RoundTrip(r)
	defer resp.Body.Close()
	if err != nil {
		qb.Logger.Error("Distribute query for ip failed, error message is" + err.Error())
		return
	}
	var response = Response{}
	respByte, err := ioutil.ReadAll(resp.Body)
	err = response.UnmarshalJSON(respByte)
	if err != nil || resp.StatusCode != http.StatusOK || response.Error() == nil {
		qb.Logger.Error("Execute distributed query error, host ip:" + ip + "JSON decode error message:" + err.Error() +
			"response error message:" + response.Error().Error())
		return
	}
	for _, result := range response.Results {
		resultChan <- result
	}
	atomic.AddInt32(c, 1)
}

// return true, the request will be forward, return false, the request will be not forward
func (qb *HttpBalance) queryBalance(w *http.ResponseWriter, q string, r *http.Request) (bool, error) {
	if agent := r.UserAgent(); agent == "InfluxDBClient" {
		return false, nil
	}
	if headerBalance := r.Header.Get("balance"); &headerBalance != nil {
		return false, nil
	}
	key, err := GetMeasurementFromInfluxQL(q)
	if err != nil {
		return false, err
	}
	if qb.measurement[key] != nil {
		return false, nil
	}
	go qb.forwardRequest(key, w, r)
	return true, nil
}
func (qb *HttpBalance) forwardRequest(measurement string, response *http.ResponseWriter, r *http.Request) {
	var err error
	ips := qb.classIpMap[qb.otherMeasurement[measurement]]
	w := *response
	if ips == nil || len(ips) == 0 {
		qb.Logger.Error("Balance error !!! Measurement does not exist in any node " +
			"in the cluster, the name of measurement is " + measurement)
		w.WriteHeader(404)
		_, err = w.Write([]byte("Measurement don't exist, Please try again later \n"))
		if err != nil {
			qb.Logger.Error("Forward Request failed, error message is" + err.Error())
		}
		return
	}
	var index int
	if len(ips) == 1 {
		index = 0
	} else {
		index = rand.Intn(len(ips) - 1)
	}
	r.URL, err = url.Parse("http://" + ips[index] + "/query?" + r.Form.Encode())
	r.Header.Add("balance", "")
	resp, err := qb.transport.RoundTrip(r)
	if err != nil {
		qb.Logger.Error("Balance error !!! ")
	}
	w.WriteHeader(resp.StatusCode)
	p, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		qb.Logger.Error("read body error\n", zapcore.Field{Interface: err})
		return
	}
	var retryTime = -1
RetryWriteResponse:
	retryTime++
	_, err = w.Write(p)
	if retryTime < 3 {
		qb.Logger.Error("!!!! Query Balance process response of the forward request error, will try 3 times")
		goto RetryWriteResponse
	}
}

func (qb *HttpBalance) writeBalance(w *http.ResponseWriter, r *http.Request, points []models.Point, db string,
	rp string, user meta.User) ([]models.Point, error) {
	if userAgent := r.Header.Get("User-Agent"); "InfluxDBClient" == userAgent {
		return points, nil
	}
	localClassPoint := make([]models.Point, len(points))
	// otherClassPoint need forward other class to be process
	otherClassPoint := make(map[uint64][]models.Point)
	newMeasurementPoint := make([]models.Point, 0)
	for _, point := range points {
		name := string(point.Name())
		if qb.measurement[name] == nil {
			classId := qb.otherMeasurement[name]
			// todo: check map value uint64
			if classId == 0 {
				newMeasurementPoint = append(newMeasurementPoint, point)
				continue
			}
			points = otherClassPoint[classId]
			if points == nil {
				points = make([]models.Point, 0)
				points = append(points, point)
			} else {
				points = append(points, point)
			}
			otherClassPoint[classId] = points
			continue
		}
		localClassPoint = append(localClassPoint, point)
	}
	// process otherClassPoint
	if len(otherClassPoint) != 0 {
		go func(otherClassPoint map[uint64][]models.Point) {
			processFailedKeys := make([]string, 0)
			retryTime := -1
		RetryForward:
			retryTime++
			for classId, otherClassPoints := range otherClassPoint {
				otherClassClient, err := qb.GetClientByClassId("InfluxForwardClient", classId)
				err = qb.ForwardPoint(*otherClassClient, otherClassPoints)
				if err != nil {
					qb.Logger.Error("Failure to retrieve client for forwarding write request, will try again")
				}
			}
			if retryTime < 3 && len(processFailedKeys) != 0 {
				goto RetryForward
			}
		}(otherClassPoint)
	}
	// process new measurement
	if len(newMeasurementPoint) != 0 {
		go func(newMeasurementPoint []models.Point) {
			qb.newPointChan <- &NewMeasurementPoint{Points: newMeasurementPoint, DB: db, Rp: rp, User: user}
		}(newMeasurementPoint)
	}

	// process point belong local class
	pointWriteOtherCluster := make(map[string][]models.Point)
	localNodePoint := make([]models.Point, 0)
	for _, localPoint := range localClassPoint {
		node := qb.ConsistentHash.Get(string(localPoint.Key()))
		if node.Id == qb.masterNode.Id {
			localNodePoint = append(localNodePoint, localPoint)
			continue
		}
		if tempPoints := pointWriteOtherCluster[node.Ip]; tempPoints == nil {
			pointWriteOtherCluster[node.Ip] = []models.Point{localPoint}
		} else {
			pointWriteOtherCluster[node.Ip] = append(tempPoints, localPoint)
		}
	}
	// process point belong local class, but belong other cluster
	go func() {
		processFailedKeys := make([]string, 0)
		retryTime := -1
	RetryForward:
		retryTime++
		for ip, value := range pointWriteOtherCluster {
			httpClient, forwardError := qb.GetClient(ip, "")
			forwardError = qb.ForwardPoint(*httpClient, value)
			if forwardError != nil {
				processFailedKeys = append(processFailedKeys, ip)
			}
		}
		if retryTime < 3 && len(processFailedKeys) != 0 {
			goto RetryForward
		}
	}()
	// process belong local cluster
	return localNodePoint, nil
}

func (qb *HttpBalance) GetClient(ip string, agent string) (*client.Client, error) {
	var err error
	httpClient := qb.clients[ip]
	if httpClient == nil {
		var httpConfig = qb.clientConfig
		httpConfig.Addr = ip
		httpConfig.UserAgent = agent
		httpClient, err = client.NewHTTPClient(httpConfig)
		if err != nil {
			qb.Logger.Error("Initial http client error")
			return nil, err
		}
	}
	return &httpClient, nil
}

func (qb *HttpBalance) GetClientByClassId(agent string, classId uint64) (*client.Client, error) {
	var err error
RetryGetClient:
	ips := qb.classIpMap[classId]
	if ips == nil {
		qb.Logger.Error("get ip from class id failed, ips is nil")
		return nil, errors.New("class " + strconv.FormatUint(classId, 10) + "is nil error")
	}
	httpClient := qb.clients[ips[0]]
	if httpClient == nil {
		var httpConfig = qb.clientConfig
		httpConfig.Addr = ips[0]
		httpConfig.UserAgent = agent
		httpClient, err = client.NewHTTPClient(httpConfig)
		if err != nil {
			qb.Logger.Error("Initial http client error, class id " + strconv.FormatUint(classId, 10) +
				"ip is" + ips[0])
			qb.classIpMap[classId] = append(ips[:1], ips[1:]...)
			if len(ips) != 1 {
				goto RetryGetClient
			}
			qb.Logger.Error("Get http client for class id" + strconv.FormatUint(classId, 10) + "failed")
			return nil, err
		}
	}
	return &httpClient, nil
}

func (qb *HttpBalance) ForwardPoint(c client.Client, points []models.Point) error {
	if c == nil {
		return errors.New("Client is nil ")
	}
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{})
	if err != nil {
		qb.Logger.Error("Unexpected error, new batch point error")
		return err
	}
	for _, pointForward := range points {
		bp.AddPoint(&client.Point{
			Pt: pointForward,
		})
	}
	return c.Write(bp)
}

var (
	ErrWrongQuote     = errors.New("wrong quote")
	ErrUnmatchedQuote = errors.New("unmatched quote")
	ErrUnclosed       = errors.New("unclosed parenthesis")
	ErrIllegalQL      = errors.New("illegal InfluxQL")
)

func FindEndWithQuote(data []byte, start int, endchar byte) (end int, unquoted []byte, err error) {
	unquoted = append(unquoted, data[start])
	start++
	for end = start; end < len(data); end++ {
		switch data[end] {
		case endchar:
			unquoted = append(unquoted, data[end])
			end++
			return
		case '\\':
			switch {
			case len(data) == end:
				err = ErrUnmatchedQuote
				return
			case data[end+1] == endchar:
				end++
				unquoted = append(unquoted, data[end])
			default:
				err = ErrWrongQuote
				return
			}
		default:
			unquoted = append(unquoted, data[end])
		}
	}
	err = ErrUnmatchedQuote
	return
}

func ScanToken(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	start := 0
	for ; start < len(data) && data[start] == ' '; start++ {
	}
	if start == len(data) {
		return 0, nil, nil
	}

	switch data[start] {
	case '"':
		advance, token, err = FindEndWithQuote(data, start, '"')
		if err != nil {
			log.Printf("scan token error: %s\n", err)
		}
		return
	case '\'':
		advance, token, err = FindEndWithQuote(data, start, '\'')
		if err != nil {
			log.Printf("scan token error: %s\n", err)
		}
		return
	case '(':
		advance = bytes.IndexByte(data[start:], ')')
		if advance == -1 {
			err = ErrUnclosed
		} else {
			advance += start + 1
		}
	case '[':
		advance = bytes.IndexByte(data[start:], ']')
		if advance == -1 {
			err = ErrUnclosed
		} else {
			advance += start + 1
		}
	case '{':
		advance = bytes.IndexByte(data[start:], '}')
		if advance == -1 {
			err = ErrUnclosed
		} else {
			advance += start + 1
		}
	default:
		advance = bytes.IndexFunc(data[start:], func(r rune) bool {
			return r == ' '
		})
		if advance == -1 {
			advance = len(data)
		} else {
			advance += start
		}

	}
	if err != nil {
		log.Printf("scan token error: %s\n", err)
		return
	}

	token = data[start:advance]
	// fmt.Printf("%s (%d, %d) = %s\n", data, start, advance, token)
	return
}

func GetMeasurementFromInfluxQL(q string) (m string, err error) {
	buf := bytes.NewBuffer([]byte(q))
	scanner := bufio.NewScanner(buf)
	scanner.Buffer([]byte(q), len(q))
	scanner.Split(ScanToken)
	var tokens []string
	for scanner.Scan() {
		tokens = append(tokens, scanner.Text())
	}
	//fmt.Printf("%v\n", tokens)

	for i := 0; i < len(tokens); i++ {
		// fmt.Printf("%v\n", tokens[i])
		if strings.ToLower(tokens[i]) == "from" {
			if i+1 < len(tokens) {
				m = getMeasurement(tokens[i+1:])
				return
			}
		}
	}

	return "", ErrIllegalQL
}

func getMeasurement(tokens []string) (m string) {
	if len(tokens) >= 2 && strings.HasPrefix(tokens[1], ".") {
		m = tokens[1]
		m = m[1:]
		if m[0] == '"' || m[0] == '\'' {
			m = m[1 : len(m)-1]
		}
		return
	}

	m = tokens[0]
	if m[0] == '/' {
		return m
	}

	if m[0] == '"' || m[0] == '\'' {
		m = m[1 : len(m)-1]
		return
	}

	index := strings.IndexByte(m, '.')
	if index == -1 {
		return
	}

	m = m[index+1:]
	if m[0] == '"' || m[0] == '\'' {
		m = m[1 : len(m)-1]
	}
	return
}
