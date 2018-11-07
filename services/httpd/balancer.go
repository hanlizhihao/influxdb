package httpd

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/influxdata/influxdb/client/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	timeout = 3
)

type Balance interface {
	SetMeasurementMapIndex(remote map[string][]string, local map[string]interface{})
	balance(w *http.ResponseWriter, q string, r *http.Request) (bool, error)
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
	transport        http.Transport
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
func (qb *QueryBalance) balance(w *http.ResponseWriter, q string, r *http.Request) (bool, error) {
	key, err := GetMeasurementFromInfluxQL(q)
	if err != nil {
		return false, err
	}
	qb.rw.RLock()
	defer qb.rw.RUnlock()
	if qb.localMeasurement[key] != nil {
		return false, nil
	}
	go qb.forwardRequest(key, w, r)
	return true, nil
}
func (qb *QueryBalance) forwardRequest(measurement string, response *http.ResponseWriter, r *http.Request) {
	var err error
	qb.rw.RLock()
	defer qb.rw.RUnlock()
	ips := qb.measurements[measurement]
	w := *response
	if ips == nil || len(ips) == 0 {
		qb.Logger.Error("Balance error !!! Measurement does not exist in any node " +
			"in the cluster, the name of measurement is " + measurement)
		w.WriteHeader(404)
		w.Write([]byte("\n"))
		return
	}
	var index int
	if len(ips) == 1 {
		index = 0
	} else {
		index = rand.Intn(len(ips) - 1)
	}
	qb.clientConfig.Addr = "http://" + ips[index]
	r.URL, err = url.Parse(qb.clientConfig.Addr + "/query?" + r.Form.Encode())
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
	w.Write(p)
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
