package etcd

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"os"
	"testing"
	"time"
)

func TestService_OpenClose(t *testing.T) {
	service := NewTestService(nil)
	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{service.Config.EtcdAddress},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	value, _ := cli.Get(context.TODO(), "mesh-common-normal-db-ip", clientv3.WithPrefix())
	if value.Count == 0 {
		t.Fatal("错误")
	}
	// Closing a closed service is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	// Closing a closed service again is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Opening an already open service is fine.
	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Reopening a previously opened service is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Tidy up.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestService_CreatesDatabase(t *testing.T) {

}

type TestService struct {
	Service       *Service
	Config        Config
	MetaClient    *internal.MetaClientMock
	WritePointsFn func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
}

func NewTestService(c *Config) *TestService {
	if c == nil {
		defaultC := NewConfig()
		c = &defaultC
	}

	service := &TestService{
		Service:    NewService(*c),
		Config:     *c,
		MetaClient: &internal.MetaClientMock{},
	}

	if testing.Verbose() {
		service.Service.WithLogger(logger.New(os.Stderr))
	}

	service.Service.MetaClient = service.MetaClient
	service.Service.PointsWriter = service
	return service
}

func (s *TestService) WritePointsPrivileged(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return s.WritePointsFn(database, retentionPolicy, consistencyLevel, points)
}
