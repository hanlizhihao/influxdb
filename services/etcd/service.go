package etcd // import "github.com/influxdata/influxdb/services/etcd"

import (
	"context"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

const (
	// Arbitrary, testing indicated that this doesn't typically get over 10
	parserChanLen = 1000

	// MaxUDPPayload is largest payload size the UDP service will accept.
	MaxUDPPayload = 64 * 1024
)

const (
	// Value is a collection of all database instances.
	TSDBCommonNodeKey = "tsdb-common-node"
	// Value is a collection of all available cluster, every item is key of cluster
	TSDBClustersKey = "tsdb-available-clusters"
	// Value is the cluster that tries to connect
	TSDBWorkKey                = "tsdb-work-cluster-"
	TSDBRecruitClustersKey     = "tsdb-recruit-clusters"
	TSDBRecruitClusterKey      = "tsdb-recruit-cluster"
	TSDBClusterAutoIncrementId = "tsdb-cluster-auto-increment-id"
	TSDBNodeAutoIncrementId    = "tsdb-node-auto-increment-id"
	statPointsReceived         = "pointsRx"
	statBytesReceived          = "bytesRx"
	statPointsParseFail        = "pointsParseFail"
	statReadFail               = "readFail"
	statBatchesTransmitted     = "batchesTx"
	statPointsTransmitted      = "pointsTx"
	statBatchesTransmitFail    = "batchesTxFail"
)

// Service is a UDP service that will listen for incoming packets of line protocol.
type Service struct {
	wg sync.WaitGroup

	mu    sync.RWMutex
	ready bool          // Has the required database been created?
	done  chan struct{} // Is the service closing or closed?

	config Config

	PointsWriter interface {
		WritePointsPrivileged(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}

	MetaClient interface {
		CreateDatabase(name string) (*meta.DatabaseInfo, error)
	}

	Logger *zap.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	d := *c.WithDefaults()
	return &Service{
		config: d,
		Logger: zap.NewNop(),
	}
}

// Open starts the service.
func (s *Service) Open() (err error) {
	s.Logger.Info("Starting register for ETCD service")
	if !s.closed() {
		return nil // Already open.
	}
	cli, err := GetEtcdClient(s.config)
	if err != nil {
		return errors.New("etcd connected failed")
	}
	ip, err := GetLocalHostIp()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cli.Put(ctx, TSDBCommonNodeKey, ip)
	cancel()
	go func() {
		cli, err := GetEtcdClient(s.config)
		if err != nil {
			s.Logger.Error("connected failed")
		}
		dbNodes := cli.Watch(context.Background(), TSDBCommonNodeKey, clientv3.WithPrefix())
		for dbNode := range dbNodes {
			for _, ev := range dbNode.Events {
				s.Logger.Info("新的节点加入" + ev.Kv.String())
			}
		}
	}()
	s.done = make(chan struct{})
	s.Logger.Info("Register successfully with etcd")
	return nil
}

// Statistics maintains statistics for the UDP service.
type Statistics struct {
	PointsReceived      int64
	BytesReceived       int64
	PointsParseFail     int64
	ReadFail            int64
	BatchesTransmitted  int64
	PointsTransmitted   int64
	BatchesTransmitFail int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) {
}

// Close closes the service and the underlying listener.
func (s *Service) Close() error {
	s.Logger.Info("Service closed")

	return nil
}

// Closed returns true if the service is currently closed.
func (s *Service) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed()
}

func (s *Service) closed() bool {
	select {
	case <-s.done:
		// Service is closing.
		return true
	default:
	}
	return s.done == nil
}

// createInternalStorage ensures that the required database has been created.
func (s *Service) createInternalStorage() error {
	s.mu.RLock()
	ready := s.ready
	s.mu.RUnlock()
	if ready {
		return nil
	}

	if _, err := s.MetaClient.CreateDatabase(s.config.Database); err != nil {
		return err
	}

	// The service is now ready.
	s.mu.Lock()
	s.ready = true
	s.mu.Unlock()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "etcd"))
}
