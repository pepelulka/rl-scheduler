// Package metrics collects NodeMetrics from registered workers by polling
// each worker's GetMetrics gRPC endpoint on a fixed interval.
package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pepelulka/rl-scheduler/internal"
	workerpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// WorkerSnapshot is the latest collected metrics for a single worker.
type WorkerSnapshot struct {
	Metrics     internal.NodeMetrics
	CollectedAt time.Time
}

// MetricsCollector periodically polls all registered workers via gRPC and
// keeps the latest WorkerSnapshot for each one.
type MetricsCollector struct {
	mu        sync.RWMutex
	snapshots map[string]WorkerSnapshot // keyed by worker host

	workers      []string // gRPC host:port addresses
	pollInterval time.Duration
}

func NewMetricsCollector(workers []string, pollInterval time.Duration) *MetricsCollector {
	return &MetricsCollector{
		snapshots:    make(map[string]WorkerSnapshot, len(workers)),
		workers:      workers,
		pollInterval: pollInterval,
	}
}

// Get returns the latest snapshot for a worker host, or false if not yet collected.
func (c *MetricsCollector) Get(workerHost string) (WorkerSnapshot, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.snapshots[workerHost]
	return s, ok
}

// All returns a copy of all latest snapshots keyed by worker host.
func (c *MetricsCollector) All() map[string]WorkerSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]WorkerSnapshot, len(c.snapshots))
	for k, v := range c.snapshots {
		out[k] = v
	}
	return out
}

// Run starts the polling loop and blocks until ctx is cancelled.
func (c *MetricsCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, host := range c.workers {
				metrics, err := fetchMetrics(ctx, host)
				if err != nil {
					fmt.Printf("failed to fetch metric from '%s': %s\n", host, err)
					// Non-fatal: worker may be temporarily unavailable.
					continue
				}
				c.mu.Lock()
				c.snapshots[host] = WorkerSnapshot{
					Metrics:     metrics,
					CollectedAt: time.Now(),
				}
				c.mu.Unlock()
			}
		}
	}
}

func fetchMetrics(ctx context.Context, host string) (internal.NodeMetrics, error) {
	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return internal.NodeMetrics{}, err
	}
	defer conn.Close()

	resp, err := workerpb.NewWorkerServiceClient(conn).GetMetrics(ctx, &workerpb.GetMetricsRequest{})
	if err != nil {
		return internal.NodeMetrics{}, err
	}

	m := resp.GetMetrics()
	return internal.NodeMetrics{
		CpuUtilPct:    m.GetCpuUtilPct(),
		CpuLimitCores: m.GetCpuLimitCores(),
		RamUsageKiB:   m.GetRamUsageKib(),
		RamMaxKiB:     m.GetRamMaxKib(),
	}, nil
}
