package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"
)

// RoundRobinScheduler распределяет задачи по воркерам по кругу.
// Если выбранный воркер перегружен (activeTasks >= maxTasksPerWorker),
// планировщик пробует следующего. Если все заняты — возвращает Wait=true.
type RoundRobinScheduler struct {
	MaxTasksPerWorker int32
	counter           atomic.Uint64
}

func NewRoundRobinScheduler(maxTasksPerWorker int32) *RoundRobinScheduler {
	return &RoundRobinScheduler{MaxTasksPerWorker: maxTasksPerWorker}
}

func (s *RoundRobinScheduler) Schedule(_ context.Context, cluster ClusterState, _ TaskInfo) (Decision, error) {
	n := len(cluster.Workers)
	if n == 0 {
		return Decision{}, fmt.Errorf("no workers registered")
	}

	start := int(s.counter.Add(1)-1) % n
	for i := 0; i < n; i++ {
		w := cluster.Workers[(start+i)%n]
		if w.ActiveTasks < s.MaxTasksPerWorker {
			return Decision{WorkerHost: w.Host}, nil
		}
	}

	return Decision{Wait: true}, nil
}
