package scheduler

import (
	"context"
	"fmt"
)

// LeastLoadedScheduler отправляет задачу воркеру с наименьшим числом
// активных задач. Если все воркеры заняты (activeTasks >= maxTasksPerWorker),
// возвращает Wait=true.
type LeastLoadedScheduler struct {
	MaxTasksPerWorker int32
}

func NewLeastLoadedScheduler(maxTasksPerWorker int32) *LeastLoadedScheduler {
	return &LeastLoadedScheduler{MaxTasksPerWorker: maxTasksPerWorker}
}

func (s *LeastLoadedScheduler) Schedule(_ context.Context, cluster ClusterState, _ TaskInfo) (Decision, error) {
	if len(cluster.Workers) == 0 {
		return Decision{}, fmt.Errorf("no workers registered")
	}

	best := cluster.Workers[0]
	for _, w := range cluster.Workers[1:] {
		if w.ActiveTasks < best.ActiveTasks {
			best = w
		}
	}

	if best.ActiveTasks >= s.MaxTasksPerWorker {
		return Decision{Wait: true}, nil
	}

	return Decision{WorkerHost: best.Host}, nil
}
