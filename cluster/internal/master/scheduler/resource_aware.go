package scheduler

import (
	"context"
	"fmt"
	"math"
)

// ResourceAwareScheduler выбирает воркер с наименьшей взвешенной нагрузкой
// по ресурсам: score = cpuWeight*CPU% + (1-cpuWeight)*RAM%.
// Воркеры с activeTasks >= maxTasksPerWorker исключаются.
// Если метрики недоступны (нули), fallback — наименьший activeTasks.
type ResourceAwareScheduler struct {
	MaxTasksPerWorker int32
	// CpuWeight задаёт долю CPU в итоговой оценке нагрузки (0..1).
	// 0.5 — равный вес CPU и RAM.
	CpuWeight float64
}

func NewResourceAwareScheduler(maxTasksPerWorker int32, cpuWeight float64) *ResourceAwareScheduler {
	return &ResourceAwareScheduler{
		MaxTasksPerWorker: maxTasksPerWorker,
		CpuWeight:         cpuWeight,
	}
}

func (s *ResourceAwareScheduler) Schedule(_ context.Context, cluster ClusterState, _ TaskInfo) (Decision, error) {
	if len(cluster.Workers) == 0 {
		return Decision{}, fmt.Errorf("no workers registered")
	}

	bestScore := math.MaxFloat64
	bestHost := ""

	for _, w := range cluster.Workers {
		if w.ActiveTasks >= s.MaxTasksPerWorker {
			continue
		}

		score := s.workerScore(w)
		if score < bestScore {
			bestScore = score
			bestHost = w.Host
		}
	}

	if bestHost == "" {
		return Decision{Wait: true}, nil
	}

	return Decision{WorkerHost: bestHost}, nil
}

// workerScore вычисляет взвешенную нагрузку воркера.
// Если метрики отсутствуют (нули), возвращает activeTasks как float.
func (s *ResourceAwareScheduler) workerScore(w WorkerState) float64 {
	m := w.Metrics
	hasMetrics := m.CpuUtilPct > 0 || m.RamUsageKiB > 0

	if !hasMetrics {
		return float64(w.ActiveTasks)
	}

	var cpuPct, ramPct float64

	cpuPct = float64(m.CpuUtilPct)

	if m.RamMaxKiB > 0 {
		ramPct = float64(m.RamUsageKiB) / float64(m.RamMaxKiB) * 100.0
	}

	return s.CpuWeight*cpuPct + (1-s.CpuWeight)*ramPct
}
