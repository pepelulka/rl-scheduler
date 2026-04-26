package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type inferWorkerJSON struct {
	ActiveTasks   int32   `json:"active_tasks"`
	CpuUtilPct    float32 `json:"cpu_util_pct"`
	CpuLimitCores float32 `json:"cpu_limit_cores"`
	RamUsageKiB   float32 `json:"ram_usage_kib"`
	RamMaxKiB     float32 `json:"ram_max_kib"`
}

type inferRequestJSON struct {
	Workers           []inferWorkerJSON `json:"workers"`
	QueueSize         int               `json:"queue_size"`
	QueueTasks        []struct{}        `json:"queue_tasks"`
	MaxTasksPerWorker int32             `json:"max_tasks_per_worker"`
}

type inferResponseJSON struct {
	RequestID   string `json:"request_id"`
	Action      int    `json:"action"`
	ActionType  string `json:"action_type"`
	WorkerIndex *int   `json:"worker_index"`
}

type feedbackRequestJSON struct {
	RequestID             string            `json:"request_id"`
	Reward                float32           `json:"reward"`
	Done                  bool              `json:"done"`
	NextWorkers           []inferWorkerJSON `json:"next_workers"`
	NextQueueSize         int               `json:"next_queue_size"`
	NextQueueTasks        []struct{}        `json:"next_queue_tasks"`
	NextMaxTasksPerWorker int32             `json:"next_max_tasks_per_worker"`
}

// RLScheduler вызывает inference-сервис для каждого решения о планировании
// и отправляет /feedback после завершения задачи для онлайн-дообучения PPO.
// При недоступности сервиса автоматически деградирует до LeastLoadedScheduler.
// Реализует Scheduler и TaskResultHandler.
type RLScheduler struct {
	inferURL          string
	maxTasksPerWorker int32
	fallback          Scheduler
	client            *http.Client

	mu            sync.Mutex
	taskToRequest map[string]string // taskID -> requestID (для assign-действий)
	lastCluster   ClusterState      // последнее известное состояние кластера
}

func NewRLScheduler(inferURL string, maxTasksPerWorker int32) *RLScheduler {
	return &RLScheduler{
		inferURL:          inferURL,
		maxTasksPerWorker: maxTasksPerWorker,
		fallback:          NewLeastLoadedScheduler(maxTasksPerWorker),
		client:            &http.Client{Timeout: 5 * time.Second},
		taskToRequest:     make(map[string]string),
	}
}

// Schedule implements Scheduler.
func (r *RLScheduler) Schedule(ctx context.Context, cluster ClusterState, task TaskInfo) (Decision, error) {
	r.mu.Lock()
	r.lastCluster = cluster
	r.mu.Unlock()

	req := inferRequestJSON{
		Workers:           workersToJSON(cluster.Workers),
		QueueSize:         cluster.QueueSize,
		QueueTasks:        []struct{}{},
		MaxTasksPerWorker: r.maxTasksPerWorker,
	}

	resp, err := r.callInfer(ctx, req)
	if err != nil {
		log.Printf("rl scheduler: /infer failed (%v), falling back to LeastLoaded", err)
		return r.fallback.Schedule(ctx, cluster, task)
	}

	if resp.ActionType == "wait" {
		reward := float32(-0.02) * float32(cluster.QueueSize)
		go r.sendFeedback(resp.RequestID, reward, cluster, cluster.QueueSize)
		return Decision{Wait: true}, nil
	}

	if resp.WorkerIndex == nil || *resp.WorkerIndex < 0 || *resp.WorkerIndex >= len(cluster.Workers) {
		log.Printf("rl scheduler: invalid worker_index %v, falling back to LeastLoaded", resp.WorkerIndex)
		return r.fallback.Schedule(ctx, cluster, task)
	}

	r.mu.Lock()
	r.taskToRequest[task.ID] = resp.RequestID
	r.mu.Unlock()

	return Decision{WorkerHost: cluster.Workers[*resp.WorkerIndex].Host}, nil
}

// OnTaskResult implements TaskResultHandler.
// Вызывается MasterService при получении ReportTaskResult от воркера.
func (r *RLScheduler) OnTaskResult(taskID string, success bool, queueSize int) {
	r.mu.Lock()
	reqID, ok := r.taskToRequest[taskID]
	if ok {
		delete(r.taskToRequest, taskID)
	}
	cluster := r.lastCluster
	r.mu.Unlock()

	if !ok {
		return
	}

	reward := float32(-0.02) * float32(queueSize)
	if success {
		reward += 1.0
	} else {
		reward -= 0.5
	}

	go r.sendFeedback(reqID, reward, cluster, queueSize)
}

func (r *RLScheduler) sendFeedback(requestID string, reward float32, nextCluster ClusterState, nextQueueSize int) {
	req := feedbackRequestJSON{
		RequestID:             requestID,
		Reward:                reward,
		Done:                  false,
		NextWorkers:           workersToJSON(nextCluster.Workers),
		NextQueueSize:         nextQueueSize,
		NextQueueTasks:        []struct{}{},
		NextMaxTasksPerWorker: r.maxTasksPerWorker,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.callFeedback(ctx, req); err != nil {
		log.Printf("rl scheduler: /feedback for %s failed: %v", requestID, err)
	}
}

func (r *RLScheduler) callInfer(ctx context.Context, req inferRequestJSON) (*inferResponseJSON, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.inferURL+"/infer", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("inference service returned HTTP %d", resp.StatusCode)
	}
	var result inferResponseJSON
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (r *RLScheduler) callFeedback(ctx context.Context, req feedbackRequestJSON) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.inferURL+"/feedback", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("inference service returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func workersToJSON(workers []WorkerState) []inferWorkerJSON {
	out := make([]inferWorkerJSON, len(workers))
	for i, w := range workers {
		cpuLimit := w.Metrics.CpuLimitCores
		if cpuLimit == 0 {
			cpuLimit = 1.0
		}
		ramMax := w.Metrics.RamMaxKiB
		if ramMax == 0 {
			ramMax = 1048576.0 // 1 GiB — дефолт до получения первых метрик
		}
		out[i] = inferWorkerJSON{
			ActiveTasks:   w.ActiveTasks,
			CpuUtilPct:    w.Metrics.CpuUtilPct,
			CpuLimitCores: cpuLimit,
			RamUsageKiB:   w.Metrics.RamUsageKiB,
			RamMaxKiB:     ramMax,
		}
	}
	return out
}
