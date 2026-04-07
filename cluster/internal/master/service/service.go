package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	mastermetrics "github.com/pepelulka/rl-scheduler/internal/master/metrics"
	"github.com/pepelulka/rl-scheduler/internal/master/scheduler"
	masterpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/master"
	workerpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	taskQueueSize   = 1024
	dispatchRetry   = 500 * time.Millisecond
	workerCallTimeout = 5 * time.Second
)

// queuedTask — задача, ожидающая размещения на воркере.
type queuedTask struct {
	id     string
	pbTask *workerpb.Task
	meta   map[string]string
}

type taskRecord struct {
	status masterpb.TaskStatus
	errMsg string
}

type MasterService struct {
	masterpb.UnimplementedMasterServiceServer

	workers []string // gRPC host:port

	metrics   *mastermetrics.MetricsCollector
	sched     scheduler.Scheduler
	taskQueue chan queuedTask

	mu      sync.RWMutex
	tasks   map[string]*taskRecord

	// clusterMeta — произвольная метаинформация уровня кластера,
	// передаётся планировщику при каждом вызове Schedule.
	clusterMeta map[string]string
}

func NewMasterService(
	workers []string,
	metrics *mastermetrics.MetricsCollector,
	sched scheduler.Scheduler,
	clusterMeta map[string]string,
) *MasterService {
	return &MasterService{
		workers:     workers,
		metrics:     metrics,
		sched:       sched,
		taskQueue:   make(chan queuedTask, taskQueueSize),
		tasks:       make(map[string]*taskRecord),
		clusterMeta: clusterMeta,
	}
}

func (s *MasterService) setTaskStatus(id string, st masterpb.TaskStatus, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[id] = &taskRecord{status: st, errMsg: errMsg}
}

// Run запускает фоновый диспетчер и блокируется до отмены ctx.
func (s *MasterService) Run(ctx context.Context) {
	s.dispatch(ctx)
}

// ── gRPC handlers ────────────────────────────────────────────────────────────

func (s *MasterService) ReportTaskResult(_ context.Context, req *masterpb.ReportTaskResultRequest) (*masterpb.ReportTaskResultResponse, error) {
	switch req.Type {
	case masterpb.TaskResultType_TASK_RESULT_TYPE_SUCCESS:
		log.Printf("task %s: success", req.TaskId)
		s.setTaskStatus(req.TaskId, masterpb.TaskStatus_TASK_STATUS_SUCCESS, "")
	case masterpb.TaskResultType_TASK_RESULT_TYPE_FAIL:
		log.Printf("task %s: fail — %s", req.TaskId, req.Error)
		s.setTaskStatus(req.TaskId, masterpb.TaskStatus_TASK_STATUS_FAIL, req.Error)
	default:
		log.Printf("task %s: unknown result type", req.TaskId)
	}
	return &masterpb.ReportTaskResultResponse{}, nil
}

func (s *MasterService) GetTaskStatus(_ context.Context, req *masterpb.GetTaskStatusRequest) (*masterpb.GetTaskStatusResponse, error) {
	s.mu.RLock()
	rec, ok := s.tasks[req.TaskId]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.TaskId)
	}
	return &masterpb.GetTaskStatusResponse{
		TaskId: req.TaskId,
		Status: rec.status,
		Error:  rec.errMsg,
	}, nil
}

func (s *MasterService) SubmitTask(_ context.Context, req *masterpb.SubmitTaskRequest) (*masterpb.SubmitTaskResponse, error) {
	taskType := taskTypeFromString(req.Type)
	if taskType == workerpb.TaskType_TASK_TYPE_UNSPECIFIED {
		return nil, fmt.Errorf("unknown task type: %q", req.Type)
	}

	task := queuedTask{
		id: generateID(),
		pbTask: &workerpb.Task{
			Type:       taskType,
			InputFile:  req.InputFile,
			OutputFile: req.OutputFile,
			Script:     req.Script,
		},
		meta: req.Meta,
	}

	s.setTaskStatus(task.id, masterpb.TaskStatus_TASK_STATUS_QUEUED, "")
	select {
	case s.taskQueue <- task:
		return &masterpb.SubmitTaskResponse{TaskId: task.id}, nil
	default:
		s.mu.Lock()
		delete(s.tasks, task.id)
		s.mu.Unlock()
		return nil, fmt.Errorf("task queue is full")
	}
}

// ── dispatcher ───────────────────────────────────────────────────────────────

func (s *MasterService) dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.taskQueue:
			s.placeTask(ctx, task)
		}
	}
}

// placeTask пытается разместить задачу на воркере.
// При Wait-решении планировщика делает паузу и повторяет попытку.
func (s *MasterService) placeTask(ctx context.Context, task queuedTask) {
	info := scheduler.TaskInfo{
		ID:   task.id,
		Type: task.pbTask.Type.String(),
		Meta: task.meta,
	}
	for {
		cluster := s.collectClusterState(ctx)
		decision, err := s.sched.Schedule(ctx, cluster, info)
		if err != nil {
			log.Printf("scheduler error for task %s: %v", task.id, err)
			decision = scheduler.Decision{Wait: true}
		}

		if decision.Wait {
			select {
			case <-ctx.Done():
				return
			case <-time.After(dispatchRetry):
				continue
			}
		}

		if err := s.sendToWorker(ctx, decision.WorkerHost, task); err != nil {
			log.Printf("failed to send task %s to %s: %v", task.id, decision.WorkerHost, err)
			s.setTaskStatus(task.id, masterpb.TaskStatus_TASK_STATUS_QUEUED, "")
			// Воркер недоступен — ждём и повторяем через планировщик.
			select {
			case <-ctx.Done():
				return
			case <-time.After(dispatchRetry):
				continue
			}
		}
		return
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (s *MasterService) collectClusterState(ctx context.Context) scheduler.ClusterState {
	ctx, cancel := context.WithTimeout(ctx, workerCallTimeout)
	defer cancel()

	workers := make([]scheduler.WorkerState, 0, len(s.workers))
	for _, host := range s.workers {
		ws := scheduler.WorkerState{Host: host}

		if snap, ok := s.metrics.Get(host); ok {
			ws.Metrics = snap.Metrics
		}

		if n, err := fetchActiveTasks(ctx, host); err == nil {
			ws.ActiveTasks = n
		}

		workers = append(workers, ws)
	}
	return scheduler.ClusterState{Workers: workers, Meta: s.clusterMeta}
}

func (s *MasterService) sendToWorker(ctx context.Context, host string, task queuedTask) error {
	ctx, cancel := context.WithTimeout(ctx, workerCallTimeout)
	defer cancel()

	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = workerpb.NewWorkerServiceClient(conn).NewTask(ctx, &workerpb.NewTaskRequest{
		TaskId: task.id,
		Task:   task.pbTask,
	})
	if err == nil {
		s.setTaskStatus(task.id, masterpb.TaskStatus_TASK_STATUS_RUNNING, "")
	}
	return err
}

func fetchActiveTasks(ctx context.Context, host string) (int32, error) {
	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	resp, err := workerpb.NewWorkerServiceClient(conn).GetStatus(ctx, &workerpb.GetStatusRequest{})
	if err != nil {
		return 0, err
	}
	return resp.ActiveTasks, nil
}

func taskTypeFromString(s string) workerpb.TaskType {
	switch s {
	case "python":
		return workerpb.TaskType_TASK_TYPE_PYTHON
	default:
		return workerpb.TaskType_TASK_TYPE_UNSPECIFIED
	}
}

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
