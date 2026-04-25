package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pepelulka/rl-scheduler/internal/worker/executor"
	workermetrics "github.com/pepelulka/rl-scheduler/internal/worker/metrics"
	masterpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/master"
	workerpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerService struct {
	workerpb.UnimplementedWorkerServiceServer

	e *executor.Executor

	masterHost string

	activeTasks atomic.Int32

	cancelsMu sync.Mutex
	cancels   map[string]context.CancelFunc

	masterReportRetries      int
	masterReportTimeInterval time.Duration

	metricsSampler *workermetrics.Sampler
}

func NewService(
	e *executor.Executor,
	masterHost string,
	stopCh <-chan struct{},
) *WorkerService {
	sampler := &workermetrics.Sampler{}
	go sampler.Run(stopCh)

	return &WorkerService{
		e:          e,
		masterHost: masterHost,

		cancels: make(map[string]context.CancelFunc),

		masterReportRetries:      3,
		masterReportTimeInterval: time.Second,

		metricsSampler: sampler,
	}
}

func (s *WorkerService) GetStatus(ctx context.Context, req *workerpb.GetStatusRequest) (*workerpb.GetStatusResponse, error) {
	n := s.activeTasks.Load()
	return &workerpb.GetStatusResponse{
		Busy:        n > 0,
		ActiveTasks: n,
	}, nil
}

func (s *WorkerService) GetMetrics(ctx context.Context, req *workerpb.GetMetricsRequest) (*workerpb.GetMetricsResponse, error) {
	snap := s.metricsSampler.Latest()
	return &workerpb.GetMetricsResponse{
		Metrics: &workerpb.NodeMetrics{
			CpuUtilPct:    snap.CpuUtilPct,
			CpuLimitCores: snap.CpuLimitCores,
			RamUsageKib:   snap.RamUsageKiB,
			RamMaxKib:     snap.RamMaxKiB,
		},
	}, nil
}

func (s *WorkerService) NewTask(ctx context.Context, req *workerpb.NewTaskRequest) (*workerpb.NewTaskResponse, error) {
	taskCtx, cancel := context.WithCancel(context.Background())

	s.cancelsMu.Lock()
	s.cancels[req.TaskId] = cancel
	s.cancelsMu.Unlock()

	s.activeTasks.Add(1)
	go s.backgroundExecTask(taskCtx, req.TaskId, req.Task)

	return &workerpb.NewTaskResponse{Accepted: true}, nil
}

func (s *WorkerService) CancelAllTasks(ctx context.Context, req *workerpb.CancelAllTasksRequest) (*workerpb.CancelAllTasksResponse, error) {
	s.cancelsMu.Lock()
	count := int32(len(s.cancels))
	for _, cancel := range s.cancels {
		cancel()
	}
	s.cancels = make(map[string]context.CancelFunc)
	s.cancelsMu.Unlock()

	return &workerpb.CancelAllTasksResponse{CancelledCount: count}, nil
}

func (s *WorkerService) removeCancel(taskId string) {
	s.cancelsMu.Lock()
	delete(s.cancels, taskId)
	s.cancelsMu.Unlock()
}

func (s *WorkerService) reportToMaster(ctx context.Context, taskId string, success bool, errMsg string) error {
	conn, err := grpc.NewClient(s.masterHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	masterClient := masterpb.NewMasterServiceClient(conn)
	var taskResultType masterpb.TaskResultType
	if success {
		taskResultType = masterpb.TaskResultType_TASK_RESULT_TYPE_SUCCESS
	} else {
		taskResultType = masterpb.TaskResultType_TASK_RESULT_TYPE_FAIL
	}
	report := &masterpb.ReportTaskResultRequest{
		TaskId: taskId,
		Error:  errMsg,
		Type:   taskResultType,
	}

	errors := make([]string, 0)
	for i := range s.masterReportRetries {
		_, err = masterClient.ReportTaskResult(ctx, report)
		if err == nil {
			return nil
		}
		errors = append(errors, err.Error())
		if i != s.masterReportRetries-1 {
			time.Sleep(s.masterReportTimeInterval)
		}
	}
	return fmt.Errorf("failed to report to master: [ %s ]", strings.Join(errors, ", "))
}

func (s *WorkerService) backgroundExecTask(ctx context.Context, taskId string, task *workerpb.Task) {
	defer s.activeTasks.Add(-1)
	defer s.removeCancel(taskId)
	defer func() {
		if r := recover(); r != nil {
			s.reportToMaster(ctx, taskId, false, "recovered from panic")
		}
	}()

	if task.Type == workerpb.TaskType_TASK_TYPE_PYTHON {
		err := s.e.ExecPythonTask(ctx, executor.PythonTask{
			InputPath:  task.InputFile,
			OutputPath: task.OutputFile,
			ScriptPath: task.Script,
		})
		if err != nil {
			err = s.reportToMaster(context.Background(), taskId, false, err.Error())
			if err != nil {
				fmt.Println("failed to report to master: %v", err)
			}
		} else {
			err = s.reportToMaster(context.Background(), taskId, true, "")
			if err != nil {
				fmt.Println("failed to report to master: %v", err)
			}
		}
	} else {
		err := s.reportToMaster(context.Background(), taskId, false, "unsupported task type")
		if err != nil {
			fmt.Println("failed to report to master: %v", err)
		}
	}
}
