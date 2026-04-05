package service

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	workermetrics "github.com/pepelulka/rl-scheduler/internal/worker/metrics"
	"github.com/pepelulka/rl-scheduler/internal/worker/executor"
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
	s.activeTasks.Add(1)
	go s.backgroundExecTask(context.Background(), req.TaskId, req.Task)

	return &workerpb.NewTaskResponse{
		Accepted: true,
	}, nil
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
	defer func() {
		if r := recover(); r != nil {
			s.reportToMaster(ctx, taskId, false, "recovered from panic")
		}
	}()

	if task.Type == workerpb.TaskType_TASK_TYPE_PYTHON {
		err := s.e.ExecPythonTask(executor.PythonTask{
			InputPath:  task.InputFile,
			OutputPath: task.OutputFile,
			ScriptPath: task.Script,
		})
		if err != nil {
			err = s.reportToMaster(ctx, taskId, false, err.Error())
			if err != nil {
				fmt.Println("failed to report to master: %v", err)
			}
		} else {
			err = s.reportToMaster(ctx, taskId, true, "")
			if err != nil {
				fmt.Println("failed to report to master: %v", err)
			}
		}
	} else {
		err := s.reportToMaster(ctx, taskId, false, "unsupported task type")
		if err != nil {
			fmt.Println("failed to report to master: %v", err)
		}
	}
}
