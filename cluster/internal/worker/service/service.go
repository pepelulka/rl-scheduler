package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pepelulka/rl-scheduler/internal/worker/executor"
	masterpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/master"
	workerpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerService struct {
	e *executor.Executor

	masterHost string

	mtx sync.Mutex

	busy bool

	masterReportRetries      int
	masterReportTimeInterval time.Duration
}

func NewService(
	e *executor.Executor,
	masterHost string,
) *WorkerService {
	return &WorkerService{
		e:          e,
		busy:       false,
		masterHost: masterHost,

		masterReportRetries:      3,
		masterReportTimeInterval: time.Second,
	}
}

func (s *WorkerService) GetStatus(ctx context.Context, req *workerpb.GetStatusRequest) (*workerpb.GetStatusResponse, error) {
	return &workerpb.GetStatusResponse{
		Busy: s.busy,
	}, nil
}

func (s *WorkerService) NewTask(ctx context.Context, req *workerpb.NewTaskRequest) (*workerpb.NewTaskResponse, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.busy {
		return &workerpb.NewTaskResponse{
			Accepted: false,
			Error:    workerpb.NewTaskError_NEW_TASK_ERROR_WORKER_IS_BUSY,
			ErrorMsg: "worker is busy",
		}, nil
	}

	s.busy = true

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
	defer func() {
		if r := recover(); r != nil {
			s.reportToMaster(
				ctx,
				taskId,
				false,
				"recovered from panic",
			)
			s.busy = false
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
	s.busy = false
}
