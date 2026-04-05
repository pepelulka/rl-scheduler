package service

import (
	"context"
	"fmt"

	masterpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/master"
)

type MasterService struct{}

func NewMasterService() *MasterService {
	return &MasterService{}
}

func (s *MasterService) ReportTaskResult(ctx context.Context, req *masterpb.ReportTaskResultRequest) (*masterpb.ReportTaskResultResponse, error) {
	if req.Type == masterpb.TaskResultType_TASK_RESULT_TYPE_FAIL {
		fmt.Println("fail:", req.Error)
	} else if req.Type == masterpb.TaskResultType_TASK_RESULT_TYPE_SUCCESS {
		fmt.Println("success, task id =", req.TaskId)
	} else {
		fmt.Println("don't know...")
	}
	return &masterpb.ReportTaskResultResponse{}, nil
}
