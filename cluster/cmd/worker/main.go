package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/pepelulka/rl-scheduler/internal/common"
	"github.com/pepelulka/rl-scheduler/internal/s3"
	"github.com/pepelulka/rl-scheduler/internal/worker/executor"
	"github.com/pepelulka/rl-scheduler/internal/worker/service"
	workerpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func setupService(ctx context.Context, cfg Config) (*service.WorkerService, error) {
	s3Cli, err := s3.NewClient(ctx, cfg.S3Config)
	if err != nil {
		return nil, err
	}
	e := executor.NewExecutor(s3Cli)

	return service.NewService(e, cfg.Master), nil
}

func main() {
	cfg := common.MustParseConfigOpt[Config]()

	address := fmt.Sprintf("127.0.0.1:%d", cfg.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	// health server is important for worker so master can ping it
	healthSrv := health.NewServer()

	grpc_health_v1.RegisterHealthServer(grpcServer, healthSrv)

	service, err := setupService(context.Background(), cfg)

	workerpb.RegisterWorkerServiceServer(grpcServer, service)

	// status of whole server
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	reflection.Register(grpcServer)

	log.Println("Starting gRPC server on", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
