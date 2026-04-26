package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pepelulka/rl-scheduler/internal/common"
	mastermetrics "github.com/pepelulka/rl-scheduler/internal/master/metrics"
	"github.com/pepelulka/rl-scheduler/internal/master/scheduler"
	"github.com/pepelulka/rl-scheduler/internal/master/service"
	masterpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/master"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg := common.MustParseConfigOpt[Config]()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	workers := make([]string, len(cfg.Workers))
	for i, w := range cfg.Workers {
		workers[i] = w.Host
	}

	pollInterval := 10 * time.Second
	if cfg.MetricsPollInterval != "" {
		if d, err := time.ParseDuration(cfg.MetricsPollInterval); err == nil {
			pollInterval = d
		}
	}

	maxTasks := cfg.MaxTasksPerWorker
	if maxTasks <= 0 {
		maxTasks = 1
	}

	metricsCollector := mastermetrics.NewMetricsCollector(workers, pollInterval)
	go metricsCollector.Run(ctx)

	var sched scheduler.Scheduler
	switch cfg.Scheduler {
	case "rl":
		inferURL := cfg.InferenceURL
		if inferURL == "" {
			inferURL = "http://localhost:8000"
		}
		sched = scheduler.NewRLScheduler(inferURL, maxTasks)
		log.Printf("Using RL scheduler, inference URL: %s", inferURL)
	default:
		sched = scheduler.NewLeastLoadedScheduler(maxTasks)
		log.Println("Using LeastLoaded scheduler")
	}

	masterService := service.NewMasterService(workers, metricsCollector, sched, nil)
	go masterService.Run(ctx)

	address := fmt.Sprintf("0.0.0.0:%d", cfg.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	masterpb.RegisterMasterServiceServer(grpcServer, masterService)
	reflection.Register(grpcServer)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	log.Println("Starting gRPC server on", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
