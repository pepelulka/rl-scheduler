package main

import (
	"fmt"
	"log"
	"net"

	"github.com/pepelulka/rl-scheduler/internal/common"
	"github.com/pepelulka/rl-scheduler/internal/master/service"
	masterpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/master"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg := common.MustParseConfigOpt[Config]()

	address := fmt.Sprintf("127.0.0.1:%d", cfg.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	healthSrv := health.NewServer()

	grpc_health_v1.RegisterHealthServer(grpcServer, healthSrv)

	masterService := service.NewMasterService()
	masterpb.RegisterMasterServiceServer(grpcServer, masterService)

	reflection.Register(grpcServer)

	log.Println("Starting gRPC server on", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
