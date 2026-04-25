package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	workerpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/worker"
)

func main() {
	hosts := flag.String("workers", "", "worker hosts, comma-separated (e.g. localhost:2001,localhost:2002)")
	flag.Parse()

	if *hosts == "" {
		fmt.Fprintln(os.Stderr, "usage: cancel -workers host1:port,host2:port,...")
		os.Exit(1)
	}

	addrs := strings.Split(*hosts, ",")
	failed := false

	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		n, err := cancelWorker(addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  ✗  %s: %v\n", addr, err)
			failed = true
		} else {
			fmt.Printf("  ✓  %s: отменено задач: %d\n", addr, n)
		}
	}

	if failed {
		os.Exit(1)
	}
}

func cancelWorker(addr string) (int32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	resp, err := workerpb.NewWorkerServiceClient(conn).CancelAllTasks(ctx, &workerpb.CancelAllTasksRequest{})
	if err != nil {
		return 0, err
	}
	return resp.CancelledCount, nil
}
