// Тестовый сценарий: загружает скрипт и входные данные в S3,
// отправляет задачи на мастер, ждёт завершения через GetTaskStatus и проверяет результаты.
//
// Скрипт читает из stdin целое число и выводит его квадрат.
// Входные данные: 1, 2, 3, ... N. Ожидаемые результаты: 1, 4, 9, ... N².
//
// Запуск:
//
//	go run ./cmd/scenario \
//	  -master localhost:2000 \
//	  -s3-endpoint http://localhost:9000 \
//	  -n 5
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pepelulka/rl-scheduler/internal/s3"
	masterpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/master"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Скрипт читает число из stdin и печатает его квадрат.
const pythonScript = `import sys
n = int(sys.stdin.read().strip())
print(n * n)
`

func main() {
	masterAddr := flag.String("master", "localhost:2000", "master gRPC address")
	s3Endpoint := flag.String("s3-endpoint", "http://localhost:9000", "S3 endpoint")
	s3Login    := flag.String("s3-login", "local", "S3 access key")
	s3Password := flag.String("s3-password", "12345678", "S3 secret key")
	s3Bucket   := flag.String("s3-bucket", "data", "S3 bucket")
	n          := flag.Int("n", 3, "number of tasks to submit")
	timeout    := flag.Duration("timeout", 60*time.Second, "how long to wait for results")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+10*time.Second)
	defer cancel()

	// Уникальный префикс для этого запуска, чтобы не пересекаться с другими.
	prefix := fmt.Sprintf("scenario/%d", time.Now().UnixMilli())

	s3Cli, err := s3.NewClient(ctx, s3.Config{
		Endpoint: *s3Endpoint,
		Login:    *s3Login,
		Password: *s3Password,
		Bucket:   *s3Bucket,
	})
	if err != nil {
		log.Fatalf("s3 init: %v", err)
	}

	// ── 1. Загрузка скрипта в S3 ─────────────────────────────────────────────
	scriptKey := prefix + "/script.py"
	if err := s3Cli.Upload(ctx, scriptKey, strings.NewReader(pythonScript)); err != nil {
		log.Fatalf("upload script: %v", err)
	}
	fmt.Printf("uploaded script → s3://%s/%s\n", *s3Bucket, scriptKey)

	// ── 2. Загрузка входных файлов ────────────────────────────────────────────
	inputKeys  := make([]string, *n)
	outputKeys := make([]string, *n)
	inputs     := make([]int, *n)
	for i := range *n {
		inputs[i] = i + 1
		inputKeys[i]  = fmt.Sprintf("%s/input_%d.txt", prefix, i)
		outputKeys[i] = fmt.Sprintf("%s/output_%d.txt", prefix, i)

		body := strconv.Itoa(inputs[i])
		if err := s3Cli.Upload(ctx, inputKeys[i], strings.NewReader(body)); err != nil {
			log.Fatalf("upload input %d: %v", i, err)
		}
	}
	fmt.Printf("uploaded %d input file(s)\n", *n)

	// ── 3. Отправка задач мастеру ─────────────────────────────────────────────
	conn, err := grpc.NewClient(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc connect: %v", err)
	}
	defer conn.Close()

	masterCli := masterpb.NewMasterServiceClient(conn)

	taskIDs := make([]string, *n)
	for i := range *n {
		resp, err := masterCli.SubmitTask(ctx, &masterpb.SubmitTaskRequest{
			Type:       "python",
			Script:     scriptKey,
			InputFile:  inputKeys[i],
			OutputFile: outputKeys[i],
			Meta:       map[string]string{"scenario_index": strconv.Itoa(i)},
		})
		if err != nil {
			log.Fatalf("submit task %d: %v", i, err)
		}
		taskIDs[i] = resp.TaskId
		fmt.Printf("  submitted task[%d] → id=%s  input=%d\n", i, resp.TaskId, inputs[i])
	}

	// ── 4. Ожидание завершения через GetTaskStatus ────────────────────────────
	fmt.Printf("\nwaiting for results (timeout %s)…\n", *timeout)

	type result struct {
		done   bool
		errMsg string // пусто — успех
	}
	results  := make([]result, *n)
	deadline := time.Now().Add(*timeout)

	for time.Now().Before(deadline) {
		allDone := true
		for i := range *n {
			if results[i].done {
				continue
			}
			allDone = false

			statusResp, err := masterCli.GetTaskStatus(ctx, &masterpb.GetTaskStatusRequest{
				TaskId: taskIDs[i],
			})
			if err != nil {
				continue
			}

			switch statusResp.Status {
			case masterpb.TaskStatus_TASK_STATUS_SUCCESS:
				results[i].done = true
			case masterpb.TaskStatus_TASK_STATUS_FAIL:
				results[i].done = true
				results[i].errMsg = fmt.Sprintf("worker error: %s", statusResp.Error)
			}
		}
		if allDone {
			break
		}
		time.Sleep(time.Second)
	}

	// ── 5. Проверка результатов из S3 для успешных задач ─────────────────────
	failed := make([]string, *n)
	for i := range *n {
		if !results[i].done || results[i].errMsg != "" {
			failed[i] = results[i].errMsg
			continue
		}
		content, err := readS3File(ctx, s3Cli, outputKeys[i])
		if err != nil {
			failed[i] = fmt.Sprintf("s3 read: %v", err)
			continue
		}
		got, parseErr := strconv.Atoi(strings.TrimSpace(content))
		want := inputs[i] * inputs[i]
		if parseErr != nil {
			failed[i] = fmt.Sprintf("bad output %q: %v", content, parseErr)
		} else if got != want {
			failed[i] = fmt.Sprintf("got %d, want %d", got, want)
		}
	}

	// ── 6. Итоги ──────────────────────────────────────────────────────────────
	fmt.Println()
	passed, errCount, pending := 0, 0, 0
	for i := range *n {
		switch {
		case !results[i].done:
			fmt.Printf("  [TIMEOUT] task[%d] id=%s  input=%d\n", i, taskIDs[i], inputs[i])
			pending++
		case failed[i] != "":
			fmt.Printf("  [FAIL]    task[%d] id=%s  input=%d  %s\n", i, taskIDs[i], inputs[i], failed[i])
			errCount++
		default:
			fmt.Printf("  [OK]      task[%d] id=%s  %d² = %d\n", i, taskIDs[i], inputs[i], inputs[i]*inputs[i])
			passed++
		}
	}
	fmt.Printf("\n%d passed, %d failed, %d timed out  (total %d)\n", passed, errCount, pending, *n)
}

func readS3File(ctx context.Context, cli *s3.Client, key string) (string, error) {
	rc, err := cli.Download(ctx, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(b)), nil
}
