// Loader — симулятор реальной нагрузки на кластер.
//
// Бесконечно отправляет задачи на мастер, чередуя фазы
// BURST (высокая частота), NORMAL и QUIET (затишье).
// Следит за тем, чтобы число задач «в полёте» не превышало
// max_queue_size (по умолчанию 10). Прерывается по Ctrl+C;
// после сигнала дожидается завершения всех отправленных задач.
//
// Usage:
//
//	go run ./cmd/loader --config local/loader.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	s3pkg "github.com/pepelulka/rl-scheduler/internal/s3"
	masterpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/master"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

// ── Config ────────────────────────────────────────────────────────────────────

type Config struct {
	Master       string       `yaml:"master"`
	S3           s3pkg.Config `yaml:"s3"`
	ResourcesDir string       `yaml:"resources_dir"`
	MaxQueueSize int          `yaml:"max_queue_size"` // max in-flight tasks; default 10
	Tasks        []TaskConfig `yaml:"tasks"`
}

type TaskConfig struct {
	Type  string `yaml:"type"`
	Sizes []int  `yaml:"sizes"`
}

// ── Phase ─────────────────────────────────────────────────────────────────────

type phaseKind int

const (
	phaseBurst  phaseKind = iota // высокая частота отправки
	phaseNormal                  // обычный темп
	phaseQuiet                   // затишье
)

func (p phaseKind) String() string {
	return [...]string{"BURST", "NORMAL", "QUIET"}[p]
}

func submissionDelay(rng *rand.Rand, p phaseKind) time.Duration {
	switch p {
	case phaseBurst:
		return time.Duration(100+rng.Intn(300)) * time.Millisecond // 0.1–0.4 s
	case phaseNormal:
		return time.Duration(600+rng.Intn(1400)) * time.Millisecond // 0.6–2.0 s
	case phaseQuiet:
		return time.Duration(3000+rng.Intn(4000)) * time.Millisecond // 3–7 s
	default:
		return time.Second
	}
}

func phaseDuration(rng *rand.Rand, p phaseKind) time.Duration {
	switch p {
	case phaseBurst:
		return time.Duration(20+rng.Intn(40)) * time.Second // 20–60 s
	case phaseNormal:
		return time.Duration(30+rng.Intn(60)) * time.Second // 30–90 s
	case phaseQuiet:
		return time.Duration(15+rng.Intn(30)) * time.Second // 15–45 s
	default:
		return 30 * time.Second
	}
}

// nextPhase выбирает следующую фазу со взвешенными переходами.
func nextPhase(rng *rand.Rand, cur phaseKind) phaseKind {
	switch cur {
	case phaseBurst:
		if rng.Intn(4) == 0 {
			return phaseQuiet
		}
		return phaseNormal
	case phaseNormal:
		r := rng.Intn(10)
		if r < 3 {
			return phaseBurst
		}
		if r < 5 {
			return phaseQuiet
		}
		return phaseNormal
	case phaseQuiet:
		if rng.Intn(3) == 0 {
			return phaseBurst
		}
		return phaseNormal
	}
	return phaseNormal
}

// ── Input generation ──────────────────────────────────────────────────────────

func generateInput(taskType string, size int) string {
	if taskType != "sort" {
		return strconv.Itoa(size)
	}
	rng := rand.New(rand.NewSource(int64(size) ^ time.Now().UnixNano()))
	const alpha = "abcdefghijklmnopqrstuvwxyz"
	var sb strings.Builder
	for i := 0; i < size; i++ {
		n := 4 + rng.Intn(8)
		for j := 0; j < n; j++ {
			sb.WriteByte(alpha[rng.Intn(len(alpha))])
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	configPath := flag.String("config", "local/loader.yaml", "path to YAML config file")
	flag.Parse()

	raw, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("read config %q: %v", *configPath, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 10
	}
	if cfg.ResourcesDir == "" {
		log.Fatal("resources_dir is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n[loader] stopping — waiting for in-flight tasks…")
		cancel()
	}()

	s3cli, err := s3pkg.NewClient(ctx, cfg.S3)
	if err != nil {
		log.Fatalf("s3 init: %v", err)
	}

	conn, err := grpc.NewClient(cfg.Master, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc connect to %s: %v", cfg.Master, err)
	}
	defer conn.Close()
	masterCli := masterpb.NewMasterServiceClient(conn)

	prefix := fmt.Sprintf("loader/%d", time.Now().UnixMilli())
	scriptKeys, err := uploadScripts(ctx, cfg.Tasks, cfg.ResourcesDir, prefix, s3cli)
	if err != nil {
		log.Fatalf("upload scripts: %v", err)
	}
	fmt.Printf("[loader] uploaded %d script(s) → s3://%s/%s/\n", len(scriptKeys), cfg.S3.Bucket, prefix)

	// Flat pool of (type, size) pairs for random picks.
	type taskSpec struct {
		taskType string
		size     int
	}
	var pool []taskSpec
	for _, tc := range cfg.Tasks {
		for _, sz := range tc.Sizes {
			pool = append(pool, taskSpec{tc.Type, sz})
		}
	}
	if len(pool) == 0 {
		log.Fatal("no tasks configured")
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	sem := make(chan struct{}, cfg.MaxQueueSize)

	var (
		totalSubmit atomic.Int64
		totalOK     atomic.Int64
		totalFail   atomic.Int64
		inFlight    atomic.Int32

		phaseMu      sync.Mutex
		currentPhase = phaseNormal
		phaseEnd     = time.Now().Add(phaseDuration(rng, phaseNormal))
	)

	var counter atomic.Int64
	var wg sync.WaitGroup

	// Periodic stats printer.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				phaseMu.Lock()
				ph := currentPhase
				phaseMu.Unlock()
				fmt.Printf("[stats] phase=%-6s  submitted=%d  in-flight=%d  ok=%d  fail=%d\n",
					ph, totalSubmit.Load(), inFlight.Load(), totalOK.Load(), totalFail.Load())
			}
		}
	}()

	fmt.Printf("[loader] master=%s  max_queue=%d  task_types=%d  — Ctrl+C to stop\n",
		cfg.Master, cfg.MaxQueueSize, len(pool))

	phaseMu.Lock()
	fmt.Printf("[phase] → %s (for ~%s)\n", currentPhase, phaseEnd.Sub(time.Now()).Round(time.Second))
	phaseMu.Unlock()

	for {
		// Check and update phase.
		phaseMu.Lock()
		if time.Now().After(phaseEnd) {
			currentPhase = nextPhase(rng, currentPhase)
			dur := phaseDuration(rng, currentPhase)
			phaseEnd = time.Now().Add(dur)
			fmt.Printf("[phase] → %s (for ~%s)\n", currentPhase, dur.Round(time.Second))
		}
		ph := currentPhase
		phaseMu.Unlock()

		// Acquire a semaphore slot; bail if ctx is cancelled.
		select {
		case <-ctx.Done():
			wg.Wait()
			printFinalStats(totalSubmit.Load(), totalOK.Load(), totalFail.Load())
			cleanupScripts(s3cli, scriptKeys)
			return
		case sem <- struct{}{}:
			// slot acquired
		case <-time.After(300 * time.Millisecond):
			// queue full, loop back
			continue
		}

		spec := pool[rng.Intn(len(pool))]
		n := counter.Add(1)
		inputKey := fmt.Sprintf("%s/%d_in.txt", prefix, n)
		outputKey := fmt.Sprintf("%s/%d_out.txt", prefix, n)

		totalSubmit.Add(1)
		inFlight.Add(1)

		wg.Add(1)
		go func(taskType string, size int, inKey, outKey string) {
			defer wg.Done()
			defer func() {
				<-sem
				inFlight.Add(-1)
			}()

			bgCtx := context.Background()

			// Upload input.
			if err := s3cli.Upload(ctx, inKey, strings.NewReader(generateInput(taskType, size))); err != nil {
				if ctx.Err() == nil {
					log.Printf("[task] upload input error (%s size=%d): %v", taskType, size, err)
				}
				totalFail.Add(1)
				return
			}

			// Submit task.
			resp, err := masterCli.SubmitTask(ctx, &masterpb.SubmitTaskRequest{
				Type:       "python",
				Script:     scriptKeys[taskType],
				InputFile:  inKey,
				OutputFile: outKey,
			})
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("[task] submit error (%s size=%d): %v", taskType, size, err)
				}
				totalFail.Add(1)
				s3cli.DeleteMany(bgCtx, []string{inKey}) //nolint:errcheck
				return
			}

			// Poll for terminal status.
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					s3cli.DeleteMany(bgCtx, []string{inKey, outKey}) //nolint:errcheck
					return
				case <-ticker.C:
					st, err := masterCli.GetTaskStatus(ctx, &masterpb.GetTaskStatusRequest{TaskId: resp.TaskId})
					if err != nil {
						continue
					}
					switch st.Status {
					case masterpb.TaskStatus_TASK_STATUS_SUCCESS:
						totalOK.Add(1)
						s3cli.DeleteMany(bgCtx, []string{inKey, outKey}) //nolint:errcheck
						return
					case masterpb.TaskStatus_TASK_STATUS_FAIL:
						totalFail.Add(1)
						log.Printf("[task] failed %s (size=%d): %s", taskType, size, st.Error)
						s3cli.DeleteMany(bgCtx, []string{inKey, outKey}) //nolint:errcheck
						return
					}
				}
			}
		}(spec.taskType, spec.size, inputKey, outputKey)

		// Phase-dependent delay before next submission.
		delay := submissionDelay(rng, ph)
		select {
		case <-ctx.Done():
			wg.Wait()
			printFinalStats(totalSubmit.Load(), totalOK.Load(), totalFail.Load())
			cleanupScripts(s3cli, scriptKeys)
			return
		case <-time.After(delay):
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func uploadScripts(ctx context.Context, tasks []TaskConfig, resourcesDir, prefix string, s3cli *s3pkg.Client) (map[string]string, error) {
	seen := map[string]bool{}
	keys := map[string]string{}
	for _, tc := range tasks {
		if seen[tc.Type] {
			continue
		}
		seen[tc.Type] = true
		scriptPath := filepath.Join(resourcesDir, tc.Type+".py")
		f, err := os.Open(scriptPath)
		if err != nil {
			return nil, fmt.Errorf("open script %s: %w", scriptPath, err)
		}
		key := fmt.Sprintf("%s/script_%s.py", prefix, tc.Type)
		err = s3cli.Upload(ctx, key, f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("upload script %s: %w", tc.Type, err)
		}
		keys[tc.Type] = key
	}
	return keys, nil
}

func cleanupScripts(s3cli *s3pkg.Client, scriptKeys map[string]string) {
	keys := make([]string, 0, len(scriptKeys))
	for _, k := range scriptKeys {
		keys = append(keys, k)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s3cli.DeleteMany(ctx, keys); err != nil {
		fmt.Fprintf(os.Stderr, "[loader] script cleanup warning: %v\n", err)
	} else {
		fmt.Printf("[loader] deleted %d script(s) from S3\n", len(keys))
	}
}

func printFinalStats(submitted, ok, fail int64) {
	fmt.Printf("[loader] finished: submitted=%d  ok=%d  fail=%d\n", submitted, ok, fail)
}
