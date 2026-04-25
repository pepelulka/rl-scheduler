// Profile — изолированный бенчмарк кластера.
//
// Для каждой комбинации (воркер, тип задачи, размер входа) запускает
// несколько повторений по одному (один воркер — одна задача в момент
// времени) и замеряет wall-time от SubmitTask до SUCCESS.
// Разные воркеры прогоняются параллельно, внутри одного воркера —
// строго последовательно. Используется pinned_worker + bypass_queue,
// чтобы задача шла напрямую к нужному воркеру, минуя диспетчерскую
// очередь.
//
// Usage:
//
//	go run ./cmd/profile --config local/profile.yaml
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
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
	Workers      []string     `yaml:"workers"`
	Tasks        []TaskConfig `yaml:"tasks"`
	Timeout      string       `yaml:"timeout"`
	OutputCSV    string       `yaml:"output_csv"`
}

// TaskConfig описывает один тип задачи для бенчмарка.
// Type — имя Python-скрипта без расширения (напр. "monte_carlo" → monte_carlo.py).
// Sizes — список значений, передаваемых скрипту на stdin:
//   - для "sort": количество случайных строк
//   - для остальных: число, напрямую передаваемое скрипту
type TaskConfig struct {
	Type    string `yaml:"type"`
	Sizes   []int  `yaml:"sizes"`
	Repeats int    `yaml:"repeats"`
}

// ── Input generation ──────────────────────────────────────────────────────────

// generateInput строит stdin-payload для задачи.
// Для "sort": size случайных строк со случайными словами.
// Для всех остальных: просто число size.
func generateInput(taskType string, size int) string {
	if taskType != "sort" {
		return strconv.Itoa(size)
	}
	rng := rand.New(rand.NewSource(int64(size)))
	const alpha = "abcdefghijklmnopqrstuvwxyz"
	var sb strings.Builder
	for i := 0; i < size; i++ {
		wordLen := 4 + rng.Intn(8)
		for j := 0; j < wordLen; j++ {
			sb.WriteByte(alpha[rng.Intn(len(alpha))])
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

// ── Measurement ───────────────────────────────────────────────────────────────

type Measurement struct {
	Worker   string
	TaskType string
	Size     int
	Repeat   int
	Duration time.Duration
	Err      string
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	configPath := flag.String("config", "profile.yaml", "path to YAML config file")
	flag.Parse()

	raw, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("read config %q: %v", *configPath, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	if cfg.ResourcesDir == "" {
		log.Fatal("resources_dir is required in config")
	}
	if len(cfg.Workers) == 0 {
		log.Fatal("no workers configured")
	}
	if len(cfg.Tasks) == 0 {
		log.Fatal("no tasks configured")
	}

	timeout := 60 * time.Minute
	if cfg.Timeout != "" {
		timeout, err = time.ParseDuration(cfg.Timeout)
		if err != nil {
			log.Fatalf("parse timeout %q: %v", cfg.Timeout, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

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

	prefix := fmt.Sprintf("profile/%d", time.Now().UnixMilli())
	scriptKeys, err := uploadScripts(ctx, cfg.Tasks, cfg.ResourcesDir, prefix, s3cli, cfg.S3.Bucket)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("uploaded %d script(s) → s3://%s/%s/\n\n", len(scriptKeys), cfg.S3.Bucket, prefix)

	fmt.Printf("%-24s  %-12s  %-12s  %-4s  %-12s  %s\n",
		"WORKER", "TASK TYPE", "SIZE", "REP", "DURATION", "STATUS")
	fmt.Println(strings.Repeat("─", 80))

	var (
		mu           sync.Mutex
		printMu      sync.Mutex
		measurements []Measurement
		wg           sync.WaitGroup
	)

	for _, w := range cfg.Workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			slug := strings.NewReplacer(":", "_", ".", "_").Replace(worker)
			for _, tc := range cfg.Tasks {
				repeats := tc.Repeats
				if repeats < 1 {
					repeats = 1
				}
				for _, size := range tc.Sizes {
					for rep := 1; rep <= repeats; rep++ {
						if ctx.Err() != nil {
							return
						}
						m := runSingleTask(ctx, worker, slug, tc.Type, size, rep,
							scriptKeys, prefix, s3cli, masterCli)

						printMu.Lock()
						status := "OK"
						if m.Err != "" {
							status = "FAIL: " + m.Err
						}
						dur := "-"
						if m.Err == "" {
							dur = m.Duration.Round(time.Millisecond).String()
						}
						fmt.Printf("%-24s  %-12s  %-12d  %-4d  %-12s  %s\n",
							worker, m.TaskType, m.Size, m.Repeat, dur, status)
						printMu.Unlock()

						mu.Lock()
						measurements = append(measurements, m)
						mu.Unlock()
					}
				}
			}
		}(w)
	}
	wg.Wait()

	fmt.Println()
	printSummary(measurements)

	if cfg.OutputCSV != "" {
		if err := writeCSV(cfg.OutputCSV, measurements); err != nil {
			fmt.Fprintf(os.Stderr, "write CSV: %v\n", err)
		} else {
			fmt.Printf("results saved to %s\n", cfg.OutputCSV)
		}
	}

	fmt.Printf("\ncleaning up s3 scripts …\n")
	var scriptList []string
	for _, k := range scriptKeys {
		scriptList = append(scriptList, k)
	}
	cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanCancel()
	if err := s3cli.DeleteMany(cleanCtx, scriptList); err != nil {
		fmt.Fprintf(os.Stderr, "cleanup warning: %v\n", err)
	} else {
		fmt.Printf("deleted %d script object(s)\n", len(scriptList))
	}
}

// ── core ──────────────────────────────────────────────────────────────────────

func uploadScripts(ctx context.Context, tasks []TaskConfig, resourcesDir, prefix string, s3cli *s3pkg.Client, bucket string) (map[string]string, error) {
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

func runSingleTask(
	ctx context.Context,
	worker, workerSlug, taskType string,
	size, rep int,
	scriptKeys map[string]string,
	prefix string,
	s3cli *s3pkg.Client,
	masterCli masterpb.MasterServiceClient,
) Measurement {
	m := Measurement{Worker: worker, TaskType: taskType, Size: size, Repeat: rep}

	inputKey := fmt.Sprintf("%s/%s_%s_%d_%d_in.txt", prefix, workerSlug, taskType, size, rep)
	outputKey := fmt.Sprintf("%s/%s_%s_%d_%d_out.txt", prefix, workerSlug, taskType, size, rep)

	if err := s3cli.Upload(ctx, inputKey, strings.NewReader(generateInput(taskType, size))); err != nil {
		m.Err = fmt.Sprintf("upload input: %v", err)
		return m
	}

	start := time.Now()
	resp, err := masterCli.SubmitTask(ctx, &masterpb.SubmitTaskRequest{
		Type:       "python",
		Script:     scriptKeys[taskType],
		InputFile:  inputKey,
		OutputFile: outputKey,
		Meta: map[string]string{
			"pinned_worker": worker,
			"bypass_queue":  "true",
		},
	})
	if err != nil {
		m.Err = fmt.Sprintf("submit: %v", err)
		_ = s3cli.DeleteMany(context.Background(), []string{inputKey})
		return m
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.Err = "timeout"
			_ = s3cli.DeleteMany(context.Background(), []string{inputKey, outputKey})
			return m
		case <-ticker.C:
			st, err := masterCli.GetTaskStatus(ctx, &masterpb.GetTaskStatusRequest{TaskId: resp.TaskId})
			if err != nil {
				continue
			}
			switch st.Status {
			case masterpb.TaskStatus_TASK_STATUS_SUCCESS:
				m.Duration = time.Since(start)
				_ = s3cli.DeleteMany(context.Background(), []string{inputKey, outputKey})
				return m
			case masterpb.TaskStatus_TASK_STATUS_FAIL:
				m.Err = st.Error
				m.Duration = time.Since(start)
				_ = s3cli.DeleteMany(context.Background(), []string{inputKey, outputKey})
				return m
			}
		}
	}
}

// ── output ────────────────────────────────────────────────────────────────────

func printSummary(measurements []Measurement) {
	type key struct {
		Worker   string
		TaskType string
		Size     int
	}
	type agg struct {
		durations []time.Duration
		errors    int
	}
	grouped := map[key]*agg{}
	for _, m := range measurements {
		k := key{m.Worker, m.TaskType, m.Size}
		if grouped[k] == nil {
			grouped[k] = &agg{}
		}
		if m.Err != "" {
			grouped[k].errors++
		} else {
			grouped[k].durations = append(grouped[k].durations, m.Duration)
		}
	}

	keys := make([]key, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Worker != keys[j].Worker {
			return keys[i].Worker < keys[j].Worker
		}
		if keys[i].TaskType != keys[j].TaskType {
			return keys[i].TaskType < keys[j].TaskType
		}
		return keys[i].Size < keys[j].Size
	})

	fmt.Println(strings.Repeat("─", 80))
	fmt.Println("SUMMARY  (wall time: SubmitTask → SUCCESS)")
	fmt.Println(strings.Repeat("─", 80))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "WORKER\tTASK TYPE\tSIZE\tOK\tERR\tMIN\tAVG\tMAX")
	sep := strings.Repeat("─", 10)
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", sep, sep, sep, "──", "───", sep, sep, sep)

	for _, k := range keys {
		a := grouped[k]
		ok := len(a.durations)
		if ok == 0 {
			fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t─\t─\t─\n",
				k.Worker, k.TaskType, k.Size, ok, a.errors)
			continue
		}
		var total time.Duration
		minD, maxD := a.durations[0], a.durations[0]
		for _, d := range a.durations {
			total += d
			if d < minD {
				minD = d
			}
			if d > maxD {
				maxD = d
			}
		}
		avg := total / time.Duration(ok)
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\n",
			k.Worker, k.TaskType, k.Size, ok, a.errors,
			fmtDur(minD), fmtDur(avg), fmtDur(maxD))
	}
	w.Flush()
}

func fmtDur(d time.Duration) string {
	ms := d.Milliseconds()
	if ms < 10_000 {
		return fmt.Sprintf("%dms", ms)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func writeCSV(path string, measurements []Measurement) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	cw := csv.NewWriter(f)
	defer cw.Flush()

	_ = cw.Write([]string{"worker", "task_type", "size", "repeat", "duration_ms", "error"})
	for _, m := range measurements {
		durMs := ""
		if m.Err == "" {
			durMs = strconv.FormatInt(m.Duration.Milliseconds(), 10)
		}
		_ = cw.Write([]string{
			m.Worker,
			m.TaskType,
			strconv.Itoa(m.Size),
			strconv.Itoa(m.Repeat),
			durMs,
			m.Err,
		})
	}
	return nil
}
