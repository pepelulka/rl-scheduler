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
// parallel_pair: если в конфиге выставлен флаг parallel_pair: true,
// каждое измерение запускает сразу ДВЕ одинаковые задачи на один воркер
// параллельно, чтобы оценить поведение метрик и времени при конкуренции.
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
	workerpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

// ── Config ────────────────────────────────────────────────────────────────────

// WorkerConfig описывает один воркер для профилирования.
//
// Два адреса нужны потому, что profile-инструмент запускается на хосте,
// а мастер — внутри Docker: адреса, по которым хост достучивается до воркеров
// (localhost:2001), не совпадают с адресами, известными мастеру (worker1:2001).
//
//   - Pinned  — адрес, передаваемый мастеру как pinned_worker.
//     Мастер использует его для отправки задачи (внутренний Docker-адрес).
//   - Metrics — адрес, по которому profile-инструмент сам вызывает GetMetrics
//     (внешний адрес, доступный с хоста). Если пусто — используется Pinned.
type WorkerConfig struct {
	Pinned  string `yaml:"pinned"`
	Metrics string `yaml:"metrics"`
}

// metricsAddr возвращает адрес для прямых вызовов GetMetrics.
func (w WorkerConfig) metricsAddr() string {
	if w.Metrics != "" {
		return w.Metrics
	}
	return w.Pinned
}

type Config struct {
	Master       string         `yaml:"master"`
	S3           s3pkg.Config   `yaml:"s3"`
	ResourcesDir string         `yaml:"resources_dir"`
	Workers      []WorkerConfig `yaml:"workers"`
	Tasks        []TaskConfig   `yaml:"tasks"`
	Timeout      string         `yaml:"timeout"`
	OutputCSV    string         `yaml:"output_csv"`
	// ParallelPair: если true — на каждый (воркер, задача, размер, повтор)
	// одновременно отправляются ДВЕ одинаковые задачи.
	ParallelPair bool `yaml:"parallel_pair"`
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
	// PairIndex: 0 = одиночный запуск, 1 = первая задача пары, 2 = вторая задача пары.
	PairIndex int
	// Метрики воркера, снятые однократно во время выполнения задачи.
	CpuUtilPct    float32
	RamUsageKib   float32
	RamMaxKib     float32
	CpuLimitCores float32
	MetricsErr    string
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

	// Создаём gRPC-клиенты для каждого воркера (для снятия метрик).
	// Ключ — Pinned-адрес (он же идентификатор воркера во всём коде).
	workerClients := make(map[string]workerpb.WorkerServiceClient, len(cfg.Workers))
	workerConns := make([]*grpc.ClientConn, 0, len(cfg.Workers))
	for _, w := range cfg.Workers {
		wconn, err := grpc.NewClient(w.metricsAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("grpc connect to worker %s: %v", w.metricsAddr(), err)
		}
		workerConns = append(workerConns, wconn)
		workerClients[w.Pinned] = workerpb.NewWorkerServiceClient(wconn)
	}
	defer func() {
		for _, c := range workerConns {
			c.Close()
		}
	}()

	prefix := fmt.Sprintf("profile/%d", time.Now().UnixMilli())
	scriptKeys, err := uploadScripts(ctx, cfg.Tasks, cfg.ResourcesDir, prefix, s3cli, cfg.S3.Bucket)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("uploaded %d script(s) → s3://%s/%s/\n\n", len(scriptKeys), cfg.S3.Bucket, prefix)

	if cfg.ParallelPair {
		fmt.Println("mode: PARALLEL PAIR (две задачи одновременно на воркер)")
	}

	fmt.Printf("%-24s  %-12s  %-12s  %-4s  %-5s  %-12s  %-8s  %-10s  %s\n",
		"WORKER", "TASK TYPE", "SIZE", "REP", "PAIR", "DURATION", "CPU%", "RAM_MiB", "STATUS")
	fmt.Println(strings.Repeat("─", 100))

	var (
		mu           sync.Mutex
		printMu      sync.Mutex
		measurements []Measurement
		wg           sync.WaitGroup
	)

	for _, w := range cfg.Workers {
		wg.Add(1)
		go func(wc WorkerConfig) {
			defer wg.Done()
			pinnedAddr := wc.Pinned
			slug := strings.NewReplacer(":", "_", ".", "_").Replace(pinnedAddr)
			workerCli := workerClients[pinnedAddr]
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

						var ms []Measurement
						if cfg.ParallelPair {
							m1, m2 := runPairTasks(ctx, pinnedAddr, slug, tc.Type, size, rep,
								scriptKeys, prefix, s3cli, masterCli, workerCli)
							ms = []Measurement{m1, m2}
						} else {
							m := runSingleTask(ctx, pinnedAddr, slug, tc.Type, size, rep,
								scriptKeys, prefix, s3cli, masterCli, workerCli)
							ms = []Measurement{m}
						}

						printMu.Lock()
						for _, m := range ms {
							status := "OK"
							if m.Err != "" {
								status = "FAIL: " + m.Err
							}
							dur := "-"
							if m.Err == "" {
								dur = m.Duration.Round(time.Millisecond).String()
							}
							cpuStr := "-"
							ramStr := "-"
							if m.MetricsErr == "" {
								cpuStr = fmt.Sprintf("%.1f", m.CpuUtilPct)
								ramStr = fmt.Sprintf("%.1f", float64(m.RamUsageKib)/1024)
							}
							pairStr := "-"
							if m.PairIndex > 0 {
								pairStr = strconv.Itoa(m.PairIndex)
							}
							fmt.Printf("%-24s  %-12s  %-12d  %-4d  %-5s  %-12s  %-8s  %-10s  %s\n",
								pinnedAddr, m.TaskType, m.Size, m.Repeat, pairStr, dur, cpuStr, ramStr, status)
						}
						printMu.Unlock()

						mu.Lock()
						measurements = append(measurements, ms...)
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

// captureMetrics однократно снимает метрики с воркера.
// Возвращает snapshot и ошибку (если недоступен).
func captureMetrics(ctx context.Context, workerCli workerpb.WorkerServiceClient) (cpu, ramUsage, ramMax, cpuLimit float32, errStr string) {
	mctx, mcancel := context.WithTimeout(ctx, 3*time.Second)
	defer mcancel()
	resp, err := workerCli.GetMetrics(mctx, &workerpb.GetMetricsRequest{})
	if err != nil {
		return 0, 0, 0, 0, err.Error()
	}
	if resp.Metrics == nil {
		return 0, 0, 0, 0, "empty metrics response"
	}
	m := resp.Metrics
	return m.CpuUtilPct, m.RamUsageKib, m.RamMaxKib, m.CpuLimitCores, ""
}

func runSingleTask(
	ctx context.Context,
	worker, workerSlug, taskType string,
	size, rep int,
	scriptKeys map[string]string,
	prefix string,
	s3cli *s3pkg.Client,
	masterCli masterpb.MasterServiceClient,
	workerCli workerpb.WorkerServiceClient,
) Measurement {
	m := Measurement{Worker: worker, TaskType: taskType, Size: size, Repeat: rep, PairIndex: 0}

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
	metricsDone := false
	for {
		select {
		case <-ctx.Done():
			m.Err = "timeout"
			_ = s3cli.DeleteMany(context.Background(), []string{inputKey, outputKey})
			return m
		case <-ticker.C:
			// Снимаем метрики однократно на первом тике.
			if !metricsDone {
				metricsDone = true
				m.CpuUtilPct, m.RamUsageKib, m.RamMaxKib, m.CpuLimitCores, m.MetricsErr =
					captureMetrics(ctx, workerCli)
			}

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

// runPairTasks отправляет ДВЕ одинаковые задачи на один воркер одновременно.
// Метрики снимаются однократно ~100 мс после отправки обеих задач.
// Возвращает два измерения (PairIndex 1 и 2).
func runPairTasks(
	ctx context.Context,
	worker, workerSlug, taskType string,
	size, rep int,
	scriptKeys map[string]string,
	prefix string,
	s3cli *s3pkg.Client,
	masterCli masterpb.MasterServiceClient,
	workerCli workerpb.WorkerServiceClient,
) (Measurement, Measurement) {
	newM := func(idx int) Measurement {
		return Measurement{Worker: worker, TaskType: taskType, Size: size, Repeat: rep, PairIndex: idx}
	}
	m1, m2 := newM(1), newM(2)

	// Загружаем два отдельных входных файла.
	key := func(idx int, suffix string) string {
		return fmt.Sprintf("%s/%s_%s_%d_%d_p%d_%s", prefix, workerSlug, taskType, size, rep, idx, suffix)
	}
	in1, out1 := key(1, "in.txt"), key(1, "out.txt")
	in2, out2 := key(2, "in.txt"), key(2, "out.txt")

	input := generateInput(taskType, size)
	var uploadWg sync.WaitGroup
	var uploadErr1, uploadErr2 error
	uploadWg.Add(2)
	go func() { defer uploadWg.Done(); uploadErr1 = s3cli.Upload(ctx, in1, strings.NewReader(input)) }()
	go func() { defer uploadWg.Done(); uploadErr2 = s3cli.Upload(ctx, in2, strings.NewReader(input)) }()
	uploadWg.Wait()
	if uploadErr1 != nil {
		m1.Err = fmt.Sprintf("upload input: %v", uploadErr1)
		m2.Err = "skipped (pair upload failed)"
		return m1, m2
	}
	if uploadErr2 != nil {
		_ = s3cli.DeleteMany(context.Background(), []string{in1})
		m2.Err = fmt.Sprintf("upload input: %v", uploadErr2)
		m1.Err = "skipped (pair upload failed)"
		return m1, m2
	}

	submitTask := func(inputKey, outputKey string) (*masterpb.SubmitTaskResponse, error) {
		return masterCli.SubmitTask(ctx, &masterpb.SubmitTaskRequest{
			Type:       "python",
			Script:     scriptKeys[taskType],
			InputFile:  inputKey,
			OutputFile: outputKey,
			Meta: map[string]string{
				"pinned_worker": worker,
				"bypass_queue":  "true",
			},
		})
	}

	start := time.Now()
	resp1, err1 := submitTask(in1, out1)
	resp2, err2 := submitTask(in2, out2)

	if err1 != nil {
		m1.Err = fmt.Sprintf("submit: %v", err1)
	}
	if err2 != nil {
		m2.Err = fmt.Sprintf("submit: %v", err2)
	}
	if m1.Err != "" || m2.Err != "" {
		_ = s3cli.DeleteMany(context.Background(), []string{in1, in2})
		return m1, m2
	}

	// Снимаем метрики однократно через 100 мс после отправки обеих задач,
	// пока обе (предположительно) ещё выполняются.
	var (
		metricOnce                sync.Once
		cpu, ramU, ramMax, cpuLim float32
		metricsErrStr             string
	)
	captureOnce := func() {
		metricOnce.Do(func() {
			cpu, ramU, ramMax, cpuLim, metricsErrStr = captureMetrics(ctx, workerCli)
		})
	}

	// Горутина отложенного снятия метрик.
	go func() {
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			captureOnce()
		case <-ctx.Done():
		}
	}()

	type result struct {
		done bool
		fail string
		dur  time.Duration
	}
	res1 := result{}
	res2 := result{}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !res1.done || !res2.done {
		select {
		case <-ctx.Done():
			m1.Err = "timeout"
			m2.Err = "timeout"
			_ = s3cli.DeleteMany(context.Background(), []string{in1, out1, in2, out2})
			return m1, m2
		case <-ticker.C:
			captureOnce()

			if !res1.done {
				st, err := masterCli.GetTaskStatus(ctx, &masterpb.GetTaskStatusRequest{TaskId: resp1.TaskId})
				if err == nil {
					switch st.Status {
					case masterpb.TaskStatus_TASK_STATUS_SUCCESS:
						res1 = result{done: true, dur: time.Since(start)}
					case masterpb.TaskStatus_TASK_STATUS_FAIL:
						res1 = result{done: true, fail: st.Error, dur: time.Since(start)}
					}
				}
			}
			if !res2.done {
				st, err := masterCli.GetTaskStatus(ctx, &masterpb.GetTaskStatusRequest{TaskId: resp2.TaskId})
				if err == nil {
					switch st.Status {
					case masterpb.TaskStatus_TASK_STATUS_SUCCESS:
						res2 = result{done: true, dur: time.Since(start)}
					case masterpb.TaskStatus_TASK_STATUS_FAIL:
						res2 = result{done: true, fail: st.Error, dur: time.Since(start)}
					}
				}
			}
		}
	}

	// Убеждаемся, что метрики точно сняты (могли не успеть за 100 мс).
	captureOnce()

	applyMetrics := func(m *Measurement) {
		m.CpuUtilPct = cpu
		m.RamUsageKib = ramU
		m.RamMaxKib = ramMax
		m.CpuLimitCores = cpuLim
		m.MetricsErr = metricsErrStr
	}

	m1.Duration = res1.dur
	m1.Err = res1.fail
	applyMetrics(&m1)

	m2.Duration = res2.dur
	m2.Err = res2.fail
	applyMetrics(&m2)

	_ = s3cli.DeleteMany(context.Background(), []string{in1, out1, in2, out2})
	return m1, m2
}

// ── output ────────────────────────────────────────────────────────────────────

func printSummary(measurements []Measurement) {
	type key struct {
		Worker   string
		TaskType string
		Size     int
		Pair     int // 0=solo, 1/2=pair index
	}
	type agg struct {
		durations []time.Duration
		cpus      []float32
		rams      []float32
		errors    int
	}
	grouped := map[key]*agg{}
	for _, m := range measurements {
		k := key{m.Worker, m.TaskType, m.Size, m.PairIndex}
		if grouped[k] == nil {
			grouped[k] = &agg{}
		}
		if m.Err != "" {
			grouped[k].errors++
		} else {
			grouped[k].durations = append(grouped[k].durations, m.Duration)
			if m.MetricsErr == "" {
				grouped[k].cpus = append(grouped[k].cpus, m.CpuUtilPct)
				grouped[k].rams = append(grouped[k].rams, m.RamUsageKib)
			}
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
		if keys[i].Size != keys[j].Size {
			return keys[i].Size < keys[j].Size
		}
		return keys[i].Pair < keys[j].Pair
	})

	fmt.Println(strings.Repeat("─", 100))
	fmt.Println("SUMMARY  (wall time: SubmitTask → SUCCESS)")
	fmt.Println(strings.Repeat("─", 100))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "WORKER\tTASK TYPE\tSIZE\tPAIR\tOK\tERR\tMIN\tAVG\tMAX\tCPU%_avg\tRAM_MiB_avg")
	sep := strings.Repeat("─", 10)
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		sep, sep, sep, "────", "──", "───", sep, sep, sep, "────────", "───────────")

	for _, k := range keys {
		a := grouped[k]
		ok := len(a.durations)
		pairStr := "-"
		if k.Pair > 0 {
			pairStr = strconv.Itoa(k.Pair)
		}
		if ok == 0 {
			fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%d\t%d\t─\t─\t─\t─\t─\n",
				k.Worker, k.TaskType, k.Size, pairStr, ok, a.errors)
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

		cpuAvgStr := "─"
		ramAvgStr := "─"
		if len(a.cpus) > 0 {
			var sumCPU float32
			for _, c := range a.cpus {
				sumCPU += c
			}
			cpuAvgStr = fmt.Sprintf("%.1f", sumCPU/float32(len(a.cpus)))
		}
		if len(a.rams) > 0 {
			var sumRAM float32
			for _, r := range a.rams {
				sumRAM += r
			}
			ramAvgStr = fmt.Sprintf("%.1f", float64(sumRAM/float32(len(a.rams)))/1024)
		}

		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n",
			k.Worker, k.TaskType, k.Size, pairStr, ok, a.errors,
			fmtDur(minD), fmtDur(avg), fmtDur(maxD), cpuAvgStr, ramAvgStr)
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

	_ = cw.Write([]string{
		"worker", "task_type", "size", "repeat", "pair_index",
		"duration_ms", "error",
		"cpu_util_pct", "ram_usage_kib", "ram_max_kib", "cpu_limit_cores", "metrics_error",
	})
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
			strconv.Itoa(m.PairIndex),
			durMs,
			m.Err,
			fmt.Sprintf("%.2f", m.CpuUtilPct),
			fmt.Sprintf("%.2f", m.RamUsageKib),
			fmt.Sprintf("%.2f", m.RamMaxKib),
			fmt.Sprintf("%.2f", m.CpuLimitCores),
			m.MetricsErr,
		})
	}
	return nil
}
