// Benchmark — сравнительное тестирование планировщиков.
//
// Запускает серию сценариев по очереди. Каждый сценарий — пакет задач,
// которые отправляются на мастер разом (без задержек). Измеряется:
//   - суммарное wall-time от первого SubmitTask до последнего SUCCESS/FAIL
//   - время выполнения каждой задачи индивидуально
//   - пропускная способность (задач/мин)
//
// Сценарии задаются в YAML-конфиге. Пример:
//
//	scenarios:
//	  - name: "monte_carlo_only"
//	    tasks:
//	      - type: monte_carlo
//	        size: 4000000
//	        count: 6
//
// Usage:
//
//	go run ./cmd/benchmark --config local/benchmark.yaml
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
	Master           string        `yaml:"master"`
	S3               s3pkg.Config  `yaml:"s3"`
	ResourcesDir     string        `yaml:"resources_dir"`
	Timeout          string        `yaml:"timeout"`            // per-scenario; default "15m"
	CooldownBetween  string        `yaml:"cooldown_between"`   // pause between scenarios; default "5s"
	OutputCSV        string        `yaml:"output_csv"`
	Scenarios        []ScenarioCfg `yaml:"scenarios"`
}

type ScenarioCfg struct {
	Name  string         `yaml:"name"`
	Tasks []TaskGroupCfg `yaml:"tasks"`
}

// TaskGroupCfg описывает группу одинаковых задач в сценарии.
type TaskGroupCfg struct {
	Type  string `yaml:"type"`
	Size  int    `yaml:"size"`
	Count int    `yaml:"count"` // сколько задач этого типа/размера отправить
}

// ── Result types ──────────────────────────────────────────────────────────────

type taskResult struct {
	taskType string
	size     int
	duration time.Duration // от SubmitTask до SUCCESS/FAIL
	err      string        // пусто = успех
}

type ScenarioResult struct {
	Name      string
	Total     int
	OK        int
	Fail      int
	TimedOut  int
	Wall      time.Duration // от первого submit до последнего завершения
	TaskTimes []taskResult
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
	configPath := flag.String("config", "local/benchmark.yaml", "path to YAML config file")
	flag.Parse()

	raw, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("read config %q: %v", *configPath, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	if len(cfg.Scenarios) == 0 {
		log.Fatal("no scenarios configured")
	}
	if cfg.ResourcesDir == "" {
		log.Fatal("resources_dir is required")
	}

	perScenarioTimeout := 15 * time.Minute
	if cfg.Timeout != "" {
		if d, err := time.ParseDuration(cfg.Timeout); err == nil {
			perScenarioTimeout = d
		}
	}
	cooldown := 5 * time.Second
	if cfg.CooldownBetween != "" {
		if d, err := time.ParseDuration(cfg.CooldownBetween); err == nil {
			cooldown = d
		}
	}

	ctx := context.Background()

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

	// Собираем все уникальные типы задач из всех сценариев и загружаем скрипты.
	prefix := fmt.Sprintf("benchmark/%d", time.Now().UnixMilli())
	scriptKeys, err := uploadScripts(ctx, cfg.Scenarios, cfg.ResourcesDir, prefix, s3cli)
	if err != nil {
		log.Fatalf("upload scripts: %v", err)
	}
	fmt.Printf("Uploaded %d script(s) → s3://%s/%s/\n\n", len(scriptKeys), cfg.S3.Bucket, prefix)

	// ── Run scenarios ─────────────────────────────────────────────────────────

	allResults := make([]ScenarioResult, 0, len(cfg.Scenarios))

	for si, sc := range cfg.Scenarios {
		if si > 0 {
			fmt.Printf("  cooling down %s…\n\n", cooldown)
			time.Sleep(cooldown)
		}

		fmt.Printf("══ Scenario %d/%d: %q ══\n", si+1, len(cfg.Scenarios), sc.Name)
		totalTasks := 0
		for _, tg := range sc.Tasks {
			totalTasks += tg.Count
		}
		fmt.Printf("   %d task(s) total\n", totalTasks)

		scCtx, scCancel := context.WithTimeout(ctx, perScenarioTimeout)
		res := runScenario(scCtx, sc, prefix, si, scriptKeys, s3cli, masterCli)
		scCancel()

		allResults = append(allResults, res)
		printScenarioResult(res)
		fmt.Println()
	}

	// ── Cleanup scripts ───────────────────────────────────────────────────────
	fmt.Print("Cleaning up scripts from S3… ")
	keys := make([]string, 0, len(scriptKeys))
	for _, k := range scriptKeys {
		keys = append(keys, k)
	}
	cleanCtx, cleanCancel := context.WithTimeout(ctx, 30*time.Second)
	defer cleanCancel()
	if err := s3cli.DeleteMany(cleanCtx, keys); err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	} else {
		fmt.Printf("deleted %d object(s)\n", len(keys))
	}

	// ── Summary table ─────────────────────────────────────────────────────────
	fmt.Println()
	printSummary(allResults)

	if cfg.OutputCSV != "" {
		if err := writeCSV(cfg.OutputCSV, allResults); err != nil {
			fmt.Fprintf(os.Stderr, "write CSV: %v\n", err)
		} else {
			fmt.Printf("Results saved to %s\n", cfg.OutputCSV)
		}
	}
}

// ── scenario runner ───────────────────────────────────────────────────────────

func runScenario(
	ctx context.Context,
	sc ScenarioCfg,
	prefix string,
	scenarioIdx int,
	scriptKeys map[string]string,
	s3cli *s3pkg.Client,
	masterCli masterpb.MasterServiceClient,
) ScenarioResult {
	res := ScenarioResult{Name: sc.Name}

	// Expand task groups into individual task specs.
	type taskSpec struct {
		taskType string
		size     int
		idx      int // global index within this scenario
	}
	var tasks []taskSpec
	for _, tg := range sc.Tasks {
		for i := 0; i < tg.Count; i++ {
			tasks = append(tasks, taskSpec{tg.Type, tg.Size, len(tasks)})
		}
	}
	res.Total = len(tasks)

	// Upload all input files in parallel.
	type uploadedTask struct {
		spec      taskSpec
		inputKey  string
		outputKey string
		uploadErr error
	}
	uploaded := make([]uploadedTask, len(tasks))

	var uploadWg sync.WaitGroup
	for i, spec := range tasks {
		i, spec := i, spec
		inputKey := fmt.Sprintf("%s/s%d_t%d_%s_%d_in.txt", prefix, scenarioIdx, spec.idx, spec.taskType, spec.size)
		outputKey := fmt.Sprintf("%s/s%d_t%d_%s_%d_out.txt", prefix, scenarioIdx, spec.idx, spec.taskType, spec.size)
		uploaded[i] = uploadedTask{spec: spec, inputKey: inputKey, outputKey: outputKey}

		uploadWg.Add(1)
		go func(i int) {
			defer uploadWg.Done()
			input := generateInput(spec.taskType, spec.size)
			uploaded[i].uploadErr = s3cli.Upload(ctx, inputKey, strings.NewReader(input))
		}(i)
	}
	uploadWg.Wait()

	// Check upload errors.
	var failedUploads []string
	for i, u := range uploaded {
		if u.uploadErr != nil {
			failedUploads = append(failedUploads, fmt.Sprintf("task[%d]: %v", i, u.uploadErr))
		}
	}
	if len(failedUploads) > 0 {
		log.Printf("upload errors:\n%s", strings.Join(failedUploads, "\n"))
	}

	// Submit all tasks as fast as possible, record start time before first submit.
	type submitted struct {
		taskID    string
		inputKey  string
		outputKey string
		taskType  string
		size      int
		submitErr error
		submitAt  time.Time
	}
	subs := make([]submitted, len(tasks))

	wallStart := time.Now()
	for i, u := range uploaded {
		if u.uploadErr != nil {
			subs[i] = submitted{taskType: u.spec.taskType, size: u.spec.size, submitErr: u.uploadErr}
			res.Fail++
			continue
		}
		resp, err := masterCli.SubmitTask(ctx, &masterpb.SubmitTaskRequest{
			Type:       "python",
			Script:     scriptKeys[u.spec.taskType],
			InputFile:  u.inputKey,
			OutputFile: u.outputKey,
		})
		submitAt := time.Now()
		if err != nil {
			subs[i] = submitted{taskType: u.spec.taskType, size: u.spec.size, submitErr: err, submitAt: submitAt}
			res.Fail++
			log.Printf("submit task[%d] error: %v", i, err)
			continue
		}
		subs[i] = submitted{
			taskID: resp.TaskId, inputKey: u.inputKey, outputKey: u.outputKey,
			taskType: u.spec.taskType, size: u.spec.size, submitAt: submitAt,
		}
	}
	fmt.Printf("   submitted %d task(s) in %s, polling…\n", res.Total-res.Fail, time.Since(wallStart).Round(time.Millisecond))

	// Poll all pending tasks until done or context expires.
	type pollState struct {
		done bool
		tr   taskResult
	}
	states := make([]pollState, len(subs))
	for i, s := range subs {
		if s.submitErr != nil {
			states[i] = pollState{done: true, tr: taskResult{
				taskType: s.taskType, size: s.size, err: s.submitErr.Error(),
			}}
		}
	}

	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	pending := res.Total - res.Fail
	var lastDone time.Time

	for pending > 0 {
		select {
		case <-ctx.Done():
			// Mark remaining as timed out.
			for i, st := range states {
				if !st.done {
					states[i] = pollState{done: true, tr: taskResult{
						taskType: subs[i].taskType, size: subs[i].size, err: "timeout",
					}}
					res.TimedOut++
				}
			}
			pending = 0
			continue
		case <-ticker.C:
		}

		for i, st := range states {
			if st.done {
				continue
			}
			if subs[i].taskID == "" {
				continue
			}
			resp, err := masterCli.GetTaskStatus(ctx, &masterpb.GetTaskStatusRequest{TaskId: subs[i].taskID})
			if err != nil {
				continue
			}
			switch resp.Status {
			case masterpb.TaskStatus_TASK_STATUS_SUCCESS:
				dur := time.Since(subs[i].submitAt)
				states[i] = pollState{done: true, tr: taskResult{subs[i].taskType, subs[i].size, dur, ""}}
				res.OK++
				pending--
				lastDone = time.Now()
				// cleanup
				go s3cli.DeleteMany(context.Background(), []string{subs[i].inputKey, subs[i].outputKey}) //nolint:errcheck
			case masterpb.TaskStatus_TASK_STATUS_FAIL:
				dur := time.Since(subs[i].submitAt)
				states[i] = pollState{done: true, tr: taskResult{subs[i].taskType, subs[i].size, dur, resp.Error}}
				res.Fail++
				pending--
				lastDone = time.Now()
				go s3cli.DeleteMany(context.Background(), []string{subs[i].inputKey, subs[i].outputKey}) //nolint:errcheck
			}
		}
	}

	if lastDone.IsZero() {
		lastDone = time.Now()
	}
	res.Wall = lastDone.Sub(wallStart)

	for _, st := range states {
		res.TaskTimes = append(res.TaskTimes, st.tr)
	}
	return res
}

// ── output ────────────────────────────────────────────────────────────────────

func printScenarioResult(res ScenarioResult) {
	fmt.Printf("   wall time : %s\n", res.Wall.Round(time.Millisecond))
	fmt.Printf("   tasks     : total=%d  ok=%d  fail=%d  timeout=%d\n", res.Total, res.OK, res.Fail, res.TimedOut)

	// Per-type breakdown.
	type key struct{ taskType string; size int }
	type agg struct {
		ok   int
		fail int
		durs []time.Duration
	}
	grouped := map[key]*agg{}
	for _, tr := range res.TaskTimes {
		k := key{tr.taskType, tr.size}
		if grouped[k] == nil {
			grouped[k] = &agg{}
		}
		if tr.err != "" {
			grouped[k].fail++
		} else {
			grouped[k].ok++
			grouped[k].durs = append(grouped[k].durs, tr.duration)
		}
	}

	keys := make([]key, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].taskType != keys[j].taskType {
			return keys[i].taskType < keys[j].taskType
		}
		return keys[i].size < keys[j].size
	})

	if len(keys) > 0 {
		fmt.Printf("   %-16s  %-10s  %-4s  %-4s  %-10s  %-10s  %-10s\n",
			"TYPE", "SIZE", "OK", "FAIL", "MIN", "AVG", "MAX")
		for _, k := range keys {
			a := grouped[k]
			if len(a.durs) == 0 {
				fmt.Printf("   %-16s  %-10d  %-4d  %-4d  %-10s  %-10s  %-10s\n",
					k.taskType, k.size, a.ok, a.fail, "─", "─", "─")
				continue
			}
			minD, maxD, total := a.durs[0], a.durs[0], time.Duration(0)
			for _, d := range a.durs {
				total += d
				if d < minD { minD = d }
				if d > maxD { maxD = d }
			}
			avg := total / time.Duration(len(a.durs))
			fmt.Printf("   %-16s  %-10d  %-4d  %-4d  %-10s  %-10s  %-10s\n",
				k.taskType, k.size, a.ok, a.fail,
				minD.Round(time.Millisecond), avg.Round(time.Millisecond), maxD.Round(time.Millisecond))
		}
	}

	if res.OK > 0 {
		tpm := float64(res.OK) / res.Wall.Minutes()
		fmt.Printf("   throughput : %.1f tasks/min\n", tpm)
	}
}

func printSummary(results []ScenarioResult) {
	fmt.Println(strings.Repeat("═", 80))
	fmt.Println("SUMMARY")
	fmt.Println(strings.Repeat("═", 80))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SCENARIO\tTOTAL\tOK\tFAIL\tTIMEOUT\tWALL TIME\tTHROUGHPUT")
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		strings.Repeat("─", 20), "─────", "──", "────", "───────", "─────────", "──────────")

	for _, r := range results {
		tpm := ""
		if r.OK > 0 {
			tpm = fmt.Sprintf("%.1f t/min", float64(r.OK)/r.Wall.Minutes())
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%s\t%s\n",
			r.Name, r.Total, r.OK, r.Fail, r.TimedOut,
			r.Wall.Round(time.Millisecond), tpm)
	}
	w.Flush()
}

// ── helpers ───────────────────────────────────────────────────────────────────

func uploadScripts(ctx context.Context, scenarios []ScenarioCfg, resourcesDir, prefix string, s3cli *s3pkg.Client) (map[string]string, error) {
	seen := map[string]bool{}
	keys := map[string]string{}
	for _, sc := range scenarios {
		for _, tg := range sc.Tasks {
			if seen[tg.Type] {
				continue
			}
			seen[tg.Type] = true
			scriptPath := filepath.Join(resourcesDir, tg.Type+".py")
			f, err := os.Open(scriptPath)
			if err != nil {
				return nil, fmt.Errorf("open script %s: %w", scriptPath, err)
			}
			key := fmt.Sprintf("%s/script_%s.py", prefix, tg.Type)
			err = s3cli.Upload(ctx, key, f)
			f.Close()
			if err != nil {
				return nil, fmt.Errorf("upload script %s: %w", tg.Type, err)
			}
			keys[tg.Type] = key
		}
	}
	return keys, nil
}

func writeCSV(path string, results []ScenarioResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	cw := csv.NewWriter(f)
	defer cw.Flush()

	_ = cw.Write([]string{
		"scenario", "task_type", "size",
		"ok", "fail", "timeout",
		"wall_ms", "min_ms", "avg_ms", "max_ms", "throughput_tpm",
	})

	for _, r := range results {
		// One row per (scenario, type, size) combination.
		type key struct{ taskType string; size int }
		type agg struct{ ok, fail, timeout int; durs []time.Duration }
		grouped := map[key]*agg{}

		for _, tr := range r.TaskTimes {
			k := key{tr.taskType, tr.size}
			if grouped[k] == nil {
				grouped[k] = &agg{}
			}
			switch tr.err {
			case "":
				grouped[k].ok++
				grouped[k].durs = append(grouped[k].durs, tr.duration)
			case "timeout":
				grouped[k].timeout++
			default:
				grouped[k].fail++
			}
		}

		keys := make([]key, 0, len(grouped))
		for k := range grouped {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].taskType != keys[j].taskType {
				return keys[i].taskType < keys[j].taskType
			}
			return keys[i].size < keys[j].size
		})

		for _, k := range keys {
			a := grouped[k]
			minMs, avgMs, maxMs := int64(0), int64(0), int64(0)
			if len(a.durs) > 0 {
				minD, maxD, total := a.durs[0], a.durs[0], time.Duration(0)
				for _, d := range a.durs {
					total += d
					if d < minD { minD = d }
					if d > maxD { maxD = d }
				}
				minMs = minD.Milliseconds()
				avgMs = (total / time.Duration(len(a.durs))).Milliseconds()
				maxMs = maxD.Milliseconds()
			}
			tpm := ""
			if r.OK > 0 {
				tpm = fmt.Sprintf("%.2f", float64(r.OK)/r.Wall.Minutes())
			}
			_ = cw.Write([]string{
				r.Name, k.taskType, strconv.Itoa(k.size),
				strconv.Itoa(a.ok), strconv.Itoa(a.fail), strconv.Itoa(a.timeout),
				strconv.FormatInt(r.Wall.Milliseconds(), 10),
				strconv.FormatInt(minMs, 10),
				strconv.FormatInt(avgMs, 10),
				strconv.FormatInt(maxMs, 10),
				tpm,
			})
		}
	}
	return nil
}
