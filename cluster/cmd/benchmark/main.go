// Benchmark scenario: submits computationally intensive tasks to the cluster
// and measures throughput and per-task latency.
//
// Three task types (all pure-Python, no external deps):
//
//	monte_carlo  — estimate π via Monte Carlo sampling
//	             complexity C → C × 1 000 000 samples
//	             C=1 ≈ 1 s,  C=5 ≈ 5 s,  C=20 ≈ 20 s
//
//	matrix_mul   — multiply two random N×N matrices
//	             complexity C → N = C × 50
//	             C=1 ≈ 0.3 s, C=3 ≈ 5 s, C=6 ≈ 40 s
//
//	prime_sieve  — Sieve of Eratosthenes up to N
//	             complexity C → N = C × 1 000 000
//	             C=1 ≈ 0.1 s, C=10 ≈ 1 s, C=100 ≈ 10 s
//
//	mixed        — random mix of the three types;
//	             each task gets a random complexity in [C/2, C*2]
//
// Usage:
//
//	go run ./cmd/benchmark \
//	  -master      localhost:2000 \
//	  -s3-endpoint http://localhost:9000 \
//	  -task-type   mixed \
//	  -complexity  3 \
//	  -n           20
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pepelulka/rl-scheduler/internal/s3"
	masterpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/master"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ── Python scripts ────────────────────────────────────────────────────────────

// Reads number of samples N from stdin, runs Monte Carlo π estimation.
const scriptMonteCarlo = `import sys, random, time
n = int(sys.stdin.read().strip())
start = time.time()
inside = 0
for _ in range(n):
    x = random.random()
    y = random.random()
    if x*x + y*y <= 1.0:
        inside += 1
pi = 4.0 * inside / n
elapsed = time.time() - start
print(f"{pi:.6f}")
print(f"elapsed={elapsed:.3f}s samples={n}")
`

// Reads matrix dimension N from stdin, multiplies two random N×N matrices.
const scriptMatrixMul = `import sys, random, time
n = int(sys.stdin.read().strip())
start = time.time()
A = [[random.random() for _ in range(n)] for _ in range(n)]
B = [[random.random() for _ in range(n)] for _ in range(n)]
C = [[0.0] * n for _ in range(n)]
for i in range(n):
    for k in range(n):
        aik = A[i][k]
        for j in range(n):
            C[i][j] += aik * B[k][j]
checksum = sum(C[i][j] for i in range(n) for j in range(n))
elapsed = time.time() - start
print(f"{checksum:.6f}")
print(f"elapsed={elapsed:.3f}s size={n}x{n}")
`

// Reads upper-bound N from stdin, runs Sieve of Eratosthenes.
const scriptPrimeSieve = `import sys, time
n = int(sys.stdin.read().strip())
start = time.time()
sieve = bytearray([1]) * (n + 1)
sieve[0] = sieve[1] = 0
i = 2
while i * i <= n:
    if sieve[i]:
        step = len(sieve[i*i::i])
        sieve[i*i::i] = b'\x00' * step
    i += 1
count = sum(sieve)
elapsed = time.time() - start
print(f"{count}")
print(f"elapsed={elapsed:.3f}s limit={n}")
`

// ── Task kinds ────────────────────────────────────────────────────────────────

type taskKind int

const (
	kindMonteCarlo taskKind = iota
	kindMatrixMul
	kindPrimeSieve
)

var allKinds = []taskKind{kindMonteCarlo, kindMatrixMul, kindPrimeSieve}

func (k taskKind) String() string {
	switch k {
	case kindMonteCarlo:
		return "monte_carlo"
	case kindMatrixMul:
		return "matrix_mul"
	case kindPrimeSieve:
		return "prime_sieve"
	}
	return "unknown"
}

func (k taskKind) script() string {
	switch k {
	case kindMonteCarlo:
		return scriptMonteCarlo
	case kindMatrixMul:
		return scriptMatrixMul
	case kindPrimeSieve:
		return scriptPrimeSieve
	}
	return ""
}

// inputForComplexity maps a user-facing complexity integer to the numeric
// parameter passed to the Python script on stdin.
func inputForComplexity(k taskKind, complexity int) int {
	switch k {
	case kindMonteCarlo:
		return complexity * 1_000_000
	case kindMatrixMul:
		return complexity * 50
	case kindPrimeSieve:
		return complexity * 1_000_000
	}
	return complexity
}

// ── Task spec & result ────────────────────────────────────────────────────────

type taskSpec struct {
	index      int
	kind       taskKind
	inputVal   int // value written into the input file
	scriptKey  string
	inputKey   string
	outputKey  string
}

type taskResult struct {
	mu         sync.Mutex
	id         string
	done       bool
	errMsg     string
	submittedAt time.Time
	finishedAt  time.Time
}

func (r *taskResult) markDone(errMsg string) {
	r.mu.Lock()
	r.done = true
	r.errMsg = errMsg
	r.finishedAt = time.Now()
	r.mu.Unlock()
}

func (r *taskResult) isDone() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	masterAddr := flag.String("master", "localhost:2000", "master gRPC address")
	s3Endpoint := flag.String("s3-endpoint", "http://localhost:9000", "S3 endpoint")
	s3Login    := flag.String("s3-login", "local", "S3 access key")
	s3Password := flag.String("s3-password", "12345678", "S3 secret key")
	s3Bucket   := flag.String("s3-bucket", "data", "S3 bucket")
	taskType   := flag.String("task-type", "mixed", "monte_carlo | matrix_mul | prime_sieve | mixed")
	complexity := flag.Int("complexity", 3, "complexity multiplier (see package doc)")
	n          := flag.Int("n", 10, "number of tasks to submit")
	timeout    := flag.Duration("timeout", 10*time.Minute, "max wait time for all tasks to finish")
	seed       := flag.Int64("seed", 0, "RNG seed for mixed mode (0 = random)")
	flag.Parse()

	if *complexity < 1 {
		log.Fatal("-complexity must be >= 1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+30*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("benchmark/%d", time.Now().UnixMilli())

	rngSeed := *seed
	if rngSeed == 0 {
		rngSeed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(rngSeed))

	s3Cli, err := s3.NewClient(ctx, s3.Config{
		Endpoint: *s3Endpoint,
		Login:    *s3Login,
		Password: *s3Password,
		Bucket:   *s3Bucket,
	})
	if err != nil {
		log.Fatalf("s3 init: %v", err)
	}

	// ── 1. Upload scripts ────────────────────────────────────────────────────
	neededKinds := kindsForType(*taskType)
	scriptKeys := make(map[taskKind]string, len(neededKinds))
	for _, k := range neededKinds {
		key := fmt.Sprintf("%s/script_%s.py", prefix, k)
		if err := s3Cli.Upload(ctx, key, strings.NewReader(k.script())); err != nil {
			log.Fatalf("upload script %s: %v", k, err)
		}
		scriptKeys[k] = key
	}
	fmt.Printf("uploaded %d script(s) → s3://%s/%s/\n", len(neededKinds), *s3Bucket, prefix)

	// ── 2. Build specs & upload inputs ──────────────────────────────────────
	specs := make([]taskSpec, *n)
	for i := range *n {
		kind := pickKind(*taskType, rng, neededKinds)
		c := pickComplexity(*taskType, *complexity, rng)

		specs[i] = taskSpec{
			index:     i,
			kind:      kind,
			inputVal:  inputForComplexity(kind, c),
			scriptKey: scriptKeys[kind],
			inputKey:  fmt.Sprintf("%s/input_%d.txt", prefix, i),
			outputKey: fmt.Sprintf("%s/output_%d.txt", prefix, i),
		}
		body := strconv.Itoa(specs[i].inputVal)
		if err := s3Cli.Upload(ctx, specs[i].inputKey, strings.NewReader(body)); err != nil {
			log.Fatalf("upload input %d: %v", i, err)
		}
	}
	fmt.Printf("uploaded %d input file(s)\n\n", *n)

	// ── 3. Connect to master ─────────────────────────────────────────────────
	conn, err := grpc.NewClient(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc connect: %v", err)
	}
	defer conn.Close()
	cli := masterpb.NewMasterServiceClient(conn)

	// ── 4. Submit tasks ──────────────────────────────────────────────────────
	results := make([]*taskResult, *n)
	taskIDs := make([]string, *n)
	submitStart := time.Now()

	fmt.Printf("submitting %d task(s) [type=%s  complexity=%d  seed=%d]:\n",
		*n, *taskType, *complexity, rngSeed)

	for i := range *n {
		resp, err := cli.SubmitTask(ctx, &masterpb.SubmitTaskRequest{
			Type:       "python",
			Script:     specs[i].scriptKey,
			InputFile:  specs[i].inputKey,
			OutputFile: specs[i].outputKey,
			Meta: map[string]string{
				"task_type":  specs[i].kind.String(),
				"input_val":  strconv.Itoa(specs[i].inputVal),
				"task_index": strconv.Itoa(i),
			},
		})
		if err != nil {
			log.Fatalf("submit task %d: %v", i, err)
		}
		taskIDs[i] = resp.TaskId
		results[i] = &taskResult{id: resp.TaskId, submittedAt: time.Now()}
		fmt.Printf("  [%3d] %-12s  input=%-12d  id=%s\n",
			i, specs[i].kind, specs[i].inputVal, resp.TaskId)
	}
	fmt.Printf("\n%d tasks submitted in %s\n", *n, time.Since(submitStart).Round(time.Millisecond))

	// ── 5. Poll until all done ───────────────────────────────────────────────
	fmt.Printf("waiting for results (timeout %s) …\n\n", *timeout)

	deadline := time.Now().Add(*timeout)
	doneCount := 0
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) && doneCount < *n {
		<-ticker.C
		for i := range *n {
			if results[i].isDone() {
				continue
			}
			st, err := cli.GetTaskStatus(ctx, &masterpb.GetTaskStatusRequest{TaskId: taskIDs[i]})
			if err != nil {
				continue
			}
			switch st.Status {
			case masterpb.TaskStatus_TASK_STATUS_SUCCESS:
				results[i].markDone("")
				doneCount++
				wall := results[i].finishedAt.Sub(results[i].submittedAt).Round(time.Millisecond)
				fmt.Printf("  [OK]   [%3d] %-12s  input=%-12d  wall=%s\n",
					i, specs[i].kind, specs[i].inputVal, wall)
			case masterpb.TaskStatus_TASK_STATUS_FAIL:
				results[i].markDone(st.Error)
				doneCount++
				fmt.Printf("  [FAIL] [%3d] %-12s  err=%s\n", i, specs[i].kind, st.Error)
			}
		}
	}

	// ── 6. Summary ───────────────────────────────────────────────────────────
	totalElapsed := time.Since(submitStart)
	fmt.Printf("\n─── summary ─────────────────────────────────────────────────────\n")

	type kindStats struct {
		ok, failed, timedOut int
		totalWall            time.Duration
		minWall, maxWall     time.Duration
	}
	stats := make(map[taskKind]*kindStats)
	for _, k := range allKinds {
		stats[k] = &kindStats{minWall: 1<<62 - 1}
	}

	for i := range *n {
		r := results[i]
		k := specs[i].kind
		st := stats[k]
		r.mu.Lock()
		isDone, isErr, wall := r.done, r.errMsg != "", r.finishedAt.Sub(r.submittedAt)
		r.mu.Unlock()

		switch {
		case !isDone:
			st.timedOut++
		case isErr:
			st.failed++
		default:
			st.ok++
			st.totalWall += wall
			if wall < st.minWall {
				st.minWall = wall
			}
			if wall > st.maxWall {
				st.maxWall = wall
			}
		}
	}

	totalOK, totalFailed, totalTimeout := 0, 0, 0
	for _, k := range neededKinds {
		st := stats[k]
		totalOK += st.ok
		totalFailed += st.failed
		totalTimeout += st.timedOut
		if st.ok+st.failed+st.timedOut == 0 {
			continue
		}
		fmt.Printf("\n  %s:\n", k)
		fmt.Printf("    tasks: %d ok  %d failed  %d timed-out\n", st.ok, st.failed, st.timedOut)
		if st.ok > 0 {
			avg := st.totalWall / time.Duration(st.ok)
			fmt.Printf("    wall:  min=%s  avg=%s  max=%s\n",
				st.minWall.Round(time.Millisecond),
				avg.Round(time.Millisecond),
				st.maxWall.Round(time.Millisecond))
		}
	}

	fmt.Printf("\ntotal:  %d ok  %d failed  %d timed-out  |  wall=%s\n",
		totalOK, totalFailed, totalTimeout, totalElapsed.Round(time.Second))
	if totalOK > 0 {
		fmt.Printf("throughput: %.3f tasks/s\n", float64(totalOK)/totalElapsed.Seconds())
	}

	// ── 7. Cleanup: delete all uploaded objects from MinIO ───────────────────
	fmt.Printf("\ncleaning up s3://%s/%s/ …\n", *s3Bucket, prefix)
	var toDelete []string
	for _, key := range scriptKeys {
		toDelete = append(toDelete, key)
	}
	for i := range *n {
		toDelete = append(toDelete, specs[i].inputKey)
		toDelete = append(toDelete, specs[i].outputKey)
	}
	cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanCancel()
	if err := s3Cli.DeleteMany(cleanCtx, toDelete); err != nil {
		fmt.Printf("cleanup warning: %v\n", err)
	} else {
		fmt.Printf("deleted %d object(s)\n", len(toDelete))
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func kindsForType(taskType string) []taskKind {
	switch taskType {
	case "monte_carlo":
		return []taskKind{kindMonteCarlo}
	case "matrix_mul":
		return []taskKind{kindMatrixMul}
	case "prime_sieve":
		return []taskKind{kindPrimeSieve}
	default: // mixed
		return allKinds
	}
}

func pickKind(taskType string, rng *rand.Rand, kinds []taskKind) taskKind {
	if len(kinds) == 1 {
		return kinds[0]
	}
	return kinds[rng.Intn(len(kinds))]
}

// pickComplexity returns a complexity value for a task.
// For mixed mode it varies randomly between [max(1, C/2), C*2] to create
// tasks with different durations — important for testing the scheduler.
func pickComplexity(taskType string, base int, rng *rand.Rand) int {
	if taskType != "mixed" {
		return base
	}
	lo := base / 2
	if lo < 1 {
		lo = 1
	}
	hi := base * 2
	return lo + rng.Intn(hi-lo+1)
}
