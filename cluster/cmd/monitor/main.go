package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pepelulka/rl-scheduler/internal/common"
	workerpb "github.com/pepelulka/rl-scheduler/proto/gen/go/v1/worker"
)

// ── config ──────────────────────────────────────────────────────────────────

type Config struct {
	Workers      []struct{ Host string `yaml:"host"` } `yaml:"workers"`
	PollInterval string                                 `yaml:"poll_interval"`
}

// ── state ────────────────────────────────────────────────────────────────────

type workerState struct {
	host        string
	cpuPct      float32
	cpuCores    float32
	ramUsedKiB  float32
	ramMaxKiB   float32
	activeTasks int32
	err         error
}

// ── messages ─────────────────────────────────────────────────────────────────

type tickMsg time.Time

type workerUpdateMsg struct {
	idx   int
	state workerState
}

// ── model ────────────────────────────────────────────────────────────────────

type model struct {
	workers      []workerState
	pollInterval time.Duration
}

func (m model) Init() tea.Cmd {
	return tea.Batch(doTick(m.pollInterval), pollAll(m.workers))
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		}
	case tickMsg:
		return m, tea.Batch(doTick(m.pollInterval), pollAll(m.workers))
	case workerUpdateMsg:
		m.workers[msg.idx] = msg.state
	}
	return m, nil
}

// ── view ─────────────────────────────────────────────────────────────────────

const barWidth = 36

func (m model) View() string {
	var sb strings.Builder
	sb.WriteString("\n  rl-scheduler monitor  (q — выход)\n\n")

	for _, w := range m.workers {
		title := fmt.Sprintf(" %s ", w.host)
		dashes := max(0, 58-len(title))
		sb.WriteString(fmt.Sprintf("  ┌─%s%s\n", title, strings.Repeat("─", dashes)))

		if w.err != nil {
			sb.WriteString(fmt.Sprintf("  │  недоступен: %s\n", w.err))
		} else {
			ramPct := float32(0)
			if w.ramMaxKiB > 0 {
				ramPct = w.ramUsedKiB / w.ramMaxKiB * 100
			}
			sb.WriteString(fmt.Sprintf("  │  CPU   %s %5.1f%%   лимит %.2f ядра\n",
				progressBar(w.cpuPct), w.cpuPct, w.cpuCores))
			sb.WriteString(fmt.Sprintf("  │  RAM   %s %5.1f%%   %s / %s\n",
				progressBar(ramPct), ramPct, formatKiB(w.ramUsedKiB), formatKiB(w.ramMaxKiB)))
			sb.WriteString(fmt.Sprintf("  │  Задач: %d\n", w.activeTasks))
		}
		sb.WriteString("  └" + strings.Repeat("─", 60) + "\n\n")
	}
	return sb.String()
}

func progressBar(pct float32) string {
	if pct < 0 {
		pct = 0
	} else if pct > 100 {
		pct = 100
	}
	filled := int(float32(barWidth) * pct / 100)
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled) + "]"
}

func formatKiB(kib float32) string {
	switch {
	case kib >= 1024*1024:
		return fmt.Sprintf("%.2f GiB", kib/1024/1024)
	case kib >= 1024:
		return fmt.Sprintf("%.1f MiB", kib/1024)
	default:
		return fmt.Sprintf("%.0f KiB", kib)
	}
}

// ── polling ──────────────────────────────────────────────────────────────────

func doTick(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func pollAll(workers []workerState) tea.Cmd {
	cmds := make([]tea.Cmd, len(workers))
	for i, w := range workers {
		cmds[i] = pollWorker(i, w.host)
	}
	return tea.Batch(cmds...)
}

func pollWorker(idx int, host string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return workerUpdateMsg{idx: idx, state: workerState{host: host, err: err}}
		}
		defer conn.Close()

		client := workerpb.NewWorkerServiceClient(conn)

		metricsResp, err := client.GetMetrics(ctx, &workerpb.GetMetricsRequest{})
		if err != nil {
			return workerUpdateMsg{idx: idx, state: workerState{host: host, err: err}}
		}
		statusResp, err := client.GetStatus(ctx, &workerpb.GetStatusRequest{})
		if err != nil {
			return workerUpdateMsg{idx: idx, state: workerState{host: host, err: err}}
		}

		met := metricsResp.Metrics
		return workerUpdateMsg{
			idx: idx,
			state: workerState{
				host:        host,
				cpuPct:      met.CpuUtilPct,
				cpuCores:    met.CpuLimitCores,
				ramUsedKiB:  met.RamUsageKib,
				ramMaxKiB:   met.RamMaxKib,
				activeTasks: statusResp.ActiveTasks,
			},
		}
	}
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	cfg := common.MustParseConfigOpt[Config]()

	pollInterval := time.Second
	if cfg.PollInterval != "" {
		if d, err := time.ParseDuration(cfg.PollInterval); err == nil {
			pollInterval = d
		}
	}

	workers := make([]workerState, len(cfg.Workers))
	for i, w := range cfg.Workers {
		workers[i] = workerState{host: w.Host}
	}

	p := tea.NewProgram(
		model{workers: workers, pollInterval: pollInterval},
		tea.WithAltScreen(),
	)
	if _, err := p.Run(); err != nil {
		fmt.Println("ошибка:", err)
	}
}
