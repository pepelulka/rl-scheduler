// Package metrics provides cgroup-aware CPU and memory measurements for a
// containerised worker. It supports cgroup v2 (default on modern Docker/Linux).
package metrics

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// cgroup v2 paths (mounted inside the container by the runtime).
	cgroupCPUStat    = "/sys/fs/cgroup/cpu.stat"
	cgroupCPUMax     = "/sys/fs/cgroup/cpu.max"
	cgroupMemCurrent = "/sys/fs/cgroup/memory.current"
	cgroupMemMax     = "/sys/fs/cgroup/memory.max"

	sampleInterval = time.Second
)

// Snapshot is a point-in-time view of the container's resource usage.
type Snapshot struct {
	CpuUtilPct    float32 // [0, 100]
	CpuLimitCores float32 // CPU quota in cores; 0 = no limit
	RamUsageKiB   float32
	RamMaxKiB     float32
}

// Sampler continuously measures CPU utilisation and memory in the background.
// Call Run to start it, then Latest to read the most recent Snapshot.
type Sampler struct {
	latest atomic.Pointer[Snapshot]
	once   sync.Once
}

// Latest returns the most recent snapshot. Returns a zero-value Snapshot
// before the first sample is collected.
func (s *Sampler) Latest() Snapshot {
	if p := s.latest.Load(); p != nil {
		return *p
	}
	return Snapshot{}
}

// Run starts the background sampling loop; blocks until ctx channel is closed.
func (s *Sampler) Run(stop <-chan struct{}) {
	s.once.Do(func() {
		ticker := time.NewTicker(sampleInterval)
		defer ticker.Stop()

		prevUsec, prevTime := readCPUUsec(), time.Now()

		for {
			select {
			case <-stop:
				return
			case now := <-ticker.C:
				curUsec := readCPUUsec()
				elapsed := now.Sub(prevTime).Seconds()

				var cpuPct float32
				if elapsed > 0 && curUsec >= prevUsec {
					// usage_usec is in microseconds; convert delta to seconds.
					cpuPct = float32((curUsec-prevUsec)/1e6/elapsed) * 100.0
				}
				prevUsec, prevTime = curUsec, now

				ramUsage, ramMax := readMemory()
				snap := &Snapshot{
					CpuUtilPct:    cpuPct,
					CpuLimitCores: readCPULimit(),
					RamUsageKiB:   float32(ramUsage / 1024),
					RamMaxKiB:     float32(ramMax / 1024),
				}
				s.latest.Store(snap)
			}
		}
	})
}

// readCPUUsec reads the cumulative CPU time from cgroup v2 cpu.stat.
// Returns 0 on any error.
func readCPUUsec() float64 {
	f, err := os.Open(cgroupCPUStat)
	if err != nil {
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "usage_usec ") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				v, _ := strconv.ParseFloat(parts[1], 64)
				return v
			}
		}
	}
	return 0
}

// readCPULimit reads /sys/fs/cgroup/cpu.max and returns the CPU quota in cores.
// The file contains "<quota> <period>" or "max <period>" (no limit).
// Returns 0 if there is no limit or the file cannot be read.
func readCPULimit() float32 {
	raw := strings.TrimSpace(readRawFile(cgroupCPUMax))
	if raw == "" {
		return 0
	}
	parts := strings.Fields(raw)
	if len(parts) != 2 || parts[0] == "max" {
		return 0
	}
	quota, err := strconv.ParseFloat(parts[0], 32)
	if err != nil || quota <= 0 {
		return 0
	}
	period, err := strconv.ParseFloat(parts[1], 32)
	if err != nil || period <= 0 {
		return 0
	}
	return float32(quota / period)
}

// readMemory returns (usage bytes, limit bytes) from cgroup v2 memory files.
// Returns (0, 0) on any error. If the container has no memory limit the kernel
// writes "max"; in that case we report 0 for the limit.
func readMemory() (usage, limit uint64) {
	usage = readUint64File(cgroupMemCurrent)
	raw := readRawFile(cgroupMemMax)
	if raw != "max" {
		limit, _ = strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	}
	return usage, limit
}

func readUint64File(path string) uint64 {
	raw := readRawFile(path)
	v, _ := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	return v
}

func readRawFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s", b)
}
