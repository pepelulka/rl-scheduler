package internal

type NodeMetrics struct {
	CpuUtilPct    float32
	CpuLimitCores float32 // CPU limit from cgroup; 0 = no limit
	RamUsageKiB   float32
	RamMaxKiB     float32
}
