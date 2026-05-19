package main

type WorkerInfo struct {
	Host string `yaml:"host"` // full host with port, e.g. "localhost:2001"
}

type Config struct {
	Port                int          `yaml:"port"`
	Workers             []WorkerInfo `yaml:"workers"`
	MetricsPollInterval string       `yaml:"metrics_poll_interval"` // e.g. "10s"; default 10s
	MaxTasksPerWorker   int32        `yaml:"max_tasks_per_worker"`  // default 1
	Scheduler           string       `yaml:"scheduler"`             // "least_loaded" (default), "round_robin", "resource_aware", "rl"
	InferenceURL        string       `yaml:"inference_url"`         // RL inference service URL; default http://localhost:8000
	ResourceAwareCPUWeight float64   `yaml:"resource_aware_cpu_weight"` // CPU weight for resource_aware scheduler (0..1); default 0.5
}
