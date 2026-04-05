package main

type WorkerInfo struct {
	Host string `yaml:"host"` // full host with port, e.g. "localhost:2001"
}

type Config struct {
	Port                int           `yaml:"port"`
	Workers             []WorkerInfo  `yaml:"workers"`
	MetricsPollInterval string        `yaml:"metrics_poll_interval"` // e.g. "10s"; default 10s
	MaxTasksPerWorker   int32         `yaml:"max_tasks_per_worker"`  // default 1
}
