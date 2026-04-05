package main

type WorkerInfo struct {
	Host string `yaml:"host"` // full host with port
}

type Config struct {
	Port    int          `yaml:"port"`
	Workers []WorkerInfo `yaml:"workers"`
}
