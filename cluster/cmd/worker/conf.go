package main

import "github.com/pepelulka/rl-scheduler/internal/s3"

type Config struct {
	Port     int       `yaml:"port"`
	Master   string    `yaml:"master"`
	S3Config s3.Config `yaml:"s3_config"`
}
