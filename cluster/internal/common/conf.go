package common

import (
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func LoadConfigFromYAML[T any](filename string) (t T, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return t, err
	}
	defer f.Close()

	err = yaml.NewDecoder(f).Decode(&t)
	return t, err
}

// only works if your binary have ONLY ONE --config option
func ParseConfigOpt[T any]() (t T, err error) {
	cfgPath := flag.String("config", "-", "config path")
	flag.Parse()

	if *cfgPath == "-" {
		return t, fmt.Errorf("config opt is required")
	}

	return LoadConfigFromYAML[T](*cfgPath)
}

func MustParseConfigOpt[T any]() T {
	return Must(
		ParseConfigOpt[T](),
	)
}
