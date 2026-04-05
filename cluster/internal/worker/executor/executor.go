package executor

import (
	"context"
	"io"
	"os"

	"github.com/pepelulka/rl-scheduler/internal/s3"
)

type Executor struct {
	s3Cli *s3.Client
}

func NewExecutor(s3Cli *s3.Client) *Executor {
	return &Executor{
		s3Cli: s3Cli,
	}
}

func (e *Executor) downloadFile(ctx context.Context, key string, fname string) error {
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := e.s3Cli.Download(ctx, key)
	if err != nil {
		return err
	}

	defer r.Close()
	_, err = io.Copy(f, r)
	return err
}

func (e *Executor) uploadFile(ctx context.Context, key string, fname string) error {
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()

	return e.s3Cli.Upload(ctx, key, f)
}
