package executor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
)

type PythonTask struct {
	InputPath  string `yaml:"input_path"`
	OutputPath string `yaml:"output_path"`
	ScriptPath string `yaml:"script_path"`
}

func (e *Executor) ExecPythonTask(ctx context.Context, task PythonTask) error {
	scriptFile, err := os.CreateTemp("", "script-*.py")
	if err != nil {
		return fmt.Errorf("create temp script: %w", err)
	}
	defer os.Remove(scriptFile.Name())
	scriptFile.Close()

	inputFile, err := os.CreateTemp("", "input-*")
	if err != nil {
		return fmt.Errorf("create temp input: %w", err)
	}
	defer os.Remove(inputFile.Name())
	inputFile.Close()

	outputFile, err := os.CreateTemp("", "output-*")
	if err != nil {
		return fmt.Errorf("create temp output: %w", err)
	}
	defer os.Remove(outputFile.Name())
	outputFile.Close()

	if err := e.downloadFile(ctx, task.ScriptPath, scriptFile.Name()); err != nil {
		return fmt.Errorf("download script: %w", err)
	}
	if err := e.downloadFile(ctx, task.InputPath, inputFile.Name()); err != nil {
		return fmt.Errorf("download input: %w", err)
	}

	stdin, err := os.Open(inputFile.Name())
	if err != nil {
		return fmt.Errorf("open input file: %w", err)
	}
	defer stdin.Close()

	stdout, err := os.Create(outputFile.Name())
	if err != nil {
		return fmt.Errorf("open output file: %w", err)
	}
	defer stdout.Close()

	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "python3", scriptFile.Name())
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("python script failed: %w\nstderr: %s", err, stderr.String())
	}

	if err := e.uploadFile(ctx, task.OutputPath, outputFile.Name()); err != nil {
		return fmt.Errorf("upload output: %w", err)
	}

	return nil
}
