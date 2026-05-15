package command

import (
	"bytes"
	"context"
	"os/exec"
)

type Commander interface {
	Run(ctx context.Context, head string, parts ...string) (output []byte, err error)
}

func NewCommander() Commander {
	return &cmder{}
}

type cmder struct {
}

func (e *cmder) Run(ctx context.Context, head string, parts ...string) (output []byte, err error) {
	cmd := exec.CommandContext(ctx, head, parts...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err = cmd.Start()
	if err != nil {
		return out.Bytes(), err
	}

	err = cmd.Wait()

	return out.Bytes(), err
}
