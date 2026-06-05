package slogwriter

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrite(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	writer := New(logger, slog.LevelInfo)
	msg := []byte(" test1 \ntest2\ntest3")

	n, err := writer.Write(msg)
	assert.Equal(t, 19, n)
	assert.NoError(t, err)
}
