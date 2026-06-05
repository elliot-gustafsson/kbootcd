package slogwriter

import (
	"bytes"
	"context"
	"log/slog"
	"unsafe"
)

type slogWriter struct {
	logger *slog.Logger
	level  slog.Level
}

func New(logger *slog.Logger, level slog.Level) *slogWriter {
	return &slogWriter{
		logger: logger,
		level:  level,
	}
}

func (w *slogWriter) Write(p []byte) (n int, err error) {
	originalLen := len(p)

	for len(p) > 0 {
		var line []byte
		idx := bytes.IndexByte(p, '\n')

		if idx == -1 {
			line = p
			p = nil
		} else {
			line = p[:idx]
			p = p[idx+1:]
		}

		msg := bytes.TrimSpace(line)

		if len(msg) > 0 {
			str := unsafe.String(unsafe.SliceData(msg), len(msg))
			w.logger.Log(context.Background(), w.level, str)
		}
	}

	return originalLen, nil
}
