// Package diagnosticlog keeps a bounded, private JSONL operator log independent
// of the database. It records failure/recovery events, not routine HTTP traffic.
package diagnosticlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const maxBytes = 4 << 20
const archives = 3

type writer struct {
	mu     sync.Mutex
	path   string
	file   *os.File
	size   int64
	limit  int64
	closed bool
}

func open(path string, limit int64) (*writer, error) {
	w := &writer{path: path, limit: limit}
	if err := w.reopen(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *writer) reopen() error {
	if info, err := os.Lstat(w.path); err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("diagnostic log must be a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	w.file, w.size = file, info.Size()
	return nil
}

func (w *writer) rotate() error {
	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		if err != nil {
			return err
		}
	}
	for n := archives; n >= 1; n-- {
		source := w.path
		if n > 1 {
			source += "." + strconv.Itoa(n-1)
		}
		target := w.path + "." + strconv.Itoa(n)
		// Rename replaces the oldest archive on Unix; explicit removal also works
		// on Windows. The source is checked first so a missing archive is harmless.
		if _, err := os.Lstat(source); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.Rename(source, target); err != nil {
			return err
		}
	}
	return w.reopen()
}

func (w *writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	if int64(len(p)) > w.limit {
		return 0, errors.New("diagnostic log record exceeds file size limit")
	}
	if w.file == nil {
		if err := w.reopen(); err != nil {
			return 0, err
		}
	}
	if w.size+int64(len(p)) > w.limit {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	// Diagnostic events are infrequent; sync each record so a process crash does
	// not strand it in a user-space buffer. Files still need a persistent volume.
	return n, w.file.Sync()
}

func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// Handler mirrors only diagnostic events to disk. If disk writes fail, the
// console still receives the original event and one warning per failure streak.
type Handler struct {
	console slog.Handler
	journal slog.Handler
	writer  *writer
	warned  *atomic.Bool
}

func New(console slog.Handler, path string) (*Handler, error) {
	w, err := open(path, maxBytes)
	if err != nil {
		return nil, fmt.Errorf("open diagnostic log: %w", err)
	}
	return &Handler{console: console, journal: slog.NewJSONHandler(w, nil), writer: w, warned: &atomic.Bool{}}, nil
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= slog.LevelInfo || h.console.Enabled(ctx, level)
}
func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	var consoleErr error
	if h.console.Enabled(ctx, r.Level) {
		consoleErr = h.console.Handle(ctx, r)
	}
	switch r.Message {
	case "maintenance.failure", "maintenance.recovered", "maintenance.interrupted", "storage.failure", "storage.interrupted":
		err := h.journal.Handle(ctx, r)
		if err != nil {
			if !h.warned.Swap(true) {
				warning := slog.NewRecord(time.Now(), slog.LevelError, "diagnostic log write failed; events remain on stderr", 0)
				warning.AddAttrs(slog.String("error", err.Error()))
				_ = h.console.Handle(ctx, warning)
			}
		} else {
			h.warned.Store(false)
		}
		return errors.Join(consoleErr, err)
	}
	return consoleErr
}
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{console: h.console.WithAttrs(attrs), journal: h.journal.WithAttrs(attrs), writer: h.writer, warned: h.warned}
}
func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{console: h.console.WithGroup(name), journal: h.journal.WithGroup(name), writer: h.writer, warned: h.warned}
}
func (h *Handler) Close() error { return h.writer.Close() }
