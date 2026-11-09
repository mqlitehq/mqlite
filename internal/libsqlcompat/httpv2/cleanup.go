package hranaV2

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mqlitehq/mqlite/internal/libsqlcompat/hrana"
)

const (
	streamCleanupTimeout   = 5 * time.Second
	streamCleanupLimit     = 16
	cleanupCapacityWarning = "mqlite: HTTP stream cleanup capacity reached; skipped best-effort closes rely on server expiry, not confirmed release"
)

// CleanupManager belongs to one database opener. Detached cleanup must never
// hold its SQL connection or writer admission while waiting for a cleanup slot.
type CleanupManager struct {
	slots   chan struct{}
	skipped atomic.Uint64
	warned  atomic.Bool
	warning func()
}

type CleanupStats struct {
	Active  int
	Skipped uint64
}

func NewCleanupManager() *CleanupManager {
	return &CleanupManager{
		slots:   make(chan struct{}, streamCleanupLimit),
		warning: func() { slog.Warn(cleanupCapacityWarning) },
	}
}

func (m *CleanupManager) Stats() CleanupStats {
	return CleanupStats{Active: len(m.slots), Skipped: m.skipped.Load()}
}

func (m *CleanupManager) closeStreamAsync(baton, url, jwt, host, encryptionKey string) {
	if baton == "" {
		return
	}
	select {
	case m.slots <- struct{}{}:
	default:
		m.skipped.Add(1)
		return
	}
	// Establish the deadline before scheduling: goroutine admission cannot buy
	// more time. Cleanup is best effort, never retried or synchronously joined.
	ctx, cancel := context.WithTimeout(context.Background(), streamCleanupTimeout)
	go func() {
		defer func() {
			cancel()
			<-m.slots
			// A user logger may block. Only an existing, completed cleanup worker
			// may call it, after releasing all HTTP resources and admission capacity.
			if m.skipped.Load() > 0 && m.warned.CompareAndSwap(false, true) {
				m.warning()
			}
		}()
		msg := hrana.PipelineRequest{Baton: baton}
		msg.Add(hrana.CloseStream())
		_, _, _ = sendPipelineRequest(ctx, &msg, url, jwt, host, encryptionKey)
	}()
}
