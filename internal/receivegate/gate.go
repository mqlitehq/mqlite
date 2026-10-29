// Package receivegate carries the broker's no-new-claim barrier through a
// Receive request context without canceling that request's database work.
package receivegate

import (
	"context"
	"errors"
	"sync"
)

// ErrDraining means a Receive reached the no-new-claim gate before completing
// any confirmed empty round. The broker maps it to retryable 503 so a caller can
// preserve its AttemptID and replay a prior commit whose response may have been
// lost; it must never be rewritten as a successful empty response.
var ErrDraining = errors.New("mqlite: broker is draining receives")

// Gate defines the atomic boundary between a claim admitted before broker drain
// and a new claim rejected after drain. A successfully admitted claim is allowed
// to finish; Stop only wakes long polls parked between claim rounds.
type Gate struct {
	mu      sync.Mutex
	stopped bool
	done    chan struct{}
	entered chan struct{}
	parked  chan struct{}
	enter   sync.Once
	park    sync.Once
}

// New returns an open claim gate.
func New() *Gate {
	return &Gate{
		done:    make(chan struct{}),
		entered: make(chan struct{}),
		parked:  make(chan struct{}),
	}
}

// TryEnterClaim atomically admits a claim unless Stop has established the drain
// barrier. A true result means that claim may finish even if Stop follows.
func (g *Gate) TryEnterClaim() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	g.enter.Do(func() { close(g.entered) })
	return true
}

// Stop establishes the no-new-claim barrier and wakes parked long polls.
func (g *Gate) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return
	}
	g.stopped = true
	close(g.done)
}

// Done closes when Stop establishes the drain barrier.
func (g *Gate) Done() <-chan struct{} { return g.done }

// MarkParked records that a Receive completed an empty claim round and reached
// its interruptible long-poll wait. It exists so shutdown integration tests can
// synchronize on the real wait boundary instead of sleeping.
func (g *Gate) MarkParked() { g.park.Do(func() { close(g.parked) }) }

// FirstClaimEntered closes at the first successful TryEnterClaim boundary.
func (g *Gate) FirstClaimEntered() <-chan struct{} { return g.entered }

// FirstParked closes when the first empty Receive reaches its long-poll wait.
func (g *Gate) FirstParked() <-chan struct{} { return g.parked }

type contextKey struct{}

// WithContext attaches gate without changing ctx cancellation semantics.
func WithContext(ctx context.Context, gate *Gate) context.Context {
	if gate == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, gate)
}

// FromContext returns the Gate attached by WithContext, if any.
func FromContext(ctx context.Context) *Gate {
	gate, _ := ctx.Value(contextKey{}).(*Gate)
	return gate
}
