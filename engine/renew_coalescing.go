package engine

import (
	"context"
	"database/sql"
	"fmt"
)

const (
	maxPendingRenewalGroups = 2
	maxRenewalMergeBytes    = 512 << 10
)

// A group only collects compatible calls already waiting for the same writer
// (MQLITE-190). At most two groups retain execution ownership at once.
// There is no collection timer, permanent coordinator, or parallel SQL writer.
// writer.mu protects membership and completion; items and published results are
// immutable. The group's independent Close owner outlives canceled callers.
type renewalGroup struct {
	e         *Engine
	queue     string
	config    queueRow
	ctx       context.Context
	cancel    context.CancelFunc
	attempt   *storageAttempt
	waiter    *remoteWriteWaiter
	members   []*renewalMember
	positions int
	bytes     int
	sealed    bool
	completed bool
	reply     renewalReply
}

type renewalMember struct {
	ctx     context.Context
	items   []SettleItem
	done    chan struct{}
	active  bool
	attempt *storageAttempt
}

type renewalReply struct {
	deadlines map[string]int64
	err       error
	panicking bool
	panic     any
}

// This is a merge eligibility bound, not a new rejection of a valid RPC. The
// conservative escaped size avoids allocating a second JSON representation.
func renewalMergeSize(queue string, items []SettleItem) (int, bool) {
	if len(queue) > (maxRenewalMergeBytes-32)/6 {
		return 0, false
	}
	n := 32 + 6*len(queue)
	for _, it := range items {
		if n > maxRenewalMergeBytes-64 || len(it.LockToken) > (maxRenewalMergeBytes-n-64)/6 {
			return 0, false
		}
		n += 64 + 6*len(it.LockToken)
	}
	return n, true
}

func (e *Engine) coalesceRenewals(ctx context.Context, queue string, q queueRow, items []SettleItem) ([]SettleResult, error, bool) {
	d := e.db
	if !d.remote || !isHTTPHranaDSN(d.dsn) {
		return nil, nil, false
	}
	size, eligible := renewalMergeSize(queue, items)
	if !eligible {
		return nil, nil, false
	}
	// The same order as Close admission: no WaitGroup Add or new member can
	// race a closing store. Never wait for SQL or invoke caller code here.
	d.closeMu.Lock()
	if d.closing {
		d.closeMu.Unlock()
		return nil, nil, false // preserve the direct path's error observation
	}
	if err := ctx.Err(); err != nil {
		d.closeMu.Unlock()
		return nil, err, false // the direct path records this caller's error
	}
	w := &d.writer
	w.mu.Lock()
	if w.closed || !w.held && w.renew.Len()+w.urgent.Len()+w.normal.Len() == 0 {
		w.mu.Unlock()
		d.closeMu.Unlock()
		return nil, nil, false // idle writers keep their original synchronous path
	}
	var g *renewalGroup
	if tail := w.renew.Back(); tail != nil {
		candidate := tail.Value.(*remoteWriteWaiter).renewal
		if candidate != nil && !candidate.sealed && candidate.ctx.Err() == nil &&
			candidate.queue == queue && candidate.config.lockDurationMs == q.lockDurationMs &&
			candidate.config.expiresAtMs == q.expiresAtMs && candidate.positions+len(items) <= MaxRenewBatch &&
			candidate.bytes+size <= maxRenewalMergeBytes {
			g = candidate
		}
	}
	created := false
	if g == nil {
		if w.renewGroups >= maxPendingRenewalGroups {
			w.mu.Unlock()
			d.closeMu.Unlock()
			return nil, nil, false // original caller-owned FIFO; no extra worker
		}
		groupCtx, attempt := startStorage(renewWriteContext(context.Background()), "write")
		groupCtx, cancel := context.WithCancel(groupCtx)
		g = &renewalGroup{e: e, queue: queue, config: q, ctx: groupCtx, cancel: cancel, attempt: attempt}
		g.waiter = &remoteWriteWaiter{ready: make(chan struct{}), queue: &w.renew, renewal: g}
		g.waiter.element = w.renew.PushBack(g.waiter)
		w.renewGroups++
		d.inFlight.Add(1)
		created = true
	}
	_, attempt := startStorage(ctx, "write")
	m := &renewalMember{ctx: ctx, items: append([]SettleItem(nil), items...), done: make(chan struct{}), active: true, attempt: attempt}
	g.members = append(g.members, m)
	g.positions += len(items)
	g.bytes += size
	w.mu.Unlock()
	d.closeMu.Unlock()
	if created {
		go g.run()
	}
	out, err := g.wait(m)
	return out, err, true
}

func (g *renewalGroup) leave(m *renewalMember) {
	w := &g.e.db.writer
	w.mu.Lock()
	cancel := g.leaveLocked(m)
	w.mu.Unlock()
	if cancel {
		g.cancel()
	}
}

func (g *renewalGroup) leaveLocked(m *renewalMember) bool {
	m.active = false
	live := false
	if !g.completed {
		for _, member := range g.members {
			if member.active && member.ctx.Err() == nil {
				live = true
				break
			}
		}
	}
	if !live {
		// Permanently close joins before canceling: a fresh caller must not
		// inherit an abandoned group still awaiting FIFO removal.
		g.sealed = true
	}
	return !live
}

func (g *renewalGroup) wait(m *renewalMember) (out []SettleResult, resultErr error) {
	resultErr = fmt.Errorf("%w: renewal caller exited before completion", ErrOutcomeUnknown)
	defer func() {
		g.leave(m)
		m.attempt.retries.Store(g.attempt.retries.Load())
		g.e.db.finishStorage(m.attempt, resultErr)
	}()
	select {
	case <-m.done:
	case <-m.ctx.Done():
	}
	w := &g.e.db.writer
	w.mu.Lock()
	complete, reply := g.completed, g.reply
	// Serialize departure against panic publication. A panic already handed
	// to this caller is propagated even when cancellation is also ready.
	cancel := false
	if !complete {
		cancel = g.leaveLocked(m)
	}
	w.mu.Unlock()
	if cancel {
		g.cancel()
	}
	if complete && reply.panicking {
		panic(reply.panic)
	}
	if err := m.ctx.Err(); err != nil {
		return nil, err
	}
	if reply.err != nil {
		return nil, reply.err
	}
	out = renewalResults(m.items, reply.deadlines)
	now := g.e.now() // each caller answers about a lease still live at its return
	for i := range out {
		if out[i].Ok && out[i].LockedUntilMs <= now {
			out[i].Ok, out[i].LockedUntilMs = false, 0
		}
	}
	return out, nil
}

// Re-evaluate cancellation after acquiring a connection, on EVERY attempt.
// Sealed membership may shrink, but cannot accept callers from a later FIFO.
func (g *renewalGroup) liveRows() []SettleItem {
	w := &g.e.db.writer
	w.mu.Lock()
	rows := make([]SettleItem, 0, g.positions)
	seen := make(map[SettleItem]bool)
	for _, m := range g.members {
		if !m.active || m.ctx.Err() != nil {
			continue
		}
		for _, it := range m.items {
			if it.LockToken != "" && !seen[it] {
				seen[it] = true
				rows = append(rows, it)
			}
		}
	}
	w.mu.Unlock()
	if len(rows) == 0 {
		g.cancel()
	}
	return rows
}

func (g *renewalGroup) execute() (map[string]int64, error) {
	d := g.e.db
	var deadlines map[string]int64
	err := d.retryRemoteReturning(g.ctx, func() (bool, bool, error) {
		var release func()
		var err error
		if g.waiter != nil {
			p := g.waiter
			g.waiter = nil
			release, err = d.writer.await(g.ctx, p)
		} else {
			// Safe retries rejoin the renew FIFO at its tail and count another
			// grant against the unchanged fairness limit.
			release, err = d.writer.acquire(g.ctx)
		}
		if err != nil {
			return false, false, err
		}
		defer release()
		conn, err := d.sql.Conn(g.ctx)
		if err != nil {
			return false, false, err
		}
		defer conn.Close()
		rows := g.liveRows()
		if err := g.ctx.Err(); err != nil {
			return false, false, err
		}
		var issued, queried bool
		deadlines, err = g.e.renewBatchStatement(g.ctx, g.queue, g.config, rows,
			func(ctx context.Context, _ string, args func() []any, scan func(*sql.Rows) error) error {
				// Reading the clock can itself be preempted. Recheck individual
				// interests after that callback too, before dispatching SQL.
				fresh := args()
				rows := g.liveRows()
				if err := ctx.Err(); err != nil {
					return err
				}
				query := renewalSQL(len(rows))
				fresh = append(fresh[:3:3], pairArgs(g.queue, rows)...)
				var queryErr error
				issued, queried, queryErr = remoteReturningOnConn(ctx, conn, query, func() []any { return fresh }, scan)
				return queryErr
			})
		return issued, queried, err
	})
	return deadlines, err
}

func (g *renewalGroup) run() {
	reply := renewalReply{err: fmt.Errorf("%w: renewal worker exited before completion", ErrOutcomeUnknown)}
	returned := false
	defer func() { g.finish(reply) }()
	// Relay, rather than swallow, a driver/clock panic to waiting callers. The
	// nested defer distinguishes Goexit (which never resumes here) from panic,
	// including panic(nil). All paths still finish the real SQL/Close owner.
	func() {
		defer func() {
			if !returned {
				reply.panic = recover()
			}
		}()
		reply.deadlines, reply.err = g.execute()
		returned = true
	}()
	if !returned {
		reply.panicking = true
	}
}

func (g *renewalGroup) finish(reply renewalReply) {
	d := g.e.db
	w := &d.writer
	w.mu.Lock()
	// beforeRemoteAttempt can reject an already-canceled group before the
	// first closure consumes its ticket. A pending or racing grant must not
	// survive this owner and strand the single writer.
	if p := g.waiter; p != nil {
		if p.element != nil {
			p.queue.Remove(p.element)
			p.element = nil
		}
		if p.granted {
			w.releaseLocked()
		}
		g.waiter = nil
	}
	g.sealed, g.completed, g.reply = true, true, reply
	w.renewGroups--
	listeners := false
	for _, m := range g.members {
		listeners = listeners || m.active
		close(m.done)
	}
	w.mu.Unlock()
	g.cancel()
	d.inFlight.Done()
	if reply.panicking && !listeners {
		panic(reply.panic) // no caller remains to propagate the original panic
	}
}
