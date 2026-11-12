package engine

import (
	"container/list"
	"context"
	"database/sql"
	"sync"
)

// remoteWriter serializes this owner's remote write attempts before they occupy
// SQL connections. The provider pool remains available to ordinary reads.
// Renewals precede settlements, each with its own FIFO and bounded burst, so
// maintenance and sends also progress. Active SQL cannot be preempted.
type remoteWriter struct {
	mu          sync.Mutex
	closed      bool
	held        bool
	urgentBurst int
	renewBurst  int
	normal      list.List
	urgent      list.List
	renew       list.List
	renewGroups int // bounded short-lived owners of pending coalesced renewals
}

const maxLeaseWriteBurst = 4

type remoteWriteWaiter struct {
	ready   chan struct{}
	queue   *list.List
	element *list.Element
	granted bool
	renewal *renewalGroup // nil for ordinary caller-owned attempts
}

type leaseWritePriorityKey struct{}
type renewWritePriorityKey struct{}

func leaseWriteContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, leaseWritePriorityKey{}, true)
}

func (e *Engine) leaseWriteContext(ctx context.Context) context.Context {
	if !e.db.remote {
		return ctx
	}
	return leaseWriteContext(ctx)
}

func renewWriteContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, renewWritePriorityKey{}, true)
}

func (e *Engine) renewWriteContext(ctx context.Context) context.Context {
	if !e.db.remote {
		return ctx
	}
	return renewWriteContext(ctx)
}

func (w *remoteWriter) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	renewal, _ := ctx.Value(renewWritePriorityKey{}).(bool)
	if !w.held && w.normal.Len() == 0 && w.urgent.Len() == 0 && w.renew.Len() == 0 {
		w.held = true
		// A new contention period starts without inherited priority debt.
		w.urgentBurst = 0
		w.renewBurst = 0
		if renewal {
			// Count this grant too if lower-priority work arrives while it runs.
			w.renewBurst = 1
		}
		w.mu.Unlock()
		return w.releaseFunc(), nil
	}
	q := &w.normal
	if renewal {
		q = &w.renew
	} else if urgent, _ := ctx.Value(leaseWritePriorityKey{}).(bool); urgent {
		q = &w.urgent
	}
	p := &remoteWriteWaiter{ready: make(chan struct{}), queue: q}
	p.element = q.PushBack(p)
	w.mu.Unlock()
	return w.await(ctx, p)
}

func (w *remoteWriter) await(ctx context.Context, p *remoteWriteWaiter) (func(), error) {
	select {
	case <-p.ready:
	case <-ctx.Done():
	}
	w.mu.Lock()
	// Cancellation can race a grant. If the grant won, return that permit
	// before reporting cancellation; no SQL connection has been acquired yet.
	if err := ctx.Err(); err != nil {
		if p.element != nil {
			p.queue.Remove(p.element)
			p.element = nil
		}
		if p.granted {
			w.releaseLocked()
		}
		w.mu.Unlock()
		return nil, err
	}
	if w.closed {
		if p.granted {
			w.releaseLocked()
		}
		w.mu.Unlock()
		return nil, ErrClosed
	}
	granted := p.granted
	w.mu.Unlock()
	if !granted {
		// ready is closed only by a grant or Close.
		return nil, ErrClosed
	}
	return w.releaseFunc(), nil
}

func (w *remoteWriter) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.releaseLocked()
		})
	}
}

func (w *remoteWriter) releaseLocked() {
	w.held = false
	if w.closed {
		return
	}
	var q *list.List
	switch {
	case w.renew.Len() > 0 && (w.normal.Len()+w.urgent.Len() == 0 || w.renewBurst < maxLeaseWriteBurst):
		q = &w.renew
		if w.renewBurst < maxLeaseWriteBurst {
			w.renewBurst++
		}
	case w.urgent.Len() > 0 && (w.normal.Len() == 0 || w.urgentBurst < maxLeaseWriteBurst):
		q = &w.urgent
		w.renewBurst = 0
		if w.urgentBurst < maxLeaseWriteBurst {
			w.urgentBurst++
		}
	case w.normal.Len() > 0:
		q = &w.normal
		w.renewBurst = 0
		w.urgentBurst = 0
	default:
		w.renewBurst = 0
		w.urgentBurst = 0
		return
	}
	p := q.Front().Value.(*remoteWriteWaiter)
	q.Remove(p.element)
	p.element = nil
	p.granted = true
	if p.renewal != nil {
		p.renewal.sealed = true // membership cannot grow after this grant
	}
	w.held = true
	close(p.ready)
}

func (w *remoteWriter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	for _, q := range []*list.List{&w.normal, &w.urgent, &w.renew} {
		for q.Len() > 0 {
			p := q.Front().Value.(*remoteWriteWaiter)
			q.Remove(p.element)
			p.element = nil
			close(p.ready)
		}
	}
}

// remoteExecAttempt holds admission until the issued statement and its SQL
// lease have finished. A false issued result is a definite pre-SQL failure.
func (d *db) remoteExecAttempt(ctx context.Context, query string, buildArgs func() []any) (result sql.Result, err error, issued bool) {
	release, err := d.writer.acquire(ctx)
	if err != nil {
		return nil, err, false
	}
	defer release()
	conn, err := d.sql.Conn(ctx)
	if err != nil {
		return nil, err, false
	}
	defer conn.Close()
	args := buildArgs()
	if err := checkCtx(ctx); err != nil {
		return nil, err, false
	}
	result, err = conn.ExecContext(ctx, query, args...)
	return result, err, true
}

// remoteReturningAttempt distinguishes pool/admission, Query, and row-reading
// failures so the existing no-replay contract for an issued RETURNING write is
// retained. Time-sensitive arguments are built only after both waits finish.
func (d *db) remoteReturningAttempt(ctx context.Context, query string, buildArgs func() []any, scan func(*sql.Rows) error) (issued, queried bool, err error) {
	release, err := d.writer.acquire(ctx)
	if err != nil {
		return false, false, err
	}
	defer release()
	conn, err := d.sql.Conn(ctx)
	if err != nil {
		return false, false, err
	}
	defer conn.Close()
	return remoteReturningOnConn(ctx, conn, query, buildArgs, scan)
}

// remoteReturningOnConn is shared by ordinary and coalesced renewals. Its
// caller retains the write permit and SQL lease through rows close and EOF.
func remoteReturningOnConn(ctx context.Context, conn *sql.Conn, query string, buildArgs func() []any, scan func(*sql.Rows) error) (issued, queried bool, err error) {
	args := buildArgs()
	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return true, false, err
	}
	defer rows.Close()
	err = scan(rows)
	if err == nil {
		err = rows.Err()
	}
	closeErr := rows.Close()
	if err == nil {
		err = closeErr
	}
	return true, true, err
}

func (d *db) remoteReturningWrite(ctx context.Context, query string, buildArgs func() []any, scan func(*sql.Rows) error) error {
	if !d.enter() {
		return ErrClosed
	}
	defer d.inFlight.Done()
	return d.retryRemoteReturning(ctx, func() (bool, bool, error) {
		return d.remoteReturningAttempt(ctx, query, buildArgs, scan)
	})
}

// The caller has already registered the operation's Close owner. Every retry
// ends its real SQL lease and permit before backing off and joining the FIFO
// again. Coalescing does not authorize any additional write replay.
func (d *db) retryRemoteReturning(ctx context.Context, attempt func() (issued, queried bool, err error)) error {
	var err error
	for i := 0; i < d.attempts(); i++ {
		if err = d.beforeRemoteAttempt(ctx, i); err != nil {
			return err
		}
		var issued, queried bool
		issued, queried, err = attempt()
		if !issued {
			if ctxErr := checkCtx(ctx); ctxErr != nil {
				return ctxErr
			}
			if d.retryable(err) {
				continue
			}
			return err
		}
		if !queried {
			if d.retryableWrite(err) {
				continue
			}
			if remoteWriteMayHaveCommitted(err) {
				return unknownRemoteWrite(err)
			}
			return err
		}
		if err != nil {
			return unknownRemoteWrite(err)
		}
		return nil
	}
	return err
}

// The permit covers exactly one transaction attempt, through driver Commit or
// Rollback and SQL-lease return. The outer retry loop backs off without it.
func (e *Engine) remoteTransactionAttempt(ctx context.Context, fn func(context.Context, *txn) error, readOnly bool) (ferr, cerr error, commitNotStarted bool, beginErr error) {
	if !readOnly {
		release, err := e.db.writer.acquire(ctx)
		if err != nil {
			return nil, nil, false, err
		}
		defer release()
	}
	observationEpoch := e.observationEpoch()
	conn, err := e.db.sql.Conn(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	// Cancellation can make database/sql's background rollback mark Tx done
	// before the driver finishes. Rollback then returns ErrTxDone immediately;
	// Conn.Close is the barrier that waits for the transaction's SQL lease.
	defer conn.Close()
	if err := checkCtx(ctx); err != nil {
		return nil, nil, false, err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, false, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := checkCtx(ctx); err != nil {
		return nil, nil, false, err
	}
	attempt := &txn{tx: tx, caller: ctx, exec: e.db.stmtCtx(ctx), observationEpoch: observationEpoch}
	if ferr = fn(e.db.stmtCtx(ctx), attempt); ferr != nil {
		return ferr, nil, false, nil
	}
	cerr, commitNotStarted = e.commitAttempt(attempt)
	return nil, cerr, commitNotStarted, nil
}
