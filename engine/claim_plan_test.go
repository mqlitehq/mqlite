package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// MQLITE-22 regression guards. r1 fixed the standard deep-backlog drain with the
// partial idx_msg_active; the same O(n^2) class survived on the ordered-claim
// paths (group_fifo/strict_fifo), where the head-of-line check planned as a
// backward rowid scan — O(n) per candidate, O(n^2) to drain. The fix splits that
// check into one indexable EXISTS per in-flight state.

// explainPlan returns the EXPLAIN QUERY PLAN detail lines for q, joined by '\n'.
func explainPlan(t *testing.T, e *Engine, q string, args ...any) string {
	t.Helper()
	rows, err := e.db.sql.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+q, args...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		b.WriteString(detail + "\n")
	}
	return b.String()
}

// TestClaimPlanPinning pins the two invariants that keep claim O(log n): the outer
// seek uses idx_msg_active (r1 fix), and no inner head-of-line probe plans as the
// `rowid<?` backward scan (the O(n^2) signature).
func TestClaimPlanPinning(t *testing.T) {
	e, _ := testEngine(t)
	seedMixedBacklog(t, e) // a few queues × every in-flight state, ANALYZEd

	dummy := []any{int64(0), "tok", "q2", int64(0), int64(0)} // lockUntil,token,queue,now,now
	for _, tc := range []struct{ name, sql string }{{"group_fifo", claimSQL}, {"strict_fifo", claimStrictSQL}} {
		plan := explainPlan(t, e, tc.sql, dummy...)
		if !strings.Contains(plan, "idx_msg_active") {
			t.Errorf("%s: outer claim no longer seeks idx_msg_active (r1 regression):\n%s", tc.name, plan)
		}
		if strings.Contains(plan, "rowid<") {
			t.Errorf("%s: head-of-line probe scans rowid<? (O(n^2) regression):\n%s", tc.name, plan)
		}
		// Pin the whole surface: a claim plan must be all index SEARCHes — any SCAN
		// (outer or probe) is a planner regression the two checks above might miss.
		if strings.Contains(plan, "SCAN") {
			t.Errorf("%s: claim plan contains a full scan:\n%s", tc.name, plan)
		}
	}
}

// TestMaintenanceQueryPlansPinned pins the background-loop queries to their partial
// indexes. The reaper/scheduler/TTL/DLQ passes run every 1–10s on the single writer, so a
// query or index change that drops one to a full table SCAN would silently degrade the
// whole broker as the table grows — and nothing else catches it. Each must SEARCH via its
// index. (The WHERE clauses mirror background.go; an index drop/rename fails this
// regardless of the SET/SELECT list, which is what the plan ignores.)
func TestMaintenanceQueryPlansPinned(t *testing.T) {
	e, _ := testEngine(t)
	seedMixedBacklog(t, e)

	for _, tc := range []struct {
		name, want, sql string
		args            []any
	}{
		{"reaper: expired locks", "idx_msg_locked",
			`UPDATE messages SET state='active', locked_until=0, lock_token=NULL WHERE state='locked' AND locked_until<=?`,
			[]any{int64(0)}},
		{"scheduler: due scheduled", "idx_msg_scheduled",
			`UPDATE messages SET state='active' WHERE state='scheduled' AND visible_at<=?`,
			[]any{int64(0)}},
		{"ttl: dead-letter expired", "idx_msg_expire",
			`UPDATE messages SET state='dead_lettered' WHERE expires_at>0 AND expires_at<=? AND state IN ('active','locked','deferred','scheduled')`,
			[]any{int64(0)}},
		{"ttl: discard expired", "idx_msg_expire",
			`DELETE FROM messages WHERE expires_at>0 AND expires_at<=? AND state IN ('active','locked','deferred','scheduled')`,
			[]any{int64(0)}},
		{"dlq retention: drop-oldest", "idx_msg_dlq",
			`SELECT id FROM messages WHERE queue=? AND state='dead_lettered' AND enqueued_at < ?`,
			[]any{"q2", int64(0)}},
		{"janitor: prune settlement receipts", "idx_settlement_expire",
			`DELETE FROM settlement_receipts WHERE expires_at < ?`, []any{int64(0)}},
		{"janitor: prune receive attempts", "idx_recv_attempt_expire",
			`DELETE FROM receive_attempts WHERE expires_at < ?`, []any{int64(0)}},
		{"janitor: prune dedup window", "idx_dedup_seen",
			`DELETE FROM dedup WHERE seen_at < ?`, []any{int64(0)}},
	} {
		plan := explainPlan(t, e, tc.sql, tc.args...)
		if !strings.Contains(plan, tc.want) {
			t.Errorf("%s: no longer SEARCHes via %s — full-scan regression:\n%s", tc.name, tc.want, plan)
		}
	}
}

// TestClaimDeepBacklogBounded is the plan-agnostic guard: a claim against a deep
// backlog stuck behind a locked head must stay fast (~20-30s @ 20k before the fix).
func TestClaimDeepBacklogBounded(t *testing.T) {
	if raceEnabled {
		// Under -race the pure-Go SQLite engine is instrumented, so seeding 20k rows
		// takes ~40s and a wall-clock budget is meaningless. TestClaimPlanPinning
		// guards the O(n^2) regression deterministically (and fast) under -race; this
		// timing check runs in the non-race coverage job.
		t.Skip("wall-clock timing is meaningless under -race (instrumented pure-Go SQLite); plan-pin test covers it")
	}
	const n, budget = 20_000, 3 * time.Second
	for _, mode := range []OrderingMode{OrderGroupFIFO, OrderStrictFIFO} {
		t.Run(string(mode), func(t *testing.T) {
			e, ctx := seedBlockedBacklog(t, mode, n)
			start := time.Now()
			msgs, err := e.Receive(ctx, "q", ReceiveOptions{})
			if err != nil || len(msgs) != 0 {
				t.Fatalf("want nothing claimable behind a locked head: got %d, err=%v", len(msgs), err)
			}
			if d := time.Since(start); d > budget {
				t.Fatalf("blocked claim over %d-deep backlog took %v (>%v): O(n^2) regression", n, d, budget)
			}
		})
	}
}

func BenchmarkClaimDeepBacklog(b *testing.B) {
	for _, mode := range []OrderingMode{OrderGroupFIFO, OrderStrictFIFO} {
		b.Run(string(mode), func(b *testing.B) {
			e, ctx := seedBlockedBacklog(b, mode, 50_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if msgs, err := e.Receive(ctx, "q", ReceiveOptions{}); err != nil || len(msgs) != 0 {
					b.Fatalf("receive: got %d err=%v", len(msgs), err)
				}
			}
		})
	}
}

// bulkInsert inserts rows into messages via one prepared statement in a tx, then
// ANALYZEs so the planner has real stats. Each row is (queue, state, lockedUntil, group).
func bulkInsert(tb testing.TB, e *Engine, rows func(exec func(q, state string, lockedUntil int64, group string))) {
	tb.Helper()
	ctx := context.Background()
	tx, err := e.db.sql.BeginTx(ctx, nil)
	if err != nil {
		tb.Fatalf("begin: %v", err)
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO messages(queue,state,locked_until,group_id,enqueued_at,body) VALUES(?,?,?,?,1700000000000,x'00')`)
	if err != nil {
		tb.Fatalf("prepare: %v", err)
	}
	rows(func(q, state string, lockedUntil int64, group string) {
		if _, err := stmt.ExecContext(ctx, q, state, lockedUntil, group); err != nil {
			tb.Fatalf("insert: %v", err)
		}
	})
	stmt.Close()
	if err := tx.Commit(); err != nil {
		tb.Fatalf("commit: %v", err)
	}
	if _, err := e.db.exec(ctx, "ANALYZE"); err != nil {
		tb.Fatalf("analyze: %v", err)
	}
}

func seedMixedBacklog(t *testing.T, e *Engine) {
	t.Helper()
	for _, q := range []string{"q0", "q1", "q2", "q3", "q4"} {
		mustQueue(t, e, q, QueueConfig{Ordering: OrderGroupFIFO})
	}
	states := []struct {
		s  string
		lu int64
	}{{"active", 0}, {"active", 0}, {"active", 0}, {"locked", 2_000_000_000_000}, {"deferred", 0}, {"scheduled", 0}}
	bulkInsert(t, e, func(exec func(q, state string, lu int64, g string)) {
		for i := 0; i < 3000; i++ {
			st := states[i%len(states)]
			exec(fmt.Sprintf("q%d", i%5), st.s, st.lu, fmt.Sprintf("g%d", i%16))
		}
	})
}

// seedBlockedBacklog builds queue "q" with one locked head and n active rows queued
// behind it in the same group — the whole queue/group is head-of-line blocked.
func seedBlockedBacklog(tb testing.TB, ordering OrderingMode, n int) (*Engine, context.Context) {
	tb.Helper()
	ctx := context.Background()
	e, err := Open(ctx, Options{
		DB:                "file:" + tb.TempDir() + "/mq.db",
		Now:               func() int64 { return 1_700_000_000_000 },
		DisableBackground: true,
	})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { _ = e.Close() })
	if err := e.CreateQueue(ctx, "q", QueueConfig{Ordering: ordering}); err != nil {
		tb.Fatalf("create queue: %v", err)
	}
	bulkInsert(tb, e, func(exec func(q, state string, lu int64, g string)) {
		exec("q", "locked", 2_000_000_000_000, "g") // blocking head
		for i := 0; i < n; i++ {
			exec("q", "active", 0, "g")
		}
	})
	return e, ctx
}
