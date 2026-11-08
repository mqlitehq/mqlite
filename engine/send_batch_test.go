package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRemoteSendPlanPreservesDedupDecisions(t *testing.T) {
	q := queueRow{name: "q", dedupWindowMs: 1000}
	ms := []OutMessage{
		{MessageID: "existing", Body: []byte("original")},
		{MessageID: "existing", Body: []byte("different")},
		{MessageID: "legacy", Body: []byte("any body remains a legacy duplicate")},
		{MessageID: "new", Body: []byte("first"), Subject: "first metadata"},
		{MessageID: "new", Body: []byte("first"), Subject: "different metadata is still a duplicate"},
		{MessageID: "new", Body: []byte("conflict")},
		{Body: []byte("content addressed")},
		{MessageID: sha256hex([]byte("content addressed")), Body: []byte("content addressed")},
		{MessageID: sha256hex([]byte("content addressed")), Body: []byte("collision")},
	}
	inputs, err := prepareDirectSend(context.Background(), q, ms, 0, StateActive, 9000)
	if err != nil {
		t.Fatal(err)
	}
	existing := map[string]directDedupEntry{
		"existing": {seq: 71, hash: sql.NullString{String: sha256hex([]byte("original")), Valid: true}},
		"legacy":   {seq: 104, hash: sql.NullString{}},
	}
	plan := planDirectSend(inputs, true, existing)
	if len(plan.inserts) != 2 || plan.deduplicated != 4 || plan.rejected != 3 {
		t.Fatalf("unexpected planned effects: inserts=%d duplicate=%d conflict=%d", len(plan.inserts), plan.deduplicated, plan.rejected)
	}
	// Arbitrary, non-contiguous identities demonstrate that slots reference
	// statement results rather than guessing a contiguous rowid interval.
	inserted := []int64{900, 1201}
	wantSeqs := []int64{71, 0, 104, 900, 900, 0, 1201, 1201, 0}
	wantConflicts := []bool{false, true, false, false, false, true, false, false, true}
	for i, slot := range plan.slots {
		seq := slot.seq
		if slot.pending > 0 {
			seq = inserted[slot.pending-1]
		}
		if seq != wantSeqs[i] || slot.conflict != wantConflicts[i] {
			t.Errorf("slot %d: seq=%d conflict=%t", i, seq, slot.conflict)
		}
	}
	if len(existing) != 2 {
		t.Fatal("planning mutated the committed lookup snapshot")
	}
	if plan.inserts[0].message.Subject != "first metadata" {
		t.Fatal("duplicate replaced the first message's metadata")
	}
	// A retry starts from the same committed snapshot, not prior provisional IDs.
	if retry := planDirectSend(inputs, true, existing); !reflect.DeepEqual(retry, plan) {
		t.Fatal("replayed plan retained provisional state")
	}
}

func TestRemoteSendPlanLeavesDedupDisabledDuplicatesDistinct(t *testing.T) {
	ms := []OutMessage{{MessageID: "same", Body: []byte("a")}, {MessageID: "same", Body: []byte("a")}, {MessageID: "same", Body: []byte("b")}}
	inputs, err := prepareDirectSend(context.Background(), queueRow{name: "q"}, ms, 0, StateActive, 10)
	if err != nil {
		t.Fatal(err)
	}
	plan := planDirectSend(inputs, false, nil)
	if len(plan.inserts) != 3 || plan.deduplicated != 0 || plan.rejected != 0 {
		t.Fatal("disabled dedup collapsed or rejected messages")
	}
	for i, slot := range plan.slots {
		if slot.pending != i+1 {
			t.Fatal("input order was changed")
		}
	}
}

func TestRemoteSendPreparationRetainsTimingAndValidation(t *testing.T) {
	q := queueRow{name: "q", defaultTTLMs: 100, dedupWindowMs: 1000}
	inputs, err := prepareDirectSend(context.Background(), q, []OutMessage{{TTLMs: 200}, {TTLMs: 50}, {}}, 5000, StateScheduled, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{5100, 5050, 5100} {
		if inputs[i].visibleAt != 5000 || inputs[i].expiresAt != want {
			t.Fatalf("timing slot %d: %+v", i, inputs[i])
		}
	}
	q = queueRow{ordering: OrderGroupFIFO}
	_, err = prepareDirectSend(context.Background(), q, []OutMessage{{TTLMs: math.MaxInt64}}, 0, StateActive, 2)
	if !errors.Is(err, ErrGroupRequired) {
		t.Fatal("group validation no longer precedes TTL validation")
	}
	_, err = prepareDirectSend(context.Background(), q, []OutMessage{{GroupID: "g", TTLMs: math.MaxInt64}, {}}, 0, StateActive, 2)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("earlier overflowing TTL no longer precedes a later missing group")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = prepareDirectSend(ctx, queueRow{}, []OutMessage{{}}, 0, StateActive, 10); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled planning continued")
	}
}

func TestRemoteSendBatchEligibilityKeepsFencedRoutesOnLegacyPath(t *testing.T) {
	e := &Engine{db: &db{remote: true, dsn: "libsql://fixture.invalid"}}
	plain := []target{{name: "q", queue: queueRow{name: "q"}}}
	ms := []OutMessage{{Body: []byte("a")}, {Body: []byte("b")}}
	if !e.canBatchDirectSend(plain, ms) {
		t.Fatal("plain HTTP target was not eligible")
	}
	for _, targets := range [][]target{nil, {plain[0], plain[0]}, {{name: "sub", fanout: true}}, {{name: "temporary", queue: queueRow{expiresAtMs: 99}}}} {
		if e.canBatchDirectSend(targets, ms) {
			t.Fatal("fenced/fan-out route was batched")
		}
	}
	for _, dsn := range []string{"file:test.db", "wss://fixture.invalid"} {
		e.db.dsn = dsn
		if e.canBatchDirectSend(plain, ms) {
			t.Fatal("non-HTTP driver was batched")
		}
	}
	e.db.dsn = "libsql://fixture.invalid"
	if e.canBatchDirectSend(plain, []OutMessage{{MessageID: "invalid-\xff"}, {}}) || e.canBatchDirectSend(plain, ms[:1]) {
		t.Fatal("legacy normalization or single-message path changed")
	}
}

func TestRemoteSendArgumentBudgetKeepsOversizedSingleMessageAllowed(t *testing.T) {
	args, err := messageInsertArgs("q", OutMessage{Body: []byte("payload"), Properties: map[string]string{"x": "y"}}, 12, 34, StateActive, 56)
	if err != nil || len(args) != 13 || insertArgsBytes(args) > remoteSendChunkBytes {
		t.Fatal("normal insert argument shape changed")
	}
	if args[2] != int64(12) || args[3] != int64(56) || args[4] != int64(34) {
		t.Fatal("timestamp binding order changed")
	}
	args, err = messageInsertArgs("q", OutMessage{Body: make([]byte, remoteSendChunkBytes)}, 0, 0, StateActive, 0)
	if err != nil || insertArgsBytes(args) <= remoteSendChunkBytes {
		t.Fatal("oversized message must use single-message transport, not be rejected")
	}
	for _, m := range []OutMessage{
		{Subject: strings.Repeat("x", remoteSendChunkBytes)},
		{Properties: map[string]string{"large": strings.Repeat("x", remoteSendChunkBytes)}},
	} {
		args, err = messageInsertArgs("q", m, 0, 0, StateActive, 0)
		if err != nil || insertArgsBytes(args) <= remoteSendChunkBytes {
			t.Fatal("nullable metadata was omitted from the transport byte bound")
		}
	}
	if boundedJSONTextBytes(strings.Repeat("x", remoteSendChunkBytes)) <= remoteSendChunkBytes {
		t.Fatal("oversized text arithmetic was not bounded")
	}
	args, err = messageInsertArgs("q", OutMessage{}, 0, 0, StateActive, 0)
	if err != nil || args[12] == nil || len(args[12].([]byte)) != 0 {
		t.Fatal("nil body normalization changed")
	}
}

func TestRemoteSendBatchRealDedupWindowAndLegacyRows(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	e := f.openEngine(t, true)
	e.now = func() int64 { return 10000 }
	ctx := context.Background()
	if err := e.CreateQueue(ctx, "q", QueueConfig{DedupWindowMs: 1000}); err != nil {
		t.Fatal(err)
	}
	original, err := e.SendOne(ctx, "q", OutMessage{MessageID: "existing", Body: []byte("original")})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := e.Receive(ctx, "q", ReceiveOptions{})
	if err != nil || len(messages) != 1 {
		t.Fatalf("initial receive=%d err=%v", len(messages), err)
	}
	if err = e.Complete(ctx, "q", original, messages[0].LockToken); err != nil {
		t.Fatal(err)
	}
	// Dedup intentionally outlives the settled message. Legacy rows have a NULL
	// hash, and seen_at exactly at the window boundary must be treated as expired.
	if _, err = f.db.Exec(`INSERT INTO dedup(queue,message_id,request_hash,seq_number,seen_at)
		VALUES ('q','legacy',NULL,900001,10000),('q','boundary','old hash',900002,9000)`); err != nil {
		t.Fatal(err)
	}
	body := []byte("content addressed")
	inputs := []OutMessage{
		{MessageID: "existing", Body: []byte("original")},
		{MessageID: "existing", Body: []byte("different")},
		{MessageID: "legacy", Body: []byte("different from unknown legacy body")},
		{MessageID: "new", Body: []byte("first"), Subject: "first metadata", TTLMs: 50},
		{MessageID: "new", Body: []byte("first"), Subject: "ignored later metadata", TTLMs: 90},
		{MessageID: "new", Body: []byte("conflict")},
		{Body: body},
		{MessageID: sha256hex(body), Body: body},
		{MessageID: "boundary", Body: []byte("new after exact boundary")},
		{},
		{Body: []byte{}},
	}
	seqs, err := e.Send(ctx, "q", inputs...)
	if err != nil || len(seqs) != len(inputs) {
		t.Fatalf("sequences=%v err=%v", seqs, err)
	}
	if seqs[0] != original || seqs[1] != 0 || seqs[2] != 900001 || seqs[5] != 0 ||
		seqs[3] <= 0 || seqs[3] != seqs[4] || seqs[6] <= 0 || seqs[6] != seqs[7] ||
		seqs[8] <= 0 || seqs[8] == 900002 || seqs[9] <= 0 || seqs[9] != seqs[10] {
		t.Fatalf("dedup identities changed: %v", seqs)
	}
	peeked, err := e.Peek(ctx, "q", PeekOptions{Max: 32})
	if err != nil || len(peeked) != 4 {
		t.Fatalf("peeked=%d err=%v", len(peeked), err)
	}
	if peeked[0].SeqNumber != seqs[3] || peeked[0].Subject != "first metadata" || peeked[0].ExpiresAtMs != 10050 {
		t.Fatal("batch duplicate replaced the original message or its TTL")
	}
	before := f.snapshot()
	replay, err := e.Send(ctx, "q", inputs...)
	after := f.snapshot()
	if err != nil || !reflect.DeepEqual(replay, seqs) || after.MessageInserts != before.MessageInserts {
		t.Fatalf("exact replay changed effects: sequences=%v err=%v", replay, err)
	}
	if calls := after.Requests - before.Requests; calls > 4 {
		t.Fatalf("all-duplicate replay issued unexpected storage calls: %d", calls)
	}
}

func TestRemoteSendBatchScheduledFieldsAndBindingOrder(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	e := f.openEngine(t, true)
	e.now = func() int64 { return 10000 }
	ctx := context.Background()
	if err := e.CreateQueue(ctx, "q", QueueConfig{Ordering: OrderGroupFIFO, DefaultTTLMs: 100}); err != nil {
		t.Fatal(err)
	}
	inputs := make([]OutMessage, 5)
	for i := range inputs {
		inputs[i] = OutMessage{MessageID: fmt.Sprintf("id-%d", i), Body: []byte{byte(i), 0, 255},
			GroupID: fmt.Sprintf("group-%d", i), CorrelationID: fmt.Sprintf("correlation-%d", i), ReplyTo: fmt.Sprintf("reply-%d", i),
			ContentType: fmt.Sprintf("content-%d", i), Subject: fmt.Sprintf("subject-%d", i),
			Properties: map[string]string{"unicode-\u952e": fmt.Sprintf("value-%d-\u503c", i)}, TTLMs: int64((i + 1) * 30)}
	}
	seqs, err := e.ScheduleBatch(ctx, "q", inputs, 15000)
	if err != nil {
		t.Fatal(err)
	}
	peeked, err := e.Peek(ctx, "q", PeekOptions{Max: 32, State: StateScheduled})
	if err != nil || len(peeked) != len(inputs) {
		t.Fatalf("peeked=%d err=%v", len(peeked), err)
	}
	for i, message := range peeked {
		want := inputs[i]
		ttl := want.TTLMs
		if ttl > 100 {
			ttl = 100
		}
		if message.SeqNumber != seqs[i] || !reflect.DeepEqual(message.Body, want.Body) || message.MessageID != want.MessageID ||
			message.GroupID != want.GroupID || message.CorrelationID != want.CorrelationID || message.ReplyTo != want.ReplyTo ||
			message.ContentType != want.ContentType || message.Subject != want.Subject || !reflect.DeepEqual(message.Properties, want.Properties) ||
			message.EnqueuedAtMs != 10000 || message.VisibleAtMs != 15000 || message.ExpiresAtMs != 15000+ttl {
			t.Fatalf("slot %d changed fields/bindings: %+v", i, message)
		}
	}
}

func TestRemoteSendBatchFailureRollsBackEveryTransportChunk(t *testing.T) {
	for _, failAt := range []int{0, remoteSendChunkCount + 6} {
		t.Run(fmt.Sprintf("insert_%d", failAt), func(t *testing.T) {
			f := newSQLiteHranaFixture(t)
			e := f.openEngine(t, true)
			ctx := context.Background()
			if err := e.CreateQueue(ctx, "q", QueueConfig{DedupWindowMs: 60000}); err != nil {
				t.Fatal(err)
			}
			inputs := make([]OutMessage, 2*remoteSendChunkCount+2)
			for i := range inputs {
				inputs[i] = OutMessage{MessageID: fmt.Sprintf("id-%03d", i), Body: []byte(fmt.Sprintf("body-%03d", i))}
			}
			if _, err := f.db.Exec(fmt.Sprintf(`CREATE TRIGGER fail_send_insert BEFORE INSERT ON messages
				WHEN NEW.message_id='id-%03d' BEGIN SELECT RAISE(ABORT,'injected send failure'); END`, failAt)); err != nil {
				t.Fatal(err)
			}
			before := f.snapshot()
			seqs, err := e.Send(ctx, "q", inputs...)
			after := f.snapshot()
			if err == nil || seqs != nil {
				t.Fatal("failed atomic batch was acknowledged")
			}
			if after.Commits != before.Commits {
				t.Fatal("failed transport chunk committed part of Send")
			}
			for _, table := range []string{"messages", "dedup"} {
				var count int
				if err := f.db.QueryRow("SELECT count(*) FROM " + table + " WHERE queue='q'").Scan(&count); err != nil || count != 0 {
					t.Fatalf("partial %s rows=%d err=%v", table, count, err)
				}
			}
		})
	}
	// Failing the second dedup chunk must undo both earlier dedup and all messages.
	t.Run("dedup_upsert", func(t *testing.T) {
		f := newSQLiteHranaFixture(t)
		e := f.openEngine(t, true)
		ctx := context.Background()
		if err := e.CreateQueue(ctx, "q", QueueConfig{DedupWindowMs: 60000}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Exec(`CREATE TRIGGER fail_send_dedup BEFORE INSERT ON dedup
			WHEN NEW.message_id='id-070' BEGIN SELECT RAISE(ABORT,'injected dedup failure'); END`); err != nil {
			t.Fatal(err)
		}
		inputs := make([]OutMessage, 2*remoteSendChunkCount+2)
		for i := range inputs {
			inputs[i] = OutMessage{MessageID: fmt.Sprintf("id-%03d", i), Body: []byte("body")}
		}
		before := f.snapshot()
		if seqs, err := e.Send(ctx, "q", inputs...); err == nil || seqs != nil {
			t.Fatal("failed dedup upsert was acknowledged")
		}
		if f.snapshot().Commits != before.Commits {
			t.Fatal("dedup failure committed earlier transport chunks")
		}
		for _, table := range []string{"messages", "dedup"} {
			var count int
			if err := f.db.QueryRow("SELECT count(*) FROM " + table + " WHERE queue='q'").Scan(&count); err != nil || count != 0 {
				t.Fatalf("partial %s rows=%d err=%v", table, count, err)
			}
		}
	})
}

func TestRemoteSendBatchSplitsTransportWithoutSplittingCommit(t *testing.T) {
	for _, variant := range []string{"count", "oversized_message"} {
		t.Run(variant, func(t *testing.T) {
			f := newSQLiteHranaFixture(t)
			e := f.openEngine(t, true)
			ctx := context.Background()
			if err := e.CreateQueue(ctx, "q", QueueConfig{DedupWindowMs: 60000}); err != nil {
				t.Fatal(err)
			}
			count := 2*remoteSendChunkCount + 2
			if variant == "oversized_message" {
				count = 3
			}
			inputs := make([]OutMessage, count)
			for i := range inputs {
				inputs[i] = OutMessage{MessageID: fmt.Sprintf("id-%03d", i), Body: []byte(fmt.Sprintf("body-%03d", i))}
			}
			if variant == "oversized_message" {
				inputs[1].Body = make([]byte, remoteSendChunkBytes)
			}
			before := f.snapshot()
			seqs, err := e.Send(ctx, "q", inputs...)
			after := f.snapshot()
			if err != nil || len(seqs) != len(inputs) {
				t.Fatalf("sequences=%d err=%v", len(seqs), err)
			}
			if after.Commits-before.Commits != 1 || after.MessageInserts-before.MessageInserts != count {
				t.Fatalf("transport chunks changed atomic effects: before=%+v after=%+v", before, after)
			}
			if variant == "count" && after.Requests-before.Requests > 12 {
				t.Fatalf("request count grew per message: %d", after.Requests-before.Requests)
			}
			if _, err := e.Send(ctx, "q", inputs...); err != nil {
				t.Fatal(err)
			}
			metrics, err := e.Stats(ctx, "q")
			if err != nil || metrics.Total != int64(count) {
				t.Fatalf("duplicate replay changed count: %+v err=%v", metrics, err)
			}
		})
	}
}

func TestRemoteSendBatchDelayIsPerTransportChunk(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	e := f.openEngine(t, true)
	ctx := context.Background()
	if err := e.CreateQueue(ctx, "q", QueueConfig{DedupWindowMs: 60000}); err != nil {
		t.Fatal(err)
	}
	inputs := make([]OutMessage, 64)
	for i := range inputs {
		inputs[i] = OutMessage{MessageID: fmt.Sprintf("id-%d", i), Body: []byte("body")}
	}
	f.setDelay(5 * time.Millisecond)
	before := f.snapshot()
	if _, err := e.Send(ctx, "q", inputs...); err != nil {
		t.Fatal(err)
	}
	if calls := f.snapshot().Requests - before.Requests; calls > 6 {
		t.Fatalf("delay amplified through %d HTTP calls", calls)
	}
}

func TestRemoteSendBatchDefiniteBusyRetryRebuildsEffects(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	e := f.openEngine(t, true)
	ctx := context.Background()
	if err := e.CreateQueue(ctx, "q", QueueConfig{DedupWindowMs: 60000}); err != nil {
		t.Fatal(err)
	}
	inputs := []OutMessage{
		{MessageID: "first", Body: []byte("body")},
		{MessageID: "first", Body: []byte("body")},
		{MessageID: "first", Body: []byte("conflict")},
		{MessageID: "second", Body: []byte("other")},
	}
	f.failNextMessageInsert()
	before := f.snapshot()
	seqs, err := e.Send(ctx, "q", inputs...)
	after := f.snapshot()
	if err != nil || len(seqs) != 4 || seqs[0] <= 0 || seqs[0] != seqs[1] || seqs[2] != 0 || seqs[3] <= 0 || seqs[3] == seqs[0] {
		t.Fatalf("replayed batch mapping=%v err=%v", seqs, err)
	}
	if after.Begins-before.Begins != 2 || after.Commits-before.Commits != 1 || after.Busy-before.Busy != 1 {
		t.Fatalf("definite busy did not replay exactly one transaction: before=%+v after=%+v", before, after)
	}
	for _, table := range []string{"messages", "dedup"} {
		var count int
		if err := f.db.QueryRow("SELECT count(*) FROM " + table + " WHERE queue='q'").Scan(&count); err != nil || count != 2 {
			t.Fatalf("retry duplicated %s rows=%d err=%v", table, count, err)
		}
	}
	e.observation.mu.Lock()
	enqueued := e.observation.events[eventKey{queue: "q", event: "enqueued"}]
	deduplicated := e.observation.events[eventKey{queue: "q", event: "deduplicated"}]
	conflicts := e.observation.events[eventKey{queue: "q", event: "dedup_conflict"}]
	e.observation.mu.Unlock()
	if enqueued != 2 || deduplicated != 1 || conflicts != 1 {
		t.Fatalf("uncommitted retry leaked observation: enqueue=%d duplicate=%d conflict=%d", enqueued, deduplicated, conflicts)
	}
}

// Exercise the sequence oracle with malformed driver responses, not a mock of
// the planner. An incomplete result stream must never authorize the commit.
func TestRemoteSendInsertResultSetsRejectIncompleteResponses(t *testing.T) {
	readFailure := errors.New("injected batch read failure")
	closeFailure := errors.New("injected batch close failure")
	for _, variant := range []string{"valid", "first_empty", "later_empty", "missing_set", "extra_set", "extra_row", "nonpositive", "scan_type", "read_error", "set_error", "close_error"} {
		t.Run(variant, func(t *testing.T) {
			data := &sendBatchRows{sets: [][][]driver.Value{{{int64(107)}}, {{int64(240)}}}, failAtSet: -1, failReadSet: -1}
			switch variant {
			case "first_empty":
				data.sets[0] = nil
			case "later_empty":
				data.sets[1] = nil
			case "missing_set":
				data.sets = data.sets[:1]
			case "extra_set":
				data.sets = append(data.sets, [][]driver.Value{{int64(300)}})
			case "extra_row":
				data.sets[0] = append(data.sets[0], []driver.Value{int64(108)})
			case "nonpositive":
				data.sets[0][0][0] = int64(0)
			case "scan_type":
				data.sets[0][0][0] = "not a sequence"
			case "read_error":
				data.failReadSet = 1
				data.readErr = readFailure
			case "set_error":
				data.failAtSet = 1
				data.setErr = readFailure
			case "close_error":
				data.closeErr = closeFailure
			}
			pool := sql.OpenDB(sendBatchRowsConnector{data})
			defer pool.Close()
			rows, err := pool.QueryContext(context.Background(), "SELECT fixture result sets")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			seqs := make([]int64, 2)
			err = scanInsertSets(rows, seqs)
			if variant == "valid" {
				if err != nil || !reflect.DeepEqual(seqs, []int64{107, 240}) {
					t.Fatalf("valid statement mapping=%v err=%v", seqs, err)
				}
			} else if err == nil {
				t.Fatal("incomplete/invalid insertion results were accepted")
			}
			if variant == "close_error" && !errors.Is(err, closeFailure) ||
				(variant == "read_error" || variant == "set_error") && !errors.Is(err, readFailure) {
				t.Fatalf("driver error identity lost: %v", err)
			}
		})
	}
}

type sendBatchRows struct {
	sets                      [][][]driver.Value
	set, row                  int
	failAtSet, failReadSet    int
	setErr, readErr, closeErr error
}

func (*sendBatchRows) Columns() []string { return []string{"id"} }
func (r *sendBatchRows) Close() error    { return r.closeErr }
func (r *sendBatchRows) Next(values []driver.Value) error {
	if r.set == r.failReadSet {
		return r.readErr
	}
	if r.row >= len(r.sets[r.set]) {
		return io.EOF
	}
	copy(values, r.sets[r.set][r.row])
	r.row++
	return nil
}
func (r *sendBatchRows) HasNextResultSet() bool { return r.set+1 < len(r.sets) }
func (r *sendBatchRows) NextResultSet() error {
	if r.set+1 == r.failAtSet {
		return r.setErr
	}
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.set++
	r.row = 0
	return nil
}

type sendBatchRowsConnector struct{ rows *sendBatchRows }

func (c sendBatchRowsConnector) Connect(context.Context) (driver.Conn, error) {
	return sendBatchRowsConn(c), nil
}
func (c sendBatchRowsConnector) Driver() driver.Driver { return sendBatchRowsDriver(c) }

type sendBatchRowsDriver struct{ rows *sendBatchRows }

func (d sendBatchRowsDriver) Open(string) (driver.Conn, error) {
	return sendBatchRowsConn(d), nil
}

type sendBatchRowsConn struct{ rows *sendBatchRows }

func (c sendBatchRowsConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected fixture prepare")
}
func (c sendBatchRowsConn) Close() error { return nil }
func (c sendBatchRowsConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected fixture transaction")
}
func (c sendBatchRowsConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return c.rows, nil
}
