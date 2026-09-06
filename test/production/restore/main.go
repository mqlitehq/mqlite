// Command restore is the SDK fixture worker for run.py. It keeps one engine open
// at explicit barriers so the driver can inspect or back up it read-only.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"time"

	mq "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
)

type entry struct {
	Queue       string        `json:"queue"`
	Sequence    int64         `json:"sequence"`
	Message     mq.OutMessage `json:"message"`
	State       string        `json:"state"`
	Deliveries  int           `json:"deliveries"`
	Token       string        `json:"token"`
	LockedUntil int64         `json:"locked_until"`
	VisibleAt   int64         `json:"visible_at"`
	Reason      string        `json:"reason"`
	Description string        `json:"description"`
}

type manifest struct {
	Now      int64    `json:"now"`
	Prefix   string   `json:"prefix"`
	Entries  []*entry `json:"entries"`
	Receipt  *entry   `json:"receipt"`
	Business string   `json:"business"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (err error) {
	if len(os.Args) != 4 || (os.Args[1] != "seed" && os.Args[1] != "restore") {
		return errors.New("usage: restore-fixture seed|restore DB MANIFEST")
	}
	var f manifest
	if os.Args[1] == "restore" {
		data, readErr := os.ReadFile(os.Args[3])
		if readErr != nil {
			return readErr
		}
		if err := json.Unmarshal(data, &f); err != nil {
			return err
		}
	} else {
		var identity [16]byte
		if _, err := rand.Read(identity[:]); err != nil {
			return err
		}
		f.Prefix = hex.EncodeToString(identity[:])
		f.Now = time.Now().UnixMilli()
	}
	now := f.Now
	var logs bytes.Buffer
	ctx := context.Background()
	e, err := mq.OpenEmbedded(ctx, "file:"+os.Args[2], mq.WithoutBackground(),
		mq.WithClock(func() int64 { return now }), mq.WithSynchronous("FULL"),
		mq.WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, e.Close()) }()
	if os.Args[1] == "seed" {
		if err := seed(ctx, e, &f); err != nil {
			return fmt.Errorf("seed: %w", err)
		}
		data, err := json.MarshalIndent(f, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(os.Args[3], append(data, '\n'), 0600); err != nil {
			return err
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"event": "ready", "mode": os.Args[1]}); err != nil {
		return err
	}
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return fmt.Errorf("barrier: %w", err)
	}
	if os.Args[1] == "seed" {
		if line != "stop\n" {
			return fmt.Errorf("unexpected seed command %q", line)
		}
	} else {
		if line != "verify\n" {
			return fmt.Errorf("unexpected restore command %q", line)
		}
		if err := verify(ctx, e, &f, &now); err != nil {
			return fmt.Errorf("restore behavior: %w", err)
		}
	}
	if logs.Len() != 0 {
		return fmt.Errorf("unexpected engine logs: %s", logs.String())
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]string{"event": "passed", "mode": os.Args[1]})
}

func message(prefix, label string) mq.OutMessage {
	// Binary data and non-ASCII metadata make body truncation/text conversions loud.
	body := append([]byte("restore:"+prefix+":"+label+":"), bytes.Repeat([]byte{0, 255, 1, 127, 195, 169}, 137)...)
	return mq.OutMessage{MessageID: prefix + ":" + label, Body: body,
		GroupID: "group:" + label, CorrelationID: "correlation:" + label,
		ReplyTo: "reply:" + label, Subject: "keep", ContentType: "application/octet-stream",
		Properties: map[string]string{"region": "eu", "label": label, "unicode": "café"}}
}

func seed(ctx context.Context, e *mq.Embedded, f *manifest) error {
	for _, q := range []string{"active", "locked", "maxed", "deferred", "scheduled", "dlq", "receipt", "outbox", "group", "strict"} {
		cfg := mq.QueueConfig{LockDuration: 30 * time.Minute, MaxDeliveryCount: 3, DedupWindow: time.Hour}
		if q == "maxed" {
			cfg.MaxDeliveryCount = 1
		}
		if q == "active" {
			no := false
			cfg.DeadLetterOnExpire = &no
			cfg.DefaultTTL = 4 * time.Hour
			cfg.DLQMaxAge, cfg.DLQMaxCount, cfg.DLQMaxBytes = -1, -1, -1
		}
		if q == "group" {
			cfg.Ordering = mq.OrderGroupFIFO
		}
		if q == "strict" {
			cfg.Ordering = mq.OrderStrictFIFO
		}
		if err := e.CreateQueue(ctx, q, cfg); err != nil {
			return err
		}
	}
	for _, q := range []string{"active", "locked", "maxed", "deferred", "scheduled", "dlq", "group", "strict"} {
		item := &entry{Queue: q, Message: message(f.Prefix, q), State: "active", VisibleAt: f.Now}
		var opts mq.SendOpts
		if q == "active" {
			item.Message.TTL = 2 * time.Hour
		}
		if q == "scheduled" {
			item.State = "scheduled"
			item.VisibleAt = f.Now + 60000
			opts.At = time.UnixMilli(item.VisibleAt)
		}
		seq, err := e.SendOne(ctx, q, item.Message, opts)
		if err != nil {
			return err
		}
		item.Sequence = seq
		if q == "locked" || q == "maxed" || q == "deferred" || q == "dlq" {
			recv := mq.RecvOpts{}
			if q == "locked" {
				recv.Attempt = f.Prefix + ":before-backup"
			}
			m, err := receive(ctx, e, item, 1, recv)
			if err != nil {
				return err
			}
			item.State, item.Deliveries = "locked", 1
			item.Token, item.LockedUntil = m.LockToken(), m.LockedUntil.UnixMilli()
			if q == "deferred" {
				if err := m.Defer(ctx); err != nil {
					return err
				}
				item.State, item.Token, item.LockedUntil = "deferred", "", 0
			}
			if q == "dlq" {
				item.Reason, item.Description = "FixtureRejected", "preserve full reason and café detail"
				if err := m.Reject(ctx, mq.RejectOpts{Reason: item.Reason, Detail: item.Description}); err != nil {
					return err
				}
				item.State, item.Token, item.LockedUntil = "dead_lettered", "", 0
			}
		}
		f.Entries = append(f.Entries, item)
	}
	if err := e.Subscribe(ctx, "events", "all-events", nil); err != nil {
		return err
	}
	if err := e.Subscribe(ctx, "events", "eu-events", &mq.Filter{Expr: `subject == "keep" && properties["region"] == "eu"`}); err != nil {
		return err
	}
	for _, label := range []string{"topic-keep", "topic-drop"} {
		out := message(f.Prefix, label)
		if label == "topic-drop" {
			out.Subject = "drop"
		}
		if _, err := e.SendOne(ctx, "events", out); err != nil {
			return err
		}
	}
	for _, q := range []string{"all-events", "eu-events"} {
		rows, err := e.Peek(ctx, q, mq.PeekOpts{Max: 10})
		if err != nil {
			return err
		}
		labels := []string{"topic-keep"}
		if q == "all-events" {
			labels = append(labels, "topic-drop")
		}
		if len(rows) != len(labels) {
			return fmt.Errorf("%s fan-out got %d rows, want %d", q, len(rows), len(labels))
		}
		for i, label := range labels {
			out := message(f.Prefix, label)
			if label == "topic-drop" {
				out.Subject = "drop"
			}
			// The sequence is assigned by the broker. Identity/content expectations
			// come from the input, and run.py checks every persisted message field.
			f.Entries = append(f.Entries, &entry{Queue: q, Sequence: rows[i].SequenceNumber, Message: out, State: "active", VisibleAt: f.Now})
		}
	}
	item := &entry{Queue: "outbox", Message: message(f.Prefix, "outbox"), State: "active", VisibleAt: f.Now}
	f.Business = f.Prefix + ":business"
	if err := e.Tx(ctx, func(tx *engine.EngineTx) error {
		if _, err := tx.SQL().ExecContext(tx.Context(), `CREATE TABLE business_orders (id TEXT PRIMARY KEY, amount INTEGER NOT NULL, payload BLOB NOT NULL) STRICT`); err != nil {
			return err
		}
		if _, err := tx.SQL().ExecContext(tx.Context(), `INSERT INTO business_orders VALUES (?, ?, ?)`, f.Business, 12345, item.Message.Body); err != nil {
			return err
		}
		var err error
		item.Sequence, err = tx.SendOne("outbox", engine.OutMessage{MessageID: item.Message.MessageID, Body: item.Message.Body,
			GroupID: item.Message.GroupID, CorrelationID: item.Message.CorrelationID, ReplyTo: item.Message.ReplyTo,
			Subject: item.Message.Subject, ContentType: item.Message.ContentType, Properties: item.Message.Properties})
		return err
	}); err != nil {
		return err
	}
	f.Entries = append(f.Entries, item)
	rollback := errors.New("intentional business rollback")
	rollbackErr := e.Tx(ctx, func(tx *engine.EngineTx) error {
		if _, err := tx.SQL().ExecContext(tx.Context(), `INSERT INTO business_orders VALUES (?, ?, ?)`, "rolled-back", 999, []byte("must not persist")); err != nil {
			return err
		}
		if _, err := tx.SendOne("outbox", engine.OutMessage{MessageID: "rolled-back", Body: []byte("must not persist")}); err != nil {
			return err
		}
		return rollback
	})
	if err := requireError(rollbackErr, rollback, "rollback transaction"); err != nil {
		return err
	}
	// Delete the highest committed ID: restoring sqlite_sequence must preserve
	// its retirement even though the messages table no longer contains that row.
	f.Receipt = &entry{Queue: "receipt", Message: message(f.Prefix, "receipt"), State: "completed"}
	var err error
	f.Receipt.Sequence, err = e.SendOne(ctx, "receipt", f.Receipt.Message)
	if err != nil {
		return err
	}
	m, err := receive(ctx, e, f.Receipt, 1, mq.RecvOpts{})
	if err != nil {
		return err
	}
	f.Receipt.Token = m.LockToken()
	return m.Complete(ctx)
}

func receive(ctx context.Context, e *mq.Embedded, want *entry, count int, opts mq.RecvOpts) (*mq.Message, error) {
	rows, err := e.Receive(ctx, want.Queue, opts)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("%s receive got %d rows, want 1", want.Queue, len(rows))
	}
	m := rows[0]
	w := want.Message
	if m.SequenceNumber != want.Sequence || m.MessageID != w.MessageID || !bytes.Equal(m.Body, w.Body) ||
		m.GroupID != w.GroupID || m.CorrelationID != w.CorrelationID || m.ReplyTo != w.ReplyTo ||
		m.Subject != w.Subject || m.ContentType != w.ContentType || !reflect.DeepEqual(m.Properties, w.Properties) ||
		m.DeliveryCount != count || m.LockToken() == "" {
		return nil, fmt.Errorf("%s/%s delivery differs from identity/content/count expectation", want.Queue, w.MessageID)
	}
	return m, nil
}

func expectEmpty(ctx context.Context, e *mq.Embedded, q string) error {
	rows, err := e.Receive(ctx, q)
	if err != nil {
		return err
	}
	if len(rows) != 0 {
		return fmt.Errorf("%s unexpectedly delivered %d messages", q, len(rows))
	}
	return nil
}

func requireError(got, want error, label string) error {
	if errors.Is(got, want) {
		return nil
	}
	if got == nil {
		return fmt.Errorf("%s: expected %s, got success", label, want.Error())
	}
	return fmt.Errorf("%s: unexpected error: %w", label, got)
}

func verify(ctx context.Context, e *mq.Embedded, f *manifest, now *int64) error {
	// A persisted completion receipt survives restoration; a different operation
	// never inherits that receipt's success.
	r := e.Message(f.Receipt.Queue, f.Receipt.Sequence, f.Receipt.Token)
	if err := r.Complete(ctx); err != nil {
		return fmt.Errorf("completion receipt replay: %w", err)
	}
	if err := requireError(r.Abandon(ctx), mq.ErrLockLost, "completion receipt with different operation"); err != nil {
		return err
	}
	for _, item := range f.Entries {
		if item.Queue == "active" {
			seq, err := e.SendOne(ctx, item.Queue, item.Message)
			if err != nil {
				return err
			}
			if seq != item.Sequence {
				return fmt.Errorf("restored dedup replay: seq=%d want=%d", seq, item.Sequence)
			}
			conflict := item.Message
			conflict.Body = []byte("same ID, different content")
			_, conflictErr := e.SendOne(ctx, item.Queue, conflict)
			if err := requireError(conflictErr, mq.ErrDedupConflict, "restored dedup conflict"); err != nil {
				return err
			}
		}
		if item.Queue == "locked" {
			old, err := receive(ctx, e, item, 1, mq.RecvOpts{Attempt: f.Prefix + ":before-backup"})
			if err != nil {
				return err
			}
			if old.LockToken() != item.Token {
				return errors.New("restored attempt response changed token")
			}
			// Recovery has revoked the old lock. Replaying the old response is
			// not a promise that its token is still valid for settlement.
			if err := requireError(old.Complete(ctx), mq.ErrLockLost, "orphan token settlement"); err != nil {
				return err
			}
			fresh, err := receive(ctx, e, item, 2, mq.RecvOpts{Attempt: f.Prefix + ":after-backup"})
			if err != nil {
				return err
			}
			if fresh.LockToken() == item.Token {
				return errors.New("recovered claim reused orphan token")
			}
			replay, err := receive(ctx, e, item, 2, mq.RecvOpts{Attempt: f.Prefix + ":after-backup"})
			if err != nil {
				return err
			}
			if replay.LockToken() != fresh.LockToken() {
				return errors.New("fresh attempt replay changed token")
			}
			if err := requireError(old.Complete(ctx), mq.ErrLockLost, "orphan token against fresh owner"); err != nil {
				return err
			}
			if err := fresh.Complete(ctx); err != nil {
				return err
			}
			continue
		}
		if item.Queue == "scheduled" || item.Queue == "deferred" || item.Queue == "maxed" || item.Queue == "dlq" {
			if err := expectEmpty(ctx, e, item.Queue); err != nil {
				return err
			}
		}
		if item.Queue == "scheduled" {
			continue
		}
		opts, count := mq.RecvOpts{}, item.Deliveries+1
		if item.Queue == "deferred" {
			opts.Pick = []int64{item.Sequence}
		}
		if item.Queue == "maxed" || item.Queue == "dlq" {
			n, err := e.Redrive(ctx, item.Queue)
			if err != nil {
				return err
			}
			if n != 1 {
				return fmt.Errorf("%s redrive count=%d want=1", item.Queue, n)
			}
			count = 1
		}
		m, err := receive(ctx, e, item, count, opts)
		if err != nil {
			return err
		}
		if err := m.Complete(ctx); err != nil {
			return err
		}
	}
	*now = f.Now + 60000
	e.Engine().RunMaintenanceOnce(ctx)
	for _, item := range f.Entries {
		if item.Queue == "scheduled" {
			m, err := receive(ctx, e, item, 1, mq.RecvOpts{})
			if err != nil {
				return err
			}
			if err := m.Complete(ctx); err != nil {
				return err
			}
		}
	}
	// Recompile/use the persisted filter after reopen. Check both included and
	// excluded messages, with full-body expectations from independent inputs.
	for _, label := range []string{"after-keep", "after-drop"} {
		out := message(f.Prefix, label)
		if label == "after-drop" {
			out.Subject = "drop"
		}
		seq, err := e.SendOne(ctx, "events", out)
		if err != nil {
			return err
		}
		if seq <= f.Receipt.Sequence {
			return fmt.Errorf("restored monotonic sequence: seq=%d retired=%d", seq, f.Receipt.Sequence)
		}
		for _, q := range []string{"all-events", "eu-events"} {
			if q == "eu-events" && label == "after-drop" {
				if err := expectEmpty(ctx, e, q); err != nil {
					return err
				}
				continue
			}
			peek, err := e.Peek(ctx, q, mq.PeekOpts{Max: 2})
			if err != nil {
				return err
			}
			if len(peek) != 1 {
				return fmt.Errorf("restored filter %s: rows=%d want=1", q, len(peek))
			}
			want := &entry{Queue: q, Sequence: peek[0].SequenceNumber, Message: out}
			m, err := receive(ctx, e, want, 1, mq.RecvOpts{})
			if err != nil {
				return err
			}
			if err := m.Complete(ctx); err != nil {
				return err
			}
		}
	}
	queues, err := e.ListQueues(ctx)
	if err != nil {
		return err
	}
	for _, q := range queues {
		stats, err := e.Stats(ctx, q.Name)
		if err != nil {
			return err
		}
		if stats.Total != 0 || stats.Active != 0 || stats.Locked != 0 || stats.Deferred != 0 || stats.Scheduled != 0 || stats.DeadLettered != 0 {
			return fmt.Errorf("%s did not converge: %+v", q.Name, stats)
		}
		rows, err := e.Peek(ctx, q.Name, mq.PeekOpts{Max: 100})
		if err != nil {
			return err
		}
		if len(rows) != 0 {
			return fmt.Errorf("%s has late/extra messages: %d", q.Name, len(rows))
		}
	}
	return nil
}
