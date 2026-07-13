package main

// The data-plane / admin commands that make the CLI a high-value first-party client for the
// common broker operations (MQLITE-92): settlement by lock token, schedule/cancel, deferred
// receive, status, test-filter, list-subscriptions. Each works in client and embedded mode
// via the shared `api` interface, and honors --output text|json. Under --output json a message
// IS a wire.Message — the same struct, and therefore the same keys, the HTTP API returns.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/wire"
)

// A message in --output json IS a wire.Message — the exact struct the HTTP API returns, not a
// CLI-specific lookalike. Emitting the canonical type (rather than a hand-copied view that
// renamed seq_number to seq and quietly dropped visible_at_ms/locked_until_ms) means
// `mqlite receive --output json` and a raw POST to /mqlite.v1.Queue/Receive produce the same
// keys, and a field added to the wire can never silently go missing from the CLI. Bodies are
// base64 and timestamps epoch-ms because that is what the wire does. Pinned by
// TestCLIJSONIsWireShape (round-2 §3.4).
//
// The lock token is emitted only where the caller needs it to settle later (receive --no-ack,
// receive-deferred); peek never locks, so it has none.

// msIfSet returns t as epoch-ms, or 0 for the zero time (so omitempty drops it).
func msIfSet(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func viewPeeked(ms []*mqlite.PeekedMessage) []wire.Message {
	out := make([]wire.Message, len(ms))
	for i, m := range ms {
		out[i] = wire.Message{
			SeqNumber: m.SequenceNumber, State: string(m.State), DeliveryCount: m.DeliveryCount,
			MessageID: m.MessageID, GroupID: m.GroupID, CorrelationID: m.CorrelationID,
			Subject: m.Subject, ReplyTo: m.ReplyTo, ContentType: m.ContentType,
			Body: m.Body, Properties: m.Properties,
			EnqueuedAtMs: msIfSet(m.EnqueuedAt), VisibleAtMs: msIfSet(m.VisibleAt),
			ExpiresAtMs: msIfSet(m.ExpiresAt), LockedUntilMs: msIfSet(m.LockedUntil),
			DeadLetterReason: m.DeadLetterReason, DeadLetterDescription: m.DeadLetterDescription,
		}
	}
	return out
}

func viewMsg(m *mqlite.Message, withToken bool) wire.Message {
	v := wire.Message{
		SeqNumber: m.SequenceNumber, DeliveryCount: m.DeliveryCount, MessageID: m.MessageID,
		GroupID: m.GroupID, CorrelationID: m.CorrelationID, Subject: m.Subject,
		ReplyTo: m.ReplyTo, ContentType: m.ContentType,
		Body: m.Body, Properties: m.Properties,
		EnqueuedAtMs: msIfSet(m.EnqueuedAt), LockedUntilMs: msIfSet(m.LockedUntil),
	}
	if withToken {
		v.LockToken = m.LockToken()
	}
	return v
}

// okResult prints a mutating command's result: a compact "ok: k=v …" line, or the object
// as JSON under --output json.
func okResult(fields map[string]any, order ...string) error {
	if jsonOut() {
		return emitJSON(fields)
	}
	var b strings.Builder
	b.WriteString("ok:")
	for _, k := range order {
		fmt.Fprintf(&b, " %s=%v", k, fields[k])
	}
	fmt.Println(b.String())
	return nil
}

// ─── settlement: complete / abandon / reject / defer / renew ────────────────────────
//
// Settle a message received earlier with `receive --no-ack` (which prints its lock
// token). This is what makes the CLI a full, stateless queue client: receive now, settle
// in a separate invocation.
func cmdSettle(ctx context.Context, verb string, args []string) error {
	fs := newFlags(verb)
	var delay time.Duration
	var reason, detail string
	if verb == "abandon" {
		fs.DurationVar(&delay, "delay", 0, "re-hide this long before redelivery (backoff)")
	}
	if verb == "reject" {
		fs.StringVar(&reason, "reason", "", "dead-letter reason")
		fs.StringVar(&detail, "detail", "", "dead-letter detail")
	}
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 3 {
		return fmt.Errorf("usage: %s <queue> <seq> <lock-token>", verb)
	}
	seq, err := strconv.ParseInt(pos[1], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid seq %q: %w", pos[1], err)
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	m := c.Message(pos[0], seq, pos[2])
	switch verb {
	case "complete":
		err = m.Complete(ctx)
	case "abandon":
		err = m.Abandon(ctx, mqlite.AbandonOpts{Delay: delay})
	case "reject":
		err = m.Reject(ctx, mqlite.RejectOpts{Reason: reason, Detail: detail})
	case "defer":
		err = m.Defer(ctx)
	case "renew":
		err = m.Renew(ctx)
	}
	if err != nil {
		return err
	}
	return okResult(map[string]any{"action": verb, "queue": pos[0], "seq": seq}, "action", "queue", "seq")
}

// ─── schedule: send with a future delivery time ─────────────────────────────────────
func cmdSchedule(ctx context.Context, args []string) error {
	fs := newFlags("schedule")
	at := fs.String("at", "", "delivery time: RFC3339 (2026-01-02T15:04:05Z) or a duration from now (e.g. 30m)")
	file := fs.String("file", "", "read body from file")
	msgID := fs.String("message-id", "", "message id (dedup/idempotency key)")
	group := fs.String("group", "", "group id (MessageGroupId)")
	subject := fs.String("subject", "", "subject (label)")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 || *at == "" {
		return fmt.Errorf("usage: schedule <queue> <body|-> --at <RFC3339|duration> [--file f --message-id id --group g --subject s]")
	}
	when, err := parseWhen(*at)
	if err != nil {
		return err
	}
	body, err := readBody(*file, pos[1:])
	if err != nil {
		return err
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	seq, err := c.SendOne(ctx, pos[0], mqlite.OutMessage{
		Body: body, MessageID: *msgID, GroupID: *group, Subject: *subject,
	}, mqlite.SendOpts{At: when})
	if err != nil {
		return err
	}
	return okResult(map[string]any{"queue": pos[0], "seq": seq, "at": when.UTC().Format(time.RFC3339Nano)}, "queue", "seq", "at")
}

// parseWhen accepts an absolute RFC3339 timestamp or a duration from now, and requires the
// result to be in the future — a clear CLI-side preflight for the common "typed a past date"
// mistake. The authoritative future check is enforced by the broker against ITS clock
// (engine.validateScheduleTime), so a skewed CLI host can't decide it (review 2026-07-12 P2-1).
func parseWhen(s string) (time.Time, error) {
	var when time.Time
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		when = t
	} else if d, derr := time.ParseDuration(s); derr == nil {
		when = time.Now().Add(d)
	} else {
		return time.Time{}, fmt.Errorf("invalid --at %q: want RFC3339 (2026-01-02T15:04:05Z) or a duration (30m)", s)
	}
	if !when.After(time.Now()) {
		return time.Time{}, fmt.Errorf("--at must be in the future (got %s)", when.Format(time.RFC3339))
	}
	return when, nil
}

// ─── cancel: delete a not-yet-activated scheduled message ───────────────────────────
func cmdCancel(ctx context.Context, args []string) error {
	fs := newFlags("cancel")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("usage: cancel <queue> <seq>")
	}
	seq, err := strconv.ParseInt(pos[1], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid seq %q: %w", pos[1], err)
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Cancel(ctx, pos[0], seq); err != nil {
		return err
	}
	return okResult(map[string]any{"queue": pos[0], "seq": seq, "action": "cancel"}, "action", "queue", "seq")
}

// ─── receive-deferred: fetch set-aside messages back by seq_number ───────────────────
func cmdReceiveDeferred(ctx context.Context, args []string) error {
	fs := newFlags("receive-deferred")
	seqCSV := fs.String("seq", "", "comma-separated deferred seq_numbers to fetch (e.g. 42,57)")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 || *seqCSV == "" {
		return fmt.Errorf("usage: receive-deferred <queue> --seq 42,57")
	}
	seqs, err := parseSeqCSV(*seqCSV)
	if err != nil {
		return err
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	msgs, err := c.Receive(ctx, pos[0], mqlite.RecvOpts{Pick: seqs})
	if err != nil {
		return err
	}
	return printMsgs(msgs, true) // deferred fetch re-locks: show tokens so they can be settled
}

func parseSeqCSV(s string) ([]int64, error) {
	var out []int64
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid seq %q: %w", part, err)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--seq listed no numbers")
	}
	return out, nil
}

// ─── status: desensitized backend snapshot ──────────────────────────────────────────
func cmdStatus(ctx context.Context, args []string) error {
	fs := newFlags("status")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 {
		return fmt.Errorf("usage: status  (takes no arguments)")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	s, err := c.Status(ctx)
	if err != nil {
		return err
	}
	if jsonOut() {
		return emitJSON(s)
	}
	if s.Version != "" {
		fmt.Printf("version:       %s\n", s.Version)
	}
	fmt.Printf("backend:       %s\n", s.Backend)
	fmt.Printf("location:      %s\n", s.Location)
	fmt.Printf("schema:        %s\n", s.SchemaVersion)
	fmt.Printf("ping:          %d ms\n", s.PingMs)
	if !s.Remote {
		fmt.Printf("size:          %d bytes\n", s.DBSizeBytes)
	}
	fmt.Printf("queues:        %d\n", s.Queues)
	fmt.Printf("subscriptions: %d\n", s.Subscriptions)
	if s.UptimeMs > 0 { // broker only — an embedded engine has no process uptime
		fmt.Printf("uptime:        %s\n", (time.Duration(s.UptimeMs) * time.Millisecond).Round(time.Second))
		fmt.Printf("auth:          %t\n", s.Auth)
	}
	return nil
}

// ─── list-subscriptions: topic membership + filter expressions ──────────────────────
func cmdListSubscriptions(ctx context.Context, args []string) error {
	fs := newFlags("list-subscriptions")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 {
		return fmt.Errorf("usage: list-subscriptions  (takes no arguments)")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	subs, err := c.ListSubscriptions(ctx)
	if err != nil {
		return err
	}
	if jsonOut() {
		if subs == nil {
			subs = []mqlite.SubscriptionInfo{} // JSON must be [] not null
		}
		return emitJSON(subs)
	}
	if len(subs) == 0 {
		fmt.Println("(no subscriptions)")
		return nil
	}
	for _, s := range subs {
		expr := s.Expr
		if expr == "" {
			expr = "(match all)"
		}
		fmt.Printf("%-20s topic=%-20s filter=%s\n", s.Name, s.Topic, expr)
	}
	return nil
}

// ─── test-filter: dry-run a subscription filter expression ──────────────────────────
func cmdTestFilter(ctx context.Context, args []string) error {
	fs := newFlags("test-filter")
	file := fs.String("file", "", "read the sample body from a file")
	subject := fs.String("subject", "", "sample message subject")
	group := fs.String("group", "", "sample message group id")
	propCSV := fs.String("prop", "", "sample properties as k=v,k=v")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("usage: test-filter <expr> [<body>|--file f --subject s --group g --prop k=v,k=v]")
	}
	expr := pos[0]

	var sample *mqlite.OutMessage
	hasSample := len(pos) > 1 || *file != "" || *subject != "" || *group != "" || *propCSV != ""
	if hasSample {
		var body []byte
		if len(pos) > 1 || *file != "" { // a body is optional — a filter may test subject/props only
			body, err = readBody(*file, pos[1:])
			if err != nil {
				return err
			}
		}
		props, err := parseProps(*propCSV)
		if err != nil {
			return err
		}
		sample = &mqlite.OutMessage{Body: body, Subject: *subject, GroupID: *group, Properties: props}
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	res, err := c.TestFilter(ctx, expr, sample, 0, 0)
	if err != nil {
		return err
	}
	// The outcome error is the same in both output modes, so a script's exit status does
	// not change just because it asked for JSON.
	var outErr error
	switch {
	case !res.Valid:
		outErr = fmt.Errorf("invalid filter: %s", res.Error)
	case res.Ran && res.Error != "":
		outErr = fmt.Errorf("filter errored on the sample: %s", res.Error)
	}
	if jsonOut() {
		if err := emitJSON(res); err != nil {
			return err
		}
		return outErr
	}
	if outErr != nil {
		return outErr
	}
	if !res.Ran {
		fmt.Println("ok: expression is valid (no sample given)")
		return nil
	}
	fmt.Printf("ok: valid; sample %s\n", map[bool]string{true: "MATCHED", false: "did NOT match"}[res.Matched])
	return nil
}

func parseProps(csv string) (map[string]string, error) {
	if csv == "" {
		return nil, nil
	}
	props := map[string]string{}
	for _, kv := range strings.Split(csv, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("invalid --prop %q: want k=v", kv)
		}
		props[strings.TrimSpace(k)] = v
	}
	return props, nil
}
