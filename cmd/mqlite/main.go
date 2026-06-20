// Command mqlite is the single-binary CLI: run a broker, or produce/consume/
// administer queues against a local DB (embedded) or a remote broker (client).
//
// Connection (read from env; the DB string is never compiled in):
//
//	MQLITE_DB=file:./mq.db | :memory: | libsql://<db>.turso.io   (embedded mode)
//	MQLITE_DB_AUTH_TOKEN=<jwt>                                    (remote Turso/libSQL)
//	MQLITE_ENDPOINT=http://host:port + MQLITE_TOKEN=<bearer>      (client mode; wins if set)
//	MQLITE_TOKENS=mqk_a,mqk_b                                     (tokens `serve` accepts)
//	MQLITE_MAX_MESSAGE_BYTES=<n>                                  (reject larger bodies)
//	MQLITE_SYNC=NORMAL|FULL|OFF                                   (durability; embedded/serve)
//	MQLITE_DLQ_MAX_AGE=14d-ish (e.g. 336h) · MQLITE_DLQ_MAX_COUNT=1000000 · MQLITE_DLQ_RETENTION=off
//	                                                             (broker DLQ retention; serve)
//
// CLI design (MQLITE-14): subcommands use the standard library `flag` package plus a
// small parseInterspersed helper (so flags may appear before or after positionals),
// with one FlagSet per command and a consistent usage/error/"ok:" style. We
// deliberately do NOT take a cobra/pflag dependency — for ~a dozen simple commands
// the stdlib is sufficient, and staying a dependency-light single binary is a goal.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mqlitehq/mqlite"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	ctx := context.Background()

	var err error
	switch cmd {
	case "serve":
		err = cmdServe(ctx, args)
	case "create-queue":
		err = cmdCreateQueue(ctx, args)
	case "subscribe", "create-subscription":
		err = cmdCreateSubscription(ctx, args)
	case "send":
		err = cmdSend(ctx, args)
	case "receive":
		err = cmdReceive(ctx, args)
	case "peek":
		err = cmdPeek(ctx, args)
	case "metrics":
		err = cmdMetrics(ctx, args)
	case "list":
		err = cmdList(ctx, args)
	case "redrive":
		err = cmdRedrive(ctx, args)
	case "purge-dlq":
		err = cmdPurgeDLQ(ctx, args)
	case "version", "-v", "--version":
		fmt.Println("mqlite", version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`mqlite ` + version + ` — small SQLite/Turso-backed message queue

usage: mqlite <command> [flags]

  serve                  run the HTTP broker (embedded engine + Serve)
  create-queue <name>    create/update a queue
  subscribe <topic> <n>  create a subscription <n> under <topic>
  send <queue> <body>    send a message (body "-" reads stdin; --file reads a file)
  receive <queue>        receive (Peek-Lock, auto-Complete unless --no-ack)
  peek <queue>           browse without locking
  metrics <queue>        show queue counters
  list                   list queues/subscriptions
  redrive <queue>        move dead-lettered messages back to active
  purge-dlq <queue>      permanently delete dead-lettered messages
  version | help

connection via env: MQLITE_ENDPOINT+MQLITE_TOKEN (client) or MQLITE_DB[+token] (embedded)
`)
}

// api is the subset shared by *mqlite.Client and *mqlite.Embedded.
type api interface {
	SendOne(ctx context.Context, queue string, m mqlite.OutMessage, opts ...mqlite.SendOpts) (int64, error)
	Receive(ctx context.Context, queue string, opts ...mqlite.RecvOpts) ([]*mqlite.Message, error)
	Peek(ctx context.Context, queue string, opts ...mqlite.PeekOpts) ([]*mqlite.PeekedMessage, error)
	CreateQueue(ctx context.Context, name string, cfg mqlite.QueueConfig) error
	Subscribe(ctx context.Context, topic, name string, f *mqlite.Filter) error
	ListQueues(ctx context.Context) ([]mqlite.QueueInfo, error)
	Stats(ctx context.Context, queue string) (mqlite.Metrics, error)
	Redrive(ctx context.Context, dlq string, opts ...mqlite.RedriveOpts) (int, error)
	Purge(ctx context.Context, queue string, opts ...mqlite.PurgeOpts) (int, error)
	Close() error
}

// parseInterspersed lets flags appear before OR after positional args
// (Go's flag package stops at the first positional otherwise).
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var positionals []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			break
		}
		positionals = append(positionals, fs.Arg(0))
		args = fs.Args()[1:]
	}
	return positionals, nil
}

// embeddedOpts builds embedded options from the environment (DB token + size cap).
func embeddedOpts() []mqlite.EmbeddedOption {
	opts := []mqlite.EmbeddedOption{mqlite.WithDBAuthToken(os.Getenv("MQLITE_DB_AUTH_TOKEN"))}
	if v := os.Getenv("MQLITE_MAX_MESSAGE_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			opts = append(opts, mqlite.WithMaxMessageBytes(n))
		}
	}
	if v := os.Getenv("MQLITE_SYNC"); v != "" { // NORMAL (default) | FULL | OFF — durability knob (MQLITE-7)
		opts = append(opts, mqlite.WithSynchronous(v))
	}
	// DLQ retention (MQLITE-21): bound the dead-letter queue by default so the broker
	// can run online long-term without the one unbounded sink filling the disk. Drop
	// oldest-first past 14 days or 1,000,000 dead letters per queue; an optional byte
	// cap (MQLITE_DLQ_MAX_BYTES, deployment-specific) is off by default. Override
	// MQLITE_DLQ_MAX_AGE / MQLITE_DLQ_MAX_COUNT, or disable with MQLITE_DLQ_RETENTION=off.
	if !strings.EqualFold(os.Getenv("MQLITE_DLQ_RETENTION"), "off") {
		age := 14 * 24 * time.Hour
		count := 1_000_000
		var maxBytes int64 // 0 = off; age+count already bound growth without knowing disk size
		if v := os.Getenv("MQLITE_DLQ_MAX_AGE"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				age = d
			}
		}
		if v := os.Getenv("MQLITE_DLQ_MAX_COUNT"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				count = n
			}
		}
		if v := os.Getenv("MQLITE_DLQ_MAX_BYTES"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				maxBytes = n
			}
		}
		opts = append(opts, mqlite.WithDLQRetention(age, count, maxBytes))
	}
	return opts
}

// dial picks client mode (MQLITE_ENDPOINT set) or embedded mode (MQLITE_DB).
func dial(ctx context.Context) (api, error) {
	if ep := os.Getenv("MQLITE_ENDPOINT"); ep != "" {
		return mqlite.Open(ctx, ep, mqlite.WithToken(os.Getenv("MQLITE_TOKEN")))
	}
	db := os.Getenv("MQLITE_DB")
	if db == "" {
		db = "file:./mq.db"
	}
	return mqlite.OpenEmbedded(ctx, db, embeddedOpts()...)
}

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	_ = fs.Parse(args)

	db := os.Getenv("MQLITE_DB")
	if db == "" {
		db = "file:./mq.db"
	}
	eng, err := mqlite.OpenEmbedded(ctx, db, embeddedOpts()...)
	if err != nil {
		return err
	}
	defer eng.Close()

	tokens := os.Getenv("MQLITE_TOKENS")
	remote := ""
	if eng.Engine().Remote() {
		remote = " (remote Turso/libSQL)"
	}
	authNote := "auth: Bearer tokens required"
	if strings.TrimSpace(tokens) == "" {
		authNote = "auth: DISABLED (no MQLITE_TOKENS set — localhost/LAN only)"
	}
	host := *addr
	if strings.HasPrefix(host, ":") {
		host = "localhost" + host
	}
	fmt.Printf("mqlite %s serving on %s — DB %s%s\n%s\nweb UI: http://%s/ui\n",
		version, *addr, redact(db), remote, authNote, host)

	sctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return eng.Serve(sctx, *addr, mqlite.WithTokenCSV(tokens), mqlite.WithVersion(version))
}

func cmdCreateQueue(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("create-queue", flag.ExitOnError)
	lock := fs.Duration("lock", 0, "lock duration (e.g. 30s)")
	maxdc := fs.Int("max-delivery", 0, "max delivery count before DLQ")
	ttl := fs.Duration("ttl", 0, "default message TTL")
	dedup := fs.Duration("dedup", 0, "dedup window (0=off)")
	ordering := fs.String("ordering", "", "ordering mode: standard|group_fifo|strict_fifo (default standard)")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("usage: create-queue <name> [--lock 30s --max-delivery 10 --ttl 1h --dedup 5m --ordering standard]")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.CreateQueue(ctx, pos[0], mqlite.QueueConfig{
		LockDuration: *lock, MaxDeliveryCount: *maxdc, DefaultTTL: *ttl, DedupWindow: *dedup,
		Ordering: mqlite.OrderingMode(*ordering),
	}); err != nil {
		return err
	}
	fmt.Println("ok: queue", pos[0])
	return nil
}

func cmdCreateSubscription(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("subscribe", flag.ExitOnError)
	prefix := fs.String("subject-prefix", "", "subject prefix filter")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 2 {
		return fmt.Errorf("usage: subscribe <topic> <subscription> [--subject-prefix p]")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	var f *mqlite.Filter
	if *prefix != "" {
		f = &mqlite.Filter{SubjectPrefix: *prefix}
	}
	if err := c.Subscribe(ctx, pos[0], pos[1], f); err != nil {
		return err
	}
	fmt.Printf("ok: subscription %s under topic %s\n", pos[1], pos[0])
	return nil
}

func cmdSend(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	file := fs.String("file", "", "read body from file")
	msgID := fs.String("message-id", "", "message id (dedup/idempotency key)")
	group := fs.String("group", "", "group id (MessageGroupId)")
	subject := fs.String("subject", "", "subject (label)")
	replyTo := fs.String("reply-to", "", "reply-to address")
	ttl := fs.Duration("ttl", 0, "message TTL")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("usage: send <queue> <body|-> [--file f --message-id id --group g --subject sub --reply-to addr --ttl 1h]")
	}
	queue := pos[0]
	body, err := readBody(*file, pos[1:])
	if err != nil {
		return err
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	seq, err := c.SendOne(ctx, queue, mqlite.OutMessage{
		Body: body, MessageID: *msgID, GroupID: *group, Subject: *subject, ReplyTo: *replyTo, TTL: *ttl,
	})
	if err != nil {
		return err
	}
	fmt.Printf("ok: seq=%d\n", seq)
	return nil
}

func cmdReceive(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("receive", flag.ExitOnError)
	max := fs.Int("max", 1, "max messages")
	wait := fs.Duration("wait", 0, "long-poll wait (e.g. 5s)")
	noAck := fs.Bool("no-ack", false, "leave messages locked (do not Complete)")
	del := fs.Bool("delete", false, "receive-and-delete (at-most-once)")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("usage: receive <queue> [--max 1 --wait 5s --no-ack --delete]")
	}
	queue := pos[0]
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	msgs, err := c.Receive(ctx, queue, mqlite.RecvOpts{Max: *max, Wait: *wait, AtMostOnce: *del})
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		fmt.Println("(no messages)")
		return nil
	}
	for _, m := range msgs {
		printMsg(m)
		if !*del && !*noAck {
			if err := m.Complete(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "  warn: complete:", err)
			}
		}
	}
	return nil
}

func cmdPeek(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("peek", flag.ExitOnError)
	state := fs.String("state", "", "filter by state (active/locked/deferred/scheduled/dead_lettered)")
	from := fs.Int64("from", 0, "start seq")
	max := fs.Int("max", 16, "max messages")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("usage: peek <queue> [--state s --from seq --max n]")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	po := mqlite.PeekOpts{From: *from, Max: *max}
	if *state != "" {
		po.State = mqlite.State(*state)
	}
	ms, err := c.Peek(ctx, pos[0], po)
	if err != nil {
		return err
	}
	if len(ms) == 0 {
		fmt.Println("(empty)")
		return nil
	}
	for _, m := range ms {
		fmt.Printf("seq=%d state=%s deliveries=%d body=%q\n", m.SequenceNumber, m.State, m.DeliveryCount, string(m.Body))
	}
	return nil
}

func cmdMetrics(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: metrics <queue>")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	m, err := c.Stats(ctx, args[0])
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(m, "", "  ")
	fmt.Println(string(b))
	return nil
}

func cmdList(ctx context.Context, args []string) error {
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	qs, err := c.ListQueues(ctx)
	if err != nil {
		return err
	}
	if len(qs) == 0 {
		fmt.Println("(no queues)")
		return nil
	}
	for _, q := range qs {
		fmt.Printf("%-24s kind=%-12s lock=%dms maxdc=%d ttl=%dms dedup=%dms\n",
			q.Name, q.Kind, q.LockDurationMs, q.MaxDeliveryCount, q.DefaultTTLMs, q.DedupWindowMs)
	}
	return nil
}

func cmdRedrive(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("redrive", flag.ExitOnError)
	to := fs.String("to", "", "target queue (default: back to source)")
	max := fs.Int("max", 0, "max messages (0=all)")
	older := fs.Duration("older-than", 0, "only messages older than this")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("usage: redrive <queue> [--to target --max n --older-than 1h]")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	moved, err := c.Redrive(ctx, pos[0], mqlite.RedriveOpts{To: *to, Max: *max, OlderThan: *older})
	if err != nil {
		return err
	}
	fmt.Printf("ok: moved %d message(s)\n", moved)
	return nil
}

func cmdPurgeDLQ(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge-dlq", flag.ExitOnError)
	max := fs.Int("max", 0, "max messages (0=all)")
	older := fs.Duration("older-than", 0, "only messages older than this")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("usage: purge-dlq <queue> [--max n --older-than 1h]")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	purged, err := c.Purge(ctx, pos[0], mqlite.PurgeOpts{Max: *max, OlderThan: *older})
	if err != nil {
		return err
	}
	fmt.Printf("ok: purged %d dead-lettered message(s)\n", purged)
	return nil
}

// ── helpers ─────────────────────────────────────────────────────────────────

func readBody(file string, rest []string) ([]byte, error) {
	if file != "" {
		return os.ReadFile(file)
	}
	if len(rest) == 0 {
		return nil, fmt.Errorf("no body given (provide a body argument, --file, or '-')")
	}
	if rest[0] == "-" {
		return io.ReadAll(io.LimitReader(os.Stdin, 16<<20))
	}
	return []byte(strings.Join(rest, " ")), nil
}

func printMsg(m *mqlite.Message) {
	fmt.Printf("seq=%d deliveries=%d", m.SequenceNumber, m.DeliveryCount)
	if m.MessageID != "" {
		fmt.Printf(" message-id=%s", m.MessageID)
	}
	if m.GroupID != "" {
		fmt.Printf(" group=%s", m.GroupID)
	}
	if m.ReplyTo != "" {
		fmt.Printf(" reply-to=%s", m.ReplyTo)
	}
	fmt.Printf(" body=%q\n", string(m.Body))
}

// redact hides any auth token embedded in a DSN before printing.
func redact(s string) string {
	if i := strings.Index(s, "authToken="); i >= 0 {
		return s[:i] + "authToken=***"
	}
	return s
}
