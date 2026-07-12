// Command mqlite is the single-binary CLI: run a broker, or produce/consume/
// administer queues against a local DB (embedded) or a remote broker (client).
//
// Connection (read from env; the DB string is never compiled in):
//
//	MQLITE_DB=file:./mq.db | :memory: | libsql://<db>.turso.io   (embedded mode)
//	MQLITE_DB_AUTH_TOKEN=<jwt>                                    (remote Turso/libSQL)
//	MQLITE_ENDPOINT=http://host:port + MQLITE_TOKEN=<bearer>      (client mode; wins if set)
//	MQLITE_ADDR=host:port       (listen address for `serve`; precedence: --addr >
//	                             MQLITE_ADDR > :6754; a blank value is rejected)
//	MQLITE_TOKENS=mqk_a,mqk_b   (tokens `serve` accepts; UNSET => a token is generated
//	                             and printed; =off disables auth — localhost/LAN only)
//	MQLITE_CORS=* | https://app.example | off   (Access-Control-Allow-Origin for `serve`;
//	                             UNSET => *, since RPCs still need a token; =off disables)
//	MQLITE_UI=on|off            (serve the embedded admin console at /ui for `serve`;
//	                             UNSET => on; =off runs headless — /ui 404s)
//	MQLITE_MAX_MESSAGE_BYTES=<n>                                  (reject larger bodies)
//	MQLITE_SYNC=NORMAL|FULL|OFF|EXTRA        (durability; embedded/serve; unknown value
//	                                          is rejected at startup, never silently NORMAL)
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
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	charmlog "github.com/charmbracelet/log"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/internal/defaults"
	ver "github.com/mqlitehq/mqlite/internal/version"
)

const version = ver.Version

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
	case "schedule":
		err = cmdSchedule(ctx, args)
	case "cancel":
		err = cmdCancel(ctx, args)
	case "receive":
		err = cmdReceive(ctx, args)
	case "receive-deferred":
		err = cmdReceiveDeferred(ctx, args)
	case "complete", "abandon", "reject", "defer", "renew":
		err = cmdSettle(ctx, cmd, args)
	case "peek":
		err = cmdPeek(ctx, args)
	case "metrics":
		err = cmdMetrics(ctx, args)
	case "status":
		err = cmdStatus(ctx, args)
	case "list":
		err = cmdList(ctx, args)
	case "list-subscriptions":
		err = cmdListSubscriptions(ctx, args)
	case "test-filter":
		err = cmdTestFilter(ctx, args)
	case "redrive":
		err = cmdRedrive(ctx, args)
	case "purge-dlq":
		err = cmdPurgeDLQ(ctx, args)
	case "vacuum":
		err = cmdVacuum(ctx, args)
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

 broker
  serve                     run the HTTP broker (embedded engine + Serve)

 admin
  create-queue <name>       create/update a queue
  subscribe <topic> <name>  create a subscription under <topic> (--expr 'predicate')
  list                      list queues
  list-subscriptions        list subscriptions with their topic + filter
  test-filter <expr>        dry-run a filter expression against an optional sample
  metrics <queue>           show queue counters
  status                    backend snapshot (backend, ping, size, counts)
  redrive <queue>           move dead-lettered messages back to active
  purge-dlq <queue>         permanently delete dead-lettered messages
  vacuum                    reclaim free DB pages to the OS (local maintenance; --full)

 messages
  send <queue> <body>       send now (body "-" reads stdin; --file reads a file)
  schedule <queue> <body>   send for future delivery (--at RFC3339|duration)
  cancel <queue> <seq>      delete a not-yet-activated scheduled message
  receive <queue>           receive (Peek-Lock, auto-Complete unless --no-ack)
  receive-deferred <queue>  fetch deferred messages back by seq (--seq 42,57)
  complete <queue> <seq> <token>   settle a --no-ack message: done
  abandon  <queue> <seq> <token>   settle: release for retry (--delay)
  reject   <queue> <seq> <token>   settle: dead-letter (--reason --detail)
  defer    <queue> <seq> <token>   settle: set aside for receive-deferred
  renew    <queue> <seq> <token>   extend the lock lease

  version | help

global flags (any command): --endpoint URL --token T --output text|json
connection: --endpoint/--token or MQLITE_ENDPOINT+MQLITE_TOKEN (client), else MQLITE_DB (embedded)
`)
}

// api is the subset shared by *mqlite.Client and *mqlite.Embedded, so every CLI command
// works identically in client (remote broker) and embedded (in-process) mode.
type api interface {
	SendOne(ctx context.Context, queue string, m mqlite.OutMessage, opts ...mqlite.SendOpts) (int64, error)
	Receive(ctx context.Context, queue string, opts ...mqlite.RecvOpts) ([]*mqlite.Message, error)
	Peek(ctx context.Context, queue string, opts ...mqlite.PeekOpts) ([]*mqlite.PeekedMessage, error)
	Message(queue string, seq int64, lockToken string) *mqlite.Message
	Cancel(ctx context.Context, queue string, seq int64) error
	CreateQueue(ctx context.Context, name string, cfg mqlite.QueueConfig) error
	Subscribe(ctx context.Context, topic, name string, f *mqlite.Filter) error
	ListQueues(ctx context.Context) ([]mqlite.QueueInfo, error)
	ListSubscriptions(ctx context.Context) ([]mqlite.SubscriptionInfo, error)
	TestFilter(ctx context.Context, expr string, sample *mqlite.OutMessage, enqueuedAtMs, visibleAtMs int64) (mqlite.FilterTestResult, error)
	Stats(ctx context.Context, queue string) (mqlite.Metrics, error)
	Status(ctx context.Context) (mqlite.StatusInfo, error)
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

// warnEnv surfaces a config value that failed to parse instead of silently swallowing
// it. These fall back to a safe default (unlike MQLITE_SYNC, which fails startup), so a
// warning is enough — but a typo shouldn't vanish without a trace (MQLITE-88).
func warnEnv(name, val string) {
	fmt.Fprintf(os.Stderr, "warning: ignoring unparseable %s=%q; using default\n", name, val)
}

// embeddedOpts builds the common embedded options EVERY CLI command shares: DB auth
// token, message size cap, durability. DLQ retention is deliberately NOT here — it is a
// broker-lifecycle policy (serveRetentionOpts), and a one-shot command like `send` or
// `receive` must not start the retention janitor (docs/cli.md documents it as
// serve-only; MQLITE-88 / P2-3).
func embeddedOpts() []mqlite.EmbeddedOption {
	opts := []mqlite.EmbeddedOption{mqlite.WithDBAuthToken(os.Getenv("MQLITE_DB_AUTH_TOKEN"))}
	if v := os.Getenv("MQLITE_MAX_MESSAGE_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			opts = append(opts, mqlite.WithMaxMessageBytes(n))
		} else {
			warnEnv("MQLITE_MAX_MESSAGE_BYTES", v)
		}
	}
	if v := os.Getenv("MQLITE_SYNC"); v != "" { // NORMAL (default) | FULL | OFF | EXTRA — validated at Open
		opts = append(opts, mqlite.WithSynchronous(v))
	}
	return opts
}

// serveRetentionOpts is the broker-only DLQ retention policy (MQLITE-21): bound the
// dead-letter queue so a long-running broker never lets that one unbounded sink fill the
// disk. Drop oldest-first past 14 days or 1,000,000 dead letters per queue; the optional
// byte cap (MQLITE_DLQ_MAX_BYTES) is off by default. Only `serve` applies it (MQLITE-88).
// Override MQLITE_DLQ_MAX_AGE / MQLITE_DLQ_MAX_COUNT, or disable with MQLITE_DLQ_RETENTION=off.
func serveRetentionOpts() []mqlite.EmbeddedOption {
	if strings.EqualFold(os.Getenv("MQLITE_DLQ_RETENTION"), "off") {
		return nil
	}
	age := 14 * 24 * time.Hour
	count := 1_000_000
	var maxBytes int64 // 0 = off; age+count already bound growth without knowing disk size
	if v := os.Getenv("MQLITE_DLQ_MAX_AGE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			age = d
		} else {
			warnEnv("MQLITE_DLQ_MAX_AGE", v)
		}
	}
	if v := os.Getenv("MQLITE_DLQ_MAX_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			count = n
		} else {
			warnEnv("MQLITE_DLQ_MAX_COUNT", v)
		}
	}
	if v := os.Getenv("MQLITE_DLQ_MAX_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			maxBytes = n
		} else {
			warnEnv("MQLITE_DLQ_MAX_BYTES", v)
		}
	}
	return []mqlite.EmbeddedOption{mqlite.WithDLQRetention(age, count, maxBytes)}
}

// Global flags shared by every data-plane command, registered via newFlags. Package-level
// because a CLI runs exactly one command per process; --endpoint/--token override the
// MQLITE_ENDPOINT/MQLITE_TOKEN env, and --output switches human text for JSON (scripting).
var (
	gEndpoint string
	gToken    string
	gOutput   string
)

// newFlags builds a command FlagSet with the global connection/output flags pre-registered,
// so every command speaks --endpoint/--token/--output uniformly.
func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.StringVar(&gEndpoint, "endpoint", "", "broker endpoint (overrides MQLITE_ENDPOINT; client mode)")
	fs.StringVar(&gToken, "token", "", "bearer token (overrides MQLITE_TOKEN)")
	fs.StringVar(&gOutput, "output", "text", "output format: text | json")
	return fs
}

// jsonOut reports whether --output json was requested.
func jsonOut() bool { return gOutput == "json" }

// emitJSON prints v as indented JSON (used when --output json).
func emitJSON(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

// dial picks client mode (--endpoint / MQLITE_ENDPOINT set) or embedded mode (MQLITE_DB).
func dial(ctx context.Context) (api, error) {
	ep := gEndpoint
	if ep == "" {
		ep = os.Getenv("MQLITE_ENDPOINT")
	}
	if ep != "" {
		tok := gToken
		if tok == "" {
			tok = os.Getenv("MQLITE_TOKEN")
		}
		return mqlite.Open(ctx, ep, mqlite.WithToken(tok))
	}
	db := os.Getenv("MQLITE_DB")
	if db == "" {
		db = "file:./mq.db"
	}
	return mqlite.OpenEmbedded(ctx, db, embeddedOpts()...)
}

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "", "listen address (default "+defaults.BrokerListenAddr+"; or set MQLITE_ADDR)")
	insecureAllowRemote := fs.Bool("insecure-allow-remote", false, "allow a non-loopback bind while auth is disabled (MQLITE_TOKENS=off)")
	_ = fs.Parse(args)

	addrSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "addr" {
			addrSet = true
		}
	})
	envAddr, envSet := os.LookupEnv("MQLITE_ADDR")
	listenAddr, err := resolveListenAddr(*addr, addrSet, envAddr, envSet)
	if err != nil {
		return err
	}
	// Validate auth config before opening the DB, so a misconfigured MQLITE_TOKENS fails
	// fast without acquiring the single-writer lock.
	tokens, authNote, err := resolveBrokerTokens(os.Getenv("MQLITE_TOKENS"))
	if err != nil {
		return err
	}
	// With auth disabled, refuse a non-loopback bind unless explicitly allowed: an open
	// broker on all interfaces is remotely reachable by anyone (MQLITE-70 / D2).
	if tokens == "" && !isLoopbackListen(listenAddr) && !*insecureAllowRemote {
		return fmt.Errorf("refusing to serve with auth disabled on non-loopback address %q: "+
			"bind loopback (e.g. 127.0.0.1:%s), set MQLITE_TOKENS, or pass --insecure-allow-remote",
			listenAddr, defaults.BrokerPort)
	}

	lg := serveLogger()
	slogger := slog.New(lg)

	db := os.Getenv("MQLITE_DB")
	if db == "" {
		db = "file:./mq.db"
	}
	// serve is the only path that applies broker DLQ retention (serveRetentionOpts).
	serveOpts := append(embeddedOpts(), serveRetentionOpts()...)
	serveOpts = append(serveOpts, mqlite.WithLogger(slogger))
	eng, err := mqlite.OpenEmbedded(ctx, db, serveOpts...)
	if err != nil {
		return err
	}
	defer eng.Close()

	corsOrigin, _ := resolveCORS(os.Getenv("MQLITE_CORS"), tokens == "")
	ui := resolveUI(os.Getenv("MQLITE_UI"))
	backend := "local"
	if eng.Engine().Remote() {
		backend = "remote Turso/libSQL"
	}
	host := listenAddr
	if strings.HasPrefix(host, ":") {
		host = "localhost" + host
	}

	lg.Info("mqlite broker", "version", version, "addr", listenAddr, "db", redact(db), "backend", backend)
	if ui {
		lg.Info("admin console", "url", "http://"+host+"/ui")
	}
	switch {
	case tokens == "":
		reach := "reachable on loopback only"
		if !isLoopbackListen(listenAddr) {
			reach = "reachable by ANYONE on the network (--insecure-allow-remote)"
		}
		lg.Warn("auth disabled — no token required; " + reach + " (set MQLITE_TOKENS to enable auth)")
	case strings.Contains(authNote, "generated"):
		lg.Info("auth enabled — generated a token (set MQLITE_TOKENS to use your own, or =off to disable)", "token", tokens)
	default:
		lg.Info("auth enabled — tokens from MQLITE_TOKENS")
	}
	corsPolicy := corsOrigin
	if corsPolicy == "" {
		corsPolicy = "off"
	}
	lg.Info("cors", "allow-origin", corsPolicy)

	sctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	// "ready" fires from WithReady, i.e. only after the listener actually binds — so a
	// bind failure surfaces as an error instead of a misleading "ready" line (MQLITE-88).
	return eng.Serve(sctx, listenAddr,
		mqlite.WithTokenCSV(tokens), mqlite.WithVersion(version),
		mqlite.WithCORS(corsOrigin), mqlite.WithRequestLog(slogger), mqlite.WithUI(ui),
		mqlite.WithReady(func() { lg.Info("ready — Ctrl-C to stop") }))
}

// resolveListenAddr picks the broker's listen address with precedence
//
//	explicit --addr  >  non-empty MQLITE_ADDR  >  defaults.BrokerListenAddr (:6754)
//
// A blank or whitespace-only explicit --addr or MQLITE_ADDR is rejected: passing an empty
// address to Go's net/http would otherwise bind the named service ":http" (port 80),
// silently breaking the deterministic-endpoint contract. Invalid but non-blank addresses
// are left to fail loudly at Listen time, which reports the offending value.
func resolveListenAddr(flagVal string, flagSet bool, env string, envSet bool) (string, error) {
	if flagSet {
		if v := strings.TrimSpace(flagVal); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("--addr is blank; pass a host:port such as %s", defaults.BrokerListenAddr)
	}
	if envSet {
		if v := strings.TrimSpace(env); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("MQLITE_ADDR is blank; unset it or pass a host:port such as %s", defaults.BrokerListenAddr)
	}
	return defaults.BrokerListenAddr, nil
}

// resolveUI decides whether the embedded admin console is served, from MQLITE_UI. Default
// (unset) is ON. "off"/"false"/"0"/"no" disable it (the broker runs headless — /ui 404s).
func resolveUI(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "off", "false", "0", "no":
		return false
	default:
		return true
	}
}

// serveLogger builds the broker's console logger: charmbracelet/log, colourised when
// stderr is a TTY (auto-detected) and plain when piped. Levels colour the startup lines
// and the per-request access log so live traffic is readable at a glance.
func serveLogger() *charmlog.Logger {
	return charmlog.NewWithOptions(os.Stderr, charmlog.Options{
		ReportTimestamp: true,
		TimeFormat:      "15:04:05.000", // millisecond precision — RPCs are sub-ms apart
	})
}

// resolveBrokerTokens decides the broker's accepted Bearer tokens from MQLITE_TOKENS,
// secure by default: if it is unset, a fresh token is generated and printed (so the
// broker is never accidentally wide open); if it is "off", auth is explicitly
// disabled (localhost/LAN); otherwise the provided comma-separated tokens are used.
// Returns the cleaned token CSV (empty = no auth), a human note for the startup banner,
// and an error if MQLITE_TOKENS is set but yields no usable token (which must fail loudly
// rather than silently disable auth — MQLITE-69).
func resolveBrokerTokens(env string) (csv, note string, err error) {
	trimmed := strings.TrimSpace(env)
	switch {
	case strings.EqualFold(trimmed, "off"):
		return "", "auth: DISABLED (MQLITE_TOKENS=off — localhost/LAN only)", nil
	case trimmed == "":
		t := mqlite.GenerateToken()
		return t, "auth: ON — generated a token (set MQLITE_TOKENS to use your own, " +
			"or MQLITE_TOKENS=off to disable):\n  " + t, nil
	default:
		// A non-blank, non-"off" value must contain at least one real token. Parse it the
		// same way the server does (split on comma, drop blank elements); if nothing is
		// left — e.g. ",", " , ", ",," — fail instead of passing an empty set to the server,
		// which would disable auth while the banner claimed it was on.
		var toks []string
		for _, t := range strings.Split(env, ",") {
			if t = strings.TrimSpace(t); t != "" {
				toks = append(toks, t)
			}
		}
		if len(toks) == 0 {
			return "", "", fmt.Errorf("MQLITE_TOKENS is set but has no usable token " +
				"(only blanks/commas); give it a token, unset it to auto-generate one, or set " +
				"MQLITE_TOKENS=off to disable auth")
		}
		return strings.Join(toks, ","), "auth: ON — Bearer tokens from MQLITE_TOKENS", nil
	}
}

// resolveCORS decides the broker's Access-Control-Allow-Origin from MQLITE_CORS, with the
// default depending on whether auth is on. A wildcard is only safe because every RPC needs
// a Bearer token; when auth is disabled that premise is gone, so the default becomes off
// (MQLITE-70 / D6). "off" always disables it; any explicit origin — or an explicit "*" — is
// honored verbatim as an opt-in, even with auth off.
func resolveCORS(env string, authOff bool) (origin, note string) {
	switch e := strings.TrimSpace(env); {
	case strings.EqualFold(e, "off"):
		return "", "cors: off"
	case e == "*" && authOff:
		return "*", "cors: * (explicit — WARNING: auth is disabled, so any origin can call this broker)"
	case e != "":
		return e, "cors: " + e
	case authOff:
		return "", "cors: off (auth disabled — set MQLITE_CORS to opt in to cross-origin)"
	default:
		return "*", "cors: * (any origin — RPCs still require a token)"
	}
}

// isLoopbackListen reports whether a listen address binds only the loopback interface. An
// empty host (":6754") or 0.0.0.0 binds all interfaces; 127.0.0.0/8, ::1, and "localhost"
// are loopback. An unresolvable hostname is treated as non-loopback (the safe default).
func isLoopbackListen(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	switch {
	case host == "":
		return false
	case strings.EqualFold(host, "localhost"):
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func cmdCreateQueue(ctx context.Context, args []string) error {
	fs := newFlags("create-queue")
	lock := fs.Duration("lock", 0, "lock duration (e.g. 30s)")
	maxdc := fs.Int("max-delivery", 0, "max delivery count before DLQ")
	ttl := fs.Duration("ttl", 0, "default message TTL")
	dedup := fs.Duration("dedup", 0, "dedup window (0=off)")
	ordering := fs.String("ordering", "", "ordering mode: standard|group_fifo|strict_fifo (default standard)")
	dlqAge := fs.Duration("dlq-max-age", 0, "per-queue DLQ retention: drop dead letters older than this (0=inherit broker default)")
	dlqCount := fs.Int("dlq-max-count", 0, "per-queue DLQ retention: keep at most N dead letters (0=inherit, -1=unbounded)")
	dlqBytes := fs.Int64("dlq-max-bytes", 0, "per-queue DLQ retention: cap dead-letter body bytes (0=inherit, -1=unbounded)")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("usage: create-queue <name> [--lock 30s --max-delivery 10 --ttl 1h --dedup 5m --ordering standard --dlq-max-age 336h --dlq-max-count 1000 --dlq-max-bytes 10485760]")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.CreateQueue(ctx, pos[0], mqlite.QueueConfig{
		LockDuration: *lock, MaxDeliveryCount: *maxdc, DefaultTTL: *ttl, DedupWindow: *dedup,
		Ordering:  mqlite.OrderingMode(*ordering),
		DLQMaxAge: *dlqAge, DLQMaxCount: *dlqCount, DLQMaxBytes: *dlqBytes,
	}); err != nil {
		return err
	}
	return okResult(map[string]any{"action": "create-queue", "queue": pos[0]}, "action", "queue")
}

func cmdCreateSubscription(ctx context.Context, args []string) error {
	fs := newFlags("subscribe")
	exprStr := fs.String("expr", "", `filter expression (expr-lang), e.g. 'subject_parts[0]=="orders"'; empty = match all`)
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 2 {
		return fmt.Errorf("usage: subscribe <topic> <subscription> [--expr 'predicate']")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	var f *mqlite.Filter
	if *exprStr != "" {
		f = &mqlite.Filter{Expr: *exprStr}
	}
	if err := c.Subscribe(ctx, pos[0], pos[1], f); err != nil {
		return err
	}
	return okResult(map[string]any{"action": "subscribe", "subscription": pos[1], "topic": pos[0]}, "action", "subscription", "topic")
}

func cmdSend(ctx context.Context, args []string) error {
	fs := newFlags("send")
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
	return okResult(map[string]any{"queue": queue, "seq": seq}, "queue", "seq")
}

func cmdReceive(ctx context.Context, args []string) error {
	fs := newFlags("receive")
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
	// Show the lock token only when the message stays locked (--no-ack), so it can be
	// settled in a later invocation (complete/abandon/reject/defer/renew <queue> <seq>
	// <token>). On auto-complete or --delete there is nothing left to settle.
	if err := printMsgs(msgs, *noAck && !*del); err != nil {
		return err
	}
	if *del || *noAck {
		return nil
	}
	// Auto-Complete each; a settlement failure must exit nonzero — otherwise automation
	// reads exit 0 and assumes the messages settled when they will redeliver (MQLITE-88).
	var settleErrs []error
	for _, m := range msgs {
		if err := m.Complete(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "  warn: complete:", err)
			settleErrs = append(settleErrs, err)
		}
	}
	if len(settleErrs) > 0 {
		return fmt.Errorf("%d of %d message(s) failed to settle (see warnings above): %w",
			len(settleErrs), len(msgs), errors.Join(settleErrs...))
	}
	return nil
}

func cmdPeek(ctx context.Context, args []string) error {
	fs := newFlags("peek")
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
	if jsonOut() {
		return emitJSON(viewPeeked(ms))
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
	fs := newFlags("metrics")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("usage: metrics <queue>")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	m, err := c.Stats(ctx, pos[0])
	if err != nil {
		return err
	}
	if jsonOut() {
		return emitJSON(m)
	}
	fmt.Printf("queue=%s active=%d locked=%d deferred=%d scheduled=%d dead=%d total=%d oldest=%dms\n",
		m.Queue, m.Active, m.Locked, m.Deferred, m.Scheduled, m.DeadLettered, m.Total, m.OldestMessageAgeMs)
	return nil
}

func cmdList(ctx context.Context, args []string) error {
	fs := newFlags("list")
	if _, err := parseInterspersed(fs, args); err != nil {
		return err
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	qs, err := c.ListQueues(ctx)
	if err != nil {
		return err
	}
	if jsonOut() {
		return emitJSON(qs)
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
	fs := newFlags("redrive")
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
	return okResult(map[string]any{"action": "redrive", "moved": moved}, "action", "moved")
}

func cmdPurgeDLQ(ctx context.Context, args []string) error {
	fs := newFlags("purge-dlq")
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
	return okResult(map[string]any{"action": "purge-dlq", "purged": purged}, "action", "purged")
}

// cmdVacuum reclaims free DB pages to the OS. It is a LOCAL maintenance command: it
// opens the file DB directly (so stop the broker first — the single-writer lock will
// otherwise reject it), runs incremental_vacuum (or a full VACUUM with --full), and
// reports the file size before/after.
func cmdVacuum(ctx context.Context, args []string) error {
	fs := newFlags("vacuum")
	full := fs.Bool("full", false, "full VACUUM (rewrites the DB, global lock) instead of incremental")
	if _, err := parseInterspersed(fs, args); err != nil {
		return err
	}
	if gEndpoint != "" || os.Getenv("MQLITE_ENDPOINT") != "" {
		return fmt.Errorf("vacuum is a local maintenance command: drop --endpoint/MQLITE_ENDPOINT and point MQLITE_DB at the file")
	}
	db := os.Getenv("MQLITE_DB")
	if db == "" {
		db = "file:./mq.db"
	}
	eng, err := mqlite.OpenEmbedded(ctx, db, embeddedOpts()...)
	if err != nil {
		return err
	}
	defer eng.Close()
	before := dbFileBytes(db)
	if err := eng.Compact(ctx, *full); err != nil {
		return err
	}
	after := dbFileBytes(db)
	kind := "incremental_vacuum"
	if *full {
		kind = "VACUUM"
	}
	if jsonOut() {
		return emitJSON(map[string]any{
			"action": kind, "before_bytes": before, "after_bytes": after, "freed_bytes": before - after,
		})
	}
	fmt.Printf("ok: %s — %.2f MiB -> %.2f MiB (freed %.2f MiB)\n",
		kind, mib(before), mib(after), mib(before-after))
	return nil
}

// dbFileBytes returns the size of a local file DSN's main DB file (0 if not a file).
func dbFileBytes(dsn string) int64 {
	p := strings.TrimPrefix(dsn, "file:")
	if p == "" || strings.Contains(strings.ToLower(p), ":memory:") {
		return 0
	}
	if fi, err := os.Stat(p); err == nil {
		return fi.Size()
	}
	return 0
}

func mib(b int64) float64 { return float64(b) / (1 << 20) }

// ── helpers ─────────────────────────────────────────────────────────────────

// maxStdinBytes caps a '-' (stdin) message body. It is a local safety net that fails loud
// rather than silently truncating; the broker's own limit (413 message_too_large) is the
// real ceiling, and --file has no cap for genuinely large payloads.
const maxStdinBytes = 16 << 20 // 16 MiB

// readCapped reads all of r but returns an error if it exceeds max bytes, instead of
// silently truncating an over-limit body (MQLITE-79).
func readCapped(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("stdin body exceeds the %d MiB CLI limit; use --file for larger messages", max>>20)
	}
	return b, nil
}

func readBody(file string, rest []string) ([]byte, error) {
	if file != "" {
		return os.ReadFile(file)
	}
	if len(rest) == 0 {
		return nil, fmt.Errorf("no body given (provide a body argument, --file, or '-')")
	}
	if rest[0] == "-" {
		return readCapped(os.Stdin, maxStdinBytes)
	}
	return []byte(strings.Join(rest, " ")), nil
}

// printMsgs renders received messages: a JSON array under --output json, else one human
// line each. withToken includes the lock token (only when the message stays locked and
// must be settled later).
func printMsgs(msgs []*mqlite.Message, withToken bool) error {
	if jsonOut() {
		views := make([]msgView, len(msgs))
		for i, m := range msgs {
			views[i] = viewMsg(m, withToken)
		}
		return emitJSON(views)
	}
	if len(msgs) == 0 {
		fmt.Println("(no messages)")
		return nil
	}
	for _, m := range msgs {
		printMsg(m, withToken)
	}
	return nil
}

func printMsg(m *mqlite.Message, withToken bool) {
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
	if withToken {
		fmt.Printf(" lock-token=%s", m.LockToken())
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
