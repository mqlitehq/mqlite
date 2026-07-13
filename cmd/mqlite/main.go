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
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	charmlog "github.com/charmbracelet/log"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/internal/defaults"
	ver "github.com/mqlitehq/mqlite/internal/version"
)

const version = ver.Version

// stdoutInvalid is set when the process was started with stdout (fd 1) closed. A consumer
// like `receive` must then refuse to acknowledge messages whose output can't reach the
// caller (review 2026-07-12 round-2 B1).
var stdoutInvalid bool

// sanitizeStdout records whether stdout can actually deliver output to the caller — fd 1 was
// closed at exec (`mqlite receive q 1>&-`, which the Go runtime silently reopens to the null
// device before main, so writes then "succeed" into a black hole) or it was redirected to the
// null device outright (`>/dev/null`). In both cases a data-plane command like `receive` must
// NOT acknowledge messages whose bodies vanish; we record the condition so it can refuse
// before claiming anything. The detection itself is platform-specific — see
// stdout_unix.go / stdout_windows.go.
func sanitizeStdout() { stdoutInvalid = stdoutUndeliverable(os.Stdout) }

func main() {
	sanitizeStdout()
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
		err = exactMeta(cmd, args)
		if err == nil {
			fmt.Println("mqlite", version)
		}
	case "help", "-h", "--help":
		err = exactMeta(cmd, args)
		if err == nil {
			usage()
		}
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

// exactMeta rejects surplus arguments on the commands that take none. `mqlite version --json`
// silently printing the plain version (and exiting 0) hides the caller's mistake — arity is a
// contract on every command, not just the ones with positionals (round-3 §3.4).
func exactMeta(cmd string, args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("usage: mqlite %s   (takes no arguments, got %q)", cmd, strings.Join(args, " "))
	}
	return nil
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
  peek <queue>              browse messages without locking (--state --from --max)
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

global flags (data/admin commands, not serve): --endpoint URL --token T --output text|json
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
	CompleteBatch(ctx context.Context, queue string, msgs ...*mqlite.Message) ([]mqlite.SettleResult, error)
	RenewBatch(ctx context.Context, queue string, msgs ...*mqlite.Message) ([]mqlite.SettleResult, error)
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
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			break
		}
		// If flag.Parse stopped at a literal `--` terminator, every remaining token is a
		// positional and must NEVER be re-parsed as a flag — standard CLI semantics, and it
		// keeps a message body like `-- hello --output json` intact instead of truncating it
		// and hijacking --output (review 2026-07-12 P1-4).
		if consumed := len(args) - len(rest); consumed > 0 && args[consumed-1] == "--" {
			positionals = append(positionals, rest...)
			break
		}
		positionals = append(positionals, rest[0])
		args = rest[1:]
	}
	// Note which flags were explicitly given (Visit reports only set flags). Presence, not
	// value: `--max 0` and "no --max at all" both parse to 0, so a conflict check that looks
	// only at values accepts `purge-dlq --all --max 0` (round-3 §3.4). dial needs the same
	// distinction to honor `--token=` and isolate an ambient token from a changed endpoint.
	gFlagSeen = map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		gFlagSeen[f.Name] = true
		switch f.Name {
		case "endpoint":
			gEndpointSet = true
		case "token":
			gTokenSet = true
		}
	})
	// Validate --output for the commands that registered it (via newFlags), so a typo like
	// `--output jsno` is a loud usage error, not a silent fall-through to text (serve does
	// not register it, so it is skipped).
	if fs.Lookup("output") != nil && gOutput != "text" && gOutput != "json" {
		return nil, fmt.Errorf("invalid --output %q: want text or json", gOutput)
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
	gEndpoint    string
	gToken       string
	gOutput      string
	gEndpointSet bool // --endpoint was explicitly passed (vs env fallback)
	gTokenSet    bool // --token was explicitly passed (so an empty value means "no token")
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

// gFlagSeen records the flags explicitly passed on the command line (set by
// parseInterspersed). A zero value is not the same as an absent flag.
var gFlagSeen = map[string]bool{}

func flagGiven(name string) bool { return gFlagSeen[name] }

// emitJSON prints v as indented JSON (used when --output json). It writes to stdout in one
// call and RETURNS the write error — a lost/closed stdout must be observable so a consumer
// (e.g. receive) never settles a message whose output failed (review 2026-07-12 P1-1).
func emitJSON(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(append(b, '\n'))
	return err
}

// dial picks client mode (--endpoint / MQLITE_ENDPOINT set) or embedded mode (MQLITE_DB).
func dial(ctx context.Context) (api, error) {
	envEp := os.Getenv("MQLITE_ENDPOINT")
	ep := envEp
	if gEndpointSet {
		ep = gEndpoint
	}
	if ep != "" {
		tok := resolveToken(ep, envEp)
		// An explicit `--token=` means "send no credential" — also strip one embedded in the
		// endpoint DSN (mqlite://secret@host), which WithToken("") would otherwise retain.
		if gTokenSet && tok == "" {
			ep = stripEndpointCredentials(ep)
		}
		return mqlite.Open(ctx, ep, mqlite.WithToken(tok))
	}
	db := os.Getenv("MQLITE_DB")
	if db == "" {
		db = "file:./mq.db"
	}
	return mqlite.OpenEmbedded(ctx, db, embeddedOpts()...)
}

// resolveToken picks the bearer token for endpoint ep. An explicit --token wins verbatim —
// including `--token=`, which means "send no token". Without --token, the ambient
// MQLITE_TOKEN is reused ONLY when talking to the environment's own endpoint; if --endpoint
// changed the authority, the token is withheld (and a warning printed) so a production
// credential is never silently sent to a different host (review 2026-07-12 P1-5).
func resolveToken(ep, envEp string) string {
	if gTokenSet {
		return gToken
	}
	if !gEndpointSet || sameEndpoint(ep, envEp) {
		return os.Getenv("MQLITE_TOKEN")
	}
	if os.Getenv("MQLITE_TOKEN") != "" {
		fmt.Fprintln(os.Stderr, "warning: --endpoint changes the target host; MQLITE_TOKEN is NOT forwarded to it — pass --token to authenticate")
	}
	return ""
}

// sameEndpoint reports whether two endpoint strings reach the same broker. The token boundary
// is the canonical IDENTITY — scheme, host, effective port AND path (a reverse proxy routes
// /prod and /dev to different backends) — not the raw text: `http://h:6754` and
// `http://h:6754/` are one broker, and re-passing the environment's own endpoint with a
// trailing slash must not cost the caller their token (round-2 §3.5). If either string can't
// be parsed we report NOT-same — an unparseable endpoint may never widen the boundary.
func sameEndpoint(a, b string) bool {
	if a == b {
		return true
	}
	na, err := mqlite.EndpointIdentity(a)
	if err != nil {
		return false
	}
	nb, err := mqlite.EndpointIdentity(b)
	if err != nil {
		return false
	}
	return na == nb
}

// stripEndpointCredentials removes any userinfo embedded in an endpoint DSN (e.g.
// mqlite://secret@host), so an explicit `--token=` truly sends no credential.
func stripEndpointCredentials(ep string) string {
	u, err := url.Parse(ep)
	if err != nil || u.User == nil {
		return ep
	}
	u.User = nil
	return u.String()
}

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "", "listen address (default "+defaults.BrokerListenAddr+"; or set MQLITE_ADDR)")
	insecureAllowRemote := fs.Bool("insecure-allow-remote", false, "allow a non-loopback bind while auth is disabled (MQLITE_TOKENS=off)")
	_ = fs.Parse(args)
	if fs.NArg() > 0 { // exact arity: serve takes flags only (round-3 §3.4)
		return fmt.Errorf("usage: serve [--addr host:port] [--insecure-allow-remote]   (no positional arguments, got %q)",
			strings.Join(fs.Args(), " "))
	}

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
	if len(pos) != 1 { // exact arity — a surplus positional is a typo, not something to ignore
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
	if len(pos) != 2 { // exact arity
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
	if len(pos) != 1 {
		return fmt.Errorf("usage: receive <queue> [--max 1 --wait 5s --no-ack | --delete]")
	}
	if *del && *noAck { // mutually exclusive — don't silently let one win
		return fmt.Errorf("--delete and --no-ack are mutually exclusive")
	}
	// If stdout can't deliver output (closed at exec, or the null device), auto-ack would
	// acknowledge messages whose bodies the caller never sees. Refuse BEFORE claiming, so
	// nothing is locked or deleted (round-2 B1). --delete (explicit at-most-once drain) and
	// --no-ack (nothing settled) stay usable.
	if stdoutInvalid && !*del && !*noAck {
		return fmt.Errorf("stdout is not writable (closed or /dev/null) — refusing to auto-acknowledge messages whose output would be discarded; use --delete to drain explicitly, or --no-ack")
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
	// Render output FIRST and require it to succeed BEFORE settling. Settling before the
	// caller actually receives the output would acknowledge a message they never got — on a
	// closed stdout / broken pipe that silently downgrades Peek-Lock to at-most-once and
	// LOSES the message (review 2026-07-12 P1-1). The lock token is shown only when the
	// message stays locked (--no-ack) so it can be settled later.
	autoAck := !*del && !*noAck
	if autoAck {
		// Renew the leases from here until this function RETURNS — covering both the output
		// write and the CompleteBatch RPC, so neither a slow sink nor a slow settle can let the
		// reaper reclaim messages whose bodies were already emitted (round-2 §3.2). Registered
		// after `defer c.Close()`, so it stops (and joins the goroutine) BEFORE the client is
		// closed — no Renew races the shutdown.
		defer startRenewer(ctx, c, queue, msgs).Stop()
	}
	outErr := printMsgs(msgs, *noAck && !*del)
	if *del || *noAck {
		return outErr // nothing to auto-settle; surface any output error as a nonzero exit
	}
	if outErr != nil {
		// Do NOT settle — leave the messages locked so they redeliver instead of vanishing.
		return fmt.Errorf("output failed — %d message(s) left locked for redelivery: %w", len(msgs), outErr)
	}
	if len(msgs) == 0 {
		return nil
	}
	// Output delivered: settle the whole batch in ONE CompleteBatch RPC. N individual
	// Completes at high latency can let later locks expire mid-batch (review 2026-07-12
	// P1-3). Report the exact seqs that failed to settle (they will redeliver — MQLITE-88).
	results, err := c.CompleteBatch(ctx, queue, msgs...)
	if err != nil {
		return err
	}
	var failed []int64
	for _, r := range results {
		if !r.Ok {
			failed = append(failed, r.SequenceNumber)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d of %d message(s) failed to settle (seq %v) — they will redeliver", len(failed), len(msgs), failed)
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
	if len(pos) != 1 { // exact arity
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
	if len(pos) != 1 { // exact arity
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
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 { // exact arity — `list extra` is a typo, not a filter
		return fmt.Errorf("usage: list")
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
		if qs == nil {
			qs = []mqlite.QueueInfo{} // JSON must be [] not null (embedded returns a nil slice)
		}
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
	if len(pos) != 1 { // exact arity — a stray positional must not be silently ignored (P1-2)
		return fmt.Errorf("usage: redrive <queue> [--to target --max n --older-than 1h]")
	}
	if *max < 0 || *older < 0 {
		return fmt.Errorf("--max and --older-than must be >= 0")
	}
	if *older > 0 && *older < time.Millisecond { // sub-ms truncates to 0 = unbounded (round-2 B2)
		return fmt.Errorf("--older-than must be at least 1ms (got %s)", *older)
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
	max := fs.Int("max", 0, "max messages to delete (0 = unbounded, then --all is required)")
	older := fs.Duration("older-than", 0, "only delete messages older than this")
	all := fs.Bool("all", false, "delete the ENTIRE DLQ (required to run with no --max/--older-than bound)")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 { // exact arity — a stray positional must not be silently ignored (P1-2)
		return fmt.Errorf("usage: purge-dlq <queue> [--max n | --older-than 1h | --all]")
	}
	if *max < 0 || *older < 0 { // a negative bound would otherwise slip past the --all guard
		return fmt.Errorf("--max and --older-than must be >= 0")
	}
	if *older > 0 && *older < time.Millisecond { // sub-ms truncates to 0 = unbounded (round-2 B2)
		return fmt.Errorf("--older-than must be at least 1ms (got %s)", *older)
	}
	// Guard an unbounded destructive purge behind an explicit --all (review 2026-07-12 P1-2).
	if *max == 0 && *older == 0 && !*all {
		return fmt.Errorf("refusing to purge the entire DLQ without a bound — pass --max/--older-than, or --all to delete everything")
	}
	// --all ("delete everything") and a bound are contradictory: the usage presents them as
	// alternatives, so accepting `--all --max 10` and quietly honoring the bound is a trap
	// (round-2 §3.3). Make the caller say which one they meant.
	if *all && (flagGiven("max") || flagGiven("older-than")) {
		return fmt.Errorf("--all deletes the entire DLQ and cannot be combined with --max/--older-than — pass one or the other")
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
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 { // exact arity — vacuum takes no positional (the DB comes from MQLITE_DB)
		return fmt.Errorf("usage: vacuum [--full]   (the DB comes from MQLITE_DB)")
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
	// A brand-new/empty DB materializes its schema pages while it is opened and vacuumed, so
	// `after` can exceed `before` — and "freed -0.12 MiB" is nonsense. Reclaimed bytes are
	// clamped at zero and growth is reported as growth (round-2 §3.6).
	freed, grew := before-after, int64(0)
	if freed < 0 {
		grew, freed = -freed, 0
	}
	if jsonOut() {
		return emitJSON(map[string]any{
			"action": kind, "before_bytes": before, "after_bytes": after,
			"freed_bytes": freed, "grew_bytes": grew,
		})
	}
	if grew > 0 {
		fmt.Printf("ok: %s — %.2f MiB -> %.2f MiB (nothing to reclaim; the DB grew %.2f MiB)\n",
			kind, mib(before), mib(after), mib(grew))
		return nil
	}
	fmt.Printf("ok: %s — %.2f MiB -> %.2f MiB (freed %.2f MiB)\n",
		kind, mib(before), mib(after), mib(freed))
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
		// Ambiguous: the caller gave a body twice. Letting --file quietly win discards the
		// text they typed (round-3 §3.4).
		if len(rest) > 0 {
			return nil, fmt.Errorf("give the body either as an argument or with --file, not both")
		}
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
	// Stream through a bufio.Writer: each message is written (and its error observed) as it
	// is formatted, so output-before-settle holds WITHOUT holding the whole batch in memory.
	// A legal 32 MiB batch %q-expands up to ~4×, which would OOM a small CLI if buffered
	// (review 2026-07-12 P2). The final Flush surfaces a lost/closed stdout (P1-1).
	w := bufio.NewWriter(os.Stdout)
	switch {
	case jsonOut():
		if err := writeMsgJSON(w, msgs, withToken); err != nil {
			return err
		}
	case len(msgs) == 0:
		if _, err := w.WriteString("(no messages)\n"); err != nil {
			return err
		}
	default:
		for _, m := range msgs {
			if _, err := w.WriteString(formatMsg(m, withToken)); err != nil {
				return err
			}
		}
	}
	return w.Flush()
}

// writeMsgJSON streams the message array element by element (never one batch-sized alloc).
func writeMsgJSON(w *bufio.Writer, msgs []*mqlite.Message, withToken bool) error {
	if _, err := w.WriteString("[\n"); err != nil {
		return err
	}
	for i, m := range msgs {
		b, err := json.MarshalIndent(viewMsg(m, withToken), "  ", "  ")
		if err != nil {
			return err
		}
		sep := "\n"
		if i < len(msgs)-1 {
			sep = ",\n"
		}
		if _, err := w.WriteString("  " + string(b) + sep); err != nil {
			return err
		}
	}
	_, err := w.WriteString("]\n")
	return err
}

// renewer keeps a received batch's Peek-Lock leases alive (best-effort) in the background —
// from the moment the batch is claimed until settlement has RETURNED. It renews at roughly
// half the tightest remaining lease, so neither a slow output sink nor a slow CompleteBatch
// can let the reaper reclaim messages whose bodies were already emitted (review 2026-07-12
// P1; round-2 §3.2 — renewal used to stop before the settle RPC, leaving exactly that
// window open on a high-latency link). During normal fast output the ticker never fires.
type renewer struct {
	cancel context.CancelFunc // aborts a Renew that is already in flight
	stop   chan struct{}
	done   chan struct{} // closed once the goroutine has exited and no Renew is in flight
	once   sync.Once
}

func startRenewer(parent context.Context, c api, queue string, msgs []*mqlite.Message) *renewer {
	// Renewals run on a context WE can cancel. The CLI's own context never expires, so a Renew
	// against a stalled broker would otherwise block in the goroutine forever — and Stop, which
	// waits for it, would hang `receive` indefinitely even though the output and the settle had
	// already succeeded.
	ctx, cancel := context.WithCancel(parent)
	r := &renewer{cancel: cancel, stop: make(chan struct{}), done: make(chan struct{})}
	if len(msgs) == 0 {
		cancel()
		close(r.done)
		return r
	}
	go func() {
		defer close(r.done)
		t := time.NewTicker(renewInterval(msgs))
		defer t.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				// ONE request for the whole batch. Renewing message-by-message cost N round
				// trips, so on a slow link the renewal pass outlasted the lease it was saving:
				// 64 messages × 50ms = 3.2s against a 2s lock, and most locks expired mid-pass
				// (review round-3). Best-effort — a failed renew only risks a redelivery.
				_, _ = c.RenewBatch(ctx, queue, msgs...)
			}
		}
	}()
	return r
}

// Stop ends renewal and waits for the goroutine to exit, so no Renew is still in flight when
// the caller closes the client. It first CANCELS any renewal already running: by the time Stop
// is called the batch is settled, so an outstanding Renew is worthless — and waiting on one
// that is hung would hang the command. Safe to call more than once.
func (r *renewer) Stop() {
	r.once.Do(func() {
		close(r.stop)
		r.cancel()
	})
	<-r.done
}

// renewInterval is a third of the tightest remaining lease in the batch, so the first renew
// lands with the lease still two-thirds alive and a missed tick is survivable.
//
// It is a FRACTION of the lease, never a fixed floor. The old version clamped to a minimum of
// one second, which meant a queue whose lock duration was one second or less had its first
// renewal scheduled at or after its own expiry — the lease could not be held at all (review
// round-3). The remaining floor is only a spin guard for a pathologically short lease; it is
// well below any lock duration the engine will hand out.
const minRenewInterval = 50 * time.Millisecond

func renewInterval(msgs []*mqlite.Message) time.Duration {
	now := time.Now()
	tightest := time.Duration(0)
	for _, m := range msgs {
		if d := m.LockedUntil.Sub(now); d > 0 && (tightest == 0 || d < tightest) {
			tightest = d
		}
	}
	if tightest <= 0 {
		tightest = 30 * time.Second // the default lock duration
	}
	if third := tightest / 3; third > minRenewInterval {
		return third
	}
	return minRenewInterval
}

func formatMsg(m *mqlite.Message, withToken bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "seq=%d deliveries=%d", m.SequenceNumber, m.DeliveryCount)
	if m.MessageID != "" {
		fmt.Fprintf(&b, " message-id=%s", m.MessageID)
	}
	if m.GroupID != "" {
		fmt.Fprintf(&b, " group=%s", m.GroupID)
	}
	if m.ReplyTo != "" {
		fmt.Fprintf(&b, " reply-to=%s", m.ReplyTo)
	}
	if withToken {
		fmt.Fprintf(&b, " lock-token=%s", m.LockToken())
	}
	fmt.Fprintf(&b, " body=%q\n", string(m.Body))
	return b.String()
}

// redact hides any auth token embedded in a DSN before printing.
func redact(s string) string {
	if i := strings.Index(s, "authToken="); i >= 0 {
		return s[:i] + "authToken=***"
	}
	return s
}
