// Command soak runs bounded mixed workloads against a dedicated candidate broker.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"syscall"
	"time"

	mq "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
)

type config struct {
	Endpoint    string        `json:"endpoint"`
	TokenEnv    string        `json:"token_env"`
	Outbox      string        `json:"outbox_db"`
	Evidence    string        `json:"evidence"`
	Duration    time.Duration `json:"duration_ns"`
	Interval    time.Duration `json:"interval_ns"`
	MaxGap      time.Duration `json:"max_gap_ns"`
	EvidenceMax int64         `json:"evidence_max_bytes"`
	OutboxMax   int64         `json:"outbox_max_bytes"`
}

type laneProgress struct {
	Name         string            `json:"name"`
	Batches      uint64            `json:"verified_batches"`
	LogicalSends uint64            `json:"logical_sends"`
	Deliveries   uint64            `json:"fresh_deliveries"`
	Replays      uint64            `json:"replayed_deliveries"`
	Recipes      map[string]uint64 `json:"recipe_hits"`
	LastVerified time.Time         `json:"last_verified_at"`
	LargestGap   float64           `json:"largest_gap_seconds"`
	BatchLatency [16]uint64        `json:"batch_latency_buckets"`
}

type runner struct {
	cfg          config
	seed         string
	client       *mq.Client
	outbox       *mq.Embedded
	started      time.Time
	stopAt       time.Time
	mu           sync.Mutex
	lanes        []laneProgress
	history      *journal
	resources    *journal
	chain        string
	errors       chan error
	auxSamples   uint64
	clockDrift   float64
	heartbeatGap float64
}

func join(errs ...error) error { return errors.Join(errs...) }

func main() {
	var cfg config
	var provenance, auditPath string
	flag.StringVar(&cfg.Endpoint, "endpoint", "", "dedicated empty broker HTTP(S) URL")
	flag.StringVar(&cfg.TokenEnv, "token-env", "MQLITE_TOKEN", "environment variable containing the broker token")
	flag.StringVar(&cfg.Outbox, "outbox-db", "", "new local file for embedded transaction workload")
	flag.StringVar(&cfg.Evidence, "evidence", "", "new or empty directory for runner evidence")
	flag.DurationVar(&cfg.Duration, "duration", time.Minute, "workload duration; less than 24h is only SMOKE")
	flag.DurationVar(&cfg.Interval, "interval", 1500*time.Millisecond, "minimum period between batches in each lane")
	flag.DurationVar(&cfg.MaxGap, "max-gap", 120*time.Second, "maximum time without a verified batch in any lane (15s..120s)")
	flag.Int64Var(&cfg.EvidenceMax, "evidence-max-bytes", 512<<20, "runner evidence budget")
	flag.Int64Var(&cfg.OutboxMax, "outbox-max-bytes", 512<<20, "outbox DB plus sidecars budget")
	flag.StringVar(&provenance, "provenance", "", "read-only JSON metadata supplied by the host wrapper")
	flag.StringVar(&auditPath, "verify-ledger", "", "offline: reconstruct expected manifests and verify completed evidence")
	flag.Parse()
	var err error
	if auditPath != "" {
		err = audit(auditPath)
	} else {
		err = execute(cfg, provenance)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "soak:", err)
		os.Exit(1)
	}
}

func execute(cfg config, provenancePath string) (resultErr error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("endpoint must be an HTTP(S) URL without credentials, query, or fragment")
	}
	if cfg.Outbox == "" || cfg.Evidence == "" || cfg.Duration <= 0 || cfg.Interval < time.Second || cfg.Interval > 10*time.Second ||
		cfg.MaxGap < 15*time.Second || cfg.MaxGap > 120*time.Second || cfg.EvidenceMax < 1<<20 || cfg.OutboxMax < 1<<20 {
		return errors.New("invalid paths, duration, interval, gap, or resource budgets")
	}
	token := os.Getenv(cfg.TokenEnv)
	if token == "" {
		return errors.New("token environment variable is empty")
	}
	cfg.Outbox, err = filepath.Abs(cfg.Outbox)
	if err != nil {
		return err
	}
	cfg.Evidence, err = filepath.Abs(cfg.Evidence)
	if err != nil {
		return err
	}
	for _, suffix := range []string{"", "-wal", "-shm", ".lock"} {
		if _, err := os.Stat(cfg.Outbox + suffix); !errors.Is(err, os.ErrNotExist) {
			return errors.New("outbox database and sidecars must be new")
		}
	}
	if err := os.MkdirAll(cfg.Evidence, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(cfg.Evidence)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("evidence directory is not empty; pending evidence is never resumed or erased")
	}
	defer func() {
		if resultErr != nil {
			resultErr = join(resultErr, atomicJSON(filepath.Join(cfg.Evidence, "result.json"), map[string]any{
				"status": "FAIL", "production_ready": false, "error": resultErr.Error(), "finished_at": time.Now().UTC()}))
		}
	}()
	if err := os.MkdirAll(filepath.Dir(cfg.Outbox), 0700); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(cfg.Evidence, "pending"), 0700); err != nil {
		return err
	}
	var provenance json.RawMessage
	if provenancePath != "" {
		info, err := os.Stat(provenancePath)
		if err != nil {
			return err
		}
		if info.Size() > 64<<10 {
			return errors.New("provenance exceeds 64 KiB")
		}
		provenance, err = os.ReadFile(provenancePath)
		if err != nil {
			return err
		}
		if !json.Valid(provenance) {
			return errors.New("provenance is not valid JSON")
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tr := &http.Transport{MaxIdleConns: 32, MaxIdleConnsPerHost: 32, MaxConnsPerHost: 32, IdleConnTimeout: time.Minute}
	defer tr.CloseIdleConnections()
	client, err := mq.Open(ctx, cfg.Endpoint, mq.WithToken(token), mq.WithHTTPClient(&http.Client{Transport: tr, Timeout: 30 * time.Second}))
	if err != nil {
		return err
	}
	qs, err := client.ListQueues(ctx)
	if err != nil {
		return err
	}
	if len(qs) != 0 {
		return errors.New("broker must be dedicated and contain no queues/subscriptions")
	}
	status, err := client.Status(ctx)
	if err != nil {
		return err
	}
	if status.Remote || status.SchemaVersion == "" || status.PingMs < 0 {
		return errors.New("candidate must report a healthy local SQLite backend")
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return err
	}
	r := &runner{cfg: cfg, seed: hex.EncodeToString(entropy[:]), client: client, errors: make(chan error, 1)}
	lg := slog.New(failLog{Handler: slog.NewJSONHandler(os.Stderr, nil), fail: r.fail})
	outbox, err := mq.OpenEmbedded(ctx, "file:"+cfg.Outbox, mq.WithSynchronous("FULL"), mq.WithLogger(lg))
	if err != nil {
		return err
	}
	r.outbox = outbox
	defer func() { resultErr = join(resultErr, outbox.Close()) }()
	if err := r.setup(ctx); err != nil {
		return err
	}
	r.history, err = newJournal(filepath.Join(cfg.Evidence, "batches.jsonl"))
	if err != nil {
		return err
	}
	defer func() { resultErr = join(resultErr, r.history.file.Close()) }()
	r.resources, err = newJournal(filepath.Join(cfg.Evidence, "resources.jsonl"))
	if err != nil {
		return err
	}
	defer func() { resultErr = join(resultErr, r.resources.file.Close()) }()
	r.started = time.Now()
	r.stopAt = r.started.Add(cfg.Duration)
	for i, name := range laneNames {
		p := laneProgress{Name: name, LastVerified: r.started, Recipes: map[string]uint64{}}
		for _, recipe := range requiredRecipes[i] {
			p.Recipes[recipe] = 0
		}
		r.lanes = append(r.lanes, p)
	}
	build, _ := debug.ReadBuildInfo()
	if err := atomicJSON(filepath.Join(cfg.Evidence, "metadata.json"), map[string]any{
		"config": cfg, "seed": r.seed, "recipe_version": recipeVersion, "started_at": r.started,
		"broker_status": status, "provenance": provenance, "runner_build": build,
		"batch_latency_bucket_upper_ms": []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 0},
	}); err != nil {
		return err
	}
	if err := r.sample(ctx); err != nil {
		return err
	}
	var wg sync.WaitGroup
	for i := range laneNames {
		wg.Add(1)
		go func(lane int) {
			defer wg.Done()
			if err := r.runLane(ctx, lane); err != nil {
				r.fail(fmt.Errorf("lane %s: %w", laneNames[lane], err))
			}
		}(i)
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	err = r.monitor(ctx, finished)
	if err != nil {
		cancel()
		wg.Wait()
		return err
	}
	if err := r.finalCheck(ctx); err != nil {
		return err
	}
	if err := r.sample(ctx); err != nil {
		return err
	}
	// Close is part of the verdict, including late background-loop errors.
	if err := outbox.Close(); err != nil {
		return err
	}
	select {
	case err := <-r.errors:
		return err
	default:
	}
	elapsed := time.Since(r.started)
	verdict, production, err := classify(cfg.Duration, elapsed)
	if err != nil {
		return err
	}
	result := map[string]any{"status": verdict, "production_ready": production,
		"elapsed_seconds": elapsed.Seconds(), "requested_seconds": cfg.Duration.Seconds(),
		"validated_activity_seconds": cfg.Duration.Seconds(), "finished_at": time.Now().UTC(),
		"lanes": r.lanes, "chain_sha256": r.chain, "verified_batches": r.history.count,
		"resource_samples": r.auxSamples, "max_clock_drift_seconds": r.clockDrift,
		"max_heartbeat_gap_seconds": r.heartbeatGap,
		"external_checks_required":  "host wrapper must also accept broker logs, resource series, OOM and container exit states"}
	if err := atomicJSON(filepath.Join(cfg.Evidence, "result.json"), result); err != nil {
		return err
	}
	if err := audit(cfg.Evidence); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func (r *runner) setup(ctx context.Context) error {
	for i, name := range laneNames {
		if i >= 6 {
			continue
		}
		cfg := mq.QueueConfig{LockDuration: 10 * time.Second, MaxDeliveryCount: 8, DedupWindow: 2 * time.Minute,
			DLQMaxAge: -1, DLQMaxCount: -1, DLQMaxBytes: -1}
		if name == "group" {
			cfg.Ordering = mq.OrderGroupFIFO
		}
		if name == "strict" {
			cfg.Ordering = mq.OrderStrictFIFO
		}
		if name == "retry" {
			cfg.LockDuration, cfg.MaxDeliveryCount = time.Second, 3
		}
		if err := r.client.CreateQueue(ctx, queue(r.seed, name), cfg); err != nil {
			return err
		}
	}
	no := false
	if err := r.client.CreateQueue(ctx, queue(r.seed, "ttl-discard"), mq.QueueConfig{
		LockDuration: 10 * time.Second, DeadLetterOnExpire: &no, DedupWindow: 2 * time.Minute}); err != nil {
		return err
	}
	if err := r.client.Subscribe(ctx, queue(r.seed, "events"), queue(r.seed, "all-events"), nil); err != nil {
		return err
	}
	if err := r.client.Subscribe(ctx, queue(r.seed, "events"), queue(r.seed, "eu-events"),
		&mq.Filter{Expr: `subject == "keep" && properties["region"] == "eu"`}); err != nil {
		return err
	}
	if err := r.outbox.CreateQueue(ctx, queue(r.seed, "outbox"), mq.QueueConfig{DedupWindow: 2 * time.Minute}); err != nil {
		return err
	}
	return r.outbox.Tx(ctx, func(tx *engine.EngineTx) error {
		_, err := tx.SQL().ExecContext(tx.Context(), `CREATE TABLE soak_business (id TEXT PRIMARY KEY, amount INTEGER NOT NULL, body BLOB NOT NULL) STRICT`)
		return err
	})
}

func (r *runner) fail(err error) {
	select {
	case r.errors <- err:
	default:
	}
}

type failLog struct {
	slog.Handler
	fail func(error)
}

func (h failLog) Handle(ctx context.Context, record slog.Record) error {
	if record.Level >= slog.LevelWarn {
		h.fail(fmt.Errorf("embedded engine log: %s", record.Message))
	}
	return h.Handler.Handle(ctx, record)
}
func (h failLog) WithAttrs(a []slog.Attr) slog.Handler {
	return failLog{h.Handler.WithAttrs(a), h.fail}
}
func (h failLog) WithGroup(g string) slog.Handler { return failLog{h.Handler.WithGroup(g), h.fail} }

func classify(requested, elapsed time.Duration) (string, bool, error) {
	if elapsed < requested {
		return "FAIL", false, errors.New("workload ended before requested duration")
	}
	if requested < 24*time.Hour {
		return "SMOKE", false, nil
	}
	return "PASS", true, nil
}

func (r *runner) monitor(ctx context.Context, done <-chan struct{}) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	last := r.started
	steps := 0
	for {
		select {
		case err := <-r.errors:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			select {
			case err := <-r.errors:
				return err
			default:
			}
			return r.checkTime(time.Now(), last)
		case <-ticker.C:
			now := time.Now()
			if err := r.checkTime(now, last); err != nil {
				return err
			}
			last = now
			if err := r.checkBudgets(); err != nil {
				return err
			}
			steps++
			if steps%5 == 0 {
				r.mu.Lock()
				err := atomicJSON(filepath.Join(r.cfg.Evidence, "progress.json"), map[string]any{
					"status": "RUNNING", "at": now.UTC(), "elapsed_seconds": now.Sub(r.started).Seconds(),
					"lanes": r.lanes, "chain_sha256": r.chain, "verified_batches": r.history.count})
				r.mu.Unlock()
				if err != nil {
					return err
				}
			}
			if steps%60 == 0 {
				if err := r.sample(ctx); err != nil {
					return err
				}
			}
		}
	}
}

func (r *runner) checkTime(now, previous time.Time) error {
	gap := now.Sub(previous)
	drift := math.Abs(float64(now.UnixNano()-r.started.UnixNano())/float64(time.Second) - now.Sub(r.started).Seconds())
	r.heartbeatGap = math.Max(r.heartbeatGap, gap.Seconds())
	r.clockDrift = math.Max(r.clockDrift, drift)
	if gap > 10*time.Second || drift > 5 {
		return fmt.Errorf("activity clock anomaly: heartbeat=%s wall/monotonic drift=%.3fs", gap, drift)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, lane := range r.lanes {
		if now.Sub(lane.LastVerified) > r.cfg.MaxGap {
			return fmt.Errorf("lane %s stalled: no verified batch for %s", lane.Name, now.Sub(lane.LastVerified))
		}
	}
	return nil
}

func (r *runner) checkBudgets() error {
	var outboxSize int64
	for _, suffix := range []string{"", "-wal", "-shm", ".lock"} {
		info, err := os.Stat(r.cfg.Outbox + suffix)
		if errors.Is(err, os.ErrNotExist) && suffix != "" {
			continue
		}
		if err != nil {
			return err
		}
		outboxSize += info.Size()
	}
	if outboxSize > r.cfg.OutboxMax {
		return fmt.Errorf("outbox budget exceeded: %d", outboxSize)
	}
	var evidenceSize int64
	err := filepath.WalkDir(r.cfg.Evidence, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		evidenceSize += info.Size()
		return nil
	})
	if err != nil {
		return err
	}
	if evidenceSize > r.cfg.EvidenceMax {
		return fmt.Errorf("evidence budget exceeded: %d", evidenceSize)
	}
	return nil
}

func (r *runner) sample(ctx context.Context) error {
	type aux struct {
		Rows  int64 `json:"rows"`
		Stale int64 `json:"stale_beyond_janitor_grace"`
	}
	counts := map[string]aux{}
	err := r.outbox.Tx(ctx, func(tx *engine.EngineTx) error {
		for _, table := range []string{"dedup", "settlement_receipts", "receive_attempts"} {
			column, cutoff := "expires_at", time.Now().Add(-2*time.Minute).UnixMilli()
			if table == "dedup" {
				column, cutoff = "seen_at", time.Now().Add(-4*time.Minute).UnixMilli()
			}
			var a aux
			if err := tx.SQL().QueryRowContext(tx.Context(), "SELECT COUNT(*), COALESCE(SUM("+column+"<?),0) FROM "+table, cutoff).Scan(&a.Rows, &a.Stale); err != nil {
				return err
			}
			if a.Stale != 0 {
				return fmt.Errorf("outbox janitor left %d stale %s rows", a.Stale, table)
			}
			counts[table] = a
		}
		return nil
	})
	if err != nil {
		return err
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	if err := r.resources.add(map[string]any{"at": time.Now().UTC(), "elapsed_seconds": time.Since(r.started).Seconds(),
		"heap_alloc_bytes": memory.HeapAlloc, "go_sys_bytes": memory.Sys, "goroutines": runtime.NumGoroutine(), "outbox_aux": counts}); err != nil {
		return err
	}
	r.auxSamples++
	return r.checkBudgets()
}

func (r *runner) finalCheck(ctx context.Context) error {
	for backend, api := range []queueAPI{r.client, r.outbox} {
		qs, err := api.ListQueues(ctx)
		if err != nil {
			return err
		}
		suffixes := []string{"ordinary", "group", "strict", "retry", "scheduled", "deferred", "ttl-discard", "all-events", "eu-events"}
		if backend == 1 {
			suffixes = []string{"outbox"}
		}
		want := make(map[string]bool, len(suffixes))
		for _, suffix := range suffixes {
			want[queue(r.seed, suffix)] = true
		}
		if len(qs) != len(want) {
			return errors.New("final queue set changed")
		}
		for _, q := range qs {
			if !want[q.Name] {
				return fmt.Errorf("unexpected final queue %s", q.Name)
			}
			delete(want, q.Name)
			if err := emptyStore(ctx, api, q.Name); err != nil {
				return err
			}
		}
	}
	if err := r.outbox.Tx(ctx, func(tx *engine.EngineTx) error {
		var count int64
		if err := tx.SQL().QueryRowContext(tx.Context(), "SELECT COUNT(*) FROM soak_business").Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("outbox has %d unverified business rows", count)
		}
		return nil
	}); err != nil {
		return err
	}
	entries, err := os.ReadDir(filepath.Join(r.cfg.Evidence, "pending"))
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("pending batches remain")
	}
	for _, lane := range r.lanes {
		if lane.Batches == 0 {
			return fmt.Errorf("lane %s made no progress", lane.Name)
		}
		for name, hits := range lane.Recipes {
			if hits == 0 {
				return fmt.Errorf("lane %s did not exercise recipe %s", lane.Name, name)
			}
		}
	}
	return nil
}

func (r *runner) verified(b *batch, started time.Time) error {
	recipes := make([]string, 0, len(b.hits))
	for recipe := range b.hits {
		recipes = append(recipes, recipe)
	}
	sort.Strings(recipes)
	want := expectedSummary(b.plan.Lane, b.plan.Batch)
	if b.sent != want.LogicalSends || b.deliveries != want.Deliveries || b.replays != want.Replays || !reflect.DeepEqual(recipes, want.Recipes) {
		return errors.New("verified batch counts or recipe set differ from the independent recipe contract")
	}
	verifiedAt := time.Now()
	record := batchRecord{Version: recipeVersion, Seed: r.seed, Lane: b.plan.Lane, Batch: b.plan.Batch,
		CreatedAt: b.plan.CreatedAt, VerifiedAt: verifiedAt.UnixMilli(), ExpectedHash: b.ledger.expectedHash,
		AckHash: b.ledger.ack.digest(), ObservedHash: b.ledger.observed.digest(), AckRecords: b.ledger.ack.count,
		Observations: b.ledger.observed.count, LogicalSends: b.sent, Deliveries: b.deliveries, Replays: b.replays, Recipes: recipes}
	r.mu.Lock()
	defer r.mu.Unlock()
	record.Previous = r.chain
	var err error
	record.Hash, err = recordHash(record)
	if err != nil {
		return err
	}
	if err := r.history.add(record); err != nil {
		return err
	}
	r.chain = record.Hash
	lane := &r.lanes[b.plan.Lane]
	lane.Batches++
	lane.LogicalSends += b.sent
	lane.Deliveries += b.deliveries
	lane.Replays += b.replays
	for _, recipe := range recipes {
		if _, ok := lane.Recipes[recipe]; !ok {
			return fmt.Errorf("unknown recipe %s", recipe)
		}
		lane.Recipes[recipe]++
	}
	gap := verifiedAt.Sub(lane.LastVerified)
	lane.LargestGap = math.Max(lane.LargestGap, gap.Seconds())
	lane.LastVerified = verifiedAt
	bucket, millis := 0, time.Since(started).Milliseconds()
	for bucket < 15 && millis > 1<<bucket {
		bucket++
	}
	lane.BatchLatency[bucket]++
	return nil
}

// Keep the expected outcomes independent from counters in the workload paths.
func expectedSummary(lane int, batch uint64) batchRecord {
	r := batchRecord{LogicalSends: []uint64{4, 4, 2, 1, 2, 2, 2, 2}[lane],
		Deliveries: []uint64{4, 5, 4, 4, 2, 3, 3, 2}[lane]}
	r.Recipes = append([]string(nil), requiredRecipes[lane]...)
	switch lane {
	case 0:
		r.Replays = 4
		r.Recipes = []string{"dedup-replay", "attempt-replay"}
		switch batch % 3 {
		case 0:
			r.Recipes = append(r.Recipes, "renew", "renew-batch", "complete-batch", "complete-replay")
		case 1:
			r.Recipes = append(r.Recipes, "renew", "renew-batch", "complete-replay")
		case 2:
			r.Recipes = append(r.Recipes, "receive-delete")
		}
	case 3:
		r.Recipes = []string{"redrive", "max-delivery-dlq"}
		switch batch % 3 {
		case 1:
			r.Deliveries, r.Recipes = 2, []string{"redrive", "explicit-reject"}
		case 2:
			r.Recipes = append(r.Recipes, "expired-token-fenced")
		}
	case 5:
		r.Recipes = []string{"deferred-excluded", "pick", "expired-deferred-excluded"}
		if batch%2 == 0 {
			r.Recipes = append(r.Recipes, "ttl-dlq")
		} else {
			r.Recipes = append(r.Recipes, "ttl-discard")
		}
	}
	sort.Strings(r.Recipes)
	return r
}

func audit(root string) error {
	var metadata struct {
		Seed    string    `json:"seed"`
		Version int       `json:"recipe_version"`
		Config  config    `json:"config"`
		Started time.Time `json:"started_at"`
	}
	data, err := os.ReadFile(filepath.Join(root, "metadata.json"))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return err
	}
	seedBytes, seedErr := hex.DecodeString(metadata.Seed)
	if metadata.Version != recipeVersion || seedErr != nil || len(seedBytes) != 16 || metadata.Started.IsZero() || metadata.Config.Duration <= 0 {
		return errors.New("unsupported recipe version")
	}
	f, err := os.Open(filepath.Join(root, "batches.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 64<<10)
	var chain string
	counts := make([]uint64, len(laneNames))
	summaries := make([]laneProgress, len(laneNames))
	for i, name := range laneNames {
		summaries[i] = laneProgress{Name: name, LastVerified: metadata.Started, Recipes: map[string]uint64{}}
	}
	var total uint64
	for scanner.Scan() {
		var record batchRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return err
		}
		if record.Version != recipeVersion || record.Seed != metadata.Seed || record.Lane < 0 || record.Lane >= len(laneNames) {
			return errors.New("invalid batch identity")
		}
		if record.Batch != counts[record.Lane] || record.Previous != chain {
			return errors.New("batch sequence or chain discontinuity")
		}
		want, err := recordHash(record)
		if err != nil {
			return err
		}
		if want != record.Hash {
			return errors.New("batch record hash mismatch")
		}
		p := makePlan(record.Seed, record.Lane, record.Batch, record.CreatedAt)
		expected, err := encoded(p)
		if err != nil {
			return err
		}
		if sumBytes(expected) != record.ExpectedHash {
			return errors.New("reconstructed expected manifest hash mismatch")
		}
		for _, value := range []string{record.AckHash, record.ObservedHash} {
			decoded, err := hex.DecodeString(value)
			if err != nil || len(decoded) != 32 {
				return errors.New("invalid acknowledgement/observation digest")
			}
		}
		if record.AckRecords == 0 || record.Observations == 0 {
			return errors.New("batch lacks acknowledgement/observation evidence")
		}
		expect := expectedSummary(record.Lane, record.Batch)
		if record.LogicalSends != expect.LogicalSends || record.Deliveries != expect.Deliveries || record.Replays != expect.Replays || !reflect.DeepEqual(record.Recipes, expect.Recipes) {
			return errors.New("batch counts or recipes differ from reconstructed contract")
		}
		lane := &summaries[record.Lane]
		verified := time.UnixMilli(record.VerifiedAt)
		if record.CreatedAt < lane.LastVerified.UnixMilli() || record.VerifiedAt < record.CreatedAt || verified.Sub(lane.LastVerified) > metadata.Config.MaxGap {
			return errors.New("batch times or lane activity gap are invalid")
		}
		lane.LastVerified = verified
		lane.LogicalSends += record.LogicalSends
		lane.Deliveries += record.Deliveries
		lane.Replays += record.Replays
		for _, recipe := range record.Recipes {
			lane.Recipes[recipe]++
		}
		counts[record.Lane]++
		total++
		chain = record.Hash
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	var result struct {
		Status       string         `json:"status"`
		Chain        string         `json:"chain_sha256"`
		Batches      uint64         `json:"verified_batches"`
		Lanes        []laneProgress `json:"lanes"`
		Production   bool           `json:"production_ready"`
		Elapsed      float64        `json:"elapsed_seconds"`
		Requested    float64        `json:"requested_seconds"`
		Activity     float64        `json:"validated_activity_seconds"`
		Finished     time.Time      `json:"finished_at"`
		ClockDrift   float64        `json:"max_clock_drift_seconds"`
		HeartbeatGap float64        `json:"max_heartbeat_gap_seconds"`
	}
	data, err = os.ReadFile(filepath.Join(root, "result.json"))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}
	if (result.Status != "SMOKE" && result.Status != "PASS") || result.Chain != chain || result.Batches != total || len(result.Lanes) != len(laneNames) {
		return errors.New("result does not match verified history")
	}
	verdict, production, err := classify(metadata.Config.Duration, time.Duration(result.Elapsed*float64(time.Second)))
	if err != nil || verdict != result.Status || production != result.Production || result.Requested != metadata.Config.Duration.Seconds() || result.Activity != result.Requested ||
		result.Finished.Sub(metadata.Started) < metadata.Config.Duration || result.ClockDrift > 5 || result.HeartbeatGap > 10 {
		return errors.New("duration, activity, or production verdict is invalid")
	}
	for i, count := range counts {
		got, want := result.Lanes[i], summaries[i]
		if count == 0 || got.Batches != count || got.Name != want.Name || got.LogicalSends != want.LogicalSends || got.Deliveries != want.Deliveries || got.Replays != want.Replays || !reflect.DeepEqual(got.Recipes, want.Recipes) {
			return errors.New("lane counts differ from history")
		}
		if got.LastVerified.UnixMilli() != want.LastVerified.UnixMilli() || got.LargestGap > metadata.Config.MaxGap.Seconds() || result.Finished.Sub(want.LastVerified) > metadata.Config.MaxGap {
			return errors.New("final lane activity differs from history")
		}
		for _, recipe := range requiredRecipes[i] {
			if want.Recipes[recipe] == 0 {
				return errors.New("required recipe was never exercised")
			}
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "pending"))
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("pending evidence remains")
	}
	fmt.Printf("ledger verified: %d batches, %s\n", total, chain)
	return nil
}
