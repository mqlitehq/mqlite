package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"time"

	native "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
)

const verificationDBHash = "b99fe180288dcf13a7b5ab799d22ab9439a1a6443ca95db999edec42f5f9d2e5"
const scenarioBudget, cleanupBudget = 240 * time.Second, 60 * time.Second
const openBudget, rpcBudget = 45 * time.Second, 15 * time.Second

type configuration struct {
	output, runtimeSHA, sdkSHA, harnessSHA string
	open                                   func(context.Context) (*native.Embedded, error)
	scenario, cleanup, handler             time.Duration
	failAt                                 string // Local negative-control seam; never exposed by the command.
	afterCreate                            func(context.Context) error
	localFixture                           bool
}

type owner struct {
	embedded *native.Embedded
	broker   *httptest.Server
	clients  map[string]*adapter
	stopped  chan struct{}
	closeErr error
	unjoined <-chan error
}

func openOwner(ctx context.Context, cfg configuration, recorder *rpcRecorder) (*owner, error) {
	openCtx, cancel := context.WithTimeout(ctx, openBudget)
	defer cancel()
	embedded, err := cfg.open(openCtx)
	if err != nil {
		return nil, err
	}
	var secret [24]byte
	if _, err := rand.Read(secret[:]); err != nil {
		_ = embedded.Close()
		return nil, err
	}
	token := hex.EncodeToString(secret[:])
	broker := httptest.NewServer(server.New(embedded.Engine(), []string{token}).Handler())
	o := &owner{embedded: embedded, broker: broker, clients: map[string]*adapter{}}
	for _, kind := range []string{"native", "sdk"} {
		client, err := newAdapter(openCtx, kind, broker.URL, token, recorder)
		if err != nil {
			_ = o.stop(openCtx)
			return nil, err
		}
		o.clients[kind] = client
	}
	return o, nil
}

func (o *owner) stop(ctx context.Context) error {
	if o.stopped == nil {
		o.stopped = make(chan struct{})
		for _, client := range o.clients {
			client.close()
		}
		o.broker.CloseClientConnections()
		go func() { o.broker.Close(); o.closeErr = o.embedded.Close(); close(o.stopped) }()
	}
	select {
	case <-o.stopped:
		return o.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func call(ctx context.Context, fn func(context.Context) error) error {
	c, cancel := context.WithTimeout(ctx, rpcBudget)
	defer cancel()
	return fn(c)
}

func zeroQueue(ctx context.Context, embedded *native.Embedded, queue string) error {
	return call(ctx, func(ctx context.Context) error {
		stats, err := embedded.Stats(ctx, queue)
		if err != nil {
			return err
		}
		if stats.Total != 0 || stats.Active != 0 || stats.Locked != 0 || stats.DeadLettered != 0 || stats.Deferred != 0 || stats.Scheduled != 0 {
			return errors.New("owned queue did not drain to zero")
		}
		return nil
	})
}

func manualReceive(ctx context.Context, l *ledger, a *adapter, queue string, count int) error {
	var messages []delivery
	if err := call(ctx, func(ctx context.Context) error {
		var err error
		messages, err = a.receive(ctx, queue, count)
		return err
	}); err != nil {
		return err
	}
	if len(messages) != count {
		return errors.New("received batch inventory mismatch")
	}
	for _, message := range messages {
		if err := l.delivered(message); err != nil {
			return err
		}
	}
	if err := call(ctx, func(ctx context.Context) error { return a.settle(ctx, queue, messages, true) }); err != nil {
		return err
	}
	if err := call(ctx, func(ctx context.Context) error { return a.settle(ctx, queue, messages, false) }); err != nil {
		return err
	}
	return l.completed(messages)
}

func basic(ctx context.Context, l *ledger, o *owner, kind, queue string) error {
	items := l.register(queue, 2, 128)
	items[1].Body = append(items[1].Body, make([]byte, 4096)...)
	// Register the final payload before any request is issued.
	l.mu.Lock()
	l.expected[items[1].ID] = items[1]
	for i := range l.report.Identities {
		if l.report.Identities[i].ID == items[1].ID {
			l.report.Identities[i].BodySHA256 = hashBytes(items[1].Body)
		}
	}
	l.mu.Unlock()
	if err := l.save(); err != nil {
		return err
	}
	a := o.clients[kind]
	var seqs []int64
	if err := call(ctx, func(ctx context.Context) error { var err error; seqs, err = a.send(ctx, queue, items); return err }); err != nil {
		return err
	}
	if err := l.acknowledge(items, seqs); err != nil {
		return err
	}
	if kind == "native" && l.failAt == "native-after-send" {
		return errors.New("controlled failure after acknowledged send")
	}
	if err := manualReceive(ctx, l, a, queue, 2); err != nil {
		return err
	}
	return zeroQueue(ctx, o.embedded, queue)
}

func transactions(ctx context.Context, l *ledger, o *owner, queue string) error {
	items := l.register(queue, 2, 64)
	var seqs []int64
	if err := l.save(); err != nil {
		return err
	}
	if err := call(ctx, func(ctx context.Context) error {
		return o.embedded.Tx(ctx, func(tx *engine.EngineTx) error {
			seqs = nil
			for _, item := range items {
				seq, err := tx.SendOne(queue, engine.OutMessage{Body: item.Body, MessageID: item.ID})
				if err != nil {
					return err
				}
				seqs = append(seqs, seq)
			}
			return nil
		})
	}); err != nil {
		return err
	}
	if err := l.acknowledge(items, seqs); err != nil {
		return err
	}
	rollback := errors.New("controlled transaction rollback")
	err := call(ctx, func(ctx context.Context) error {
		return o.embedded.Tx(ctx, func(tx *engine.EngineTx) error {
			for i := 0; i < 2; i++ {
				if _, err := tx.SendOne(queue, engine.OutMessage{Body: []byte("rollback"), MessageID: fmt.Sprintf("%s-rollback-%d", queue, i)}); err != nil {
					return err
				}
			}
			return rollback
		})
	})
	if !errors.Is(err, rollback) {
		return errors.New("transaction did not return the controlled rollback")
	}
	if err := call(ctx, func(ctx context.Context) error {
		stats, err := o.embedded.Stats(ctx, queue)
		if err != nil {
			return err
		}
		if stats.Total != 2 || stats.Active != 2 {
			return errors.New("rollback changed committed inventory")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := manualReceive(ctx, l, &adapter{kind: "engine", embedded: o.embedded}, queue, 2); err != nil {
		return err
	}
	return zeroQueue(ctx, o.embedded, queue)
}

func managed(ctx context.Context, l *ledger, o *owner, trace *rpcRecorder, kind, queue string, hold time.Duration) (resultErr error) {
	scenarioDeadline, bounded := ctx.Deadline()
	if !bounded || time.Until(scenarioDeadline) <= 30*time.Second {
		return errors.New("not enough scenario budget to start and join a managed receiver")
	}
	workCtx, workCancel := context.WithDeadline(ctx, scenarioDeadline.Add(-30*time.Second))
	defer workCancel()
	ctx = workCtx
	items := l.register(queue, 1, 64)
	if err := l.save(); err != nil {
		return err
	}
	a := o.clients[kind]
	var seqs []int64
	if err := call(ctx, func(ctx context.Context) error { var err error; seqs, err = a.send(ctx, queue, items); return err }); err != nil {
		return err
	}
	if err := l.acknowledge(items, seqs); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	failures := make(chan error, 1)
	delivered := make(chan delivery, 1)
	var invocations atomic.Int64
	go func() {
		done <- a.run(runCtx, queue, func(handlerCtx context.Context, message delivery) error {
			invocations.Add(1)
			if err := l.delivered(message); err != nil {
				select {
				case failures <- err:
				default:
				}
				return err
			}
			select {
			case delivered <- message:
			default:
			}
			timer := time.NewTimer(hold)
			defer timer.Stop()
			select {
			case <-timer.C:
				return nil
			case <-handlerCtx.Done():
				return handlerCtx.Err()
			}
		}, func(err error) {
			select {
			case failures <- err:
			default:
			}
		})
	}()
	joined, joinAttempted := false, false
	postRunError := func() error {
		select {
		case err := <-failures:
			return err
		default:
		}
		if invocations.Load() != 1 {
			return errors.New("managed handler invocation count was not exactly one")
		}
		return nil
	}
	joinReceiver := func() error {
		joinAttempted = true
		cancel()
		deadline := time.Now().Add(30 * time.Second)
		if scenarioDeadline.Before(deadline) {
			deadline = scenarioDeadline
		}
		joinCtx, joinCancel := context.WithDeadline(context.Background(), deadline)
		defer joinCancel()
		select {
		case err := <-done:
			joined = true
			if !errors.Is(err, context.Canceled) {
				return errors.New("receiver did not end with caller cancellation")
			}
			return postRunError()
		case <-joinCtx.Done():
			o.unjoined = done
			return errors.New("receiver did not join within the scenario confirmation budget")
		}
	}
	defer func() {
		cancel()
		if !joined && !joinAttempted {
			resultErr = errors.Join(resultErr, joinReceiver())
		}
		if joined {
			resultErr = errors.Join(resultErr, postRunError())
		}
		l.mu.Lock()
		l.report.Managed = append(l.report.Managed, managedCheck{Queue: queue, Invocations: invocations.Load(), Joined: joined})
		l.mu.Unlock()
	}()
	var message delivery
	select {
	case message = <-delivered:
	case err := <-failures:
		return err
	case err := <-done:
		joined = true
		return fmt.Errorf("receiver exited before delivery: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-failures:
			return err
		case err := <-done:
			joined = true
			return fmt.Errorf("receiver exited before completion: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		if trace.count(queue, "Complete", message.Sequence)+trace.count(queue, "CompleteBatch", message.Sequence) == 0 {
			continue
		}
		if trace.count(queue, "Renew", message.Sequence)+trace.count(queue, "RenewBatch", message.Sequence) == 0 {
			return errors.New("short-lease handler completed without a confirmed renewal")
		}
		if err := zeroQueue(ctx, o.embedded, queue); err != nil {
			return err
		}
		if err := joinReceiver(); err != nil {
			return err
		}
		return l.completed([]delivery{message})
	}
}

func cleanupOwned(ctx context.Context, l *ledger, o *owner) error {
	var first error
	for i := range l.report.Resources {
		resource := &l.report.Resources[i]
		if !resource.Eligible {
			continue
		}
		err := call(ctx, func(ctx context.Context) error {
			for {
				result, err := o.embedded.DeleteQueueWithOptions(ctx, resource.Name, native.DeleteQueueOpts{Force: true})
				if errors.Is(err, native.ErrQueueNotFound) {
					resource.Deleted = true
					break
				}
				if err != nil {
					return err
				}
				if result.Deleted && result.State == "deleted" {
					resource.Deleted = true
					break
				}
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			_, err := o.embedded.Stats(ctx, resource.Name)
			if !errors.Is(err, native.ErrQueueNotFound) {
				return errors.New("deleted owned resource still answers Stats")
			}
			return nil
		})
		if err != nil {
			l.fail("cleanup-owned", err)
			if first == nil {
				first = err
			}
		}
		_ = l.save()
	}
	err := call(ctx, func(ctx context.Context) error {
		queues, err := o.embedded.ListQueues(ctx)
		if err != nil {
			return err
		}
		for i := range l.report.Resources {
			resource := &l.report.Resources[i]
			if !resource.Eligible {
				continue
			}
			present := false
			for _, queue := range queues {
				if queue.Name == resource.Name {
					present = true
				}
			}
			resource.Absent = resource.Deleted && !present
			if !resource.Absent {
				return errors.New("owned resource cleanup was not confirmed")
			}
		}
		return nil
	})
	if err != nil {
		l.fail("cleanup-readback", err)
		if first == nil {
			first = err
		}
	}
	return first
}

func run(cfg configuration) (result report, resultErr error) {
	if cfg.scenario == 0 {
		cfg.scenario = scenarioBudget
	}
	if cfg.cleanup == 0 {
		cfg.cleanup = cleanupBudget
	}
	if cfg.handler == 0 {
		cfg.handler = 4 * time.Second
	}
	start := time.Now()
	finalDeadline := start.Add(cfg.scenario + cfg.cleanup)
	if err := os.MkdirAll(cfg.output, 0o700); err != nil {
		return result, err
	}
	if _, err := os.Stat(filepath.Join(cfg.output, "evidence.json")); err == nil {
		return result, errors.New("evidence destination already contains a run")
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return result, err
	}
	prefix := "m164-" + hex.EncodeToString(nonce[:])
	l := &ledger{output: cfg.output, expected: map[string]entry{}, failAt: cfg.failAt, report: report{Scope: "standard-Turso bounded compatibility smoke; not throughput or soak acceptance", Prefix: prefix, RuntimeSHA: cfg.runtimeSHA, SDKSHA: cfg.sdkSHA, HarnessSHA: cfg.harnessSHA, ScenarioSeconds: int(cfg.scenario.Seconds()), CleanupSeconds: int(cfg.cleanup.Seconds())}}
	if cfg.localFixture {
		l.report.Scope = "local SQLite/loopback harness validation; not Turso evidence"
	}
	trace := &rpcRecorder{allowed: map[string]bool{}}
	for _, suffix := range []string{"tx", "native", "sdk", "managed-native", "managed-sdk", "reopen"} {
		name := prefix + "-" + suffix
		l.report.Resources = append(l.report.Resources, resource{Name: name})
		trace.allowed[name] = true
	}
	var o *owner
	defer func() {
		cleanupDeadline := time.Now().Add(cfg.cleanup)
		if finalDeadline.Before(cleanupDeadline) {
			cleanupDeadline = finalDeadline
		}
		cleanupCtx, cancel := context.WithDeadline(context.Background(), cleanupDeadline)
		defer cancel()
		canCleanup := true
		if o != nil && o.unjoined != nil {
			select {
			case <-o.unjoined:
				o.unjoined = nil
			case <-cleanupCtx.Done():
				l.fail("unjoined-receiver", cleanupCtx.Err())
				resultErr = errors.Join(resultErr, cleanupCtx.Err())
				canCleanup = false
			}
		}
		if canCleanup && o != nil && o.stopped != nil {
			if err := o.stop(cleanupCtx); err != nil {
				l.fail("join-closing-owner", err)
				resultErr = err
			} else {
				o = nil
			}
		}
		eligible := false
		for _, resource := range l.report.Resources {
			eligible = eligible || resource.Eligible
		}
		if canCleanup && o == nil && eligible && cleanupCtx.Err() == nil {
			var err error
			o, err = openOwner(cleanupCtx, cfg, trace)
			if err != nil {
				l.fail("cleanup-reopen", err)
				resultErr = err
			}
		}
		if canCleanup && o != nil && o.stopped == nil {
			if err := cleanupOwned(cleanupCtx, l, o); err != nil {
				resultErr = err
			}
		}
		if canCleanup && o != nil {
			if err := o.stop(cleanupCtx); err != nil {
				l.fail("final-owner-close", err)
				resultErr = err
			}
		}
		trace.mu.Lock()
		events := append([]rpcEvent(nil), trace.events...)
		trace.mu.Unlock()
		l.mu.Lock()
		l.report.Trace = events
		l.report.Pass = resultErr == nil && len(l.report.Failures) == 0
		l.mu.Unlock()
		summary := fmt.Sprintf("# Bounded compatibility smoke\n\nScope: %s.\n\nPassed: %t. Message identities: %d. Owned resources: %d.\n\nRuntime: `%s`; SDK: `%s`; harness: `%s`.\n\nThis is not a throughput, capacity, long-duration, or live-lease restart guarantee.\n", l.report.Scope, l.report.Pass, len(l.report.Identities), len(l.report.Resources), l.report.RuntimeSHA, l.report.SDKSHA, l.report.HarnessSHA)
		if err := os.WriteFile(filepath.Join(cfg.output, "summary.md"), []byte(summary), 0o600); err != nil {
			l.fail("summary-evidence", err)
			resultErr = err
		}
		if err := l.save(); err != nil {
			resultErr = err
		}
		l.mu.Lock()
		raw, copyErr := json.Marshal(l.report)
		if copyErr == nil {
			copyErr = json.Unmarshal(raw, &result)
		}
		if copyErr != nil {
			resultErr = copyErr
		}
		l.mu.Unlock()
	}()
	if err := l.save(); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.scenario)
	defer cancel()
	if err := l.timed("initial-open", func() error { var err error; o, err = openOwner(ctx, cfg, trace); return err }); err != nil {
		return result, err
	}
	if err := l.timed("owned-name-preflight", func() error {
		return call(ctx, func(ctx context.Context) error {
			queues, err := o.embedded.ListQueues(ctx)
			if err != nil {
				return err
			}
			for _, queue := range queues {
				if trace.allowed[queue.Name] {
					return errors.New("owned random name already exists")
				}
			}
			for i := range l.report.Resources {
				l.report.Resources[i].Eligible = true
			}
			return nil
		})
	}); err != nil {
		return result, err
	}
	for i := range l.report.Resources {
		resource := &l.report.Resources[i]
		lock := 30 * time.Second
		if i == 3 || i == 4 {
			lock = 3 * time.Second
		}
		if err := l.timed("create-owned", func() error {
			return call(ctx, func(ctx context.Context) error {
				err := o.embedded.CreateQueue(ctx, resource.Name, native.QueueConfig{LockDuration: lock, MaxDeliveryCount: 3})
				if err == nil {
					resource.Created = true
				}
				return err
			})
		}); err != nil {
			return result, err
		}
	}
	if cfg.afterCreate != nil {
		if err := l.timed("local-budget-control", func() error { return cfg.afterCreate(ctx) }); err != nil {
			return result, err
		}
	}
	queues := l.report.Resources
	if err := l.timed("transaction-commit-rollback", func() error { return transactions(ctx, l, o, queues[0].Name) }); err != nil {
		return result, err
	}
	for i, kind := range []string{"native", "sdk"} {
		if err := l.timed("manual-"+kind, func() error { return basic(ctx, l, o, kind, queues[i+1].Name) }); err != nil {
			return result, err
		}
	}
	for i, kind := range []string{"native", "sdk"} {
		if err := l.timed("managed-"+kind, func() error { return managed(ctx, l, o, trace, kind, queues[i+3].Name, cfg.handler) }); err != nil {
			return result, err
		}
	}
	if err := l.timed("acknowledge-unclaimed-before-reopen", func() error {
		items := l.register(queues[5].Name, 2, 64)
		if err := l.save(); err != nil {
			return err
		}
		var seqs []int64
		if err := call(ctx, func(ctx context.Context) error {
			var err error
			seqs, err = o.clients["native"].send(ctx, queues[5].Name, items)
			return err
		}); err != nil {
			return err
		}
		return l.acknowledge(items, seqs)
	}); err != nil {
		return result, err
	}
	if err := l.timed("close-before-reopen", func() error { return o.stop(ctx) }); err != nil {
		return result, err
	}
	o = nil
	if err := l.timed("reopen", func() error { var err error; o, err = openOwner(ctx, cfg, trace); return err }); err != nil {
		return result, err
	}
	for _, kind := range []string{"native", "sdk"} {
		if err := l.timed("consume-after-reopen-"+kind, func() error { return manualReceive(ctx, l, o.clients[kind], queues[5].Name, 1) }); err != nil {
			return result, err
		}
	}
	if err := l.timed("final-ledger", func() error {
		if err := l.verifyIdentities(); err != nil {
			return err
		}
		for _, queue := range queues {
			if err := zeroQueue(ctx, o.embedded, queue.Name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return result, err
	}
	return result, nil
}

func main() {
	defer func() {
		if recover() != nil {
			fmt.Fprintln(os.Stderr, "Bounded compatibility harness panicked; no raw panic is emitted.")
			os.Exit(1)
		}
	}()
	dsn, token := os.Getenv("MQLITE_TEST_DB"), os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")
	if !validRemoteDestination(dsn, token, os.Getenv("MQLITE_TEST_DB_RESET_CONFIRM")) {
		fmt.Fprintln(os.Stderr, "Dedicated verification configuration rejected.")
		os.Exit(2)
	}
	output := os.Getenv("M164_EVIDENCE_DIR")
	if output == "" {
		fmt.Fprintln(os.Stderr, "Evidence directory is required.")
		os.Exit(2)
	}
	cfg := configuration{output: filepath.Clean(output), runtimeSHA: os.Getenv("M164_RUNTIME_SHA"), sdkSHA: os.Getenv("M164_SDK_SHA"), harnessSHA: os.Getenv("M164_HARNESS_SHA")}
	commit := regexp.MustCompile(`^[0-9a-f]{40}$`)
	sourceHash := regexp.MustCompile(`^[0-9a-f]{64}$`)
	if !commit.MatchString(cfg.runtimeSHA) || !commit.MatchString(cfg.sdkSHA) || !sourceHash.MatchString(cfg.harnessSHA) {
		fmt.Fprintln(os.Stderr, "Verified runtime, SDK and harness identities are required.")
		os.Exit(2)
	}
	cfg.open = func(ctx context.Context) (*native.Embedded, error) {
		return native.OpenEmbedded(ctx, dsn, native.WithDBAuthToken(token), native.WithBrokerMode(), native.WithoutBackground(), native.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	}
	result, err := run(cfg)
	if err != nil || !result.Pass {
		fmt.Fprintln(os.Stderr, "Bounded compatibility verification failed; inspect sanitized evidence.")
		os.Exit(1)
	}
	fmt.Println("Bounded compatibility verification passed; this is not a capacity or soak result.")
}

func validRemoteDestination(dsn, token, reset string) bool {
	return validRemoteConfigurationHash(hashBytes([]byte(dsn)), token, reset)
}

func validRemoteConfigurationHash(destinationHash, token, reset string) bool {
	return destinationHash == verificationDBHash && token != "" && reset == ""
}
