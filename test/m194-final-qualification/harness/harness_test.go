package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	native "github.com/mqlitehq/mqlite"
)

func localConfiguration(t *testing.T) (configuration, string) {
	t.Helper()
	db := filepath.Join(t.TempDir(), "verification.db")
	open := func(ctx context.Context) (*native.Embedded, error) {
		return native.OpenEmbedded(ctx, "file:"+db, native.WithBrokerMode(), native.WithoutBackground(), native.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	}
	cfg := configuration{output: filepath.Join(t.TempDir(), "evidence"), open: open, localFixture: true, runtimeSHA: "local-fixture", sdkSHA: "local-fixture", harnessSHA: "local-fixture"}
	e, err := open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CreateQueue(context.Background(), "unrelated", native.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SendOne(context.Background(), "unrelated", native.OutMessage{MessageID: "outside-owned-prefix", Body: []byte("preserve-exactly")}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	return cfg, db
}

func verifyOwnedCleanupAndUnrelatedQueue(t *testing.T, cfg configuration, result report) {
	t.Helper()
	for _, resource := range result.Resources {
		if resource.Eligible && (!resource.Deleted || !resource.Absent) {
			t.Fatalf("owned cleanup unresolved: %+v", resource)
		}
	}
	e, err := cfg.open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, resource := range result.Resources {
		_, err := e.Stats(context.Background(), resource.Name)
		if !errors.Is(err, native.ErrQueueNotFound) {
			t.Fatalf("owned resource remains: %s", resource.Name)
		}
	}
	messages, err := e.Receive(context.Background(), "unrelated", native.RecvOpts{Max: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].MessageID != "outside-owned-prefix" || string(messages[0].Body) != "preserve-exactly" {
		t.Fatal("unrelated queue data was touched")
	}
	if err := messages[0].Complete(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLocalFunctionalLedgerAndPreciseCleanup(t *testing.T) {
	cfg, _ := localConfiguration(t)
	result, err := run(cfg)
	if err != nil || !result.Pass {
		t.Fatalf("local functional verification failed: %v, failures=%+v", err, result.Failures)
	}
	if len(result.Identities) != 10 || len(result.Resources) != 6 {
		t.Fatal("wrong finite workload inventory")
	}
	if len(result.Managed) != 2 {
		t.Fatal("both managed receivers must be recorded")
	}
	for _, check := range result.Managed {
		if check.Invocations != 1 || !check.Joined {
			t.Fatal("managed receiver did not join after one handler")
		}
	}
	for _, id := range result.Identities {
		if !id.Acknowledged || !id.Completed || id.Deliveries != 1 || id.Sequence <= 0 {
			t.Fatalf("incomplete identity: %+v", id)
		}
	}
	verifyOwnedCleanupAndUnrelatedQueue(t, cfg, result)
}

func TestFailureAfterSendStillCleansOnlyOwnedNames(t *testing.T) {
	cfg, _ := localConfiguration(t)
	cfg.failAt = "native-after-send"
	result, err := run(cfg)
	if err == nil || result.Pass || len(result.Failures) == 0 {
		t.Fatal("controlled post-send failure was accepted")
	}
	verifyOwnedCleanupAndUnrelatedQueue(t, cfg, result)
}

func TestScenarioDeadlinePreservesCleanupReserve(t *testing.T) {
	cfg, _ := localConfiguration(t)
	cfg.scenario = time.Second
	cfg.cleanup = 5 * time.Second
	entered := false
	cfg.afterCreate = func(ctx context.Context) error { entered = true; <-ctx.Done(); return ctx.Err() }
	start := time.Now()
	result, err := run(cfg)
	if !entered || err == nil || result.Pass || time.Since(start) > 6*time.Second {
		t.Fatalf("deadline oracle failed: entered=%v, err=%v, elapsed=%v", entered, err, time.Since(start))
	}
	verifyOwnedCleanupAndUnrelatedQueue(t, cfg, result)
}

func TestLedgerRejectsIdentityValueSequenceAndDuplicateFaults(t *testing.T) {
	for _, fault := range []string{"identity", "value", "sequence", "delivery-count", "duplicate"} {
		t.Run(fault, func(t *testing.T) {
			l := &ledger{expected: map[string]entry{}}
			items := l.register("owned", 1, 64)
			if err := l.acknowledge(items, []int64{7}); err != nil {
				t.Fatal(err)
			}
			message := delivery{Queue: "owned", ID: items[0].ID, Sequence: 7, Body: items[0].Body, Count: 1}
			switch fault {
			case "identity":
				message.ID = "unknown"
			case "value":
				message.Body = []byte("different")
			case "sequence":
				message.Sequence = 8
			case "delivery-count":
				message.Count = 2
			case "duplicate":
				if err := l.delivered(message); err != nil {
					t.Fatal(err)
				}
			}
			if l.delivered(message) == nil {
				t.Fatal("corrupted delivery was accepted")
			}
		})
	}
	l := &ledger{expected: map[string]entry{}}
	items := l.register("owned", 2, 64)
	if l.acknowledge(items, []int64{7, 7}) == nil {
		t.Fatal("duplicate send sequences were accepted")
	}
	for _, record := range l.report.Identities {
		if record.Acknowledged {
			t.Fatal("invalid batch left a partial positive acknowledgement")
		}
	}
	if l.completed([]delivery{{Queue: "owned", ID: items[0].ID, Sequence: 7}}) == nil {
		t.Fatal("completion without a confirmed send was accepted")
	}
}

func TestRemoteDestinationRejectsWrongBackendMissingSecretAndReset(t *testing.T) {
	if !validRemoteConfigurationHash(verificationDBHash, "fixture", "") {
		t.Fatal("correct verification configuration was rejected")
	}
	if validRemoteConfigurationHash(verificationDBHash, "", "") || validRemoteConfigurationHash(verificationDBHash, "fixture", "1") {
		t.Fatal("missing credential or reset authorization escaped the guard")
	}
	for _, input := range []struct{ dsn, token, reset string }{
		{"", "fixture", ""},
		{"libsql://not-verification.invalid", "fixture", ""},
		{"turso://not-verification.invalid", "fixture", ""},
		{"libsql://not-verification.invalid", "", ""},
		{"libsql://not-verification.invalid", "fixture", "1"},
	} {
		if validRemoteDestination(input.dsn, input.token, input.reset) {
			t.Fatal("unsafe live configuration was accepted")
		}
	}
	if scenarioBudget != 240*time.Second || cleanupBudget != 60*time.Second || openBudget != 45*time.Second || rpcBudget != 15*time.Second {
		t.Fatal("review fixed live budgets before changing them")
	}
}

func TestManagedOracleRejectsLateErrorAndDuplicateAfterAcknowledgement(t *testing.T) {
	for _, kind := range []string{"native", "sdk"} {
		for _, fault := range []string{"late-error", "duplicate"} {
			t.Run(kind+"-"+fault, func(t *testing.T) {
				cfg, _ := localConfiguration(t)
				if err := os.MkdirAll(cfg.output, 0o700); err != nil {
					t.Fatal(err)
				}
				const queue = "managed-oracle"
				trace := &rpcRecorder{allowed: map[string]bool{queue: true}}
				o, err := openOwner(context.Background(), cfg, trace)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := o.stop(ctx); err != nil {
						t.Error("fixture owner did not join")
					}
				}()
				if err := o.embedded.CreateQueue(context.Background(), queue, native.QueueConfig{LockDuration: 3 * time.Second, MaxDeliveryCount: 3}); err != nil {
					t.Fatal(err)
				}
				l := &ledger{output: cfg.output, expected: map[string]entry{}}
				a := o.clients[kind]
				var injected atomic.Bool
				a.runOverride = func(ctx context.Context, q string, handler func(context.Context, delivery) error, onError func(error)) error {
					messages, err := a.receive(ctx, q, 1)
					if err != nil {
						return err
					}
					if len(messages) != 1 {
						return errors.New("fixture receive inventory mismatch")
					}
					if err := handler(ctx, messages[0]); err != nil {
						return err
					}
					if err := a.settle(ctx, q, messages, true); err != nil {
						return err
					}
					if err := a.settle(ctx, q, messages, false); err != nil {
						return err
					}
					<-ctx.Done()
					injected.Store(true)
					if fault == "late-error" {
						onError(errors.New("controlled late receiver error"))
					} else {
						_ = handler(ctx, messages[0])
					}
					return ctx.Err()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
				defer cancel()
				err = managed(ctx, l, o, trace, kind, queue, time.Millisecond)
				if !injected.Load() {
					t.Fatal("late failure injection was not reached")
				}
				if err == nil {
					t.Fatal("receiver error or duplicate after Complete acknowledgement was accepted as PASS")
				}
			})
		}
	}
}

func TestManagedWorkCannotBorrowTheCleanupReserve(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	l := &ledger{expected: map[string]entry{}}
	err := managed(ctx, l, &owner{}, &rpcRecorder{}, "native", "owned", time.Millisecond)
	if err == nil || len(l.report.Identities) != 0 {
		t.Fatal("managed work started without enough scenario time to join")
	}
}
