package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"testing"
)

const expiredTransactionProviderFixture = "SQLite error: interactive transaction was rolled back because the stream was idle for too long; retry the transaction"

func TestRemoteTransactionExpiryClassificationIsDefiniteAndExact(t *testing.T) {
	provider := errors.New(expiredTransactionProviderFixture)
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"provider leaf", provider, true},
		{"pinned driver wrapper", fmt.Errorf("failed to execute SQL:\n%w", provider), true},
		{"generic timeout", errors.New("Transaction timed out"), false},
		{"generic rollback", errors.New("transaction was rolled back; retry"), false},
		{"quoted explanation", errors.New("query mentioned: " + expiredTransactionProviderFixture), false},
		{"post-commit checkpoint", errors.New("Checkpoint failed: " + expiredTransactionProviderFixture), false},
		{"appended explanation", errors.New(expiredTransactionProviderFixture + " after an unknown commit"), false},
		{"unknown outcome", fmt.Errorf("%w: %w", ErrOutcomeUnknown, provider), false},
		{"joined EOF", errors.Join(io.EOF, provider), false},
		{"joined arbitrary", errors.Join(errors.New("transport result unavailable"), provider), false},
		{"caller canceled", errors.Join(context.Canceled, provider), false},
		{"network wrapper", &net.OpError{Op: "read", Net: "tcp", Err: provider}, false},
		{"URL wrapper", &url.Error{Op: "Post", Err: provider}, false},
		{"EOF outer text", fmt.Errorf("unexpected EOF reading reply: %w", provider), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote, local := &db{remote: true}, &db{}
			if remote.retryableWrite(tc.err) {
				t.Fatal("text-only rollback explanation authorized issued write replay")
			}
			if tc.want && !remote.retryable(tc.err) {
				t.Fatal("definite expiry is not retried on read/statement path")
			}
			if local.retryable(tc.err) || local.retryableWrite(tc.err) {
				t.Fatal("local retry behavior changed")
			}
		})
	}
	detail := describeError(fmt.Errorf("failed to execute SQL:\n%w", provider), 1)
	if detail.Category != "transaction_expired" || detail.SQLiteCode != 0 || detail.Message == "" {
		t.Fatalf("rollback receipt not diagnosed without invented numeric code: %+v", detail)
	}
}
func newExpiredTransactionProvider(stage string) *rustBusyProvider {
	return &rustBusyProvider{failStage: stage, remaining: 1, failureMessage: expiredTransactionProviderFixture, failureCode: "SQLITE_ABORT", rollbackOnFailure: true}
}
func TestRemoteTransactionExpiryReplaysWholeAbortedTransaction(t *testing.T) {
	for _, stage := range []string{"statement", "commit"} {
		t.Run(stage, func(t *testing.T) {
			p := newExpiredTransactionProvider(stage)
			if stage == "statement" {
				p.skipAtStage = 1
			} // Roll back a prior successful write too.
			e := newRustBusyEngine(t, p)
			runs := 0
			err := e.inTx(context.Background(), func(ctx context.Context, tx *txn) error {
				runs++
				for i := 0; i < 2; i++ {
					if _, err := tx.ExecContext(ctx, "UPDATE validation_counter SET value=value+1"); err != nil {
						return err
					}
				}
				tx.record("q", "enqueued", 1)
				return nil
			})
			if stage == "commit" {
				if !errors.Is(err, ErrOutcomeUnknown) || runs != 1 {
					t.Fatalf("untyped COMMIT outcome runs=%d err=%v", runs, err)
				}
				p.mu.Lock()
				defer p.mu.Unlock()
				if p.applied != 0 || p.expiredRollbacks != 1 {
					t.Fatalf("effects=%d rollbacks=%d", p.applied, p.expiredRollbacks)
				}
				return
			}
			if err != nil {
				t.Fatalf("explicit rollback receipt not replayed: %v", err)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if runs != 2 || p.expiredRollbacks != 1 || p.applied != 2 || e.observation.events[eventKey{"q", "enqueued"}] != 1 {
				t.Fatalf("runs=%d provider rollbacks=%d writes durably applied=%d, want2/1/2", runs, p.expiredRollbacks, p.applied)
			}
		})
	}
}
func TestRemoteTransactionExpiryRetriesDirectReadAndWrite(t *testing.T) {
	for _, read := range []bool{true, false} {
		t.Run(fmt.Sprint(read), func(t *testing.T) {
			stage := "statement"
			if read {
				stage = "read"
			}
			p := newExpiredTransactionProvider(stage)
			e := newRustBusyEngine(t, p)
			if read {
				p.applied = 7
				var value int
				if err := e.db.queryRowScan(context.Background(), []any{&value}, "SELECT value FROM validation_counter"); err != nil || value != 7 {
					t.Fatalf("read retry value=%d err=%v", value, err)
				}
			} else if _, err := e.db.exec(context.Background(), "UPDATE validation_counter SET value=value+1"); !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("untyped write rollback text=%v, want unknown", err)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.failures != 1 || (read && p.reads != 2) || (!read && (p.writes != 1 || p.applied != 0)) {
				t.Fatal("direct retry accounting/effect mismatch")
			}
		})
	}
}
func TestRemoteTransactionExpiryExhaustionAndUnknownCommitStayDistinct(t *testing.T) {
	p := newExpiredTransactionProvider("statement")
	p.remaining = maxConnAttempts + 1
	e := newRustBusyEngine(t, p)
	runs := 0
	err := e.inTx(context.Background(), func(ctx context.Context, tx *txn) error {
		runs++
		_, err := tx.ExecContext(ctx, "UPDATE validation_counter SET value=value+1")
		return err
	})
	if err == nil || errors.Is(err, ErrOutcomeUnknown) || storageErrorCode(err) != "connection" || runs != maxConnAttempts {
		t.Fatalf("expiry bound or diagnosis wrong runs=%d err=%v", runs, err)
	}
	p.mu.Lock()
	if p.applied != 0 {
		t.Error("exhausted aborted attempts committed effects")
	}
	p.mu.Unlock()
	d, state := remoteDBFailingFirst(t, 0)
	state.commitErr = errors.Join(io.EOF, errors.New(expiredTransactionProviderFixture))
	runs = 0
	err = (&Engine{db: d}).inTx(context.Background(), func(context.Context, *txn) error { runs++; return nil })
	if !errors.Is(err, ErrOutcomeUnknown) || runs != 1 {
		t.Fatalf("unknown commit became a safe expiry retry runs=%d err=%v", runs, err)
	}
}
