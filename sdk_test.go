package mqlite_test

// Blackbox SDK tests (package mqlite_test) — exercise the public API the way a
// caller would. Organized into sections:
//
//   - Harness         newBroker: an in-memory broker + connected client
//   - Remote / SDK    Client RPCs over HTTP (round-trip, auth, batch, receiver loop,
//                     broad handler exercise)
//   - Embedded        the in-process engine (runnable example / single-writer)
//   - Build guards    go.mod floor / dependency-freeze intent
//
// White-box internals live in receiver_internal_test.go (package mqlite).

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// Keep the released public type's exact field order, tags and arity. This also
// preserves external unkeyed SubscriptionInfo literals at compile time.
var _ struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Expr  string `json:"expr,omitempty"`
} = mqlite.SubscriptionInfo{}

// Keep the original Subscribe method while the options-bearing form evolves
// independently for fixed-lifetime creation.
var _ interface {
	Subscribe(context.Context, string, string, *mqlite.Filter) error
} = (*mqlite.Client)(nil)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRemoteQueueNotFoundMatchesSpecificAndGeneralErrors(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"code":"not_found","message":"mqlite: queue not found","resource":"queue"}`)
	}))
	defer stub.Close()

	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Stats(context.Background(), "missing")
	if !errors.Is(err, mqlite.ErrQueueNotFound) || !errors.Is(err, mqlite.ErrNotFound) {
		t.Fatalf("Stats error = %v, want both ErrQueueNotFound and ErrNotFound", err)
	}
}

type closeTrackingTransport struct {
	base           http.RoundTripper
	roundTripCalls atomic.Int32
	closeCalls     atomic.Int32
}

func (t *closeTrackingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.roundTripCalls.Add(1)
	if t.base != nil {
		return t.base.RoundTrip(req)
	}
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(`{"queues":[]}`)),
		ContentLength: int64(len(`{"queues":[]}`)),
		Request:       req,
	}, nil
}

func (t *closeTrackingTransport) CloseIdleConnections() {
	t.closeCalls.Add(1)
}

// ─── Harness ────────────────────────────────────────────────────────────────

// newBroker starts an in-memory broker over httptest and returns a connected client.
func newBroker(t *testing.T, token string) (*mqlite.Client, *mqlite.Embedded) {
	t.Helper()
	ctx := context.Background()
	// Not t.TempDir(): on Windows a pure-Go SQLite (modernc) file handle can linger
	// briefly after Close, and t.TempDir()'s auto-RemoveAll fails the test when it
	// can't delete the still-open file. Use a manual dir with a best-effort cleanup
	// (the CI runner reaps temp dirs anyway) so this teardown race can't flake.
	dir, err := os.MkdirTemp("", "mqlite-test-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	eng, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(dir, "mq.db"))
	if err != nil {
		t.Fatalf("open embedded: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	var tokens []string
	if token != "" {
		tokens = []string{token}
	}
	ts := httptest.NewServer(server.New(eng.Engine(), tokens).Handler())
	t.Cleanup(ts.Close)

	cli, err := mqlite.Open(ctx, ts.URL, mqlite.WithToken(token))
	if err != nil {
		t.Fatalf("open client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli, eng
}

// ─── Remote / SDK over HTTP ─────────────────────────────────────────────────

// Exercise the N1 clock-skew regression through the real engine, HTTP handler,
// wire decoder, managed Receiver, and terminal settlement path.
func TestRemoteReceiverAutoRenewToleratesBrokerClockBehindClient(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rtt          time.Duration
		receiveDelay time.Duration
		handlerFor   time.Duration
	}{
		{name: "local", handlerFor: 50 * time.Millisecond},
		{name: "150ms RTT", rtt: 150 * time.Millisecond, handlerFor: 500 * time.Millisecond},
		{name: "long receive then fast renew", receiveDelay: 1200 * time.Millisecond, handlerFor: 500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			eng, err := mqlite.OpenEmbedded(
				context.Background(),
				":memory:",
				mqlite.WithClock(func() int64 { return time.Now().Add(-2 * time.Second).UnixMilli() }),
				mqlite.WithoutBackground(),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer eng.Close()
			if err := eng.CreateQueue(context.Background(), "q", mqlite.QueueConfig{LockDuration: time.Second, MaxDeliveryCount: 3}); err != nil {
				t.Fatal(err)
			}
			broker := server.New(eng.Engine(), []string{"clock-test"}).Handler()
			if tc.rtt > 0 || tc.receiveDelay > 0 {
				next := broker
				broker = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					delay := tc.rtt
					if r.URL.Path == wire.PathReceive && tc.receiveDelay > 0 {
						delay = tc.receiveDelay
					}
					time.Sleep(delay)
					next.ServeHTTP(w, r)
				})
			}
			ts := httptest.NewServer(broker)
			defer ts.Close()
			client, err := mqlite.Open(context.Background(), ts.URL, mqlite.WithToken("clock-test"))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if _, err := client.SendOne(context.Background(), "q", mqlite.OutMessage{Body: []byte("clock skew")}); err != nil {
				t.Fatal(err)
			}

			handlerCalls := 0
			var observed []error
			err = client.Receiver("q", mqlite.WithAutoRenew(), mqlite.WithErrorHandler(func(err error) {
				observed = append(observed, err)
			})).Run(ctx, func(context.Context, *mqlite.Message) error {
				handlerCalls++
				time.Sleep(tc.handlerFor)
				cancel()
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled after handler", err)
			}
			if handlerCalls != 1 {
				t.Fatalf("handler calls = %d, want one", handlerCalls)
			}
			if len(observed) != 0 {
				t.Fatalf("receiver errors = %v, want none", observed)
			}
			metrics, err := eng.Stats(context.Background(), "q")
			if err != nil {
				t.Fatal(err)
			}
			if metrics.Total != 0 || metrics.DeadLettered != 0 {
				t.Fatalf("queue after completion: total=%d dead_lettered=%d, want 0/0", metrics.Total, metrics.DeadLettered)
			}
		})
	}
}

// The public boundary is identical with and without HTTP, including the exact
// millisecond at which a lease expires, before maintenance has reclaimed it.
func TestSDKSettlementDeadline(t *testing.T) {
	type settlementClient interface {
		Receive(context.Context, string, ...mqlite.RecvOpts) ([]*mqlite.Message, error)
		CompleteBatch(context.Context, string, ...*mqlite.Message) ([]mqlite.SettleResult, error)
		RenewBatch(context.Context, string, ...*mqlite.Message) ([]mqlite.SettleResult, error)
	}
	for _, storage := range []string{"memory", "file"} {
		for _, transport := range []string{"embedded", "http"} {
			for _, operation := range []string{"Complete", "Abandon", "Reject", "Defer", "Renew", "CompleteBatch", "RenewBatch"} {
				for _, offset := range []int64{-1, 0, 1} {
					t.Run(fmt.Sprintf("%s/%s/%s/offset_%d", storage, transport, operation, offset), func(t *testing.T) {
						ctx := context.Background()
						var clock atomic.Int64
						clock.Store(1700000000000)
						dsn := ":memory:"
						if storage == "file" {
							// Match newBroker's best-effort Windows file-handle cleanup.
							dir, err := os.MkdirTemp("", "mqlite-deadline-*")
							if err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { _ = os.RemoveAll(dir) })
							dsn = "file:" + filepath.Join(dir, "mq.db")
						}
						eng, err := mqlite.OpenEmbedded(ctx, dsn, mqlite.WithClock(clock.Load), mqlite.WithoutBackground())
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = eng.Close() })
						var cli settlementClient = eng
						if transport == "http" {
							ts := httptest.NewServer(server.New(eng.Engine(), []string{"deadline-test"}).Handler())
							t.Cleanup(ts.Close)
							remote, err := mqlite.Open(ctx, ts.URL, mqlite.WithToken("deadline-test"))
							if err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { _ = remote.Close() })
							cli = remote
						}
						if err := eng.CreateQueue(ctx, "q", mqlite.QueueConfig{LockDuration: time.Second}); err != nil {
							t.Fatal(err)
						}
						seq, err := eng.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("deadline payload"), MessageID: "deadline-id", Properties: map[string]string{"key": "value"}})
						if err != nil {
							t.Fatal(err)
						}
						messages, err := cli.Receive(ctx, "q", mqlite.RecvOpts{Max: 1})
						if err != nil || len(messages) != 1 || messages[0].SequenceNumber != seq {
							t.Fatalf("receive: %+v, %v", messages, err)
						}
						msg := messages[0]
						before, err := eng.Peek(ctx, "q", mqlite.PeekOpts{State: mqlite.Locked})
						if err != nil || len(before) != 1 {
							t.Fatalf("peek before: %+v, %v", before, err)
						}
						clock.Store(msg.LockedUntil.UnixMilli() + offset)
						var results []mqlite.SettleResult
						switch operation {
						case "Complete":
							err = msg.Complete(ctx)
						case "Abandon":
							err = msg.Abandon(ctx, mqlite.AbandonOpts{Delay: 2 * time.Second})
						case "Reject":
							err = msg.Reject(ctx, mqlite.RejectOpts{Reason: "reason", Detail: "detail"})
						case "Defer":
							err = msg.Defer(ctx)
						case "Renew":
							err = msg.Renew(ctx)
						case "CompleteBatch":
							results, err = cli.CompleteBatch(ctx, "q", msg)
						case "RenewBatch":
							results, err = cli.RenewBatch(ctx, "q", msg)
						}
						if strings.HasSuffix(operation, "Batch") {
							if err != nil || len(results) != 1 || results[0].SequenceNumber != seq || results[0].Ok != (offset < 0) {
								t.Fatalf("batch result: %+v, %v", results, err)
							}
						} else if (offset < 0 && err != nil) || (offset >= 0 && !errors.Is(err, mqlite.ErrLockLost)) {
							t.Fatalf("offset %d: got %v", offset, err)
						}
						if offset >= 0 {
							after, err := eng.Peek(ctx, "q", mqlite.PeekOpts{State: mqlite.Locked})
							if err != nil || !reflect.DeepEqual(before, after) {
								t.Fatalf("expired request mutated message: before=%+v after=%+v err=%v", before, after, err)
							}
						}
					})
				}
			}
		}
	}
}

// CompleteBatch over HTTP: receive a batch, then settle it in one round-trip.
func TestRemoteCompleteBatch(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "mqk_test")
	if err := cli.CreateQueue(ctx, "orders", mqlite.QueueConfig{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := cli.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("x")}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	msgs, err := cli.Receive(ctx, "orders", mqlite.RecvOpts{Max: 3})
	if err != nil || len(msgs) != 3 {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}
	res, err := cli.CompleteBatch(ctx, "orders", msgs...)
	if err != nil {
		t.Fatalf("CompleteBatch: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("want 3 results, got %d", len(res))
	}
	for _, r := range res {
		if !r.Ok {
			t.Fatalf("want all Ok=true, got %+v", res)
		}
	}
	if m, _ := cli.Stats(ctx, "orders"); m.Total != 0 {
		t.Fatalf("queue should be empty after batch complete, total=%d", m.Total)
	}
}

func TestRemoteBatchSettlementRejectsMalformedSuccess(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		body string
		want error
	}{
		{name: "complete missing result", path: wire.PathCompleteBatch, body: `{"results":[{"seq_number":7,"ok":true}]}`, want: mqlite.ErrOutcomeUnknown},
		{name: "complete reordered", path: wire.PathCompleteBatch, body: `{"results":[{"seq_number":8,"ok":true},{"seq_number":7,"ok":true}]}`, want: mqlite.ErrOutcomeUnknown},
		{name: "complete with renewal deadline", path: wire.PathCompleteBatch, body: `{"results":[{"seq_number":7,"ok":true,"locked_until_ms":1700000000000},{"seq_number":8,"ok":true}]}`, want: mqlite.ErrOutcomeUnknown},
		{name: "complete truncated JSON", path: wire.PathCompleteBatch, body: `{"results":[`, want: mqlite.ErrOutcomeUnknown},
		{name: "renew missing deadline", path: wire.PathRenewBatch, body: `{"results":[{"seq_number":7,"ok":true},{"seq_number":8,"ok":false}]}`, want: mqlite.ErrOutcomeUnknown},
		{name: "renew stale with deadline", path: wire.PathRenewBatch, body: `{"results":[{"seq_number":7,"ok":true,"locked_until_ms":1700000000000},{"seq_number":8,"ok":false,"locked_until_ms":1700000000001}]}`, want: mqlite.ErrOutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.path {
					t.Errorf("path = %q, want %q", r.URL.Path, tc.path)
				}
				_, _ = io.WriteString(w, tc.body)
			}))
			defer stub.Close()
			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			messages := []*mqlite.Message{
				client.Message("jobs", 7, "lock-7"),
				client.Message("jobs", 8, "lock-8"),
			}
			if tc.path == wire.PathCompleteBatch {
				_, err = client.CompleteBatch(context.Background(), "jobs", messages...)
			} else {
				_, err = client.RenewBatch(context.Background(), "jobs", messages...)
			}
			if err == nil {
				t.Fatal("malformed successful response was accepted")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, tc.want)
			}
			if strings.Contains(err.Error(), tc.body) {
				t.Fatalf("error exposed the untrusted response body: %v", err)
			}
		})
	}
}

func TestRemoteBatchSettlementRejectsNilMessageWithoutRequest(t *testing.T) {
	var calls atomic.Int32
	stub := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.CompleteBatch(context.Background(), "jobs", (*mqlite.Message)(nil)); !errors.Is(err, mqlite.ErrInvalidArgument) {
		t.Fatalf("CompleteBatch error = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.RenewBatch(context.Background(), "jobs", (*mqlite.Message)(nil)); !errors.Is(err, mqlite.ErrInvalidArgument) {
		t.Fatalf("RenewBatch error = %v, want ErrInvalidArgument", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid calls reached the broker %d times", calls.Load())
	}
}

func TestRemoteCompleteBatchHTTPRejectionIsNotOutcomeUnknown(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"code":"permission_denied","message":"not allowed"}`)
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.CompleteBatch(context.Background(), "jobs", client.Message("jobs", 7, "lock-7"))
	if !errors.Is(err, mqlite.ErrPermissionDenied) || errors.Is(err, mqlite.ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want only ErrPermissionDenied", err)
	}
}

// A remote publish to a topic that no subscription filter accepts is a valid no-op —
// (0, nil), matching the embedded SDK — not a spurious ErrDedupConflict (MQLITE-75).
func TestRemoteTopicNoMatchIsNoOp(t *testing.T) {
	cli, _ := newBroker(t, "tok")
	ctx := context.Background()
	if err := cli.Subscribe(ctx, "events", "sub", &mqlite.Filter{Expr: `subject == "never"`}); err != nil {
		t.Fatal(err)
	}
	seq, err := cli.SendOne(ctx, "events", mqlite.OutMessage{Body: []byte("x"), Subject: "other"})
	if err != nil {
		t.Fatalf("no-match publish should be a no-op, got err %v", err)
	}
	if seq != 0 {
		t.Fatalf("no-match publish should return seq 0, got %d", seq)
	}
}

func TestRemoteRoundTrip(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "mqk_test")

	if err := cli.CreateQueue(ctx, "orders", mqlite.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	seq, err := cli.SendOne(ctx, "orders", mqlite.OutMessage{
		Body: []byte("hello"), MessageID: "m1", Subject: "order.created",
		Properties: map[string]string{"tenant": "acme"},
	})
	if err != nil || seq != 1 {
		t.Fatalf("send: seq=%d err=%v", seq, err)
	}

	msgs, err := cli.Receive(ctx, "orders", mqlite.RecvOpts{Wait: 2 * time.Second})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	m := msgs[0]
	if string(m.Body) != "hello" || m.MessageID != "m1" || m.Properties["tenant"] != "acme" {
		t.Fatalf("roundtrip mismatch: %+v", m)
	}
	if err := m.Complete(ctx); err != nil {
		t.Fatalf("complete: %v", err)
	}
	mt, _ := cli.Stats(ctx, "orders")
	if mt.Total != 0 {
		t.Fatalf("queue should be empty, got %+v", mt)
	}
}

func TestBatchTTLRemoteEmbeddedParity(t *testing.T) {
	ctx := context.Background()
	cli, embedded := newBroker(t, "mqk_ttl")
	const queueTTL = 5 * time.Minute
	for _, tc := range []struct {
		name   string
		create func(string) error
		send   func(string, ...mqlite.OutMessage) ([]int64, error)
		peek   func(string, ...mqlite.PeekOpts) ([]*mqlite.PeekedMessage, error)
	}{
		{
			name: "remote",
			create: func(queue string) error {
				return cli.CreateQueue(ctx, queue, mqlite.QueueConfig{DefaultTTL: queueTTL})
			},
			send: func(queue string, messages ...mqlite.OutMessage) ([]int64, error) {
				return cli.Send(ctx, queue, messages...)
			},
			peek: func(queue string, opts ...mqlite.PeekOpts) ([]*mqlite.PeekedMessage, error) {
				return cli.Peek(ctx, queue, opts...)
			},
		},
		{
			name: "embedded",
			create: func(queue string) error {
				return embedded.CreateQueue(ctx, queue, mqlite.QueueConfig{DefaultTTL: queueTTL})
			},
			send: func(queue string, messages ...mqlite.OutMessage) ([]int64, error) {
				return embedded.Send(ctx, queue, messages...)
			},
			peek: func(queue string, opts ...mqlite.PeekOpts) ([]*mqlite.PeekedMessage, error) {
				return embedded.Peek(ctx, queue, opts...)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queue := "mixed-ttl-" + tc.name
			if err := tc.create(queue); err != nil {
				t.Fatalf("create: %v", err)
			}
			messages := []mqlite.OutMessage{
				{Body: []byte("default")},
				{Body: []byte("short"), TTL: time.Minute},
				{Body: []byte("capped"), TTL: 10 * time.Minute},
			}
			if _, err := tc.send(queue, messages...); err != nil {
				t.Fatalf("send: %v", err)
			}
			peeked, err := tc.peek(queue, mqlite.PeekOpts{Max: 10})
			if err != nil {
				t.Fatalf("peek: %v", err)
			}
			want := map[string]time.Duration{"default": queueTTL, "short": time.Minute, "capped": queueTTL}
			if len(peeked) != len(want) {
				t.Fatalf("peek count=%d, want %d", len(peeked), len(want))
			}
			for _, message := range peeked {
				if got, expected := message.ExpiresAt.Sub(message.VisibleAt), want[string(message.Body)]; got != expected {
					t.Errorf("%s effective TTL=%s, want %s", message.Body, got, expected)
				}
			}
		})
	}
}

func TestNegativeMessageTTLSDKParity(t *testing.T) {
	ctx := context.Background()
	cli, embedded := newBroker(t, "mqk_negative_ttl")
	for _, tc := range []struct {
		name      string
		create    func(string) error
		sendOne   func(string, mqlite.OutMessage) (int64, error)
		sendBatch func(string, ...mqlite.OutMessage) ([]int64, error)
		stats     func(string) (mqlite.Metrics, error)
	}{
		{
			name:    "remote",
			create:  func(queue string) error { return cli.CreateQueue(ctx, queue, mqlite.QueueConfig{}) },
			sendOne: func(queue string, message mqlite.OutMessage) (int64, error) { return cli.SendOne(ctx, queue, message) },
			sendBatch: func(queue string, messages ...mqlite.OutMessage) ([]int64, error) {
				return cli.Send(ctx, queue, messages...)
			},
			stats: func(queue string) (mqlite.Metrics, error) { return cli.Stats(ctx, queue) },
		},
		{
			name:   "embedded",
			create: func(queue string) error { return embedded.CreateQueue(ctx, queue, mqlite.QueueConfig{}) },
			sendOne: func(queue string, message mqlite.OutMessage) (int64, error) {
				return embedded.SendOne(ctx, queue, message)
			},
			sendBatch: func(queue string, messages ...mqlite.OutMessage) ([]int64, error) {
				return embedded.Send(ctx, queue, messages...)
			},
			stats: func(queue string) (mqlite.Metrics, error) { return embedded.Stats(ctx, queue) },
		},
	} {
		for _, invalid := range []struct {
			name string
			ttl  time.Duration
		}{{"negative", -time.Nanosecond}, {"positive_sub_millisecond", time.Nanosecond}} {
			for _, operation := range []string{"SendOne", "Send"} {
				t.Run(tc.name+"/"+invalid.name+"/"+operation, func(t *testing.T) {
					queue := fmt.Sprintf("invalid-ttl-%s-%s-%s", tc.name, invalid.name, operation)
					if err := tc.create(queue); err != nil {
						t.Fatalf("create: %v", err)
					}
					message := mqlite.OutMessage{Body: []byte("invalid"), TTL: invalid.ttl}
					var err error
					if operation == "SendOne" {
						_, err = tc.sendOne(queue, message)
					} else {
						_, err = tc.sendBatch(queue, mqlite.OutMessage{Body: []byte("must-not-commit")}, message)
					}
					if !errors.Is(err, mqlite.ErrInvalidArgument) {
						t.Fatalf("error=%v, want ErrInvalidArgument", err)
					}
					stats, err := tc.stats(queue)
					if err != nil {
						t.Fatalf("stats: %v", err)
					}
					if stats.Total != 0 {
						t.Fatalf("invalid request wrote %d rows", stats.Total)
					}
				})
			}
		}
	}
}

func TestRemoteBatchTTLWireCompatibility(t *testing.T) {
	ctx := context.Background()
	t.Run("uniform batch keeps legacy request TTL", func(t *testing.T) {
		var requests atomic.Int64
		stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			var raw struct {
				TTLMs    int64                        `json:"ttl_ms"`
				Messages []map[string]json.RawMessage `json:"messages"`
			}
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				t.Errorf("decode: %v", err)
			}
			if raw.TTLMs != 60_000 || (len(raw.Messages) != 1 && len(raw.Messages) != 2) {
				t.Errorf("request TTL=%d messages=%d", raw.TTLMs, len(raw.Messages))
			}
			for i, message := range raw.Messages {
				if _, present := message["ttl_ms"]; present {
					t.Errorf("messages[%d] unexpectedly carried ttl_ms", i)
				}
			}
			seqs := make([]int64, len(raw.Messages))
			for i := range seqs {
				seqs[i] = int64(i + 1)
			}
			_ = json.NewEncoder(w).Encode(wire.SendResponse{SeqNumbers: seqs})
		}))
		defer stub.Close()
		cli, err := mqlite.Open(ctx, stub.URL)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer cli.Close()
		if _, err := cli.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("single"), TTL: time.Minute}); err != nil {
			t.Fatalf("send one: %v", err)
		}
		if _, err := cli.Send(ctx, "q",
			mqlite.OutMessage{Body: []byte("a"), TTL: time.Minute},
			mqlite.OutMessage{Body: []byte("b"), TTL: time.Minute}); err != nil {
			t.Fatalf("send: %v", err)
		}
		if got := requests.Load(); got != 2 {
			t.Fatalf("requests=%d, want SendOne plus Send", got)
		}
	})

	t.Run("mixed batch uses per-message TTL", func(t *testing.T) {
		stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var raw map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				t.Errorf("decode: %v", err)
			}
			if _, present := raw["ttl_ms"]; present {
				t.Error("mixed request unexpectedly carried top-level ttl_ms")
			}
			var messages []map[string]json.RawMessage
			if err := json.Unmarshal(raw["messages"], &messages); err != nil {
				t.Errorf("messages: %v", err)
			}
			if len(messages) != 3 {
				t.Fatalf("messages=%d, want 3", len(messages))
			}
			if _, present := messages[0]["ttl_ms"]; present {
				t.Error("zero per-message TTL should be omitted")
			}
			for i, want := range []int64{60_000, 120_000} {
				var got int64
				if err := json.Unmarshal(messages[i+1]["ttl_ms"], &got); err != nil || got != want {
					t.Errorf("messages[%d].ttl_ms=%d err=%v, want %d", i+1, got, err, want)
				}
			}
			_ = json.NewEncoder(w).Encode(wire.SendResponse{SeqNumbers: []int64{1, 2, 3}})
		}))
		defer stub.Close()
		cli, err := mqlite.Open(ctx, stub.URL)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer cli.Close()
		if _, err := cli.Send(ctx, "q",
			mqlite.OutMessage{Body: []byte("default")},
			mqlite.OutMessage{Body: []byte("one"), TTL: time.Minute},
			mqlite.OutMessage{Body: []byte("two"), TTL: 2 * time.Minute}); err != nil {
			t.Fatalf("send: %v", err)
		}
	})

	t.Run("invalid SDK TTL never contacts an old broker", func(t *testing.T) {
		var requests atomic.Int64
		stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			_ = json.NewEncoder(w).Encode(wire.SendResponse{SeqNumbers: []int64{1, 2}})
		}))
		defer stub.Close()
		cli, err := mqlite.Open(ctx, stub.URL)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer cli.Close()
		for _, ttl := range []time.Duration{-time.Nanosecond, time.Nanosecond} {
			if _, err := cli.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("x"), TTL: ttl}); !errors.Is(err, mqlite.ErrInvalidArgument) {
				t.Errorf("SendOne TTL=%s error=%v, want ErrInvalidArgument", ttl, err)
			}
			if _, err := cli.Send(ctx, "q",
				mqlite.OutMessage{Body: []byte("valid")},
				mqlite.OutMessage{Body: []byte("invalid"), TTL: ttl}); !errors.Is(err, mqlite.ErrInvalidArgument) {
				t.Errorf("Send TTL=%s error=%v, want ErrInvalidArgument", ttl, err)
			}
		}
		if got := requests.Load(); got != 0 {
			t.Fatalf("invalid SDK TTL issued %d HTTP requests", got)
		}
	})

	t.Run("old strict broker fails closed for mixed batch", func(t *testing.T) {
		type legacyMessage struct {
			Body []byte `json:"body,omitempty"`
		}
		type legacySendRequest struct {
			Queue    string          `json:"queue"`
			Messages []legacyMessage `json:"messages"`
			TTLMs    int64           `json:"ttl_ms,omitempty"`
		}
		stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			decoder := json.NewDecoder(r.Body)
			decoder.DisallowUnknownFields()
			var legacy legacySendRequest
			if err := decoder.Decode(&legacy); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(wire.ErrorBody{Code: "invalid_argument", Message: err.Error()})
				return
			}
			_ = json.NewEncoder(w).Encode(wire.SendResponse{SeqNumbers: []int64{1, 2}})
		}))
		defer stub.Close()
		cli, err := mqlite.Open(ctx, stub.URL)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer cli.Close()
		if _, err := cli.Send(ctx, "q",
			mqlite.OutMessage{Body: []byte("default")},
			mqlite.OutMessage{Body: []byte("short"), TTL: time.Minute}); !errors.Is(err, mqlite.ErrInvalidArgument) {
			t.Fatalf("mixed batch error=%v, want ErrInvalidArgument", err)
		}
		if _, err := cli.Send(ctx, "q",
			mqlite.OutMessage{Body: []byte("a"), TTL: time.Minute},
			mqlite.OutMessage{Body: []byte("b"), TTL: time.Minute}); err != nil {
			t.Fatalf("uniform legacy batch: %v", err)
		}
	})
}

func TestRemoteSendRejectsMismatchedSequenceAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		name string
		seqs []int64
		call func(*mqlite.Client) error
	}{
		{
			name: "SendOne extra sequence",
			seqs: []int64{11, 12},
			call: func(client *mqlite.Client) error {
				_, err := client.SendOne(context.Background(), "jobs", mqlite.OutMessage{Body: []byte("one")})
				return err
			},
		},
		{
			name: "Send missing sequence",
			seqs: []int64{11},
			call: func(client *mqlite.Client) error {
				_, err := client.Send(context.Background(), "jobs",
					mqlite.OutMessage{Body: []byte("one")},
					mqlite.OutMessage{Body: []byte("two")})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(wire.SendResponse{SeqNumbers: tc.seqs})
			}))
			defer stub.Close()
			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			err = tc.call(client)
			if !errors.Is(err, mqlite.ErrOutcomeUnknown) {
				t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("Send made %d requests, want one", calls.Load())
			}
		})
	}
}

func TestRemoteAuth(t *testing.T) {
	ctx := context.Background()
	eng, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	ts := httptest.NewServer(server.New(eng.Engine(), []string{"right"}).Handler())
	t.Cleanup(ts.Close)

	bad, _ := mqlite.Open(ctx, ts.URL, mqlite.WithToken("wrong"))
	if err := bad.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err == nil {
		t.Fatal("expected auth failure with wrong token")
	}
	good, _ := mqlite.Open(ctx, ts.URL, mqlite.WithToken("right"))
	if err := good.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatalf("good token should succeed: %v", err)
	}
}

// TestRemoteOutcomeUnknownPropagates pins the HTTP half of the ErrOutcomeUnknown
// contract (MQLITE-59): a broker whose remote write lost its commit ack emits a
// distinct "outcome_unknown" envelope, and the SDK reconstructs the typed sentinel so
// a caller can errors.Is it and reconcile — instead of a generic 500 it would blindly
// retry into a double-apply. A stub broker stands in because a real engine only emits
// this on a remote-transport fault that can't be induced hermetically.
func TestRemoteOutcomeUnknownPropagates(t *testing.T) {
	ctx := context.Background()
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(wire.ErrorBody{
			Code:    "outcome_unknown",
			Message: "remote commit lost its acknowledgement",
		})
	}))
	defer stub.Close()

	cli, err := mqlite.Open(ctx, stub.URL)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := cli.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("x")}); !errors.Is(err, mqlite.ErrOutcomeUnknown) {
		t.Fatalf("SendOne error = %v; want errors.Is ErrOutcomeUnknown", err)
	}
}

// TestRemoteInvalidArgumentPropagates checks that a caller-side request error
// (empty name / unknown enum) round-trips as the typed ErrInvalidArgument through a
// real broker — server maps it to 400 invalid_argument and the SDK reconstructs the
// sentinel — instead of leaking a raw 500 (MQLITE-86).
func TestRemoteInvalidArgumentPropagates(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "")

	if err := cli.CreateQueue(ctx, "", mqlite.QueueConfig{}); !errors.Is(err, mqlite.ErrInvalidArgument) {
		t.Errorf("empty name: err = %v, want ErrInvalidArgument", err)
	}
	if err := cli.CreateQueue(ctx, "q", mqlite.QueueConfig{Ordering: mqlite.OrderingMode("fifo")}); !errors.Is(err, mqlite.ErrInvalidArgument) {
		t.Errorf("bad ordering: err = %v, want ErrInvalidArgument", err)
	}
}

// TestServeReadyAfterBind checks the broker announces readiness only after the listener
// actually binds: WithReady fires on a good bind (and cancelling ctx shuts down cleanly),
// but a bind failure returns an error WITHOUT firing ready — so automation waiting for
// "ready" never connects to a broker that failed to come up (MQLITE-88).
func TestServeReadyAfterBind(t *testing.T) {
	ctx := context.Background()
	eng, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	// Occupy an address so the first Serve's bind fails.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	var readyOnFail atomic.Bool
	if err := eng.Serve(ctx, busy.Addr().String(), mqlite.WithReady(func() { readyOnFail.Store(true) })); err == nil {
		t.Fatal("Serve on a busy address must return a bind error")
	}
	if readyOnFail.Load() {
		t.Fatal("ready must NOT fire when the bind fails")
	}

	// A free address: ready fires, then cancelling ctx returns from a clean shutdown.
	sctx, cancel := context.WithCancel(ctx)
	readyCh := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- eng.Serve(sctx, "127.0.0.1:0", mqlite.WithReady(func() { close(readyCh) }))
	}()
	select {
	case <-readyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("ready never fired on a good bind")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after ctx cancel")
	}
}

// TestServeShutdownCancelsLongPoll verifies that graceful shutdown does not wait
// out an already-admitted Receive long poll. The broker owns that long-poll lifetime:
// once shutdown begins, a parked Receive must observe cancellation before it can
// start another claim round, and Serve must be free to close the engine promptly.
func TestServeShutdownCancelsLongPoll(t *testing.T) {
	ctx := context.Background()
	eng, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.CreateQueue(ctx, "shutdown-long-poll", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	serveCtx, cancelServe := context.WithCancel(ctx)
	ready := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- eng.Serve(serveCtx, addr, mqlite.WithReady(func() { close(ready) }))
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not become ready")
	}

	cli, err := mqlite.Open(ctx, "http://"+addr)
	if err != nil {
		cancelServe()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	receiveCtx, cancelReceive := context.WithCancel(ctx)
	defer cancelReceive()
	type receiveResult struct {
		messages []*mqlite.Message
		err      error
	}
	receiveDone := make(chan receiveResult, 1)
	go func() {
		messages, err := cli.Receive(receiveCtx, "shutdown-long-poll", mqlite.RecvOpts{Wait: 20 * time.Second})
		receiveDone <- receiveResult{messages: messages, err: err}
	}()

	// Let the request complete its empty claim round and park in the long poll.
	time.Sleep(200 * time.Millisecond)
	select {
	case result := <-receiveDone:
		cancelServe()
		t.Fatalf("long poll returned before shutdown: %v", result.err)
	default:
	}

	start := time.Now()
	cancelServe()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("clean shutdown returned %v", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("shutdown took %s with a parked long poll, want <= 2s", elapsed)
		}
	case <-time.After(2 * time.Second):
		// Release the request so a failure does not leave this test waiting for the
		// full 20-second poll before cleanup can finish.
		cancelReceive()
		select {
		case <-serveDone:
		case <-time.After(5 * time.Second):
		}
		t.Fatal("Serve remained blocked by a parked long poll after shutdown")
	}
	select {
	case result := <-receiveDone:
		if result.err != nil {
			t.Fatalf("long poll returned an error during graceful shutdown: %v", result.err)
		}
		if len(result.messages) != 0 {
			t.Fatalf("long poll claimed %d message(s) after shutdown began", len(result.messages))
		}
	case <-time.After(2 * time.Second):
		cancelReceive()
		t.Fatal("long poll client remained blocked after Serve returned")
	}
	// cancelServe requests shutdown, but Gate.Stop is the actual no-new-claim
	// linearization boundary and runs in the Serve goroutine. Waiting for Serve
	// above avoids treating scheduler order as part of the public contract. The
	// atomic Stop-vs-claim ordering itself has deterministic engine gate tests.
	if _, err := eng.SendOne(ctx, "shutdown-long-poll", mqlite.OutMessage{Body: []byte("after-shutdown")}); err != nil {
		t.Fatal(err)
	}
	peeked, err := eng.Peek(ctx, "shutdown-long-poll", mqlite.PeekOpts{Max: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(peeked) != 1 || string(peeked[0].Body) != "after-shutdown" {
		t.Fatalf("post-shutdown message was claimed instead of remaining active: %+v", peeked)
	}
}

// TestServeMetricsToggle verifies the public SDK's opt-in on the API listener,
// including discovery, authentication and graceful shutdown in both modes.
func TestServeMetricsToggle(t *testing.T) {
	for _, mode := range []string{"default", "off", "on"} {
		t.Run(mode, func(t *testing.T) {
			embedded, err := mqlite.OpenEmbedded(context.Background(), ":memory:", mqlite.WithoutBackground())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = embedded.Close() })
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := ln.Addr().String()
			ln.Close()
			ctx, cancel := context.WithCancel(context.Background())
			ready, done := make(chan struct{}), make(chan error, 1)
			opts := []mqlite.ServeOption{
				mqlite.WithTokens("admin-secret"), mqlite.WithMonitorTokens("monitor-secret"),
				mqlite.WithReady(func() { close(ready) }),
			}
			if mode != "default" {
				opts = append(opts, mqlite.WithMetrics(mode == "on"))
			}
			go func() { done <- embedded.Serve(ctx, addr, opts...) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("clean shutdown: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("Serve did not stop")
				}
			})
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("broker did not become ready")
			}
			client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
			resp, err := client.Get("http://" + addr + "/")
			if err != nil {
				t.Fatal(err)
			}
			var card map[string]json.RawMessage
			err = json.NewDecoder(resp.Body).Decode(&card)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			value, present := card["metrics"]
			if (mode == "on") != present || (present && string(value) != `"/metrics"`) {
				t.Fatalf("metrics discovery = %s, present %v", value, present)
			}
			for _, token := range []string{"", "wrong", "monitor-secret", "admin-secret"} {
				req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/metrics", nil)
				if err != nil {
					t.Fatal(err)
				}
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				want := http.StatusNotFound
				if mode == "on" {
					want = http.StatusUnauthorized
					if token == "monitor-secret" || token == "admin-secret" {
						want = http.StatusOK
					}
				}
				if resp.StatusCode != want {
					t.Errorf("metrics HTTP %d, want %d", resp.StatusCode, want)
				}
			}
		})
	}
	for _, tokens := range [][]string{nil, {" ", ""}} {
		t.Run("requires-auth", func(t *testing.T) {
			embedded, err := mqlite.OpenEmbedded(context.Background(), ":memory:", mqlite.WithoutBackground())
			if err != nil {
				t.Fatal(err)
			}
			defer embedded.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = embedded.Serve(ctx, "127.0.0.1:0", mqlite.WithMetrics(true), mqlite.WithTokens(tokens...))
			if err == nil || !strings.Contains(err.Error(), "metrics require administrator authentication") {
				t.Fatalf("metrics without authentication = %v", err)
			}
		})
	}
}

// TestRemoteSettleByToken is the SDK contract the CLI's settle-later commands rely on
// (MQLITE-92): receive a message, read its LockToken(), then settle it through a freshly
// rehydrated Client.Message(queue, seq, token) — as a separate `mqlite complete …`
// invocation would against a running broker. A spent token must then be fenced out.
func TestRemoteSettleByToken(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "")
	if err := cli.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("hi")}); err != nil {
		t.Fatal(err)
	}
	msgs, err := cli.Receive(ctx, "q", mqlite.RecvOpts{Max: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	tok := msgs[0].LockToken()
	if tok == "" {
		t.Fatal("LockToken() returned empty")
	}
	seq := msgs[0].SequenceNumber

	// Rehydrate a settleable handle from (queue, seq, token) and complete it.
	if err := cli.Message("q", seq, tok).Complete(ctx); err != nil {
		t.Fatalf("complete via rehydrated handle: %v", err)
	}
	m, err := cli.Stats(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if m.Total != 0 || m.Locked != 0 {
		t.Fatalf("after complete: total=%d locked=%d, want 0/0", m.Total, m.Locked)
	}
	// The SAME token is idempotent (settlement-receipt lost-response replay); a WRONG
	// token is fenced out.
	if err := cli.Message("q", seq, tok).Complete(ctx); err != nil {
		t.Errorf("re-completing with the same token should be idempotent, got %v", err)
	}
	if err := cli.Message("q", seq, "not-the-token").Complete(ctx); err == nil {
		t.Error("completing with a wrong lock token should fail (fencing)")
	}
}

// TestRemoteStatusSubsFilter round-trips the read-only admin surface the CLI exposes
// (status / list-subscriptions / test-filter) against a real broker (MQLITE-92).
func TestRemoteStatusSubsFilter(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "")
	if err := cli.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := cli.Subscribe(ctx, "ev", "s1", &mqlite.Filter{Expr: `subject == "x"`}); err != nil {
		t.Fatal(err)
	}

	s, err := cli.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Backend == "" || s.SchemaVersion == "" {
		t.Errorf("status incomplete: %+v", s)
	}

	subs, err := cli.ListSubscriptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, su := range subs {
		if su.Name == "s1" && su.Topic == "ev" && su.Expr == `subject == "x"` {
			found = true
		}
	}
	if !found {
		t.Errorf("s1 not found in subscriptions: %+v", subs)
	}

	if r, err := cli.TestFilter(ctx, `subject == "x"`, &mqlite.OutMessage{Subject: "x"}, 0, 0); err != nil || !r.Valid || !r.Ran || !r.Matched {
		t.Errorf("test-filter match: r=%+v err=%v", r, err)
	}
	if r, _ := cli.TestFilter(ctx, `subject == "x"`, &mqlite.OutMessage{Subject: "y"}, 0, 0); !r.Valid || r.Matched {
		t.Errorf("test-filter no-match: %+v", r)
	}
	if r, _ := cli.TestFilter(ctx, `subject ==`, nil, 0, 0); r.Valid {
		t.Errorf("test-filter invalid should be !Valid: %+v", r)
	}
}

// TestRemoteNegativeDurationRejected pins the SDK side of the negative-limit fix: a
// sub-millisecond negative OlderThan must not truncate to 0 and become an unbounded purge —
// it maps to a negative and is rejected as ErrInvalidArgument (MQLITE-93 codex follow-up).
func TestRemoteNegativeDurationRejected(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "")
	if err := cli.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Purge(ctx, "q", mqlite.PurgeOpts{OlderThan: -time.Nanosecond}); !errors.Is(err, mqlite.ErrInvalidArgument) {
		t.Errorf("sub-ms negative OlderThan should be rejected, got %v", err)
	}
}

// TestRemoteSchedulePastRejected proves the future-schedule check is enforced at the broker
// (engine clock), not just the CLI — a past schedule via the SDK is rejected regardless of
// the caller's clock (MQLITE-94 codex follow-up).
func TestRemoteSchedulePastRejected(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "")
	if err := cli.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("x")}, mqlite.SendOpts{At: time.Now().Add(-time.Hour)}); !errors.Is(err, mqlite.ErrInvalidArgument) {
		t.Errorf("a past schedule should be rejected at the broker, got %v", err)
	}
}

func TestReceiverRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, _ := newBroker(t, "")

	if err := cli.CreateQueue(ctx, "jobs", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		if _, err := cli.SendOne(ctx, "jobs", mqlite.OutMessage{Body: []byte("job")}); err != nil {
			t.Fatal(err)
		}
	}

	var processed int64
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cli.Receiver("jobs", mqlite.WithConcurrency(3)).Run(runCtx, func(c context.Context, m *mqlite.Message) error {
			if atomic.AddInt64(&processed, 1) >= n {
				stop()
			}
			return nil // -> auto Complete
		})
	}()

	deadline := time.After(8 * time.Second)
	for atomic.LoadInt64(&processed) < n {
		select {
		case <-deadline:
			t.Fatalf("processed only %d/%d", atomic.LoadInt64(&processed), n)
		case <-time.After(20 * time.Millisecond):
		}
	}
	stop()
	<-done // wait for Run (and its in-flight settles) to fully stop before cleanup
	//        closes the engine — else Windows can't delete the still-open DB file.
}

// TestBrokerExercise drives most Client RPCs (and therefore most server handlers
// and wire round-trips) through one in-memory broker, to lock the remote contract
// and lift SDK/server coverage (MQLITE-26).
func TestBrokerExercise(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "") // no auth

	if err := cli.CreateQueue(ctx, "q", mqlite.QueueConfig{MaxDeliveryCount: 2}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := cli.Send(ctx, "q",
		mqlite.OutMessage{Body: []byte("a"), MessageID: "a"},
		mqlite.OutMessage{Body: []byte("b")},
	); err != nil {
		t.Fatalf("send: %v", err)
	}

	if m, err := cli.Stats(ctx, "q"); err != nil || m.Active != 2 || m.Total != 2 {
		t.Fatalf("stats: %+v err=%v (want active=2,total=2)", m, err)
	}
	if qs, err := cli.ListQueues(ctx); err != nil || len(qs) == 0 {
		t.Fatalf("list queues: %v n=%d", err, len(qs))
	}

	// Complete one.
	recv := func() *mqlite.Message {
		t.Helper()
		ms, err := cli.Receive(ctx, "q", mqlite.RecvOpts{Max: 1, Wait: 2 * time.Second})
		if err != nil || len(ms) != 1 {
			t.Fatalf("receive: %v n=%d", err, len(ms))
		}
		return ms[0]
	}
	if err := recv().Complete(ctx); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// Abandon the other -> back to active -> receive again -> Reject -> DLQ.
	if err := recv().Abandon(ctx, mqlite.AbandonOpts{Delay: 0}); err != nil {
		t.Fatalf("abandon: %v", err)
	}
	m := recv()
	if err := m.Renew(ctx); err != nil { // extend the lease while we hold it
		t.Fatalf("renew: %v", err)
	}
	if err := m.Reject(ctx, mqlite.RejectOpts{Reason: "bad", Detail: "nope"}); err != nil {
		t.Fatalf("reject: %v", err)
	}

	// It is now dead-lettered: peek the DLQ, redrive it back, then purge.
	dlq, err := cli.Peek(ctx, "q", mqlite.PeekOpts{State: mqlite.DeadLettered})
	if err != nil || len(dlq) != 1 {
		t.Fatalf("peek dlq: %v n=%d", err, len(dlq))
	}
	if moved, err := cli.Redrive(ctx, "q"); err != nil || moved != 1 {
		t.Fatalf("redrive: %v moved=%d", err, moved)
	}
	// Drain it to the DLQ again (maxDelivery=2: this is delivery 2) and purge.
	mm := recv()
	_ = mm.Reject(ctx, mqlite.RejectOpts{Reason: "again"})
	if purged, err := cli.Purge(ctx, "q"); err != nil || purged != 1 {
		t.Fatalf("purge: %v purged=%d", err, purged)
	}

	// Defer + retrieve-by-seq.
	seq, err := cli.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("deferred")})
	if err != nil {
		t.Fatalf("send for defer: %v", err)
	}
	if err := recv().Defer(ctx); err != nil {
		t.Fatalf("defer: %v", err)
	}
	picked, err := cli.Receive(ctx, "q", mqlite.RecvOpts{Pick: []int64{seq}})
	if err != nil || len(picked) != 1 || string(picked[0].Body) != "deferred" {
		t.Fatalf("receive deferred by seq: %v n=%d", err, len(picked))
	}
	_ = picked[0].Complete(ctx)

	// Topic + subscription fan-out (Subscribe creates the subscription queue).
	if err := cli.Subscribe(ctx, "events", "subA", nil); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := cli.SendOne(ctx, "events", mqlite.OutMessage{Body: []byte("evt")}); err != nil {
		t.Fatalf("publish to topic: %v", err)
	}
	got, err := cli.Receive(ctx, "subA", mqlite.RecvOpts{Wait: 2 * time.Second})
	if err != nil || len(got) != 1 || string(got[0].Body) != "evt" {
		t.Fatalf("receive from subscription: %v n=%d", err, len(got))
	}
	_ = got[0].Complete(ctx)
}

// Per-queue DLQ retention (MQLITE-29) set via the remote CreateQueue is stored and
// honored end-to-end: the wire QueueConfig round-trips into the engine and reapDLQ
// caps that queue's DLQ to its own bound.
func TestRemoteQueueRetentionConfig(t *testing.T) {
	ctx := context.Background()
	cli, emb := newBroker(t, "")
	if err := cli.CreateQueue(ctx, "rq", mqlite.QueueConfig{MaxDeliveryCount: 1, DLQMaxCount: 2}); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := cli.SendOne(ctx, "rq", mqlite.OutMessage{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	// Dead-letter all five (reject moves straight to the DLQ).
	for i := 0; i < 5; i++ {
		ms, err := cli.Receive(ctx, "rq", mqlite.RecvOpts{Max: 1, Wait: 2 * time.Second})
		if err != nil || len(ms) != 1 {
			t.Fatalf("receive %d: %v n=%d", i, err, len(ms))
		}
		if err := ms[0].Reject(ctx, mqlite.RejectOpts{Reason: "x"}); err != nil {
			t.Fatalf("reject %d: %v", i, err)
		}
	}
	emb.Engine().RunMaintenanceOnce(ctx) // drive retention deterministically
	if mt, _ := cli.Stats(ctx, "rq"); mt.DeadLettered != 2 {
		t.Fatalf("per-queue DLQMaxCount=2 via remote CreateQueue: want 2, got %d", mt.DeadLettered)
	}
}

// ─── Embedded (in-process) ──────────────────────────────────────────────────

// Example_embedded runs the whole queue in-process — no broker, no HTTP — against a
// local SQLite file, and shows the single-process / single-writer guarantee: a
// second opener of the same DB is rejected with ErrDBLocked (MQLITE-6 / MQLITE-15).
func Example_embedded() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "mqlite-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	dsn := "file:" + filepath.Join(dir, "mq.db")

	eng, err := mqlite.OpenEmbedded(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = eng.Close() }()

	if err := eng.CreateQueue(ctx, "orders", mqlite.QueueConfig{}); err != nil {
		log.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("order-42")}); err != nil {
		log.Fatal(err)
	}

	msgs, err := eng.Receive(ctx, "orders", mqlite.RecvOpts{})
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range msgs {
		fmt.Printf("received: %s\n", m.Body)
		_ = m.Complete(ctx) // at-least-once; the handler must be idempotent
	}

	// Embedded mode is single-writer: a second process — or a second OpenEmbedded on
	// the same file — is rejected rather than racing the first.
	if _, err := mqlite.OpenEmbedded(ctx, dsn); errors.Is(err, mqlite.ErrDBLocked) {
		fmt.Println("second open rejected: single writer")
	}

	// Output:
	// received: order-42
	// second open rejected: single writer
}

// ─── Build / dependency guards ──────────────────────────────────────────────

// TestGoModFloorStaysAt121 guards the go.mod floor. MQLite pins it at go 1.21 so
// the SDK stays embeddable in older projects (MQLITE-1). That floor also freezes
// modernc.org/sqlite at v1.36.1 and golang.org/x/sys at v0.30.0 — every later
// release of either requires go >= 1.23 (see docs/dependencies.md). The go 1.21.x
// CI matrix enforces that the code still *builds*; this test enforces *intent*, so
// a floor bump fails with a clear message instead of a cryptic toolchain error and
// the dependency freeze can't be lifted by accident.
func TestGoModFloorStaysAt121(t *testing.T) {
	b, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	const want = "go 1.21"
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "go 1.") {
			if line != want {
				t.Fatalf("go.mod floor = %q, want %q — raising it drops embedding "+
					"compatibility and unfreezes sqlite/x/sys. If intended, update "+
					"docs/dependencies.md and the Dependabot ignore rules deliberately.", line, want)
			}
			return
		}
	}
	t.Fatal("no `go 1.x` directive found in go.mod")
}

// TestReleaseDocsPinLatestDatedRelease keeps an unreleased source version from
// becoming a fictional install target (MQLITE-158). CHANGELOG's first dated,
// non-withdrawn entry is the repository's offline release source of truth. The
// website generator uses the same rule; keep the hand-written repository docs in
// lockstep so a candidate cannot advertise a tag, module or image that does not
// exist yet.
func TestReleaseDocsPinLatestDatedRelease(t *testing.T) {
	changelog, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		t.Fatalf("read CHANGELOG.md: %v", err)
	}
	releaseRE := regexp.MustCompile(`(?m)^## (v\d+\.\d+\.\d+(?:-rc\.\d+)?) — \d{4}-\d{2}-\d{2}(?: \(([^)]*)\))?\s*$`)
	withdrawnRE := regexp.MustCompile(`(?i)\b(?:withdrawn|retracted)\b`)
	latest := ""
	for _, match := range releaseRE.FindAllStringSubmatch(string(changelog), -1) {
		if !withdrawnRE.MatchString(match[2]) {
			latest = match[1]
			break
		}
	}
	if latest == "" {
		t.Fatal("CHANGELOG.md has no dated, supported release heading")
	}

	versionFile, err := os.ReadFile(filepath.Join("internal", "version", "version.go"))
	if err != nil {
		t.Fatalf("read source version: %v", err)
	}
	versionMatch := regexp.MustCompile(`(?m)^const Version = "(\d+\.\d+\.\d+)"$`).FindStringSubmatch(string(versionFile))
	if len(versionMatch) != 2 {
		t.Fatal("cannot read the single source version constant")
	}
	sourceTag := "v" + versionMatch[1]
	releaseNumber := strings.TrimPrefix(latest, "v")

	docPaths := []string{
		"README.md",
		filepath.Join("docs", "api-reference.md"),
		filepath.Join("docs", "deployment.md"),
		filepath.Join("docs", "operations.md"),
		filepath.Join("docs", "retention.md"),
		filepath.Join("ops", "observability", "README.md"),
	}
	docs := make(map[string]string, len(docPaths))
	var allDocs strings.Builder
	for _, name := range docPaths {
		body, readErr := os.ReadFile(name)
		if readErr != nil {
			t.Fatalf("read %s: %v", name, readErr)
		}
		docs[name] = string(body)
		allDocs.Write(body)
		allDocs.WriteByte('\n')
	}
	for name, needles := range map[string][]string{
		"README.md": {
			"github.com/mqlitehq/mqlite@" + latest,
			"releases/tag/" + latest,
			"ghcr.io/mqlitehq/mqlite:" + releaseNumber,
		},
		filepath.Join("docs", "deployment.md"): {
			"ghcr.io/mqlitehq/mqlite:" + releaseNumber,
		},
		filepath.Join("ops", "observability", "README.md"): {
			"git checkout " + latest,
		},
	} {
		for _, needle := range needles {
			if !strings.Contains(docs[name], needle) {
				t.Errorf("%s does not pin latest dated release %s: missing %q", name, latest, needle)
			}
		}
	}

	if sourceTag == latest {
		return
	}
	for _, forbidden := range []string{
		"github.com/mqlitehq/mqlite@" + sourceTag,
		"releases/tag/" + sourceTag,
		"ghcr.io/mqlitehq/mqlite:" + strings.TrimPrefix(sourceTag, "v"),
		"git checkout " + sourceTag,
	} {
		if strings.Contains(allDocs.String(), forbidden) {
			t.Errorf("release docs advertise unreleased source %s via %q; latest dated release is %s", sourceTag, forbidden, latest)
		}
	}
}

// TestConceptsCompanionReview requires deliberate review of the complete concepts
// pair. The HTML contains hand-written interactive examples, so it is not generated
// from Markdown. Review both files together before updating these hashes (MQLITE-110).
func TestConceptsCompanionReview(t *testing.T) {
	want := map[string]string{
		"concepts.md":   "125f4b5247864a6518d50a738da11ee5086fb8a4125169cd1eaea07ef543723a",
		"concepts.html": "1aaab8fd4d8b845fb73376a6c7a36c8ef6b424da3f056fad58509a032acfb2ca",
	}
	for name, digest := range want {
		t.Run(name, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join("docs", name))
			if err != nil {
				t.Fatal(err)
			}
			// Git may check text out with CRLF on Windows; content is the contract.
			canonical := strings.ReplaceAll(string(body), "\r\n", "\n")
			if got := fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))); got != digest {
				t.Fatalf("%s changed: review the full Markdown/interactive HTML pair, then update the golden (%s)", name, got)
			}
		})
	}
}

// TestCreateQueueDocMatchesWireConfig keeps docs/api-reference.md honest: the
// CreateQueue config table MUST list exactly the json fields of wire.QueueConfigJSON
// — no more, no less. This is the doc-side analogue of the MCP wire-shape guard:
// add/rename/remove a config field on the wire type (or drift the table) and this
// fails, so the reference can't silently fall behind the contract (this caught a
// real gap — three dlq_* fields had been omitted from the table).
func TestCreateQueueDocMatchesWireConfig(t *testing.T) {
	// json tags actually on the wire type
	want := map[string]bool{}
	rt := reflect.TypeOf(wire.QueueConfigJSON{})
	for i := 0; i < rt.NumField(); i++ {
		tag := strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]
		if tag != "" && tag != "-" {
			want[tag] = true
		}
	}

	// the CreateQueue section of the reference (up to the next "### " heading)
	b, err := os.ReadFile(filepath.Join("docs", "api-reference.md"))
	if err != nil {
		t.Fatalf("read api-reference.md: %v", err)
	}
	doc := string(b)
	start := strings.Index(doc, "### CreateQueue")
	if start < 0 {
		t.Fatal("no `### CreateQueue` section in api-reference.md")
	}
	section := doc[start:]
	if end := strings.Index(section[len("### CreateQueue"):], "\n### "); end >= 0 {
		section = section[:len("### CreateQueue")+end]
	}

	// field names are the first table column: lines like  | `field_name` | ...
	got := map[string]bool{}
	re := regexp.MustCompile("(?m)^\\| `([a-z0-9_]+)` \\|")
	for _, m := range re.FindAllStringSubmatch(section, -1) {
		got[m[1]] = true
	}

	for f := range want {
		if !got[f] {
			t.Errorf("wire.QueueConfigJSON field %q is NOT documented in the api-reference.md "+
				"CreateQueue config table — add a row for it", f)
		}
	}
	for f := range got {
		if !want[f] {
			t.Errorf("api-reference.md documents config field %q that is not in "+
				"wire.QueueConfigJSON — stale row, remove or rename it", f)
		}
	}
}

// TestStatusInfoMatchesWireStatus pins the SDK's StatusInfo to wire.StatusResponse field for
// field. StatusInfo had quietly renamed db_size_bytes to size_bytes and dropped uptime_ms and
// auth, so `mqlite status --output json` and a raw POST to /mqlite.v1.AdminService/Status disagreed
// (round-2 §3.4). Comparing the whole tag set — not just the field someone last touched —
// makes ANY future divergence loud and forces a deliberate decision.
func TestStatusInfoMatchesWireStatus(t *testing.T) {
	tags := func(rt reflect.Type) map[string]bool {
		out := map[string]bool{}
		for i := 0; i < rt.NumField(); i++ {
			if tag := strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]; tag != "" && tag != "-" {
				out[tag] = true
			}
		}
		return out
	}
	wireTags := tags(reflect.TypeOf(wire.StatusResponse{}))
	sdkTags := tags(reflect.TypeOf(mqlite.StatusInfo{}))
	for k := range wireTags {
		if !sdkTags[k] {
			t.Errorf("StatusInfo is missing the wire field %q — status JSON would drop it", k)
		}
	}
	for k := range sdkTags {
		if !wireTags[k] {
			t.Errorf("StatusInfo has %q, which is not on the wire — the CLI must not invent keys", k)
		}
	}
}

func TestStatusUsesLifecycleClockAndAcceptsLegacyBroker(t *testing.T) {
	ctx := context.Background()
	const now = int64(1_800_000_000_123)
	embedded, err := mqlite.OpenEmbedded(ctx, ":memory:", mqlite.WithClock(func() int64 { return now }), mqlite.WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer embedded.Close()
	status, err := embedded.Status(ctx)
	if err != nil || status.ServerTimeMs != now {
		t.Fatalf("embedded status server_time_ms=%d, want %d: %v", status.ServerTimeMs, now, err)
	}

	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wire.PathStatus {
			t.Errorf("path = %q, want %q", r.URL.Path, wire.PathStatus)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"0.4.0","backend":"memory","remote":false,"location":":memory:","schema_version":"7","ping_ms":0,"db_size_bytes":0,"queues":0,"subscriptions":0,"uptime_ms":1,"auth":false}`))
	}))
	defer legacy.Close()
	client, err := mqlite.Open(ctx, legacy.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	status, err = client.Status(ctx)
	if err != nil || status.ServerTimeMs != 0 {
		t.Fatalf("legacy status clock=%d, want unavailable zero: %v", status.ServerTimeMs, err)
	}
}

// TestSubMsOlderThanIsBounded is the row-count proof the round-2 report asked for: a POSITIVE
// sub-millisecond age bound must stay a BOUND. `purge-dlq --older-than 1ns` used to truncate to
// OlderThanMs=0 — which the engine reads as "no bound" — and wipe the entire DLQ while looking
// bounded. Any positive duration now rounds UP to 1ms, so a message younger than the bound
// survives; the old code deleted it.
//
// The clock is frozen, so this asserts the semantics and never races real elapsed time.
func TestSubMsOlderThanIsBounded(t *testing.T) {
	ctx := context.Background()
	now := int64(1_000_000_000_000)
	clock := func() int64 { return now }

	// Two dead letters: one aged well past any bound, one created "now".
	newDLQ := func(t *testing.T) *mqlite.Embedded {
		t.Helper()
		e, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"), mqlite.WithClock(clock))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
		if err := e.CreateQueue(ctx, "q", mqlite.QueueConfig{MaxDeliveryCount: 1}); err != nil {
			t.Fatal(err)
		}
		deadLetter := func() {
			if _, err := e.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			ms, err := e.Receive(ctx, "q", mqlite.RecvOpts{Max: 1})
			if err != nil || len(ms) != 1 {
				t.Fatalf("receive: %v n=%d", err, len(ms))
			}
			if err := ms[0].Reject(ctx, mqlite.RejectOpts{Reason: "poison"}); err != nil {
				t.Fatal(err)
			}
		}
		now -= time.Hour.Milliseconds() // the old one is dead-lettered an hour ago…
		deadLetter()
		now += time.Hour.Milliseconds() // …and the fresh one right now
		deadLetter()
		if m, _ := e.Stats(ctx, "q"); m.DeadLettered != 2 {
			t.Fatalf("setup: dead_lettered = %d, want 2", m.DeadLettered)
		}
		return e
	}

	// Every positive sub-ms bound behaves as >= 1ms: the hour-old dead letter goes, the
	// zero-age one stays. Unbounded (the bug) would have taken both.
	for _, d := range []time.Duration{time.Nanosecond, 999999 * time.Nanosecond, time.Millisecond, 1500 * time.Microsecond} {
		t.Run("purge "+d.String(), func(t *testing.T) {
			e := newDLQ(t)
			n, err := e.Purge(ctx, "q", mqlite.PurgeOpts{OlderThan: d})
			if err != nil {
				t.Fatalf("purge: %v", err)
			}
			if n != 1 {
				t.Errorf("purged %d with --older-than %s, want 1 — a positive bound must never mean \"unbounded\"", n, d)
			}
			if m, _ := e.Stats(ctx, "q"); m.DeadLettered != 1 {
				t.Errorf("dead_lettered = %d after a bounded purge, want 1 (the fresh message must survive)", m.DeadLettered)
			}
		})
		t.Run("redrive "+d.String(), func(t *testing.T) {
			e := newDLQ(t)
			n, err := e.Redrive(ctx, "q", mqlite.RedriveOpts{OlderThan: d})
			if err != nil {
				t.Fatalf("redrive: %v", err)
			}
			if n != 1 {
				t.Errorf("redrove %d with --older-than %s, want 1", n, d)
			}
			if m, _ := e.Stats(ctx, "q"); m.DeadLettered != 1 || m.Active != 1 {
				t.Errorf("dead=%d active=%d after a bounded redrive, want 1/1", m.DeadLettered, m.Active)
			}
		})
	}
}

// TestEmbeddedErrClosedThroughSDK is the SDK-boundary check for the round-8 close contract: an
// operation on a closed embedded engine surfaces mqlite.ErrClosed through the PUBLIC package, so a
// caller can errors.Is against it without reaching into engine.
func TestEmbeddedErrClosedThroughSDK(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "mq.db")
	e, err := mqlite.OpenEmbedded(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("x")}); !errors.Is(err, mqlite.ErrClosed) {
		t.Fatalf("an operation on a closed embedded engine must be mqlite.ErrClosed, got %v", err)
	}
}

// MQLITE-103: the public raw SQL handle is outside the engine's per-statement guards.
// Pin every EngineTx method so a new transaction operation needs a cancellation classification.
func TestEmbeddedTxCancellation(t *testing.T) {
	txType := reflect.TypeOf((*engine.EngineTx)(nil))
	var methods []string
	for i := 0; i < txType.NumMethod(); i++ {
		methods = append(methods, txType.Method(i).Name)
	}
	if want := []string{"Context", "SQL", "SendOne"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("EngineTx methods = %v, want %v; extend the cancellation contract for new methods", methods, want)
	}
	for _, store := range []string{"memory", "file"} {
		t.Run(store, func(t *testing.T) {
			ctx := context.Background()
			dsn := ":memory:"
			if store == "file" {
				dsn = "file:" + filepath.Join(t.TempDir(), "mq.db")
			}
			e, err := mqlite.OpenEmbedded(ctx, dsn, mqlite.WithoutBackground())
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if err := e.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
				t.Fatal(err)
			}
			if err := e.Tx(ctx, func(tx *engine.EngineTx) error {
				_, err := tx.SQL().ExecContext(tx.Context(), `CREATE TABLE business (id INTEGER PRIMARY KEY)`)
				return err
			}); err != nil {
				t.Fatal(err)
			}

			t.Run("already cancelled", func(t *testing.T) {
				caller, cancel := context.WithCancel(ctx)
				cancel()
				entered := false
				err := e.Tx(caller, func(*engine.EngineTx) error {
					entered = true
					return nil
				})
				if !errors.Is(err, context.Canceled) || entered {
					t.Fatalf("pre-cancelled Tx: err=%v callback entered=%v", err, entered)
				}
			})

			t.Run("raw SQL after cancellation rolls back", func(t *testing.T) {
				caller, cancel := context.WithCancel(ctx)
				defer cancel()
				returnedNil := false
				err := e.Tx(caller, func(tx *engine.EngineTx) error {
					if _, err := tx.SQL().ExecContext(tx.Context(), `INSERT INTO business VALUES (1)`); err != nil {
						return err
					}
					if _, err := tx.SendOne("q", engine.OutMessage{Body: []byte("before")}); err != nil {
						return err
					}
					cancel()
					if tx.Context().Err() != nil || tx.Context().Done() != nil {
						return fmt.Errorf("local execution context must remain uncancellable")
					}
					// Every raw statement method can still write using the protected context.
					if _, err := tx.SQL().ExecContext(tx.Context(), `INSERT INTO business VALUES (2)`); err != nil {
						return fmt.Errorf("raw ExecContext after cancellation: %w", err)
					}
					rows, err := tx.SQL().QueryContext(tx.Context(), `INSERT INTO business VALUES (3) RETURNING id`)
					if err != nil {
						return fmt.Errorf("raw QueryContext after cancellation: %w", err)
					}
					for rows.Next() {
					}
					readErr := rows.Err()
					closeErr := rows.Close()
					if err := errors.Join(readErr, closeErr); err != nil {
						return err
					}
					var id int
					if err := tx.SQL().QueryRowContext(tx.Context(), `INSERT INTO business VALUES (4) RETURNING id`).Scan(&id); err != nil {
						return fmt.Errorf("raw QueryRowContext after cancellation: %w", err)
					}
					// The public mqlite-owned operation observes the original caller, not tx.Context().
					if _, err := tx.SendOne("q", engine.OutMessage{Body: []byte("after")}); !errors.Is(err, context.Canceled) {
						return fmt.Errorf("SendOne after cancellation: got %w, want context.Canceled", err)
					}
					var business, messages int
					if err := tx.SQL().QueryRowContext(tx.Context(),
						`SELECT (SELECT COUNT(*) FROM business), (SELECT COUNT(*) FROM messages)`).Scan(&business, &messages); err != nil {
						return err
					}
					if business != 4 || messages != 1 {
						return fmt.Errorf("before rollback: business=%d messages=%d, want 4 and 1", business, messages)
					}
					returnedNil = true
					return nil // Cancellation must roll everything back even if the callback reports success.
				})
				if !errors.Is(err, context.Canceled) || !returnedNil {
					t.Fatalf("Tx must roll back after the callback returns nil: err=%v returned nil=%v", err, returnedNil)
				}
				if err := e.Tx(ctx, func(tx *engine.EngineTx) error {
					var business int
					if err := tx.SQL().QueryRowContext(tx.Context(), `SELECT COUNT(*) FROM business`).Scan(&business); err != nil {
						return err
					}
					if business != 0 {
						return fmt.Errorf("cancelled transaction committed %d business writes", business)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				stats, err := e.Stats(ctx, "q")
				if err != nil || stats.Total != 0 {
					t.Fatalf("queue after rollback: stats=%+v err=%v", stats, err)
				}
				if _, err := e.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("healthy")}); err != nil {
					t.Fatalf("writer after rollback: %v", err)
				}
			})
		})
	}
}

// TestEmbeddedCloseStopsRunPromptly is the end-to-end twin of the engine-level long-poll-wake test
// (round-8): a Receiver.Run long-polling an EMPTY embedded queue must return promptly when another
// goroutine calls Close — not sleep out its ~20s poll window against a torn-down engine. The
// fake-source receiver test cannot see this; only a real engine with a real parked waiter can.
func TestEmbeddedCloseStopsRunPromptly(t *testing.T) {
	ctx := context.Background()
	e, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- e.Receiver("q").Run(ctx, func(context.Context, *mqlite.Message) error { return nil })
	}()
	time.Sleep(150 * time.Millisecond) // let Run park in its long-poll

	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-runDone:
		if !errors.Is(err, mqlite.ErrClosed) {
			t.Fatalf("Run returned %v, want mqlite.ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Receiver.Run was still blocked 3s after Close returned — the long-poll waiter was " +
			"not woken (it would sleep out its full poll window)")
	}
}

// TestRunCancelledThenClosedReturnsCanceled pins the graceful-shutdown ordering (round-8, codex):
// the caller cancels Run's context while a handler is active, then closes the engine. The worker's
// cleanup settle hits the closed engine and reports ErrClosed — an artifact of the shutdown itself,
// which must NOT displace the caller's own story: Run returns context.Canceled. (ErrClosed stays the
// answer when the engine is closed under a receiver whose context is live —
// TestEmbeddedCloseStopsRunPromptly.)
func TestRunCancelledThenClosedReturnsCanceled(t *testing.T) {
	ctx := context.Background()
	e, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	rctx, cancel := context.WithCancel(ctx)
	inHandler := make(chan struct{})
	release := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- e.Receiver("q").Run(rctx, func(context.Context, *mqlite.Message) error {
			close(inHandler)
			<-release // hold the handler until cancel AND Close have both happened
			return nil
		})
	}()
	<-inHandler

	cancel()                          // 1. the caller cancels…
	if err := e.Close(); err != nil { // 2. …then tears the engine down
		t.Fatalf("close: %v", err)
	}
	close(release) // 3. the handler finishes; its cleanup settle hits the closed engine

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run after cancel-then-Close returned %v — the caller cancelled first, so Run "+
				"must report context.Canceled, not an ErrClosed artifact of its own shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel + Close + handler completion")
	}
}

// TestEmbeddedCloseWaitsForPublicTx is the round-7 P1 regression pinned on the PUBLIC API surface
// (the white-box twin lives in engine/storage_test.go): while a public Tx is open and holding the
// single writer, Close must block until the Tx commits, keep the file lock held the whole time (a
// second Open gets ErrDBLocked), and only then release it. Nothing below the SDK is touched here,
// so a future refactor of the internal gate cannot hide this failure. Every wait is gated on an
// observable state change, never on a fixed sleep — the round-8 rule for this layer.
func TestEmbeddedCloseWaitsForPublicTx(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "mq.db")
	e, err := mqlite.OpenEmbedded(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	inTx := make(chan struct{})
	closeReturned := make(chan struct{})
	txDone := make(chan error, 1)

	// A public transaction that has enqueued and now holds the single writer, parked.
	go func() {
		txDone <- e.Tx(ctx, func(tx *engine.EngineTx) error {
			if _, err := tx.SendOne("q", engine.OutMessage{Body: []byte("close-tx")}); err != nil {
				return err
			}
			close(inTx)
			<-release // hold the transaction open
			// Deterministic ordering proof at the moment the callback resumes: if Close has
			// ALREADY returned, the file lock was released while this Tx was still open —
			// exactly the round-7 P1.
			select {
			case <-closeReturned:
				return fmt.Errorf("Close returned while a public Tx was still open")
			default:
			}
			return nil
		})
	}()
	<-inTx // the transaction is open and holds the writer

	closed := make(chan error, 1)
	go func() {
		closed <- e.Close()
		close(closeReturned)
	}()

	// Deterministic signals while the Tx is still open — none of them may touch the DB conn
	// (the Tx holds the single writer, so any DB op would queue behind it and deadlock the
	// test). The second-Open probe is safe: it is a sidecar file-lock attempt, not a DB op.
	//
	// 1. Close must not have returned: the Tx has not committed yet.
	select {
	case err := <-closed:
		t.Fatalf("Close returned while a public Tx was still open (err=%v) — it released the "+
			"writer/file lock early", err)
	default:
		// good: Close is blocked on the transaction
	}
	// 2. The file lock must still be held: a second engine on the same file is refused.
	if e2, err := mqlite.OpenEmbedded(ctx, dsn); !errors.Is(err, mqlite.ErrDBLocked) {
		if e2 != nil {
			_ = e2.Close()
		}
		t.Fatalf("a second Open during an in-flight public Tx + pending Close must get "+
			"ErrDBLocked, got %v", err)
	}

	close(release) // let the transaction commit and return

	if err := <-txDone; err != nil {
		t.Fatalf("Tx: %v", err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the transaction committed")
	}

	// Now — and only now — the file can be reopened, and the Tx's enqueue is durable.
	e3, err := mqlite.OpenEmbedded(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen after a clean close: %v", err)
	}
	defer e3.Close()
	msgs, err := e3.Peek(ctx, "q")
	if err != nil {
		t.Fatalf("peek after reopen: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Body) != "close-tx" {
		t.Fatalf("the Tx's enqueue is not durable after reopen: got %d message(s)", len(msgs))
	}
}

// Receive-delete has no lease, including a cached attempt replay; peek-lock
// retains its full lease shape. Compare every exported Message field in both modes.
func TestSDKReceiveDeleteLeaseShape(t *testing.T) {
	for _, storage := range []string{"memory", "file"} {
		for _, transport := range []string{"embedded", "http"} {
			for _, atMostOnce := range []bool{false, true} {
				for _, attempt := range []string{"", "same-attempt"} {
					t.Run(fmt.Sprintf("%s/%s/delete_%t/attempt_%t", storage, transport, atMostOnce, attempt != ""), func(t *testing.T) {
						ctx := context.Background()
						const now = int64(1700000000000)
						dsn := ":memory:"
						if storage == "file" {
							dir, err := os.MkdirTemp("", "mqlite-delete-lease-*")
							if err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { _ = os.RemoveAll(dir) })
							dsn = "file:" + filepath.Join(dir, "mq.db")
						}
						eng, err := mqlite.OpenEmbedded(ctx, dsn, mqlite.WithoutBackground(), mqlite.WithClock(func() int64 { return now }))
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = eng.Close() })
						receive := eng.Receive
						if transport == "http" {
							ts := httptest.NewServer(server.New(eng.Engine(), []string{"delete-test"}).Handler())
							t.Cleanup(ts.Close)
							remote, err := mqlite.Open(ctx, ts.URL, mqlite.WithToken("delete-test"))
							if err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { _ = remote.Close() })
							receive = remote.Receive
						}
						if err := eng.CreateQueue(ctx, "q", mqlite.QueueConfig{LockDuration: time.Second}); err != nil {
							t.Fatal(err)
						}
						out := mqlite.OutMessage{Body: []byte{0, 255, 128, 3}, MessageID: "id", GroupID: "group", CorrelationID: "correlation", ReplyTo: "reply", Subject: "subject", ContentType: "binary", Properties: map[string]string{"key": "value"}}
						seq, err := eng.SendOne(ctx, "q", out)
						if err != nil {
							t.Fatal(err)
						}
						want := &mqlite.Message{SequenceNumber: seq, Body: out.Body, MessageID: out.MessageID, GroupID: out.GroupID, CorrelationID: out.CorrelationID, ReplyTo: out.ReplyTo, Subject: out.Subject, ContentType: out.ContentType, Properties: out.Properties, DeliveryCount: 1, EnqueuedAt: time.UnixMilli(now)}
						if !atMostOnce {
							want.LockedUntil = time.UnixMilli(now + 1000)
						}
						wantJSON, err := json.Marshal(want)
						if err != nil {
							t.Fatal(err)
						}
						rounds := 1
						if attempt != "" {
							rounds = 2
						}
						var token string
						for round := 0; round < rounds; round++ {
							messages, err := receive(ctx, "q", mqlite.RecvOpts{Max: 1, AtMostOnce: atMostOnce, Attempt: attempt})
							if err != nil || len(messages) != 1 {
								t.Fatalf("receive round %d: %+v, %v", round, messages, err)
							}
							msg := messages[0]
							gotJSON, err := json.Marshal(msg)
							if err != nil || string(gotJSON) != string(wantJSON) {
								t.Fatalf("complete message shape: got=%s want=%s err=%v", gotJSON, wantJSON, err)
							}
							if (msg.LockToken() == "") != atMostOnce || (round > 0 && msg.LockToken() != token) {
								t.Fatal("wrong token presence or replay identity")
							}
							token = msg.LockToken()
							if atMostOnce && !errors.Is(msg.Complete(ctx), mqlite.ErrLockLost) {
								t.Fatal("receive-delete handle unexpectedly settled")
							}
						}
						stats, err := eng.Stats(ctx, "q")
						wantRetained := int64(1)
						if atMostOnce {
							wantRetained = 0
						}
						if err != nil || stats.Total != wantRetained || stats.Locked != wantRetained {
							t.Fatalf("retained state: %+v, %v", stats, err)
						}
					})
				}
			}
		}
	}
}

// ─── Managed access keys ────────────────────────────────────────────────────

func TestManagedKeySDKParity(t *testing.T) {
	type keyAdmin interface {
		CreateKey(context.Context, mqlite.CreateKeyOptions) (mqlite.CreateKeyResult, error)
		ListKeys(context.Context, string, int) (mqlite.KeyPage, error)
		RevokeKey(context.Context, string) error
	}
	ctx := context.Background()
	for _, mode := range []string{"embedded", "http"} {
		t.Run(mode, func(t *testing.T) {
			client, embedded := newBroker(t, "administrator")
			var admin keyAdmin = embedded
			if mode == "http" {
				admin = client
			}
			page, err := admin.ListKeys(ctx, "", 0)
			if err != nil || page.Keys == nil || len(page.Keys) != 0 || page.NextAfterID != "" {
				t.Fatalf("empty page = %+v, %v", page, err)
			}
			ids := []string{strings.Repeat("1", 32), strings.Repeat("2", 32)}
			for i, id := range ids {
				opts := mqlite.CreateKeyOptions{ID: id, Name: fmt.Sprintf("worker-%d", i), Permissions: []string{"listen", "send"}, ExpiresAtMs: time.Now().Add(time.Hour).UnixMilli()}
				result, err := admin.CreateKey(ctx, opts)
				if err != nil {
					t.Fatal(err)
				}
				if result.Key.ID != id || result.Key.Name != opts.Name || result.Key.CreatedAtMs <= 0 || result.Key.ExpiresAtMs != opts.ExpiresAtMs || result.Key.RevokedAtMs != 0 || !reflect.DeepEqual(result.Key.Permissions, []string{"send", "listen"}) || !regexp.MustCompile(`^mqk_[0-9a-f]{64}$`).MatchString(result.Token) {
					t.Fatal("created key metadata or token format differs from contract")
				}
				duplicate, err := admin.CreateKey(ctx, opts)
				if !errors.Is(err, mqlite.ErrKeyConflict) || !reflect.DeepEqual(duplicate, mqlite.CreateKeyResult{}) {
					t.Fatalf("repeated ID must conflict and reveal no secret: %v", err)
				}
			}
			for i, after := range []string{"", ids[0]} {
				page, err = admin.ListKeys(ctx, after, 1)
				if err != nil || len(page.Keys) != 1 || page.Keys[0].ID != ids[i] {
					t.Fatalf("page %d = %+v, %v", i, page, err)
				}
				wantNext := ""
				if i == 0 {
					wantNext = ids[0]
				}
				if page.NextAfterID != wantNext {
					t.Fatalf("page %d next = %q, want %q", i, page.NextAfterID, wantNext)
				}
			}
			for i := 0; i < 2; i++ {
				if err := admin.RevokeKey(ctx, ids[0]); err != nil {
					t.Fatalf("revoke %d: %v", i, err)
				}
			}
			page, err = admin.ListKeys(ctx, "", 100)
			if err != nil || len(page.Keys) != 2 || page.Keys[0].RevokedAtMs <= 0 {
				t.Fatalf("revoked metadata must remain visible: %+v, %v", page, err)
			}
			if err := admin.RevokeKey(ctx, strings.Repeat("f", 32)); !errors.Is(err, mqlite.ErrNotFound) {
				t.Fatalf("missing revoke = %v", err)
			}
			for _, bad := range []mqlite.CreateKeyOptions{
				{Name: "missing-id", Permissions: []string{"send"}},
				{ID: "BAD", Name: "invalid-id", Permissions: []string{"send"}},
				{ID: mqlite.GenerateKeyID(), Name: "bad-permission", Permissions: []string{"read"}},
				{ID: mqlite.GenerateKeyID(), Permissions: []string{"manage"}},
				{ID: mqlite.GenerateKeyID(), Name: "past", Permissions: []string{"manage"}, ExpiresAtMs: 1},
			} {
				if _, err := admin.CreateKey(ctx, bad); !errors.Is(err, mqlite.ErrInvalidArgument) {
					t.Fatalf("invalid create %+v: %v", bad, err)
				}
			}
			for _, invalid := range []struct {
				after string
				limit int
			}{{"bad", 1}, {"", -1}, {"", 1001}} {
				if _, err := admin.ListKeys(ctx, invalid.after, invalid.limit); !errors.Is(err, mqlite.ErrInvalidArgument) {
					t.Fatalf("invalid list: %v", err)
				}
			}
			if err := admin.RevokeKey(ctx, "bad"); !errors.Is(err, mqlite.ErrInvalidArgument) {
				t.Fatalf("invalid revoke: %v", err)
			}
		})
	}
}

func TestManagedKeySDKAuthorization(t *testing.T) {
	ctx := context.Background()
	emb, err := mqlite.OpenEmbedded(ctx, ":memory:", mqlite.WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer emb.Close()
	ts := httptest.NewServer(server.New(emb.Engine(), []string{"bootstrap"}).Handler())
	defer ts.Close()
	open := func(token string) *mqlite.Client {
		c, err := mqlite.Open(ctx, ts.URL, mqlite.WithToken(token))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	admin := open("bootstrap")
	manager, err := admin.CreateKey(ctx, mqlite.CreateKeyOptions{ID: mqlite.GenerateKeyID(), Name: "manager", Permissions: []string{"send", "listen", "manage"}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manager.Key.Permissions, []string{"manage"}) {
		t.Fatal("manage must canonicalize all rights")
	}
	managerClient := open(manager.Token)
	sender, err := managerClient.CreateKey(ctx, mqlite.CreateKeyOptions{ID: mqlite.GenerateKeyID(), Name: "sender", Permissions: []string{"send"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	senderClient := open(sender.Token)
	if _, err := senderClient.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if _, err := senderClient.Receive(ctx, "q"); !errors.Is(err, mqlite.ErrPermissionDenied) {
		t.Fatalf("sender receive = %v", err)
	}
	if _, err := senderClient.CreateKey(ctx, mqlite.CreateKeyOptions{ID: mqlite.GenerateKeyID(), Name: "escalation", Permissions: []string{"manage"}}); !errors.Is(err, mqlite.ErrPermissionDenied) {
		t.Fatalf("sender create = %v", err)
	}
	if _, err := senderClient.ListKeys(ctx, "", 1); !errors.Is(err, mqlite.ErrPermissionDenied) {
		t.Fatalf("sender list = %v", err)
	}
	if err := senderClient.RevokeKey(ctx, manager.Key.ID); !errors.Is(err, mqlite.ErrPermissionDenied) {
		t.Fatalf("sender revoke = %v", err)
	}
	if err := managerClient.RevokeKey(ctx, sender.Key.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := senderClient.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("revoked")}); !errors.Is(err, mqlite.ErrUnauthenticated) {
		t.Fatalf("revoked sender = %v", err)
	}
	if err := managerClient.RevokeKey(ctx, manager.Key.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := managerClient.ListKeys(ctx, "", 1); !errors.Is(err, mqlite.ErrUnauthenticated) {
		t.Fatalf("self-revoked manager = %v", err)
	}
	if _, err := admin.ListKeys(ctx, "", 0); err != nil {
		t.Fatalf("bootstrap survives managed revocation: %v", err)
	}
}

func TestManagedKeySDKSortedPaginationParity(t *testing.T) {
	type keyAdmin interface {
		ListKeys(context.Context, string, int) (mqlite.KeyPage, error)
		ListKeysWithOptions(context.Context, mqlite.ListKeysOptions) (mqlite.KeyPage, error)
	}
	ctx := context.Background()
	for _, mode := range []string{"embedded", "http"} {
		t.Run(mode, func(t *testing.T) {
			var clock atomic.Int64
			clock.Store(100)
			emb, err := mqlite.OpenEmbedded(ctx, ":memory:", mqlite.WithClock(clock.Load), mqlite.WithoutBackground())
			if err != nil {
				t.Fatal(err)
			}
			defer emb.Close()
			var admin keyAdmin = emb
			if mode == "http" {
				ts := httptest.NewServer(server.New(emb.Engine(), []string{"administrator"}).Handler())
				defer ts.Close()
				client, err := mqlite.Open(ctx, ts.URL, mqlite.WithToken("administrator"))
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				admin = client
			}
			id := func(prefix string) string { return strings.Repeat(prefix, 32) }
			for _, fixture := range []struct {
				prefix  string
				created int64
				expires int64
			}{{"a", 100, 0}, {"1", 200, 250}, {"f", 200, 0}, {"7", 300, 0}} {
				clock.Store(fixture.created)
				if _, err := emb.CreateKey(ctx, mqlite.CreateKeyOptions{ID: id(fixture.prefix), Name: "worker-" + fixture.prefix, Permissions: []string{"send"}, ExpiresAtMs: fixture.expires}); err != nil {
					t.Fatal(err)
				}
			}
			if err := emb.RevokeKey(ctx, id("f")); err != nil {
				t.Fatal(err)
			}
			for _, order := range []string{"", "id_asc", "created_desc"} {
				want := []string{id("1"), id("7"), id("a"), id("f")}
				if order == "created_desc" {
					want = []string{id("7"), id("f"), id("1"), id("a")}
				}
				for _, limit := range []int{0, 1, 2, 3} {
					opts := mqlite.ListKeysOptions{Sort: order, Limit: limit}
					var got []string
					for round := 0; round < 5; round++ {
						page, err := admin.ListKeysWithOptions(ctx, opts)
						if err != nil {
							t.Fatalf("sort=%q limit=%d: %v", order, limit, err)
						}
						for _, key := range page.Keys {
							got = append(got, key.ID)
							if key.ID == id("f") && key.RevokedAtMs != 300 || key.ID == id("1") && key.ExpiresAtMs != 250 {
								t.Fatal("revoked and expired metadata must remain visible")
							}
						}
						if page.NextAfterID == "" {
							break
						}
						if len(page.Keys) != limit || page.NextAfterID != page.Keys[len(page.Keys)-1].ID || page.NextAfterID == opts.AfterID {
							t.Fatal("continuation must use the last public ID and make progress")
						}
						opts.AfterID = page.NextAfterID
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("sort=%q limit=%d got=%v want=%v", order, limit, got, want)
					}
				}
			}
			// ID order still accepts an absent predecessor for lost-create reconciliation.
			legacy, err := admin.ListKeys(ctx, id("0"), 1)
			if err != nil || len(legacy.Keys) != 1 || legacy.Keys[0].ID != id("1") {
				t.Fatalf("legacy predecessor page = %+v, %v", legacy, err)
			}
			for _, invalid := range []mqlite.ListKeysOptions{
				{Sort: "created_desc", AfterID: id("0")}, {Sort: "created_desc", AfterID: "bad"},
				{Sort: "created_desc", Limit: -1}, {Sort: "created_desc", Limit: 1001}, {Sort: "newest"},
			} {
				if _, err := admin.ListKeysWithOptions(ctx, invalid); !errors.Is(err, mqlite.ErrInvalidArgument) {
					t.Fatalf("invalid list options %+v: %v", invalid, err)
				}
			}
		})
	}
}

func TestManagedKeySDKSortedResponseValidation(t *testing.T) {
	ctx := context.Background()
	id := func(prefix string) string { return strings.Repeat(prefix, 32) }
	key := func(prefix string, created int64) mqlite.AccessKey {
		return mqlite.AccessKey{ID: id(prefix), Name: "worker", Permissions: []string{"send"}, CreatedAtMs: created}
	}
	opts := mqlite.ListKeysOptions{Sort: "created_desc", AfterID: id("5"), Limit: 2}
	for _, tc := range []struct {
		name string
		page mqlite.KeyPage
		ok   bool
	}{
		{"creation order allows ascending IDs", mqlite.KeyPage{Keys: []mqlite.AccessKey{key("2", 300), key("f", 200)}, NextAfterID: id("f")}, true},
		{"equal timestamps use descending IDs", mqlite.KeyPage{Keys: []mqlite.AccessKey{key("f", 200), key("2", 200)}}, true},
		{"ascending timestamps", mqlite.KeyPage{Keys: []mqlite.AccessKey{key("f", 200), key("2", 300)}}, false},
		{"tie with ascending IDs", mqlite.KeyPage{Keys: []mqlite.AccessKey{key("2", 200), key("f", 200)}}, false},
		{"duplicate ID with different timestamp", mqlite.KeyPage{Keys: []mqlite.AccessKey{key("2", 300), key("2", 200)}}, false},
		{"cursor repeated in page", mqlite.KeyPage{Keys: []mqlite.AccessKey{key("5", 300)}}, false},
		{"cursor differs from last record", mqlite.KeyPage{Keys: []mqlite.AccessKey{key("2", 300), key("f", 200)}, NextAfterID: id("2")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var got wire.ListKeysRequest
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil || got != opts || r.URL.Path != wire.PathListKeys {
					t.Error("list options were not forwarded unchanged")
				}
				_ = json.NewEncoder(w).Encode(tc.page)
			}))
			defer ts.Close()
			client, err := mqlite.Open(ctx, ts.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			got, err := client.ListKeysWithOptions(ctx, opts)
			if (err == nil) != tc.ok || calls.Load() != 1 {
				t.Fatalf("valid=%v error=%v calls=%d", tc.ok, err, calls.Load())
			}
			if tc.ok && !reflect.DeepEqual(got, tc.page) || !tc.ok && !reflect.DeepEqual(got, mqlite.KeyPage{}) {
				t.Fatal("valid page changed or invalid response returned partial metadata")
			}
		})
	}
}

func TestManagedKeyCreateLostResponseIsNotRetried(t *testing.T) {
	ctx := context.Background()
	emb, err := mqlite.OpenEmbedded(ctx, ":memory:", mqlite.WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer emb.Close()
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var opts mqlite.CreateKeyOptions
		if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
			t.Error(err)
			return
		}
		if _, err := emb.CreateKey(r.Context(), opts); err != nil {
			t.Error(err)
			return
		}
		// The DB commit succeeded, but a truncated reply cannot disclose the secret.
		_, _ = w.Write([]byte(`{"key":`))
	}))
	defer ts.Close()
	c, err := mqlite.Open(ctx, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateKey(ctx, mqlite.CreateKeyOptions{}); !errors.Is(err, mqlite.ErrInvalidArgument) || calls.Load() != 0 {
		t.Fatal("invalid ID must fail before issuing a request")
	}
	id := mqlite.GenerateKeyID()
	result, err := c.CreateKey(ctx, mqlite.CreateKeyOptions{ID: id, Name: "uncertain", Permissions: []string{"manage"}})
	if err == nil || !reflect.DeepEqual(result, mqlite.CreateKeyResult{}) || calls.Load() != 1 {
		t.Fatal("uncertain create must return no secret and never retry")
	}
	page, err := emb.ListKeys(ctx, "", 0)
	if err != nil || len(page.Keys) != 1 || page.Keys[0].ID != id {
		t.Fatalf("retained ID must identify committed key: %+v, %v", page, err)
	}
	if err := emb.RevokeKey(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteHTTPErrorIncludesStatusAndBoundsBodies(t *testing.T) {
	assertHTTPStatus := func(t *testing.T, err error, want int) {
		t.Helper()
		var statusErr *mqlite.HTTPError
		if !errors.As(err, &statusErr) || statusErr.StatusCode != want || statusErr.HTTPStatus() != want {
			t.Fatalf("error = %v, want typed HTTP status %d", err, want)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("HTTP %d", want)) {
			t.Fatalf("error = %v, want visible HTTP status", err)
		}
	}

	t.Run("empty gateway response retains status", func(t *testing.T) {
		stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer stub.Close()
		client, err := mqlite.Open(context.Background(), stub.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		_, err = client.Stats(context.Background(), "jobs")
		if err == nil {
			t.Fatal("empty 503 response was accepted")
		}
		assertHTTPStatus(t, err, http.StatusServiceUnavailable)
	})

	t.Run("oversized error body is discarded", func(t *testing.T) {
		const marker = "untrusted-upstream-secret"
		body := `{"code":"invalid_argument","message":"` + marker + `"}`
		httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode:    http.StatusBadGateway,
				Header:        make(http.Header),
				Body:          io.NopCloser(strings.NewReader(body)),
				ContentLength: (64 << 10) + 1,
				Request:       req,
			}, nil
		})}
		client, err := mqlite.Open(context.Background(), "http://broker.invalid", mqlite.WithHTTPClient(httpClient))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		_, err = client.ListQueues(context.Background())
		if err == nil || errors.Is(err, mqlite.ErrInvalidArgument) || strings.Contains(err.Error(), marker) {
			t.Fatalf("oversized error response was trusted or exposed: %v", err)
		}
		assertHTTPStatus(t, err, http.StatusBadGateway)
	})

	t.Run("unknown length error body is bounded", func(t *testing.T) {
		const marker = "secret-after-error-limit"
		body := `{"code":"invalid_argument","message":"` + strings.Repeat("x", 64<<10) + marker + `"}`
		httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode:    http.StatusServiceUnavailable,
				Header:        make(http.Header),
				Body:          io.NopCloser(strings.NewReader(body)),
				ContentLength: -1,
				Request:       req,
			}, nil
		})}
		client, err := mqlite.Open(context.Background(), "http://broker.invalid", mqlite.WithHTTPClient(httpClient))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		_, err = client.ListQueues(context.Background())
		if err == nil || errors.Is(err, mqlite.ErrInvalidArgument) || strings.Contains(err.Error(), marker) {
			t.Fatalf("oversized chunked error response was trusted or exposed: %v", err)
		}
		assertHTTPStatus(t, err, http.StatusServiceUnavailable)
	})

	t.Run("oversized success body is rejected", func(t *testing.T) {
		body := `{"queues":[]}`
		httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        make(http.Header),
				Body:          io.NopCloser(strings.NewReader(body)),
				ContentLength: (64 << 20) + 1,
				Request:       req,
			}, nil
		})}
		client, err := mqlite.Open(context.Background(), "http://broker.invalid", mqlite.WithHTTPClient(httpClient))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		queues, err := client.ListQueues(context.Background())
		if err == nil || queues != nil {
			t.Fatalf("oversized successful response = %+v, %v; want rejection", queues, err)
		}
	})
}

func TestRemoteRPCDoesNotFollowRedirect(t *testing.T) {
	const locationMarker = "redirect-location-secret"
	var sourceCalls atomic.Int32
	var targetCalls atomic.Int32
	var callerRedirectCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sourceCalls.Add(1)
		w.Header().Set("Location", target.URL+wire.PathPurge+"?token="+locationMarker)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	callerHTTPClient := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		callerRedirectCalls.Add(1)
		return nil
	}}
	client, err := mqlite.Open(context.Background(), source.URL, mqlite.WithHTTPClient(callerHTTPClient))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Purge(context.Background(), "jobs")
	var statusErr *mqlite.HTTPError
	if err == nil || !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("redirect error = %v, want typed HTTP 307", err)
	}
	if strings.Contains(err.Error(), locationMarker) || strings.Contains(err.Error(), target.URL) {
		t.Fatalf("redirect error exposed an untrusted Location: %v", err)
	}
	if sourceCalls.Load() != 1 || targetCalls.Load() != 0 {
		t.Fatalf("redirect calls source=%d target=%d, want 1/0", sourceCalls.Load(), targetCalls.Load())
	}
	if callerRedirectCalls.Load() != 0 || callerHTTPClient.CheckRedirect == nil {
		t.Fatalf("MQLite invoked or mutated caller redirect policy: calls=%d", callerRedirectCalls.Load())
	}
}

func TestRemoteDefaultHTTPTransportReusesConcurrentConnections(t *testing.T) {
	const parallel = 12
	arrived := [2]chan struct{}{make(chan struct{}, parallel), make(chan struct{}, parallel)}
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var releaseOnce [2]sync.Once
	releaseWave := func(wave int) {
		releaseOnce[wave].Do(func() { close(release[wave]) })
	}
	requestCtx, cancelRequests := context.WithCancel(context.Background())
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			cancelRequests()
			for wave := range release {
				releaseWave(wave)
			}
		})
	}
	var calls atomic.Int32
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		wave := int(calls.Add(1)-1) / parallel
		if wave >= len(arrived) {
			t.Errorf("unexpected request wave %d", wave)
			return
		}
		arrived[wave] <- struct{}{}
		select {
		case <-release[wave]:
		case <-req.Context().Done():
			return
		}
		_, _ = io.WriteString(w, `{"queues":[]}`)
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	defer client.Close()
	// Registered after Client.Close and Server.Close so every failure releases
	// blocked handlers before either close waits for their connections to exit.
	defer cleanup()

	runWave := func(ctx context.Context, wave int) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make(chan error, parallel)
		wg.Add(parallel)
		for i := 0; i < parallel; i++ {
			go func() {
				defer wg.Done()
				_, callErr := client.ListQueues(ctx)
				errs <- callErr
			}()
		}
		for i := 0; i < parallel; i++ {
			select {
			case <-arrived[wave]:
			case <-time.After(5 * time.Second):
				cleanup()
				wg.Wait()
				t.Fatalf("wave %d reached only %d/%d concurrent requests", wave, i, parallel)
			}
		}
		releaseWave(wave)
		wg.Wait()
		close(errs)
		for callErr := range errs {
			if callErr != nil {
				cleanup()
				t.Fatalf("wave %d request: %v", wave, callErr)
			}
		}
	}

	runWave(requestCtx, 0)
	var reused atomic.Int32
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		if info.Reused {
			reused.Add(1)
		}
	}}
	runWave(httptrace.WithClientTrace(requestCtx, trace), 1)
	if reused.Load() != parallel {
		cleanup()
		t.Fatalf("second wave reused %d/%d connections; default pool is too small", reused.Load(), parallel)
	}
}

func TestRemoteClientCloseOwnsOnlyDefaultTransport(t *testing.T) {
	t.Run("default transport closes its idle connection", func(t *testing.T) {
		closed := make(chan struct{}, 1)
		stub := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"queues":[]}`)
		}))
		stub.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				select {
				case closed <- struct{}{}:
				default:
				}
			}
		}
		stub.Start()
		defer stub.Close()
		client, err := mqlite.Open(context.Background(), stub.URL)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.ListQueues(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatal("Client.Close left its default idle connection open")
		}
	})

	t.Run("custom transport remains caller owned", func(t *testing.T) {
		transport := &closeTrackingTransport{}
		client, err := mqlite.Open(context.Background(), "http://broker.invalid", mqlite.WithHTTPClient(&http.Client{Transport: transport}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.ListQueues(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if transport.closeCalls.Load() != 0 {
			t.Fatalf("Client.Close closed caller-owned transport %d times", transport.closeCalls.Load())
		}
	})

	t.Run("wrapped global default remains compatible and caller owned", func(t *testing.T) {
		original := http.DefaultTransport
		transport := &closeTrackingTransport{}
		http.DefaultTransport = transport
		t.Cleanup(func() { http.DefaultTransport = original })

		client, err := mqlite.Open(context.Background(), "http://broker.invalid")
		if err != nil {
			t.Fatalf("Open with wrapped http.DefaultTransport: %v", err)
		}
		if _, err := client.ListQueues(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if transport.roundTripCalls.Load() != 1 {
			t.Fatalf("wrapped default RoundTrip calls = %d, want one", transport.roundTripCalls.Load())
		}
		if transport.closeCalls.Load() != 0 {
			t.Fatalf("Client.Close closed wrapped global transport %d times", transport.closeCalls.Load())
		}
	})

	t.Run("wrapped global default still cannot opt into redirects", func(t *testing.T) {
		var targetCalls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			targetCalls.Add(1)
		}))
		defer target.Close()
		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", target.URL+wire.PathPurge)
			w.WriteHeader(http.StatusTemporaryRedirect)
		}))
		defer source.Close()

		original := http.DefaultTransport
		transport := &closeTrackingTransport{base: original}
		http.DefaultTransport = transport
		t.Cleanup(func() { http.DefaultTransport = original })

		client, err := mqlite.Open(context.Background(), source.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Purge(context.Background(), "jobs")
		var statusErr *mqlite.HTTPError
		if err == nil || !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusTemporaryRedirect {
			t.Fatalf("redirect error = %v, want typed HTTP 307", err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if transport.roundTripCalls.Load() != 1 || targetCalls.Load() != 0 {
			t.Fatalf("redirect calls wrapper=%d target=%d, want 1/0", transport.roundTripCalls.Load(), targetCalls.Load())
		}
		if transport.closeCalls.Load() != 0 {
			t.Fatalf("Client.Close closed wrapped global transport %d times", transport.closeCalls.Load())
		}
	})
}

func TestHTTPAuthorizationErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		want       error
	}{
		{"bare unauthorized", "Unauthorized", http.StatusUnauthorized, mqlite.ErrUnauthenticated},
		{"bare forbidden", "Forbidden", http.StatusForbidden, mqlite.ErrPermissionDenied},
		{"proxy forbidden code", `{"code":"forbidden","message":"policy denied"}`, http.StatusForbidden, mqlite.ErrPermissionDenied},
		{"structured forbidden", `{"code":"permission_denied","message":"permission required"}`, http.StatusForbidden, mqlite.ErrPermissionDenied},
		{"conflict", `{"code":"key_conflict","message":"key ID already exists"}`, http.StatusConflict, mqlite.ErrKeyConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()
			c, err := mqlite.Open(context.Background(), ts.URL)
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.ListKeys(context.Background(), "", 1)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			var statusErr *mqlite.HTTPError
			if !errors.As(err, &statusErr) || statusErr.StatusCode != tc.status {
				t.Fatalf("got %v, want typed HTTP status %d", err, tc.status)
			}
		})
	}
}

func TestManagedKeySDKRejectsInvalidSuccess(t *testing.T) {
	ctx := context.Background()
	id := strings.Repeat("a", 32)
	token := "mqk_" + strings.Repeat("b", 64)
	key := wire.AccessKey{ID: id, Name: "worker", Permissions: []string{"manage"}, CreatedAtMs: 100}
	metadata, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	goodCreate := `{"key":` + string(metadata) + `,"token":"` + token + `"}`
	goodList := `{"keys":[` + string(metadata) + `]}`
	for _, tc := range []struct {
		name, operation, body string
		truncated             bool
	}{
		{"create null", "create", "null", false},
		{"create empty", "create", "{}", false},
		{"create missing metadata", "create", `{"key":{"id":"` + id + `"},"token":"` + token + `"}`, false},
		{"create wrong id", "create", strings.Replace(goodCreate, id, strings.Repeat("c", 32), 1), false},
		{"create wrong secret", "create", strings.Replace(goodCreate, token, "partial-secret", 1), false},
		{"create wrong rights", "create", strings.Replace(goodCreate, `"manage"`, `"send"`, 1), false},
		{"create wrong name", "create", strings.Replace(goodCreate, `"worker"`, `"other"`, 1), false},
		{"create wrong expiry", "create", strings.Replace(goodCreate, `"expires_at_ms":0`, `"expires_at_ms":200`, 1), false},
		{"create trailing JSON", "create", goodCreate + `{}`, false},
		{"create oversized", "create", strings.Repeat(" ", wire.MaxKeyResponseBytes) + goodCreate, false},
		{"create truncated read", "create", goodCreate, true},
		{"list null", "list", "null", false},
		{"list empty object", "list", "{}", false},
		{"list null keys", "list", `{"keys":null}`, false},
		{"list missing metadata", "list", `{"keys":[{"id":"` + id + `"}]}`, false},
		{"list backward cursor", "list", `{"keys":[` + string(metadata) + `],"next_after_id":"` + strings.Repeat("0", 32) + `"}`, false},
		{"list repeated id", "list", `{"keys":[` + string(metadata) + `,` + string(metadata) + `]}`, false},
		{"list trailing JSON", "list", goodList + `{}`, false},
		{"list oversized", "list", strings.Repeat(" ", wire.MaxKeyResponseBytes) + goodList, false},
		{"list truncated read", "list", goodList, true},
		{"revoke null", "revoke", "null", false},
		{"revoke empty", "revoke", "{}", false},
		{"revoke false", "revoke", `{"ok":false}`, false},
		{"revoke trailing JSON", "revoke", `{"ok":true}{}`, false},
		{"revoke oversized", "revoke", strings.Repeat(" ", wire.MaxKeyResponseBytes) + `{"ok":true}`, false},
		{"revoke truncated read", "revoke", `{"ok":true}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if tc.truncated {
					w.Header().Set("Content-Length", fmt.Sprint(len(tc.body)+50))
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()
			c, err := mqlite.Open(ctx, ts.URL)
			if err != nil {
				t.Fatal(err)
			}
			switch tc.operation {
			case "create":
				var result mqlite.CreateKeyResult
				result, err = c.CreateKey(ctx, mqlite.CreateKeyOptions{ID: id, Name: "worker", Permissions: []string{"manage"}})
				if !reflect.DeepEqual(result, mqlite.CreateKeyResult{}) || err == nil || !strings.Contains(err.Error(), id) {
					t.Fatal("invalid create returned credentials or lost the request ID")
				}
			case "list":
				var result mqlite.KeyPage
				result, err = c.ListKeys(ctx, "", 2)
				if !reflect.DeepEqual(result, mqlite.KeyPage{}) {
					t.Fatal("invalid list returned partial metadata")
				}
			case "revoke":
				err = c.RevokeKey(ctx, id)
			}
			if err == nil || calls.Load() != 1 || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "partial-secret") {
				t.Fatalf("malformed response accepted, retried, or exposed response contents; requests=%d", calls.Load())
			}
			if tc.operation != "list" && !errors.Is(err, mqlite.ErrOutcomeUnknown) {
				t.Fatalf("malformed write response must preserve uncertainty: %v", err)
			}
		})
	}
	// Error responses from proxies must not reintroduce a secret via message/code.
	for _, body := range []string{`{"code":"internal","message":"` + token + `"}`, `{"code":"` + token + `","message":"partial-secret"}`} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(body))
		}))
		c, err := mqlite.Open(ctx, ts.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.CreateKey(ctx, mqlite.CreateKeyOptions{ID: id, Name: "worker", Permissions: []string{"manage"}})
		ts.Close()
		if err == nil || !strings.Contains(err.Error(), id) || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "partial-secret") {
			t.Fatal("create error omitted its request ID or exposed untrusted error text")
		}
	}
}

// ─── Canonical observability across embedded and HTTP ───

func TestSDKObserveCanonicalAndMonitor(t *testing.T) {
	ctx := context.Background()
	embedded, err := mqlite.OpenEmbedded(ctx, ":memory:", mqlite.WithoutBackground(), mqlite.WithClock(func() int64 { return 1800000000000 }))
	if err != nil {
		t.Fatal(err)
	}
	defer embedded.Close()
	if err := embedded.CreateQueue(ctx, "observed", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := embedded.SendOne(ctx, "observed", mqlite.OutMessage{Body: []byte("secret body")}); err != nil {
		t.Fatal(err)
	}
	srv := server.New(embedded.Engine(), []string{"observe-admin"})
	srv.Version = "test"
	srv.MonitorTokens = []string{"observe-monitor"}
	httpServer := httptest.NewServer(srv.Handler())
	defer httpServer.Close()
	local, err := embedded.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if local.HTTP.State != "not_applicable" || local.Access != "embedded" || local.Version == "" {
		t.Fatalf("embedded domain: %+v", local.HTTP)
	}
	encoded, _ := json.Marshal(local)
	if _, err := wire.DecodeObserveResponse(encoded); err != nil {
		t.Fatalf("embedded canonical response: %v", err)
	}
	if strings.Contains(string(encoded), "secret body") || strings.Contains(string(encoded), "observe-admin") {
		t.Fatal("observation leaked a secret")
	}
	for _, identity := range []struct{ token, access string }{{"observe-admin", "manage"}, {"observe-monitor", "monitor"}} {
		client, err := mqlite.Open(ctx, httpServer.URL, mqlite.WithToken(identity.token))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		remote, err := client.Observe(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if remote.Access != identity.access || remote.HTTP.State != "available" {
			t.Fatalf("HTTP visibility = %q/%q", remote.Access, remote.HTTP.State)
		}
		if !reflect.DeepEqual(local.Queues, remote.Queues) || !reflect.DeepEqual(local.Messages, remote.Messages) {
			t.Fatal("native and HTTP queue/counter projections differ")
		}
		if identity.access == "monitor" {
			if _, err := client.Status(ctx); !errors.Is(err, mqlite.ErrPermissionDenied) {
				t.Fatalf("monitor Status: %v", err)
			}
			if _, err := client.SendOne(ctx, "observed", mqlite.OutMessage{Body: []byte("denied")}); !errors.Is(err, mqlite.ErrPermissionDenied) {
				t.Fatalf("monitor Send: %v", err)
			}
		}
	}
	if err := embedded.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := embedded.Observe(ctx)
	if err != nil || after.Collection.State != "unavailable" || after.Queues != nil || len(after.Messages) == 0 {
		t.Fatalf("partial embedded observation lost availability/counters: %v %+v", err, after.Collection)
	}
}

func TestSDKObserveRejectsInvalidSuccessAndPreservesErrors(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		sentinel   error
	}{
		{"empty", `{}`, 200, nil}, {"null", `null`, 200, nil}, {"trailing", `{} {}`, 200, nil},
		{"unauthorized", `{}`, 401, mqlite.ErrUnauthenticated}, {"forbidden", `{}`, 403, mqlite.ErrPermissionDenied},
		{"unsupported", `{"code":"unimplemented","message":"no such path"}`, 404, mqlite.ErrUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != wire.PathObserve {
					t.Errorf("Observe path = %s", r.URL.Path)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()
			client, err := mqlite.Open(context.Background(), ts.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			_, err = client.Observe(context.Background())
			if err == nil || tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
				t.Fatalf("Observe error = %v, want %v", err, tc.sentinel)
			}
		})
	}
}

func TestSDKMonitorTokenServeValidation(t *testing.T) {
	embedded, err := mqlite.OpenEmbedded(context.Background(), ":memory:", mqlite.WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer embedded.Close()
	for _, options := range [][]mqlite.ServeOption{
		{mqlite.WithMonitorTokens("monitor")},
		{mqlite.WithTokens("admin"), mqlite.WithMonitorTokens("admin")},
		{mqlite.WithTokens("admin"), mqlite.WithMonitorTokens(" ")},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := embedded.Serve(ctx, "127.0.0.1:0", options...); err == nil {
			t.Fatal("invalid monitor configuration accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := embedded.Serve(ctx, "127.0.0.1:0", mqlite.WithTokens("admin"), mqlite.WithMonitorTokens("monitor"), mqlite.WithReady(cancel)); err != nil {
		t.Fatalf("valid monitor configuration: %v", err)
	}
}

func TestRemoteDeleteQueue(t *testing.T) {
	c, e := newBroker(t, "delete-test")
	ctx := context.Background()
	if err := c.CreateQueue(ctx, "delete-me", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Send(ctx, "delete-me", mqlite.OutMessage{Body: []byte("keep")}); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteQueue(ctx, "delete-me"); !errors.Is(err, mqlite.ErrQueueNotEmpty) {
		t.Fatalf("typed conflict: %v", err)
	}
	ms, err := c.Receive(ctx, "delete-me", mqlite.RecvOpts{})
	if err != nil || len(ms) != 1 || string(ms[0].Body) != "keep" {
		t.Fatalf("protected message: %v %v", ms, err)
	}
	if err := ms[0].Complete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteQueue(ctx, "delete-me"); err != nil {
		t.Fatal(err)
	}
	qs, err := c.ListQueues(ctx)
	if err != nil || len(qs) != 0 {
		t.Fatalf("deleted queue still listed: %v %v", qs, err)
	}
	if err := c.DeleteQueue(ctx, "delete-me"); !errors.Is(err, mqlite.ErrNotFound) {
		t.Fatalf("typed not found: %v", err)
	}
	if err := c.CreateQueue(ctx, "delete-me", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteQueue(ctx, "delete-me"); err != nil {
		t.Fatal(err)
	}
	if err := e.CreateQueue(ctx, "embedded", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteQueue(ctx, "embedded"); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteListQueuesDefaultsLegacyStateToActive(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wire.PathListQueues {
			t.Errorf("path = %q, want %q", r.URL.Path, wire.PathListQueues)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"queues":[{"name":"legacy","kind":"queue","lock_duration_ms":30000,"max_delivery_count":10,"default_ttl_ms":0,"dedup_window_ms":0}]}`))
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	queues, err := client.ListQueues(context.Background())
	if err != nil || len(queues) != 1 || queues[0].Name != "legacy" || queues[0].State != "active" {
		t.Fatalf("legacy queue inventory = %+v, %v", queues, err)
	}
}

func TestRemoteSubscribeKeepsLegacyWireShapeAndValidatesTTL(t *testing.T) {
	var bodies [][]byte
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wire.PathSubscribe {
			t.Errorf("path = %q", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		bodies = append(bodies, body)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	if err := client.Subscribe(ctx, "events", "durable", nil); err != nil {
		t.Fatal(err)
	}
	if err := client.SubscribeWithOptions(ctx, "events", "temporary", &mqlite.Filter{Expr: "true"}, mqlite.SubscribeOptions{TTL: 1500 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || string(bodies[0]) != `{"topic":"events","name":"durable"}` ||
		string(bodies[1]) != `{"topic":"events","name":"temporary","filter":{"expr":"true"},"ttl_ms":1500}` {
		t.Fatalf("subscribe bodies = %q", bodies)
	}
	for _, ttl := range []time.Duration{-1, 1, time.Millisecond - 1} {
		if err := client.SubscribeWithOptions(ctx, "events", "invalid", nil, mqlite.SubscribeOptions{TTL: ttl}); !errors.Is(err, mqlite.ErrInvalidArgument) {
			t.Fatalf("TTL %s = %v", ttl, err)
		}
	}
	if len(bodies) != 2 {
		t.Fatalf("invalid TTL reached broker: %d requests", len(bodies))
	}
}

func TestRemoteListSubscriptionsPreservesLegacyShape(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case wire.PathListSubscriptions:
			_, _ = w.Write([]byte(`{"subscriptions":[{"topic":"events","name":"legacy"}]}`))
		case wire.PathListSubscriptionsPage:
			_, _ = w.Write([]byte(`{"subscriptions":[{"topic":"events","name":"legacy","state":"active","created_at_ms":100,"expires_at_ms":0}],"total":1}`))
		default:
			t.Errorf("path = %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	subs, err := client.ListSubscriptions(context.Background())
	if err != nil || len(subs) != 1 || subs[0].Topic != "events" || subs[0].Name != "legacy" {
		t.Fatalf("legacy subscription inventory = %+v, %v", subs, err)
	}
	encoded, err := json.Marshal(subs)
	if err != nil || string(encoded) != `[{"topic":"events","name":"legacy"}]` {
		t.Fatalf("legacy subscription JSON = %s, %v", encoded, err)
	}
	page, err := client.ListSubscriptionsWithOptions(context.Background(), mqlite.ListSubscriptionsOptions{Topic: "events"})
	if err != nil || page.Total != 1 || len(page.Subscriptions) != 1 || page.Subscriptions[0].State != "active" ||
		page.Subscriptions[0].CreatedAtMs != 100 || page.Subscriptions[0].ExpiresAtMs != 0 {
		t.Fatalf("legacy paged inventory = %+v, %v", page, err)
	}
	encoded, err = json.Marshal(page)
	if err != nil || string(encoded) != `{"subscriptions":[{"topic":"events","name":"legacy","state":"active","created_at_ms":100,"expires_at_ms":0}],"total":1}` {
		t.Fatalf("paged subscription JSON = %s, %v", encoded, err)
	}
}

func TestRemoteListSubscriptionsPageRejectsMalformedSuccess(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response string
		opts     mqlite.ListSubscriptionsOptions
	}{
		{"null root", `null`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"missing fields", `{}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"null subscriptions", `{"subscriptions":null,"total":0}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"missing item state", `{"subscriptions":[{"topic":"events","name":"a","created_at_ms":10,"expires_at_ms":0}],"total":1}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"missing created time", `{"subscriptions":[{"topic":"events","name":"a","state":"active","expires_at_ms":0}],"total":1}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"invalid expiry", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":10}],"total":1}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"wrong scope", `{"subscriptions":[{"topic":"other","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":1}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"unordered", `{"subscriptions":[{"topic":"events","name":"b","state":"active","created_at_ms":10,"expires_at_ms":0},{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":2}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"invalid state", `{"subscriptions":[{"topic":"events","name":"a","state":"deleting","created_at_ms":10,"expires_at_ms":0}],"total":1}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"incomplete first page", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":2}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
		{"over page size", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0},{"topic":"events","name":"b","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":2,"next_after":"cursor"}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 1}},
		{"trailing JSON", `{"subscriptions":[],"total":0} {}`, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != wire.PathListSubscriptionsPage {
					t.Errorf("path = %q", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.response))
			}))
			defer stub.Close()
			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if page, err := client.ListSubscriptionsWithOptions(context.Background(), tc.opts); err == nil {
				t.Fatalf("accepted malformed page: %+v", page)
			}
			if calls.Load() != 1 {
				t.Fatalf("inventory made %d requests, want one", calls.Load())
			}
		})
	}
}

func TestSubscriptionLifecycleSDK(t *testing.T) {
	type subscriptionAPI interface {
		Subscribe(context.Context, string, string, *mqlite.Filter) error
		Send(context.Context, string, ...mqlite.OutMessage) ([]int64, error)
		DeleteSubscription(context.Context, string, string) error
		DeleteSubscriptionWithOptions(context.Context, string, string, ...mqlite.DeleteSubscriptionOpts) (mqlite.DeleteResult, error)
		ListSubscriptions(context.Context) ([]mqlite.SubscriptionInfo, error)
		ListSubscriptionsWithOptions(context.Context, mqlite.ListSubscriptionsOptions) (mqlite.SubscriptionPage, error)
	}
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%t", remote), func(t *testing.T) {
			ctx := context.Background()
			embedded, err := mqlite.OpenEmbedded(ctx, ":memory:", mqlite.WithoutBackground(), mqlite.WithMaxSubscriptions(2))
			if err != nil {
				t.Fatal(err)
			}
			defer embedded.Close()
			var api subscriptionAPI = embedded
			if remote {
				ts := httptest.NewServer(server.New(embedded.Engine(), nil).Handler())
				defer ts.Close()
				client, err := mqlite.Open(ctx, ts.URL)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				api = client
			}

			for _, name := range []string{"a", "b"} {
				if err := api.Subscribe(ctx, "events", name, nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := api.Subscribe(ctx, "events", "overflow", nil); !errors.Is(err, mqlite.ErrSubscriptionLimit) {
				t.Fatalf("subscription limit = %v", err)
			}
			legacy, err := api.ListSubscriptions(ctx)
			if err != nil || len(legacy) != 2 {
				t.Fatalf("active inventory = %+v, %v", legacy, err)
			}
			first, err := api.ListSubscriptionsWithOptions(ctx, mqlite.ListSubscriptionsOptions{Topic: "events", Limit: 1})
			if err != nil || first.Total != 2 || len(first.Subscriptions) != 1 || first.NextAfter == "" {
				t.Fatalf("first page = %+v, %v", first, err)
			}
			second, err := api.ListSubscriptionsWithOptions(ctx, mqlite.ListSubscriptionsOptions{Topic: "events", After: first.NextAfter, Limit: 1})
			if err != nil || second.Total != 2 || len(second.Subscriptions) != 1 || second.NextAfter != "" || second.Subscriptions[0].Name == first.Subscriptions[0].Name {
				t.Fatalf("second page = %+v, %v", second, err)
			}

			if _, err := api.Send(ctx, "events", mqlite.OutMessage{Body: []byte("retained")}); err != nil {
				t.Fatal(err)
			}
			if err := api.DeleteSubscription(ctx, "events", "a"); !errors.Is(err, mqlite.ErrSubscriptionNotEmpty) {
				t.Fatalf("nonempty delete = %v", err)
			}
			if _, err := api.DeleteSubscriptionWithOptions(ctx, "wrong", "a", mqlite.DeleteSubscriptionOpts{Force: true}); !errors.Is(err, mqlite.ErrSubscriptionNotFound) {
				t.Fatalf("wrong-topic delete = %v", err)
			}
			result, err := api.DeleteSubscriptionWithOptions(ctx, "events", "a", mqlite.DeleteSubscriptionOpts{Force: true})
			if err != nil || !result.Deleted || result.State != "deleted" {
				t.Fatalf("force delete = %+v, %v", result, err)
			}
			active, err := api.ListSubscriptions(ctx)
			if err != nil || len(active) != 1 || active[0].Name != "b" {
				t.Fatalf("post-delete active inventory = %+v, %v", active, err)
			}
		})
	}
	if embedded, err := mqlite.OpenEmbedded(context.Background(), ":memory:", mqlite.WithMaxSubscriptions(-1)); !errors.Is(err, mqlite.ErrInvalidArgument) {
		if embedded != nil {
			_ = embedded.Close()
		}
		t.Fatalf("negative subscription limit = %v", err)
	}
}

func TestSubscriptionFixedLifetimeSDK(t *testing.T) {
	type subscriptionTTLAPI interface {
		Subscribe(context.Context, string, string, *mqlite.Filter) error
		SubscribeWithOptions(context.Context, string, string, *mqlite.Filter, mqlite.SubscribeOptions) error
		ListSubscriptions(context.Context) ([]mqlite.SubscriptionInfo, error)
		ListSubscriptionsWithOptions(context.Context, mqlite.ListSubscriptionsOptions) (mqlite.SubscriptionPage, error)
		SendOne(context.Context, string, mqlite.OutMessage, ...mqlite.SendOpts) (int64, error)
		Receive(context.Context, string, ...mqlite.RecvOpts) ([]*mqlite.Message, error)
		Stats(context.Context, string) (mqlite.Metrics, error)
	}
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%t", remote), func(t *testing.T) {
			ctx := context.Background()
			const createdAt = int64(1_700_000_000_000)
			var now atomic.Int64
			now.Store(createdAt)
			embedded, err := mqlite.OpenEmbedded(ctx, ":memory:", mqlite.WithoutBackground(), mqlite.WithClock(now.Load))
			if err != nil {
				t.Fatal(err)
			}
			defer embedded.Close()
			var api subscriptionTTLAPI = embedded
			if remote {
				ts := httptest.NewServer(server.New(embedded.Engine(), nil).Handler())
				defer ts.Close()
				client, err := mqlite.Open(ctx, ts.URL)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				api = client
			}

			oneSecond := mqlite.SubscribeOptions{TTL: time.Second}
			if err := api.SubscribeWithOptions(ctx, "requests", "temporary", &mqlite.Filter{Expr: `subject == "one"`}, oneSecond); err != nil {
				t.Fatal(err)
			}
			assertPage := func(wantExpr, wantState string, wantCreated, wantExpires int64) {
				t.Helper()
				page, err := api.ListSubscriptionsWithOptions(ctx, mqlite.ListSubscriptionsOptions{
					Topic: "requests", IncludeDeleting: true,
				})
				if err != nil || page.Total != 1 || len(page.Subscriptions) != 1 {
					t.Fatalf("inventory = %+v, %v", page, err)
				}
				got := page.Subscriptions[0]
				if got.Topic != "requests" || got.Name != "temporary" || got.Expr != wantExpr ||
					got.State != wantState || got.CreatedAtMs != wantCreated || got.ExpiresAtMs != wantExpires {
					t.Fatalf("subscription = %+v", got)
				}
			}
			assertPage(`subject == "one"`, "active", createdAt, createdAt+1000)

			// Neither a legacy update, an idempotent retry, nor message activity
			// moves the original absolute deadline.
			now.Store(createdAt + 100)
			if err := api.Subscribe(ctx, "requests", "temporary", &mqlite.Filter{Expr: `subject == "two"`}); err != nil {
				t.Fatal(err)
			}
			if err := api.SubscribeWithOptions(ctx, "requests", "temporary", &mqlite.Filter{Expr: `subject == "three"`}, oneSecond); err != nil {
				t.Fatal(err)
			}
			if _, err := api.SendOne(ctx, "requests", mqlite.OutMessage{Body: []byte("reply"), Subject: "three"}); err != nil {
				t.Fatal(err)
			}
			messages, err := api.Receive(ctx, "temporary", mqlite.RecvOpts{AtMostOnce: true})
			if err != nil || len(messages) != 1 {
				t.Fatalf("receive = %d, %v", len(messages), err)
			}
			assertPage(`subject == "three"`, "active", createdAt, createdAt+1000)

			// A different positive TTL is a name conflict and must not apply the
			// accompanying filter update.
			if err := api.SubscribeWithOptions(ctx, "requests", "temporary", &mqlite.Filter{Expr: `subject == "wrong"`}, mqlite.SubscribeOptions{TTL: 2 * time.Second}); !errors.Is(err, mqlite.ErrNameConflict) {
				t.Fatalf("changed TTL = %v", err)
			}
			assertPage(`subject == "three"`, "active", createdAt, createdAt+1000)

			for _, ttl := range []time.Duration{-time.Second, time.Nanosecond, time.Millisecond - 1} {
				if err := api.SubscribeWithOptions(ctx, "requests", "invalid", nil, mqlite.SubscribeOptions{TTL: ttl}); !errors.Is(err, mqlite.ErrInvalidArgument) {
					t.Fatalf("TTL %s = %v", ttl, err)
				}
			}
			now.Store(math.MaxInt64 - 500)
			if err := api.SubscribeWithOptions(ctx, "requests", "overflow", nil, oneSecond); !errors.Is(err, mqlite.ErrInvalidArgument) {
				t.Fatalf("overflow TTL = %v", err)
			}

			// Expiration is a logical force delete at the exact deadline, even
			// before bounded physical cleanup runs.
			now.Store(createdAt + 1000)
			if _, err := api.Stats(ctx, "temporary"); !errors.Is(err, mqlite.ErrQueueNotFound) && !errors.Is(err, mqlite.ErrNotFound) {
				t.Fatalf("expired stats = %v", err)
			}
			if active, err := api.ListSubscriptions(ctx); err != nil || len(active) != 0 {
				t.Fatalf("expired active inventory = %+v, %v", active, err)
			}
			assertPage(`subject == "three"`, "deleting", createdAt, createdAt+1000)
			if err := api.Subscribe(ctx, "requests", "temporary", nil); !errors.Is(err, mqlite.ErrNameConflict) {
				t.Fatalf("reuse before cleanup = %v", err)
			}

			embedded.Engine().RunMaintenanceOnce(ctx)
			page, err := api.ListSubscriptionsWithOptions(ctx, mqlite.ListSubscriptionsOptions{Topic: "requests", IncludeDeleting: true})
			if err != nil || page.Total != 0 || len(page.Subscriptions) != 0 {
				t.Fatalf("post-cleanup inventory = %+v, %v", page, err)
			}
			now.Store(createdAt + 1001)
			if err := api.Subscribe(ctx, "requests", "temporary", nil); err != nil {
				t.Fatal(err)
			}
			assertPage("", "active", createdAt+1001, 0)
			if err := api.SubscribeWithOptions(ctx, "requests", "temporary", &mqlite.Filter{Expr: "true"}, oneSecond); !errors.Is(err, mqlite.ErrNameConflict) {
				t.Fatalf("permanent to temporary = %v", err)
			}
			assertPage("", "active", createdAt+1001, 0)
		})
	}
}

func TestQueueForceDeleteSDK(t *testing.T) {
	type deleteAPI interface {
		CreateQueue(context.Context, string, mqlite.QueueConfig) error
		Send(context.Context, string, ...mqlite.OutMessage) ([]int64, error)
		SendOne(context.Context, string, mqlite.OutMessage, ...mqlite.SendOpts) (int64, error)
		DeleteQueue(context.Context, string) error
		DeleteQueueWithOptions(context.Context, string, ...mqlite.DeleteQueueOpts) (mqlite.DeleteResult, error)
		ListQueues(context.Context) ([]mqlite.QueueInfo, error)
	}
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%t", remote), func(t *testing.T) {
			ctx := context.Background()
			embedded, err := mqlite.OpenEmbedded(ctx, ":memory:", mqlite.WithoutBackground())
			if err != nil {
				t.Fatal(err)
			}
			defer embedded.Close()
			var api deleteAPI = embedded
			if remote {
				ts := httptest.NewServer(server.New(embedded.Engine(), nil).Handler())
				defer ts.Close()
				client, err := mqlite.Open(ctx, ts.URL)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				api = client
			}

			if err := api.CreateQueue(ctx, "discard", mqlite.QueueConfig{}); err != nil {
				t.Fatal(err)
			}
			backlog := make([]mqlite.OutMessage, 5000)
			for i := range backlog {
				backlog[i].Body = []byte("discard")
			}
			if _, err := api.Send(ctx, "discard", backlog...); err != nil {
				t.Fatal(err)
			}
			result, err := api.DeleteQueueWithOptions(ctx, "discard", mqlite.DeleteQueueOpts{Force: true})
			if err != nil || result.Deleted || result.State != "deleting" {
				t.Fatalf("initial force delete = %+v, %v", result, err)
			}
			queues, err := api.ListQueues(ctx)
			if err != nil || len(queues) != 1 || queues[0].Name != "discard" || queues[0].State != "deleting" {
				t.Fatalf("pending inventory = %+v, %v", queues, err)
			}
			if _, err := api.SendOne(ctx, "discard", mqlite.OutMessage{Body: []byte("late")}); err == nil {
				t.Fatal("retired queue accepted a new message")
			}
			for pass := 0; pass < 10 && len(queues) != 0; pass++ {
				embedded.Engine().RunMaintenanceOnce(ctx)
				queues, err = api.ListQueues(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(queues) != 0 {
				t.Fatalf("deleted queue still listed = %+v, %v", queues, err)
			}
		})
	}
}

func TestRemoteDeleteQueueLostAcknowledgementCanBeReconciled(t *testing.T) {
	ctx := context.Background()
	embedded, err := mqlite.OpenEmbedded(ctx, ":memory:", mqlite.WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer embedded.Close()
	if err := embedded.CreateQueue(ctx, "discard", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	backlog := make([]mqlite.OutMessage, 1500)
	for i := range backlog {
		backlog[i].Body = []byte("discard")
	}
	if _, err := embedded.Send(ctx, "discard", backlog...); err != nil {
		t.Fatal(err)
	}

	realHandler := server.New(embedded.Engine(), nil).Handler()
	var loseFirstAck atomic.Bool
	var deleteCalls atomic.Int32
	loseFirstAck.Store(true)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == wire.PathDeleteQueue {
			deleteCalls.Add(1)
		}
		if r.URL.Path == wire.PathDeleteQueue && loseFirstAck.CompareAndSwap(true, false) {
			recorder := httptest.NewRecorder()
			realHandler.ServeHTTP(recorder, r)
			ack, decodeErr := wire.DecodeDeleteResponse(recorder.Body.Bytes())
			if recorder.Code != http.StatusOK || decodeErr != nil || ack.Deleted || ack.State != "deleting" {
				t.Errorf("broker pending acknowledgement = status %d body %q: %v", recorder.Code, recorder.Body.String(), decodeErr)
			}
			partial := []byte(`{"deleted":false`)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", fmt.Sprint(len(partial)+32))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(partial)
			return
		}
		realHandler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	client, err := mqlite.Open(ctx, proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.DeleteQueueWithOptions(ctx, "discard", mqlite.DeleteQueueOpts{Force: true}); !errors.Is(err, mqlite.ErrOutcomeUnknown) {
		t.Fatalf("lost acknowledgement = %v, want ErrOutcomeUnknown", err)
	}
	queues, err := client.ListQueues(ctx)
	if err != nil || len(queues) != 1 || queues[0].Name != "discard" || queues[0].State != "deleting" {
		t.Fatalf("reconciliation inventory = %+v, %v", queues, err)
	}
	embedded.Engine().RunMaintenanceOnce(ctx)
	queues, err = client.ListQueues(ctx)
	if err != nil || len(queues) != 0 {
		t.Fatalf("completed deletion inventory = %+v, %v", queues, err)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("reconciliation issued %d deletion requests, want exactly one", got)
	}
}

func TestRemoteDeleteQueueTransportFailureIsOutcomeUnknown(t *testing.T) {
	transportFailure := errors.New("connection dropped")
	var calls atomic.Int32
	httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, transportFailure
	})}
	client, err := mqlite.Open(context.Background(), "http://broker.invalid", mqlite.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.DeleteQueueWithOptions(context.Background(), "q", mqlite.DeleteQueueOpts{Force: true})
	if !errors.Is(err, mqlite.ErrOutcomeUnknown) || errors.Is(err, transportFailure) || !strings.Contains(err.Error(), transportFailure.Error()) {
		t.Fatalf("delete transport failure = %v, want only outcome unknown in the error chain and the transport cause as text", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("delete made %d transport attempts, want one", got)
	}

	err = client.CreateQueue(context.Background(), "q", mqlite.QueueConfig{})
	if !errors.Is(err, transportFailure) || errors.Is(err, mqlite.ErrOutcomeUnknown) {
		t.Fatalf("non-delete transport failure = %v, want unchanged transport cause", err)
	}
}

func TestRemoteLifecycleDeleteServerErrorsAreOutcomeUnknown(t *testing.T) {
	const untrusted = "upstream secret must not be echoed"
	for _, tc := range []struct {
		name string
		path string
		hint string
	}{
		{"queue", wire.PathDeleteQueue, "ListQueues"},
		{"subscription", wire.PathDeleteSubscription, "ListSubscriptionsWithOptions with IncludeDeleting: true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != tc.path {
					t.Errorf("path = %q, want %q", r.URL.Path, tc.path)
				}
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"code":"internal","message":"` + untrusted + `"}`))
			}))
			defer stub.Close()
			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if tc.path == wire.PathDeleteQueue {
				_, err = client.DeleteQueueWithOptions(context.Background(), "orders", mqlite.DeleteQueueOpts{Force: true})
			} else {
				_, err = client.DeleteSubscriptionWithOptions(context.Background(), "events", "worker", mqlite.DeleteSubscriptionOpts{Force: true})
			}
			if !errors.Is(err, mqlite.ErrOutcomeUnknown) || !strings.Contains(err.Error(), "HTTP 502") ||
				!strings.Contains(err.Error(), tc.hint) || strings.Contains(err.Error(), untrusted) {
				t.Fatalf("delete server error = %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("delete made %d requests, want one", calls.Load())
			}
		})
	}
}

func TestRemoteDeleteQueueDoesNotFollowRedirect(t *testing.T) {
	var sourceCalls atomic.Int32
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceCalls.Add(1)
		http.Redirect(w, r, target.URL+wire.PathDeleteQueue, http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	client, err := mqlite.Open(context.Background(), source.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DeleteQueueWithOptions(context.Background(), "q", mqlite.DeleteQueueOpts{Force: true})
	if !errors.Is(err, mqlite.ErrOutcomeUnknown) {
		t.Fatalf("redirected delete = %v, want outcome unknown", err)
	}
	if got := sourceCalls.Load(); got != 1 {
		t.Fatalf("source received %d deletion requests, want one", got)
	}
	if got := targetCalls.Load(); got != 0 {
		t.Fatalf("redirect target received %d deletion requests, want zero", got)
	}
}

func TestRemoteDeleteSubscriptionTransportSafety(t *testing.T) {
	t.Run("older broker", func(t *testing.T) {
		stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(wire.ErrorBody{Code: "not_found", Message: "no such path: " + r.URL.Path})
		}))
		defer stub.Close()
		client, err := mqlite.Open(context.Background(), stub.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		_, err = client.DeleteSubscriptionWithOptions(context.Background(), "events", "worker")
		if !errors.Is(err, mqlite.ErrUnsupported) || errors.Is(err, mqlite.ErrSubscriptionNotFound) {
			t.Fatalf("older broker = %v, want only ErrUnsupported", err)
		}
	})

	t.Run("transport failure", func(t *testing.T) {
		transportFailure := errors.New("connection dropped")
		var calls atomic.Int32
		httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			if req.URL.Path != wire.PathDeleteSubscription {
				t.Errorf("path = %q", req.URL.Path)
			}
			return nil, transportFailure
		})}
		client, err := mqlite.Open(context.Background(), "http://broker.invalid", mqlite.WithHTTPClient(httpClient))
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.DeleteSubscriptionWithOptions(context.Background(), "events", "worker", mqlite.DeleteSubscriptionOpts{Force: true})
		if !errors.Is(err, mqlite.ErrOutcomeUnknown) || errors.Is(err, transportFailure) ||
			!strings.Contains(err.Error(), transportFailure.Error()) ||
			!strings.Contains(err.Error(), "ListSubscriptionsWithOptions with IncludeDeleting: true") {
			t.Fatalf("delete transport failure = %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("delete made %d transport attempts, want one", got)
		}
	})

	t.Run("redirect", func(t *testing.T) {
		var sourceCalls atomic.Int32
		var targetCalls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			targetCalls.Add(1)
		}))
		defer target.Close()
		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sourceCalls.Add(1)
			http.Redirect(w, r, target.URL+wire.PathDeleteSubscription, http.StatusTemporaryRedirect)
		}))
		defer source.Close()
		client, err := mqlite.Open(context.Background(), source.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		_, err = client.DeleteSubscriptionWithOptions(context.Background(), "events", "worker", mqlite.DeleteSubscriptionOpts{Force: true})
		if !errors.Is(err, mqlite.ErrOutcomeUnknown) ||
			!strings.Contains(err.Error(), "ListSubscriptionsWithOptions with IncludeDeleting: true") {
			t.Fatalf("redirected delete = %v", err)
		}
		if sourceCalls.Load() != 1 || targetCalls.Load() != 0 {
			t.Fatalf("redirect calls source=%d target=%d, want 1/0", sourceCalls.Load(), targetCalls.Load())
		}
	})

	t.Run("invalid acknowledgement", func(t *testing.T) {
		var calls atomic.Int32
		stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if r.URL.Path != wire.PathDeleteSubscription {
				t.Errorf("path = %q", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"deleted":true,"state":"deleting"}`))
		}))
		defer stub.Close()
		client, err := mqlite.Open(context.Background(), stub.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		_, err = client.DeleteSubscriptionWithOptions(context.Background(), "events", "worker", mqlite.DeleteSubscriptionOpts{Force: true})
		if !errors.Is(err, mqlite.ErrOutcomeUnknown) ||
			!strings.Contains(err.Error(), "ListSubscriptionsWithOptions with IncludeDeleting: true") {
			t.Fatalf("invalid acknowledgement = %v", err)
		}
		if calls.Load() != 1 {
			t.Fatalf("invalid acknowledgement made %d calls, want one", calls.Load())
		}
	})
}

func TestDeleteQueueAcknowledgementContract(t *testing.T) {
	type capturedRequest struct {
		path string
		body map[string]any
		err  error
	}
	for _, tc := range []struct {
		name       string
		response   string
		force      bool
		want       mqlite.DeleteResult
		wantErr    error
		useWrapper bool
	}{
		{name: "deleted", response: `{"deleted":true,"state":"deleted"}`, want: mqlite.DeleteResult{Deleted: true, State: "deleted"}},
		{name: "pending", response: `{"deleted":false,"state":"deleting"}`, force: true, want: mqlite.DeleteResult{State: "deleting"}},
		{name: "wrapper pending", response: `{"deleted":false,"state":"deleting"}`, useWrapper: true},
		{name: "missing", response: `{}`, wantErr: mqlite.ErrOutcomeUnknown},
		{name: "mismatched deleted", response: `{"deleted":true,"state":"deleting"}`, wantErr: mqlite.ErrOutcomeUnknown},
		{name: "mismatched pending", response: `{"deleted":false,"state":"deleted"}`, wantErr: mqlite.ErrOutcomeUnknown},
		{name: "unknown state", response: `{"deleted":false,"state":"unknown"}`, wantErr: mqlite.ErrOutcomeUnknown},
		{name: "missing deleted", response: `{"state":"deleting"}`, wantErr: mqlite.ErrOutcomeUnknown},
		{name: "wrong deleted type", response: `{"deleted":"false","state":"deleting"}`, wantErr: mqlite.ErrOutcomeUnknown},
		{name: "trailing object", response: `{"deleted":false,"state":"deleting"}{}`, wantErr: mqlite.ErrOutcomeUnknown},
		{name: "oversized", response: strings.Repeat(" ", wire.MaxDeleteResponseBytes+1), wantErr: mqlite.ErrOutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := make(chan capturedRequest, 1)
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				err := json.NewDecoder(r.Body).Decode(&body)
				request <- capturedRequest{path: r.URL.Path, body: body, err: err}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.response))
			}))
			defer stub.Close()
			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			var result mqlite.DeleteResult
			if tc.useWrapper {
				err = client.DeleteQueue(context.Background(), "q")
			} else {
				result, err = client.DeleteQueueWithOptions(context.Background(), "q", mqlite.DeleteQueueOpts{Force: tc.force})
			}
			if !errors.Is(err, tc.wantErr) || tc.wantErr == nil && result != tc.want {
				t.Fatalf("result = %+v, error = %v; want %+v, %v", result, err, tc.want, tc.wantErr)
			}
			got := <-request
			wantBody := map[string]any{"name": "q"}
			if tc.force {
				wantBody["force"] = true
			}
			if got.err != nil || got.path != wire.PathDeleteQueue || !reflect.DeepEqual(got.body, wantBody) {
				t.Fatalf("delete request = path %q body %v err %v", got.path, got.body, got.err)
			}
		})
	}
}

// ── Semantic acknowledgement integrity ─────────────────────────────────────

func TestRemoteSingleWriteRequiresSemanticAcknowledgement(t *testing.T) {
	const marker = "semantic-ack-marker-must-not-be-reflected"
	for _, operation := range []struct {
		name string
		path string
		call func(context.Context, *mqlite.Client) error
	}{
		{name: "complete", path: wire.PathComplete, call: func(ctx context.Context, client *mqlite.Client) error {
			return client.Message("jobs", 7, "lock-7").Complete(ctx)
		}},
		{name: "abandon", path: wire.PathAbandon, call: func(ctx context.Context, client *mqlite.Client) error {
			return client.Message("jobs", 7, "lock-7").Abandon(ctx)
		}},
		{name: "reject", path: wire.PathReject, call: func(ctx context.Context, client *mqlite.Client) error {
			return client.Message("jobs", 7, "lock-7").Reject(ctx)
		}},
		{name: "defer", path: wire.PathDefer, call: func(ctx context.Context, client *mqlite.Client) error {
			return client.Message("jobs", 7, "lock-7").Defer(ctx)
		}},
		{name: "renew", path: wire.PathRenew, call: func(ctx context.Context, client *mqlite.Client) error {
			return client.Message("jobs", 7, "lock-7").Renew(ctx)
		}},
		{name: "cancel", path: wire.PathCancel, call: func(ctx context.Context, client *mqlite.Client) error {
			return client.Cancel(ctx, "jobs", 7)
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			var calls int32
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				atomic.AddInt32(&calls, 1)
				if request.URL.Path != operation.path {
					t.Errorf("path = %q, want %q", request.URL.Path, operation.path)
				}
				_, _ = io.WriteString(w, `{"future":"`+marker+`"}`)
			}))
			defer stub.Close()
			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			err = operation.call(context.Background(), client)
			if !errors.Is(err, mqlite.ErrOutcomeUnknown) {
				t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("error reflected untrusted successful response: %v", err)
			}
			if got := atomic.LoadInt32(&calls); got != 1 {
				t.Fatalf("calls = %d, want 1", got)
			}
		})
	}
}

func TestRemoteExplicitNegativeSettlementRemainsLockLost(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"ok":false}`)
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	err = client.Message("jobs", 7, "lock-7").Complete(context.Background())
	if !errors.Is(err, mqlite.ErrLockLost) || errors.Is(err, mqlite.ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want only ErrLockLost", err)
	}
}

func TestRemoteCancelRejectsNegativeSuccessfulAcknowledgement(t *testing.T) {
	var calls int32
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = io.WriteString(w, `{"ok":false}`)
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	err = client.Cancel(context.Background(), "jobs", 7)
	if !errors.Is(err, mqlite.ErrOutcomeUnknown) || errors.Is(err, mqlite.ErrNotFound) {
		t.Fatalf("error = %v, want only ErrOutcomeUnknown", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestRemoteBatchSettlementRequiresPerItemAcknowledgement(t *testing.T) {
	const marker = "batch-semantic-ack-marker"
	for _, operation := range []struct {
		name        string
		path        string
		outcomeLost bool
		call        func(context.Context, *mqlite.Client, *mqlite.Message) error
	}{
		{name: "complete", path: wire.PathCompleteBatch, outcomeLost: true, call: func(ctx context.Context, client *mqlite.Client, message *mqlite.Message) error {
			_, err := client.CompleteBatch(ctx, "jobs", message)
			return err
		}},
		{name: "renew", path: wire.PathRenewBatch, outcomeLost: true, call: func(ctx context.Context, client *mqlite.Client, message *mqlite.Message) error {
			_, err := client.RenewBatch(ctx, "jobs", message)
			return err
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			var calls int32
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				atomic.AddInt32(&calls, 1)
				if request.URL.Path != operation.path {
					t.Errorf("path = %q, want %q", request.URL.Path, operation.path)
				}
				_, _ = io.WriteString(w, `{"results":[{"seq_number":7,"future":"`+marker+`"}]}`)
			}))
			defer stub.Close()
			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			err = operation.call(context.Background(), client, client.Message("jobs", 7, "lock-7"))
			if err == nil {
				t.Fatal("batch response without ok was accepted")
			}
			if operation.outcomeLost && !errors.Is(err, mqlite.ErrOutcomeUnknown) {
				t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
			}
			if errors.Is(err, mqlite.ErrLockLost) {
				t.Fatalf("missing ok was misclassified as ErrLockLost: %v", err)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("error reflected untrusted successful response: %v", err)
			}
			if got := atomic.LoadInt32(&calls); got != 1 {
				t.Fatalf("calls = %d, want 1", got)
			}
		})
	}
}

func TestRemoteReceiveRequiresExplicitMessageBatch(t *testing.T) {
	for _, testCase := range []struct {
		name string
		body string
		ok   bool
	}{
		{name: "missing", body: `{"future":true}`},
		{name: "null", body: `{"messages":null}`},
		{name: "empty", body: `{"messages":[]}`, ok: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, testCase.body)
			}))
			defer stub.Close()
			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			messages, err := client.Receive(context.Background(), "jobs")
			if testCase.ok {
				if err != nil || len(messages) != 0 {
					t.Fatalf("Receive = %+v, %v; want empty success", messages, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Receive accepted %s", testCase.body)
			}
		})
	}
}

func TestRemoteAtMostOnceReceiveTreatsInvalidSuccessAsOutcomeUnknown(t *testing.T) {
	const marker = "at-most-once-response-marker-must-not-be-reflected"
	for _, testCase := range []struct {
		name string
		body string
	}{
		{name: "missing messages", body: `{"future":"` + marker + `"}`},
		{name: "null messages", body: `{"messages":null,"future":"` + marker + `"}`},
		{name: "truncated messages", body: `{"messages":[{"subject":"` + marker + `"}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var calls int32
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				atomic.AddInt32(&calls, 1)
				if request.URL.Path != wire.PathReceive {
					t.Errorf("path = %q, want %q", request.URL.Path, wire.PathReceive)
				}
				_, _ = io.WriteString(w, testCase.body)
			}))
			defer stub.Close()
			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			messages, err := client.Receive(context.Background(), "jobs", mqlite.RecvOpts{AtMostOnce: true})
			if messages != nil || !errors.Is(err, mqlite.ErrOutcomeUnknown) {
				t.Fatalf("Receive = %+v, %v; want nil and ErrOutcomeUnknown", messages, err)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("error reflected untrusted successful response: %v", err)
			}
			if got := atomic.LoadInt32(&calls); got != 1 {
				t.Fatalf("calls = %d, want 1", got)
			}
		})
	}
}

func TestRemotePeekLockReceiveKeepsInvalidSuccessRetryable(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Receive(context.Background(), "jobs", mqlite.RecvOpts{Attempt: "same-attempt"})
	if err == nil || errors.Is(err, mqlite.ErrOutcomeUnknown) {
		t.Fatalf("PeekLock Receive error = %v, want retryable invalid-success classification", err)
	}
}

func TestReceiverReplaysExactSettlementAfterInvalidSemanticAcknowledgement(t *testing.T) {
	const marker = "managed-settlement-marker"
	var receiveCalls int32
	var completeCalls int32
	var requestMu sync.Mutex
	var completeBodies []string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case wire.PathReceive:
			if atomic.AddInt32(&receiveCalls, 1) == 1 {
				_, _ = fmt.Fprintf(w, `{"messages":[{"seq_number":7,"lock_token":"lock-7","locked_until_ms":%d}]}`, time.Now().Add(time.Minute).UnixMilli())
				return
			}
			<-request.Context().Done()
		case wire.PathComplete:
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read Complete request: %v", err)
				return
			}
			requestMu.Lock()
			completeBodies = append(completeBodies, string(body))
			requestMu.Unlock()
			if atomic.AddInt32(&completeCalls, 1) == 1 {
				_, _ = io.WriteString(w, `{"future":"`+marker+`"}`)
				return
			}
			_, _ = io.WriteString(w, `{"ok":true}`)
		default:
			http.Error(w, "unexpected route", http.StatusNotFound)
		}
	}))
	defer stub.Close()
	client, err := mqlite.Open(context.Background(), stub.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	runCtx, cancel := context.WithCancel(context.Background())
	var reportedMu sync.Mutex
	var reported []error
	err = client.Receiver("jobs", mqlite.WithConcurrency(1), mqlite.WithPrefetch(1), mqlite.WithErrorHandler(func(err error) {
		reportedMu.Lock()
		reported = append(reported, err)
		reportedMu.Unlock()
	})).Run(runCtx, func(context.Context, *mqlite.Message) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt32(&completeCalls); got != 2 {
		t.Fatalf("Complete calls = %d, want exact replay", got)
	}
	requestMu.Lock()
	bodies := append([]string(nil), completeBodies...)
	requestMu.Unlock()
	if len(bodies) != 2 || bodies[0] != bodies[1] {
		t.Fatalf("Complete replay changed request: %q", bodies)
	}
	reportedMu.Lock()
	errorsSeen := append([]error(nil), reported...)
	reportedMu.Unlock()
	if len(errorsSeen) != 0 {
		t.Fatalf("recovered invalid acknowledgement was reported: %v", errorsSeen)
	}
}

func TestRemoteSendInvalidSuccessIsOutcomeUnknown(t *testing.T) {
	const marker = "untrusted-ack-marker"
	tests := []struct {
		name          string
		body          string
		contentLength int64
		call          func(*mqlite.Client) error
	}{
		{
			name: "SendOne truncated JSON",
			body: `{"seq_numbers":["` + marker,
			call: func(client *mqlite.Client) error {
				_, err := client.SendOne(context.Background(), "jobs", mqlite.OutMessage{Body: []byte("one")})
				return err
			},
		},
		{
			name: "Send batch truncated JSON",
			body: `{"seq_numbers":["` + marker,
			call: func(client *mqlite.Client) error {
				_, err := client.Send(context.Background(), "jobs",
					mqlite.OutMessage{Body: []byte("one")},
					mqlite.OutMessage{Body: []byte("two")})
				return err
			},
		},
		{
			name:          "SendOne oversized acknowledgement",
			body:          marker,
			contentLength: 1 << 30,
			call: func(client *mqlite.Client) error {
				_, err := client.SendOne(context.Background(), "jobs", mqlite.OutMessage{Body: []byte("one")})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if test.contentLength != 0 {
					w.Header().Set("Content-Length", fmt.Sprint(test.contentLength))
				}
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, test.body)
			}))
			defer stub.Close()

			client, err := mqlite.Open(context.Background(), stub.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			err = test.call(client)
			if !errors.Is(err, mqlite.ErrOutcomeUnknown) {
				t.Fatalf("Send error = %v, want ErrOutcomeUnknown", err)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("Send error reflected untrusted acknowledgement body: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("Send made %d requests, want one", calls.Load())
			}
		})
	}
}
