package remotesdk_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	broker "github.com/mqlitehq/mqlite"
	sdk "github.com/mqlitehq/mqlite-go"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

func TestTursoHTTPBrokerDelivery(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB for the dedicated remote SDK pipeline test")
	}
	if dsn != "libsql://mqlite-verify-jiba.aws-ap-northeast-1.turso.io" {
		t.Fatal("remote SDK pipeline refused: not the dedicated verification database")
	}
	token := os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")
	if token == "" {
		t.Fatal("remote SDK pipeline requires the dedicated database token")
	}
	exerciseHTTPBrokerDelivery(t, "turso", dsn, token)
}

func TestSQLiteHTTPBrokerDelivery(t *testing.T) {
	exerciseHTTPBrokerDelivery(t, "sqlite", ":memory:", "")
}

// This deliberately follows the main SDK's HTTP-pipeline ledger, while every
// client operation and managed handler uses the independent remote-only SDK.
func exerciseHTTPBrokerDelivery(t *testing.T, backend, dsn, databaseToken string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	fail := func(stage string, err error) {
		t.Helper()
		if err == nil {
			return
		}
		// Never print provider errors: their text may contain secrets or URLs.
		status := 0
		var responseError *sdk.HTTPError
		if errors.As(err, &responseError) && responseError != nil {
			status = responseError.StatusCode
		}
		t.Fatalf("remote SDK pipeline stage=%s failed type=%T canceled=%t deadline=%t outcome_unknown=%t lock_lost=%t permission_denied=%t not_found=%t http_status=%d", stage, err,
			errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, sdk.ErrOutcomeUnknown),
			errors.Is(err, sdk.ErrLockLost), errors.Is(err, sdk.ErrPermissionDenied), errors.Is(err, sdk.ErrNotFound), status)
	}
	eng, err := broker.OpenEmbedded(ctx, dsn, broker.WithDBAuthToken(databaseToken), broker.WithBrokerMode(),
		broker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	fail("broker open", err)
	defer func() {
		if err := eng.Close(); err != nil {
			t.Error("remote SDK pipeline engine close failed")
		}
	}()
	const total = 24
	const brokerToken = "remote-sdk-pipeline-local-test-token"
	queue := fmt.Sprintf("remote-sdk-pipeline-%d-%d", os.Getpid(), time.Now().UnixNano())
	var mu sync.Mutex
	completed := make(map[int64]bool, total)
	var renewals, notifications int
	allCompleted := make(chan struct{})
	base := server.New(eng.Engine(), []string{brokerToken}).Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Queue string `json:"queue"`
			Seq   int64  `json:"seq_number"`
		}
		if r.URL.Path == wire.PathComplete {
			data, readErr := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if readErr != nil || json.Unmarshal(data, &request) != nil {
				http.Error(w, "invalid test completion request", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
		}
		recorder := httptest.NewRecorder()
		base.ServeHTTP(recorder, r)
		for name, values := range recorder.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(recorder.Code)
		written, writeErr := w.Write(recorder.Body.Bytes())
		if recorder.Code != http.StatusOK || writeErr != nil || written != recorder.Body.Len() {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case wire.PathComplete:
			var response wire.SettleResponse
			if json.Unmarshal(recorder.Body.Bytes(), &response) == nil && response.Ok && request.Queue == queue && !completed[request.Seq] {
				completed[request.Seq] = true
				if len(completed) == total {
					close(allCompleted)
				}
			}
		case wire.PathRenewBatch:
			var response wire.RenewBatchResponse
			if json.Unmarshal(recorder.Body.Bytes(), &response) == nil {
				for _, result := range response.Results {
					if result.Ok && result.LockedUntilMs > 0 {
						renewals++
					}
				}
			}
		}
	}))
	defer ts.Close()
	client, err := sdk.Open(ctx, ts.URL, sdk.WithToken(brokerToken))
	fail("client open", err)
	defer client.Close()
	fail("create own queue", client.CreateQueue(ctx, queue, sdk.QueueConfig{LockDuration: 30 * time.Second, MaxDeliveryCount: 1}))
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if _, err := client.DeleteQueueWithOptions(cleanup, queue, sdk.DeleteQueueOpts{Force: true}); err != nil {
			t.Error("remote SDK pipeline own-queue cleanup failed")
		}
	}()
	type expected struct {
		seq  int64
		body []byte
	}
	sent := make(map[string]expected, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("binary-%02d", i)
		body := []byte{byte(i), 0, 255, 128, byte(i ^ 127)}
		seq, err := client.SendOne(ctx, queue, sdk.OutMessage{MessageID: id, Body: body})
		fail("send", err)
		if seq <= 0 {
			t.Fatal("remote SDK pipeline Send returned an invalid sequence")
		}
		sent[id] = expected{seq: seq, body: body}
	}
	runCtx, stopReceiver := context.WithCancel(ctx)
	seen := make(map[string]int, total)
	invalidDelivery := false
	runDone := make(chan error, 1)
	go func() {
		runDone <- client.Receiver(queue, sdk.WithConcurrency(4), sdk.WithAutoRenew(),
			sdk.WithErrorHandler(func(error) { mu.Lock(); notifications++; mu.Unlock() })).
			Run(runCtx, func(handlerCtx context.Context, message *sdk.Message) error {
				want, exists := sent[message.MessageID]
				mu.Lock()
				seen[message.MessageID]++
				valid := exists && message.SequenceNumber == want.seq && bytes.Equal(message.Body, want.body) &&
					message.DeliveryCount == 1 && seen[message.MessageID] == 1
				invalidDelivery = invalidDelivery || !valid
				mu.Unlock()
				if !valid {
					return errors.New("remote SDK pipeline delivery validation failed")
				}
				if message.MessageID == "binary-00" {
					// Observe a real renewal at the 30s lease's first 10s point.
					timer := time.NewTimer(11 * time.Second)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-handlerCtx.Done():
						return handlerCtx.Err()
					}
				}
				return nil
			})
	}()
	joined := false
	defer func() {
		stopReceiver()
		if !joined {
			select {
			case <-runDone:
			case <-time.After(10 * time.Second):
				t.Error("remote SDK pipeline Receiver did not stop within cleanup budget")
			}
		}
	}()
	select {
	case <-allCompleted:
	case err := <-runDone:
		joined = true
		fail("Receiver stopped before all completions", err)
		t.Fatal("remote SDK pipeline Receiver ended before all completions")
	case <-ctx.Done():
		t.Fatal("remote SDK pipeline deadline reached before all completions")
	}
	stopReceiver()
	select {
	case err = <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("remote SDK pipeline Receiver exceeded its shutdown budget")
	}
	joined = true
	if !errors.Is(err, context.Canceled) {
		fail("Receiver terminal result", err)
		t.Fatal("remote SDK pipeline Receiver did not report cancellation")
	}
	stats, err := client.Stats(ctx, queue)
	fail("final stats", err)
	mu.Lock()
	defer mu.Unlock()
	if invalidDelivery || len(seen) != total || len(completed) != total || renewals == 0 ||
		stats.Total != 0 || stats.Active != 0 || stats.Locked != 0 || stats.DeadLettered != 0 {
		t.Fatalf("remote SDK pipeline accounting failed invalid=%t handled=%d complete=%d renewals=%d total=%d locked=%d dlq=%d",
			invalidDelivery, len(seen), len(completed), renewals, stats.Total, stats.Locked, stats.DeadLettered)
	}
	for id, want := range sent {
		if seen[id] != 1 || !completed[want.seq] {
			t.Fatal("remote SDK pipeline per-message handler/Complete accounting failed")
		}
	}
	t.Logf("remote SDK pipeline backend=%s sent=%d handled=%d confirmed_complete=%d confirmed_renewals=%d notifications=%d final_total=%d dlq=%d",
		backend, len(sent), len(seen), len(completed), renewals, notifications, stats.Total, stats.DeadLettered)
}
