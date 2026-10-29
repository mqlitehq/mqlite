package mqlite

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/wire"
)

func TestShutdownCancellationTargetsReceiveOnly(t *testing.T) {
	shutdownCtx, beginShutdown := context.WithCancel(context.Background())
	defer beginShutdown()
	receiveStarted := make(chan struct{}, 2)
	receiveDone := make(chan error, 2)
	sendStarted := make(chan struct{}, 1)
	sendDone := make(chan error, 1)
	releaseSend := make(chan struct{})
	var releaseSendOnce sync.Once
	release := func() { releaseSendOnce.Do(func() { close(releaseSend) }) }
	defer release()

	h := cancelReceivesOnShutdown(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wire.PathReceive:
			receiveStarted <- struct{}{}
			<-r.Context().Done()
			receiveDone <- r.Context().Err()
		case wire.PathSend:
			sendStarted <- struct{}{}
			select {
			case <-r.Context().Done():
				sendDone <- r.Context().Err()
			case <-releaseSend:
				sendDone <- nil
			}
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}), shutdownCtx)

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, wire.PathReceive, nil))
	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, wire.PathSend, nil))
	for name, started := range map[string]<-chan struct{}{"Receive": receiveStarted, "Send": sendStarted} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("%s handler did not start", name)
		}
	}

	beginShutdown()
	select {
	case err := <-receiveDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Receive context error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Receive context was not canceled at shutdown")
	}
	select {
	case err := <-sendDone:
		t.Fatalf("Send handler ended during Receive cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case err := <-sendDone:
		if err != nil {
			t.Fatalf("Send context was canceled during graceful drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send handler did not finish after release")
	}

	// A Receive admitted after the barrier must inherit an already-canceled
	// context instead of beginning a new claim round.
	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, wire.PathReceive, nil))
	select {
	case <-receiveStarted:
	case <-time.After(time.Second):
		t.Fatal("post-shutdown Receive handler did not start")
	}
	select {
	case err := <-receiveDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("post-shutdown Receive context error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("post-shutdown Receive context was not already canceled")
	}
}
