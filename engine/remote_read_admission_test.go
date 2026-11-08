package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRemoteReadSnapshotsRemainAvailableDuringWrite(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	e := f.openEngine(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.CreateQueue(ctx, "q", QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := e.Subscribe(ctx, "topic", "sub", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("committed")}); err != nil {
		t.Fatal(err)
	}
	inserted, unblock := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(unblock) }) }
	defer release()
	f.afterMessageInsert(func() { once.Do(func() { close(inserted); <-unblock }) })
	done := make(chan error, 1)
	go func() {
		_, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("uncommitted")})
		done <- err
	}()
	select {
	case <-inserted:
	case <-ctx.Done():
		t.Fatal("write did not start")
	}
	for _, name := range []string{"peek", "subscriptions"} {
		t.Run(name, func(t *testing.T) {
			readCtx, stop := context.WithTimeout(ctx, 300*time.Millisecond)
			defer stop()
			if name == "peek" {
				rows, err := e.Peek(readCtx, "q", PeekOptions{})
				if err != nil || len(rows) != 1 || string(rows[0].Body) != "committed" {
					t.Fatalf("read snapshot while writer holds transaction: rows=%d err=%v", len(rows), err)
				}
			} else {
				page, err := e.ListSubscriptionsWithOptions(readCtx, ListSubscriptionsOptions{})
				if err != nil || page.Total != 1 || len(page.Subscriptions) != 1 || page.Subscriptions[0].Name != "sub" {
					t.Fatalf("subscription snapshot while writer holds transaction: page=%+v err=%v", page, err)
				}
			}
		})
	}
	// Reads must not observe the uncommitted row or release the writer's permit.
	select {
	case err := <-done:
		t.Fatalf("write returned before fixture released it: %v", err)
	default:
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
