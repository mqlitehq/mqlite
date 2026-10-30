package receivegate

import (
	"context"
	"sync"
	"testing"
)

func TestGateStopAndClaimAdmissionAreAtomic(t *testing.T) {
	gate := New()
	if !gate.TryEnterClaim() {
		t.Fatal("first claim was not admitted")
	}
	select {
	case <-gate.FirstClaimEntered():
	default:
		t.Fatal("admitted claim did not publish its boundary")
	}

	gate.Stop()
	gate.Stop() // idempotent
	if gate.TryEnterClaim() {
		t.Fatal("claim entered after Stop")
	}
	select {
	case <-gate.Done():
	default:
		t.Fatal("Stop did not close Done")
	}
}

func TestGateConcurrentStopNeverAdmitsAfterward(t *testing.T) {
	for i := 0; i < 100; i++ {
		gate := New()
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			gate.Stop()
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = gate.TryEnterClaim() // either ordering is valid at this boundary
		}()
		close(start)
		wg.Wait()
		if gate.TryEnterClaim() {
			t.Fatal("claim entered after concurrent Stop completed")
		}
	}
}

func TestContextRoundTrip(t *testing.T) {
	gate := New()
	ctx := WithContext(context.Background(), gate)
	if got := FromContext(ctx); got != gate {
		t.Fatalf("gate = %p, want %p", got, gate)
	}
	if got := FromContext(context.Background()); got != nil {
		t.Fatalf("unexpected gate %p", got)
	}
}
