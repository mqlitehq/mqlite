package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	mq "github.com/mqlitehq/mqlite"
)

func loadOwnership(out, backend string, confirmed bool) (ownershipRecord, error) {
	var m ownershipRecord
	if !confirmed {
		return m, failure("prior_owner_exit_confirmation_required")
	}
	p := filepath.Join(out, "ownership.json")
	st, err := os.Stat(p)
	if err != nil || st.Size() > 64*1024 {
		return m, failure("ownership_manifest_unavailable")
	}
	data, err := os.ReadFile(p)
	if err != nil || json.Unmarshal(data, &m) != nil {
		return m, failure("ownership_manifest_invalid")
	}
	if m.Schema != "m162-owned-queues-v1" || m.Backend != backend || m.RuntimeSHA != runtimeSHA || m.SDK_SHA != sdkSHA || m.RunnerSHA != runnerSHA || m.PID <= 0 || len(m.Queues) > 2 {
		return m, failure("ownership_manifest_mismatch")
	}
	if m.HostHash == localHostHash() {
		p, _ := os.FindProcess(m.PID)
		err := p.Signal(syscall.Signal(0))
		if err == nil || !errors.Is(err, syscall.ESRCH) && !errors.Is(err, os.ErrProcessDone) {
			return m, failure("prior_owner_still_present")
		}
	}
	seen := map[string]bool{}
	for _, q := range m.Queues {
		suffix := strings.TrimPrefix(q.Queue, "m162-budget-")
		_, err := hex.DecodeString(suffix)
		if !strings.HasPrefix(q.Queue, "m162-budget-") || len(suffix) != 24 || err != nil || seen[q.Queue] || !inList(q.SDK, "native", "lowfloor") {
			return m, failure("ownership_scope_invalid")
		}
		seen[q.Queue] = true
	}
	return m, nil
}
func runCleanup(out, backend, dsn, token string, confirmed bool) int {
	m, err := loadOwnership(out, backend, confirmed)
	if err != nil {
		fmt.Println("supplemental cleanup refused:", safeClass(err))
		return 2
	}
	r := struct {
		Schema         string   `json:"schema"`
		Passed         bool     `json:"passed"`
		Failure        string   `json:"failure,omitempty"`
		Absent         []string `json:"absent"`
		OwnerJoined    bool     `json:"owner_joined"`
		ProviderJoined bool     `json:"provider_joined"`
	}{Schema: "m162-supplemental-cleanup-v1", Absent: []string{}}
	if err := writeJSON(out, "cleanup-result.json", r); err != nil {
		fmt.Println("supplemental cleanup evidence unavailable")
		return 2
	}
	trace, err := newProviderObserver(dsn)
	if err != nil {
		fmt.Println("supplemental cleanup setup failed")
		return 2
	}
	restore := trace.install()
	defer restore()
	ctx, cancel := context.WithTimeout(context.Background(), cleanupBudget)
	defer cancel()
	o, err := openOwner(ctx, caseConfig{dsn: dsn, token: token, trace: trace, events: &safeLog{}})
	if err == nil {
		for _, q := range m.Queues {
			absent := false
			for ctx.Err() == nil {
				_, err = o.e.DeleteQueueWithOptions(ctx, q.Queue, mq.DeleteQueueOpts{Force: true})
				if err != nil && !errors.Is(err, mq.ErrQueueNotFound) {
					break
				}
				var qs []mq.QueueInfo
				qs, err = o.e.ListQueues(ctx)
				if err != nil {
					break
				}
				present := false
				for _, actual := range qs {
					present = present || actual.Name == q.Queue
				}
				if !present {
					absent = true
					break
				}
				select {
				case <-time.After(50 * time.Millisecond):
				case <-ctx.Done():
				}
			}
			if !absent {
				if err == nil {
					err = failure("owned_queue_absence_unconfirmed")
				}
				break
			}
			r.Absent = append(r.Absent, q.Queue)
		}
	}
	if err != nil {
		r.Failure = safeClass(err)
	}
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer joinCancel()
	if o != nil {
		closeErr := o.close(joinCtx)
		r.OwnerJoined = o.closed
		if closeErr != nil && r.Failure == "" {
			r.Failure = safeClass(closeErr)
		}
	} else {
		r.OwnerJoined = true
	}
	r.ProviderJoined = trace.join(joinCtx.Done())
	r.Passed = r.Failure == "" && r.OwnerJoined && r.ProviderJoined && len(r.Absent) == len(m.Queues)
	if writeJSON(out, "cleanup-result.json", r) != nil {
		fmt.Println("cannot persist supplemental cleanup result")
		return 2
	}
	fmt.Printf("supplemental owned cleanup passed=%t\n", r.Passed)
	if !r.Passed {
		return 1
	}
	return 0
}
