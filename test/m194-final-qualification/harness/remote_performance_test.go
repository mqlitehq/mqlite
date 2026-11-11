package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	native "github.com/mqlitehq/mqlite"
)

type remoteResourceSample struct {
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
	HeapAlloc           uint64  `json:"heap_alloc_bytes"`
	Goroutines          int     `json:"goroutines"`
	ConfirmedIdentities int     `json:"confirmed_identities"`
}
type remoteQualificationReport struct {
	qualificationReport
	Samples []remoteResourceSample    `json:"resource_samples"`
	Calls   map[string]map[string]int `json:"rpc_calls"`
}

// This finite workload uses the existing authenticated Broker owner and clients.
// Its separate local fixture exercises the same accounting without remote keys.
func TestRemoteReceiveQualification(t *testing.T) {
	stage := os.Getenv("M153_REMOTE_STAGE")
	if stage != "profiles" && stage != "sustained" && stage != "controls" {
		t.Skip("explicit finite remote stage required")
	}
	output := os.Getenv("M153_REMOTE_OUT")
	if output == "" {
		t.Fatal("result path required")
	}
	fixture := stage == "controls"
	profile := os.Getenv("M153_REMOTE_PROFILE")
	if profile == "" && fixture {
		profile = "plain"
	}
	if profile != "plain" && profile != "escaped" {
		t.Fatal("explicit metadata profile required")
	}
	dsn, token := os.Getenv("MQLITE_TEST_DB"), os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")
	if !fixture && !validRemoteDestination(dsn, token, os.Getenv("MQLITE_TEST_DB_RESET_CONFIRM")) {
		t.Fatal("dedicated verification configuration rejected")
	}
	cfg := configuration{output: filepath.Dir(output), localFixture: fixture}
	if fixture {
		dsn = "file:" + filepath.Join(t.TempDir(), "local.db")
	}
	cfg.open = func(ctx context.Context) (*native.Embedded, error) {
		return native.OpenEmbedded(ctx, dsn, native.WithDBAuthToken(token), native.WithBrokerMode(), native.WithoutBackground(), native.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("random resource identity unavailable")
	}
	prefix := "m153-" + hex.EncodeToString(nonce[:])
	l := &ledger{output: filepath.Dir(output), expected: map[string]entry{}, report: report{Scope: "finite remote Receive client-path qualification", Prefix: prefix}}
	trace := &rpcRecorder{allowed: map[string]bool{}}
	result := remoteQualificationReport{qualificationReport: qualificationReport{Stage: stage}}
	if err := os.MkdirAll(filepath.Dir(output), 0700); err != nil {
		t.Fatal("evidence destination unavailable")
	}
	defer func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		if err != nil || os.WriteFile(output, append(raw, '\n'), 0600) != nil {
			t.Error("result publication failed")
		}
	}()
	o, err := openOwner(ctx, cfg, trace)
	if err != nil {
		l.fail("open", err)
		_ = l.save()
		t.Fatal("owner open failed")
	}
	defer func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), cleanupBudget)
		defer cleanupCancel()
		if err := cleanupOwned(cleanup, l, o); err != nil {
			t.Error("exact owned-resource cleanup failed")
			result.Passed = false
		}
		if err := o.stop(cleanup); err != nil {
			l.fail("close", err)
			t.Error("owner close did not join")
			result.Passed = false
		} else {
			result.CloseJoined = true
			for i := range result.Rows {
				result.Rows[i].Deleted = true
			}
		}
		trace.mu.Lock()
		l.report.Trace = append([]rpcEvent(nil), trace.events...)
		result.Calls = trace.calls
		trace.mu.Unlock()
		l.report.Pass = result.Passed && !t.Failed()
		if err := l.save(); err != nil {
			t.Error("final cleanup evidence failed")
		}
	}()
	operation := func(stage string, fn func(context.Context) error) {
		t.Helper()
		if err := call(ctx, fn); err != nil {
			l.fail(stage, err)
			_ = l.save()
			t.Fatal("bounded workload operation failed")
		}
	}
	queues := map[string]string{}
	for _, kind := range []string{"native", "sdk"} {
		for _, batch := range []int{1, 8} {
			name := fmt.Sprintf("%s-%s-%d", prefix, kind, batch)
			key := fmt.Sprintf("%s-%d", kind, batch)
			queues[key] = name
			trace.allowed[name] = true
			operation("preflight", func(c context.Context) error {
				_, err := o.embedded.Stats(c, name)
				if !errors.Is(err, native.ErrQueueNotFound) {
					return errors.New("random owned name is already present or unobservable")
				}
				return nil
			})
			l.report.Resources = append(l.report.Resources, resource{Name: name, Eligible: true})
			if err := l.save(); err != nil {
				t.Fatal("resource manifest could not be saved before creation")
			}
			operation("create", func(c context.Context) error {
				return o.embedded.CreateQueue(c, name, native.QueueConfig{LockDuration: 60 * time.Second, MaxDeliveryCount: 1})
			})
			l.report.Resources[len(l.report.Resources)-1].Created = true
		}
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutines := runtime.NumGoroutine()
	confirmed := 0
	workStarted := time.Now()
	lastSample := time.Time{}
	sample := func(force bool) {
		now := time.Now()
		if !force && !lastSample.IsZero() && now.Sub(lastSample) < 10*time.Second {
			return
		}
		if !lastSample.IsZero() && now.Sub(lastSample) > 30*time.Second {
			t.Fatal("resource/progress sample gap exceeded 30 seconds")
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		result.Samples = append(result.Samples, remoteResourceSample{ElapsedSeconds: now.Sub(workStarted).Seconds(), HeapAlloc: memory.HeapAlloc, Goroutines: runtime.NumGoroutine(), ConfirmedIdentities: confirmed})
		lastSample = now
		raw, err := json.MarshalIndent(result.Samples, "", "  ")
		if err != nil || os.WriteFile(filepath.Join(filepath.Dir(output), "resource-samples.json"), append(raw, '\n'), 0600) != nil {
			t.Fatal("resource sample publication failed")
		}
	}
	sample(true)
	cycle := func(kind string, batch, index int, digest hash.Hash, profile string) float64 {
		t.Helper()
		queue := queues[fmt.Sprintf("%s-%d", kind, batch)]
		items := make([]entry, batch)
		for i := range items {
			id := fmt.Sprintf("%s-%s-%d-%d", queue, profile, index, i)
			body := append([]byte(id+":"), bytes.Repeat([]byte("p"), 256)...)
			items[i] = entry{ID: id, Queue: queue, Body: body[:256], Metadata: metadataFor(profile, id)}
		}
		l.mu.Lock()
		for _, item := range items {
			l.expected[item.ID] = item
			l.report.Identities = append(l.report.Identities, identity{ID: item.ID, Queue: item.Queue, BodySHA256: hashBytes(item.Body), MetadataSHA256: metadataDigest(item.Metadata), MetadataProfile: profile})
		}
		l.mu.Unlock()
		if err := l.save(); err != nil {
			t.Fatal("identity inventory publication failed")
		}
		trace.mu.Lock()
		beforeSend, beforeReceive, beforeComplete := trace.calls[queue]["Send"], trace.calls[queue]["Receive"], trace.calls[queue]["CompleteBatch"]
		trace.mu.Unlock()
		var sequences []int64
		operation("send", func(c context.Context) error {
			var err error
			sequences, err = o.clients[kind].send(c, queue, items)
			return err
		})
		if err := l.acknowledge(items, sequences); err != nil {
			l.fail("send-ledger", err)
			t.Fatal("send acknowledgement ledger failed")
		}
		var messages []delivery
		start := time.Now()
		operation("receive", func(c context.Context) error {
			var err error
			messages, err = o.clients[kind].receive(c, queue, batch)
			return err
		})
		latency := float64(time.Since(start).Nanoseconds()) / 1e6
		if err := verifyQualificationBatch(items, sequences, messages, digest); err != nil {
			l.fail("identity-ledger", err)
			t.Fatal("identity, sequence, body or delivery ledger mismatch")
		}
		for _, message := range messages {
			if err := l.delivered(message); err != nil {
				l.fail("receive-ledger", err)
				t.Fatal("receive identity ledger failed")
			}
		}
		operation("complete", func(c context.Context) error { return o.clients[kind].settle(c, queue, messages, false) })
		for _, message := range messages {
			if trace.count(queue, "CompleteBatch", message.Sequence) != 1 {
				t.Fatal("successful Complete acknowledgement missing or repeated")
			}
		}
		if err := l.completed(messages); err != nil {
			l.fail("complete-ledger", err)
			t.Fatal("completion identity ledger failed")
		}
		trace.mu.Lock()
		exactCalls := trace.calls[queue]["Send"] == beforeSend+1 && trace.calls[queue]["Receive"] == beforeReceive+1 && trace.calls[queue]["CompleteBatch"] == beforeComplete+1
		trace.mu.Unlock()
		if !exactCalls {
			t.Fatal("Send, Receive or Complete was replayed or omitted")
		}
		confirmed += len(messages)
		if err := l.save(); err != nil {
			t.Fatal("completed identity publication failed")
		}
		sample(false)
		return latency
	}
	if stage == "profiles" || fixture {
		for _, kind := range []string{"native", "sdk"} {
			for _, batch := range []int{1, 8} {
				digest := sha256.New()
				cycle(kind, batch, 0, digest, profile)
				var allocationBefore, allocationAfter runtime.MemStats
				runtime.ReadMemStats(&allocationBefore)
				start := time.Now()
				latencies := []float64{cycle(kind, batch, 1, digest, profile), cycle(kind, batch, 2, digest, profile)}
				sort.Float64s(latencies)
				runtime.ReadMemStats(&allocationAfter)
				result.Rows = append(result.Rows, qualificationRow{AllocatedBytes: allocationAfter.TotalAlloc - allocationBefore.TotalAlloc, Allocations: allocationAfter.Mallocs - allocationBefore.Mallocs, Profile: profile, Client: kind, Batch: batch, Cycles: 3, Count: batch * 3, WallMS: float64(time.Since(start).Nanoseconds()) / 1e6, ReceiveP95MS: latencies[len(latencies)-1], IDsSHA: hex.EncodeToString(digest.Sum(nil))})
			}
		}
	} else {
		digests := map[string]hash.Hash{}
		rows := map[string]*qualificationRow{}
		for _, kind := range []string{"native", "sdk"} {
			for _, metadataProfile := range []string{"plain", "escaped"} {
				key := kind + "-" + metadataProfile
				digests[key] = sha256.New()
				rows[key] = &qualificationRow{Profile: metadataProfile, Client: kind, Batch: 8}
			}
		}
		start := time.Now()
		for index := 0; time.Since(start) < 180*time.Second; index++ {
			cycleStart := time.Now()
			kind := []string{"native", "sdk"}[index%2]
			metadataProfile := []string{"plain", "escaped"}[(index/2)%2]
			key := kind + "-" + metadataProfile
			cycle(kind, 8, index, digests[key], metadataProfile)
			rows[key].Cycles++
			rows[key].Count += 8
			if delay := 200*time.Millisecond - time.Since(cycleStart); delay > 0 {
				time.Sleep(delay)
			}
		}
		result.ElapsedSeconds = time.Since(start).Seconds()
		for _, key := range []string{"native-plain", "native-escaped", "sdk-plain", "sdk-escaped"} {
			rows[key].IDsSHA = hex.EncodeToString(digests[key].Sum(nil))
			result.Rows = append(result.Rows, *rows[key])
		}
	}
	sample(true)
	for _, queue := range queues {
		operation("final-drain", func(c context.Context) error { return zeroQueue(c, o.embedded, queue) })
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	result.HeapGrowth = int64(after.HeapAlloc) - int64(before.HeapAlloc)
	result.GoroutineGrowth = runtime.NumGoroutine() - goroutines
	if result.HeapGrowth > 16<<20 || result.GoroutineGrowth > 16 {
		t.Fatal("resource growth exceeded the fixed budget")
	}
	for i := range result.Rows {
		result.Rows[i].Empty = true
	}
	result.Passed = true
}
