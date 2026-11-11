package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	native "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/server"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type qualificationCounts struct {
	mu      sync.Mutex
	byQueue map[string]map[string]int
}

func (c *qualificationCounts) add(q, op string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byQueue[q] == nil {
		c.byQueue[q] = map[string]int{}
	}
	c.byQueue[q][op]++
}

type qualificationRow struct {
	Profile        string         `json:"profile"`
	Client         string         `json:"client"`
	Batch          int            `json:"batch"`
	Cycles         int            `json:"cycles"`
	Count          int            `json:"message_count"`
	WallMS         float64        `json:"wall_ms"`
	ReceiveP95MS   float64        `json:"receive_p95_ms"`
	AllocatedBytes uint64         `json:"measured_allocated_bytes"`
	Allocations    uint64         `json:"measured_allocations"`
	IDsSHA         string         `json:"identity_sequence_body_sha256"`
	RPCs           map[string]int `json:"rpcs"`
	Empty          bool           `json:"empty"`
	Deleted        bool           `json:"deleted"`
}
type qualificationReport struct {
	Stage           string             `json:"stage"`
	Passed          bool               `json:"passed"`
	Rows            []qualificationRow `json:"rows"`
	ElapsedSeconds  float64            `json:"elapsed_seconds"`
	HeapGrowth      int64              `json:"heap_growth"`
	GoroutineGrowth int                `json:"goroutine_growth"`
	DBBytes         int64              `json:"db_and_wal_bytes"`
	Integrity       bool               `json:"integrity"`
	ForeignKeys     bool               `json:"foreign_keys"`
	CloseJoined     bool               `json:"close_joined"`
}

func verifyQualificationBatch(items []entry, seqs []int64, messages []delivery, digest hash.Hash) error {
	if len(items) != len(seqs) || len(messages) != len(items) {
		return errors.New("batch counts differ")
	}
	wanted := map[string]int{}
	seen := map[string]bool{}
	tokens := map[int64]bool{}
	for i, x := range items {
		if seqs[i] <= 0 || tokens[seqs[i]] {
			return errors.New("invalid acknowledgement sequence")
		}
		tokens[seqs[i]] = true
		wanted[x.ID] = i
	}
	for _, m := range messages {
		i, ok := wanted[m.ID]
		if !ok || seen[m.ID] || m.Sequence != seqs[i] || m.Count != 1 || !bytes.Equal(m.Body, items[i].Body) || !reflect.DeepEqual(m.Metadata, items[i].Metadata) {
			return errors.New("identity/body/sequence/delivery mismatch")
		}
		seen[m.ID] = true
	}
	for i, x := range items {
		fmt.Fprintf(digest, "%s:%d:%x:%s\n", x.ID, seqs[i], sha256.Sum256(x.Body), metadataDigest(x.Metadata))
	}
	return nil
}
func TestReceiveQualificationRejectsLedgerFaults(t *testing.T) {
	for _, fault := range []string{"identity", "body", "sequence", "delivery", "duplicate", "missing", "bad-ack", "metadata"} {
		t.Run(fault, func(t *testing.T) {
			items := []entry{{ID: "a", Body: []byte("a")}, {ID: "b", Body: []byte("b")}}
			seqs := []int64{1, 2}
			ms := []delivery{{ID: "a", Body: []byte("a"), Sequence: 1, Count: 1}, {ID: "b", Body: []byte("b"), Sequence: 2, Count: 1}}
			switch fault {
			case "identity":
				ms[0].ID = "x"
			case "body":
				ms[0].Body = []byte("x")
			case "sequence":
				ms[0].Sequence = 3
			case "delivery":
				ms[0].Count = 2
			case "duplicate":
				ms[1] = ms[0]
			case "missing":
				ms = ms[:1]
			case "bad-ack":
				seqs[0] = -1
			case "metadata":
				ms[0].Metadata.Subject = "damaged"
			}
			if verifyQualificationBatch(items, seqs, ms, sha256.New()) == nil {
				t.Fatal("fault accepted")
			}
		})
	}
	if err := verifyQualificationBatch([]entry{{ID: "a", Body: []byte("a")}}, []int64{1}, []delivery{{ID: "a", Body: []byte("a"), Sequence: 1, Count: 1}}, sha256.New()); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveAcknowledgementQualification(t *testing.T) {
	stage := os.Getenv("M153_QUAL_STAGE")
	if stage != "profiles" && stage != "sustained" {
		t.Fatal("explicit fixed stage required")
	}
	output := os.Getenv("M153_QUAL_OUT")
	if output == "" {
		t.Fatal("result path required")
	}
	started := time.Now()
	result := qualificationReport{Stage: stage}
	defer func() {
		raw, _ := json.MarshalIndent(result, "", "  ")
		if err := os.WriteFile(output, append(raw, '\n'), 0600); err != nil {
			t.Error(err)
		}
	}()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutines := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	dir := t.TempDir()
	dbfile := filepath.Join(dir, "queue.db")
	e, err := native.OpenEmbedded(ctx, "file:"+dbfile, native.WithBrokerMode(), native.WithoutBackground(), native.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, nil))))
	if err != nil {
		t.Fatal(err)
	}
	counts := &qualificationCounts{byQueue: map[string]map[string]int{}}
	handler := server.New(e.Engine(), []string{"local-qualification-token"}).Handler()
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var a struct {
			Queue string `json:"queue"`
		}
		if r.Method == http.MethodPost {
			raw, er := osReadBody(r)
			if er != nil {
				http.Error(w, "request", 400)
				return
			}
			_ = json.Unmarshal(raw, &a)
			counts.add(a.Queue, r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
		}
		handler.ServeHTTP(w, r)
	}))
	trace := &rpcRecorder{allowed: map[string]bool{}}
	o := &owner{embedded: e, broker: hs, clients: map[string]*adapter{}}
	for _, kind := range []string{"native", "sdk"} {
		a, er := newAdapter(ctx, kind, hs.URL, "local-qualification-token", trace)
		if er != nil {
			t.Fatal(er)
		}
		o.clients[kind] = a
	}
	defer func() {
		stop, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		if er := o.stop(stop); er != nil {
			t.Error(er)
		}
	}()
	op := func(fn func(context.Context) error) {
		q, c := context.WithTimeout(ctx, time.Second)
		defer c()
		if er := fn(q); er != nil {
			t.Fatal(er)
		}
	}
	rowCycle := func(kind, q string, batch, index int, digest hash.Hash, profile string) float64 {
		items := make([]entry, batch)
		for i := range items {
			id := fmt.Sprintf("%s-%d-%d", q, index, i)
			body := append([]byte(id+":"), bytes.Repeat([]byte("p"), 256)...)
			items[i] = entry{ID: id, Queue: q, Body: body[:256], Metadata: metadataFor(profile, id)}
		}
		var seqs []int64
		op(func(c context.Context) error { var er error; seqs, er = o.clients[kind].send(c, q, items); return er })
		var ms []delivery
		s := time.Now()
		op(func(c context.Context) error { var er error; ms, er = o.clients[kind].receive(c, q, batch); return er })
		latency := float64(time.Since(s).Nanoseconds()) / 1e6
		if er := verifyQualificationBatch(items, seqs, ms, digest); er != nil {
			t.Fatal(er)
		}
		op(func(c context.Context) error { return o.clients[kind].settle(c, q, ms, false) })
		trace.mu.Lock()
		trace.events = nil
		trace.mu.Unlock()
		return latency
	}
	makeQueue := func(q string) {
		trace.allowed[q] = true
		op(func(c context.Context) error {
			return e.CreateQueue(c, q, native.QueueConfig{LockDuration: 30 * time.Second, MaxDeliveryCount: 1})
		})
	}
	finish := func(row *qualificationRow, q string, digest hash.Hash) {
		op(func(c context.Context) error { return zeroQueue(c, e, q) })
		row.Empty = true
		row.IDsSHA = hex.EncodeToString(digest.Sum(nil))
		counts.mu.Lock()
		row.RPCs = counts.byQueue[q]
		counts.mu.Unlock()
		if row.RPCs["Send"] != row.Cycles || row.RPCs["Receive"] != row.Cycles || row.RPCs["CompleteBatch"] != row.Cycles {
			t.Fatalf("unexpected RPC/replay counts: %+v", row.RPCs)
		}
		op(func(c context.Context) error { return e.DeleteQueue(c, q) })
		if _, er := e.Stats(ctx, q); !errors.Is(er, native.ErrQueueNotFound) {
			t.Fatal("owned queue remains")
		}
		row.Deleted = true
	}
	if stage == "profiles" {
		for _, kind := range []string{"native", "sdk"} {
			for _, batch := range []int{1, 64} {
				for _, profile := range []string{"plain", "escaped"} {
					q := fmt.Sprintf("qual-%s-%d-%s", kind, batch, profile)
					makeQueue(q)
					dig := sha256.New()
					for i := 0; i < 4; i++ {
						rowCycle(kind, q, batch, i, dig, profile)
					}
					var latencies []float64
					var allocationBefore, allocationAfter runtime.MemStats
					runtime.ReadMemStats(&allocationBefore)
					s := time.Now()
					for i := 4; i < 260; i++ {
						latencies = append(latencies, rowCycle(kind, q, batch, i, dig, profile))
					}
					runtime.ReadMemStats(&allocationAfter)
					row := qualificationRow{AllocatedBytes: allocationAfter.TotalAlloc - allocationBefore.TotalAlloc, Allocations: allocationAfter.Mallocs - allocationBefore.Mallocs, Profile: profile, Client: kind, Batch: batch, Cycles: 260, Count: batch * 260, WallMS: float64(time.Since(s).Nanoseconds()) / 1e6}
					sort.Float64s(latencies)
					row.ReceiveP95MS = latencies[len(latencies)*95/100]
					finish(&row, q, dig)
					result.Rows = append(result.Rows, row)
				}
			}
		}
	} else {
		digests := map[string]hash.Hash{}
		rows := map[string]*qualificationRow{}
		for _, kind := range []string{"native", "sdk"} {
			for _, profile := range []string{"plain", "escaped"} {
				key := kind + "-" + profile
				makeQueue("sustain-" + key)
				digests[key] = sha256.New()
				rows[key] = &qualificationRow{Profile: profile, Client: kind, Batch: 16}
			}
		}
		s := time.Now()
		next := s
		index := 0
		for time.Since(s) < 180*time.Second {
			kind := []string{"native", "sdk"}[index%2]
			profile := []string{"plain", "escaped"}[(index/2)%2]
			key := kind + "-" + profile
			rowCycle(kind, "sustain-"+key, 16, index, digests[key], profile)
			rows[key].Cycles++
			rows[key].Count += 16
			index++
			next = next.Add(50 * time.Millisecond)
			if delay := time.Until(next); delay > 0 {
				time.Sleep(delay)
			}
		}
		result.ElapsedSeconds = time.Since(s).Seconds()
		for _, key := range []string{"native-plain", "native-escaped", "sdk-plain", "sdk-escaped"} {
			finish(rows[key], "sustain-"+key, digests[key])
			result.Rows = append(result.Rows, *rows[key])
		}
		if result.ElapsedSeconds < 180 {
			t.Fatal("sustained duration short")
		}
	}
	stop, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	if er := o.stop(stop); er != nil {
		t.Fatal(er)
	}
	result.CloseJoined = true
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbfile)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	var check string
	if err = db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&check); err != nil || check != "ok" {
		t.Fatal("integrity failed")
	}
	result.Integrity = true
	fk, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	if fk.Next() || fk.Err() != nil {
		t.Fatal("foreign keys failed")
	}
	_ = fk.Close()
	_ = db.Close()
	result.ForeignKeys = true
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if info, er := os.Stat(dbfile + suffix); er == nil {
			result.DBBytes += info.Size()
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	result.HeapGrowth = int64(after.HeapAlloc) - int64(before.HeapAlloc)
	result.GoroutineGrowth = runtime.NumGoroutine() - goroutines
	if result.HeapGrowth > 16<<20 || result.GoroutineGrowth > 16 {
		t.Fatalf("postdrain resource budget exceeded: heap=%d goroutines=%d", result.HeapGrowth, result.GoroutineGrowth)
	}
	if stage == "profiles" {
		result.ElapsedSeconds = time.Since(started).Seconds()
	}
	result.Passed = true
}

func osReadBody(r *http.Request) ([]byte, error) {
	b, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(b))
	return b, e
}
