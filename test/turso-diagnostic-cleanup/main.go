// This one-time command cleans only the interrupted diagnostic manifest entry.
// It never accepts a target, provider URL, SQL statement or token as an argument.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

const approvedDSNSHA256 = "b72bfdad8f3e7696b3d216a2e947705b8efbd6c183e77f44d2cdcb16cef544b3"
const targetQueue = "diag_5b3a11f4b0594ecd_09_native"
const priorRun = "5b3a11f4b0594ecd"
const priorCase = "control_pool4_c32_b1"
const priorActionsRun = 42437949470

type credentials struct{ dsn, token string }
type failure struct {
	Class string `json:"class"`
}

func preflight(lookup func(string) (string, bool)) (credentials, *failure) {
	for _, key := range []string{"MQLITE_TEST_DB", "MQLITE_TEST_DB_RESET_CONFIRM"} {
		if _, exists := lookup(key); exists {
			return credentials{}, &failure{"legacy_reset_environment_refused"}
		}
	}
	dsn, _ := lookup("MQLITE_DIAG_STANDARD_DB")
	token, _ := lookup("MQLITE_DIAG_STANDARD_TOKEN")
	if dsn == "" || token == "" {
		return credentials{}, &failure{"test_credentials_missing"}
	}
	if !matchesDSN(dsn, approvedDSNSHA256) {
		return credentials{}, &failure{"test_database_not_allowlisted"}
	}
	return credentials{dsn, token}, nil
}

func matchesDSN(dsn, approvedHash string) bool {
	hash := sha256.Sum256([]byte(dsn))
	return hex.EncodeToString(hash[:]) == approvedHash
}

type gauge struct {
	Observed   uint64 `json:"observed"`
	Active     int64  `json:"active"`
	OpenBodies int64  `json:"open_bodies"`
}
type trackedTransport struct {
	next       http.RoundTripper
	observed   atomic.Uint64
	active     atomic.Int64
	openBodies atomic.Int64
}

// No URL, request, response content or error is retained by the observer.
func (t *trackedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.observed.Add(1)
	t.active.Add(1)
	defer t.active.Add(-1)
	response, err := t.next.RoundTrip(req)
	if response != nil && response.Body != nil {
		t.openBodies.Add(1)
		response.Body = &trackedBody{ReadCloser: response.Body, release: func() { t.openBodies.Add(-1) }}
	}
	return response, err
}
func (t *trackedTransport) snapshot() gauge {
	return gauge{t.observed.Load(), t.active.Load(), t.openBodies.Load()}
}

type trackedBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *trackedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}

type quietResult struct {
	ObservedQuiet                bool    `json:"observed_quiet"`
	WaitedMS                     float64 `json:"waited_ms"`
	Final                        gauge   `json:"final"`
	HiddenDriverGoroutinesJoined bool    `json:"hidden_driver_goroutines_joined"`
}

// This is an observation window, not a proof that hidden driver goroutines joined.
func waitQuiet(ctx context.Context, snapshot func() gauge, window, poll time.Duration) quietResult {
	start := time.Now()
	stableSince := start
	previous := snapshot()
	timer := time.NewTicker(poll)
	defer timer.Stop()
	for {
		current := snapshot()
		if current != previous || current.Active != 0 || current.OpenBodies != 0 {
			stableSince = time.Now()
		}
		previous = current
		if current.Active == 0 && current.OpenBodies == 0 && time.Since(stableSince) >= window {
			return quietResult{ObservedQuiet: true, WaitedMS: float64(time.Since(start)) / float64(time.Millisecond), Final: current}
		}
		select {
		case <-ctx.Done():
			return quietResult{WaitedMS: float64(time.Since(start)) / float64(time.Millisecond), Final: snapshot()}
		case <-timer.C:
		}
	}
}

type store interface {
	Stats(context.Context, string) (engine.Metrics, error)
	DeleteQueueWithOptions(context.Context, string, engine.DeleteOptions) (engine.DeleteResult, error)
	Close() error
}
type openStore func(context.Context, engine.Options) (store, error)
type limits struct{ close, quietBudget, quietWindow, poll time.Duration }

type counts struct {
	Active       int64 `json:"active"`
	Locked       int64 `json:"locked"`
	Deferred     int64 `json:"deferred"`
	Scheduled    int64 `json:"scheduled"`
	DeadLettered int64 `json:"dead_lettered"`
	Total        int64 `json:"total"`
}
type report struct {
	Scope                      string      `json:"scope"`
	Backend                    string      `json:"backend"`
	PriorActionsRun            int64       `json:"prior_actions_run"`
	PriorRun                   string      `json:"prior_run"`
	PriorCase                  string      `json:"prior_case"`
	Queue                      string      `json:"exact_owned_queue"`
	Started                    string      `json:"started"`
	Finished                   string      `json:"finished"`
	SourceSHA                  string      `json:"source_sha"`
	Passed                     bool        `json:"passed"`
	Blocked                    bool        `json:"blocked"`
	Failure                    *failure    `json:"failure,omitempty"`
	BeforeStatsValid           bool        `json:"before_stats_valid"`
	BeforeLogicallyAbsent      bool        `json:"before_logically_absent"`
	Before                     counts      `json:"before_counts"`
	DeleteCalls                int         `json:"delete_calls"`
	Deleted                    bool        `json:"deleted"`
	DeleteState                string      `json:"delete_state"`
	AlreadyAbsent              bool        `json:"already_absent"`
	AbsentReadback             bool        `json:"absent_readback"`
	EngineOpened               bool        `json:"engine_opened"`
	EngineCloseJoined          bool        `json:"engine_close_joined"`
	OwnerReleased              bool        `json:"owner_released"`
	ProviderQuiet              quietResult `json:"provider_quiet"`
	RawErrorPublished          bool        `json:"raw_error_published"`
	HistoricalRunResultChanged bool        `json:"historical_run_result_changed"`
}

func newReport() report {
	sha := os.Getenv("MQLITE_DIAG_SOURCE_SHA")
	if data, err := hex.DecodeString(sha); err != nil || len(data) != 20 {
		sha = "unrecorded"
	}
	return report{Scope: "one_interrupted_manifest_entry_only_not_a_workload_or_acceptance_retry", Backend: "standard", PriorActionsRun: priorActionsRun, PriorRun: priorRun, PriorCase: priorCase, Queue: targetQueue, SourceSHA: sha, Started: time.Now().UTC().Format(time.RFC3339Nano)}
}

func cleanup(ctx context.Context, c credentials, open openStore, tracker *trackedTransport, bound limits) (r report) {
	r = newReport()
	var db store
	defer func() {
		if db != nil {
			done := make(chan error, 1)
			go func() { done <- db.Close() }()
			select {
			case err := <-done:
				r.EngineCloseJoined = true
				if err != nil {
					r.Blocked = true
					if r.Failure == nil {
						r.Failure = &failure{"close_failed"}
					}
				}
			case <-time.After(bound.close):
				r.Blocked = true
				if r.Failure == nil {
					r.Failure = &failure{"close_did_not_join"}
				}
			}
		}
		if db == nil || r.EngineCloseJoined {
			quietCtx, cancel := context.WithTimeout(context.Background(), bound.quietBudget)
			r.ProviderQuiet = waitQuiet(quietCtx, tracker.snapshot, bound.quietWindow, bound.poll)
			cancel()
			if !r.ProviderQuiet.ObservedQuiet {
				r.Blocked = true
				if r.Failure == nil {
					r.Failure = &failure{"provider_not_quiet"}
				}
			}
		} else {
			r.ProviderQuiet.Final = tracker.snapshot()
		}
		r.OwnerReleased = !r.Blocked && r.ProviderQuiet.ObservedQuiet && (db == nil || r.EngineCloseJoined)
		r.Passed = r.Failure == nil && r.OwnerReleased && r.AbsentReadback && (r.Deleted || r.AlreadyAbsent)
		r.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	}()
	var err error
	db, err = open(ctx, engine.Options{DB: c.dsn, AuthToken: c.token, DisableBackground: true, PreserveUnexpiredLeases: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		db = nil
		r.Failure = &failure{"open_failed"}
		return
	}
	if db == nil {
		r.Failure = &failure{"open_returned_no_owner"}
		return
	}
	r.EngineOpened = true
	m, err := db.Stats(ctx, targetQueue)
	if err == nil {
		if m.Queue != targetQueue {
			r.Failure = &failure{"before_stats_scope_mismatch"}
			return
		}
		r.BeforeStatsValid = true
		r.Before = counts{m.Active, m.Locked, m.Deferred, m.Scheduled, m.DeadLettered, m.Total}
	} else if errors.Is(err, engine.ErrQueueNotFound) {
		// Stats hides logically deleting queues; it does not prove physical absence.
		r.BeforeLogicallyAbsent = true
	} else {
		r.Failure = &failure{"before_stats_failed"}
		return
	}
	for i := 0; i < 64; i++ {
		if ctx.Err() != nil {
			r.Failure = &failure{"cleanup_context_ended"}
			return
		}
		r.DeleteCalls++
		d, err := db.DeleteQueueWithOptions(ctx, targetQueue, engine.DeleteOptions{Force: true})
		if errors.Is(err, engine.ErrQueueNotFound) {
			r.AlreadyAbsent = true
			r.DeleteState = "not_found"
			break
		}
		if err != nil {
			r.Failure = &failure{"delete_failed_no_replay"}
			return
		}
		if !d.Deleted || d.State != "deleting" && d.State != "deleted" {
			r.Failure = &failure{"delete_ack_invalid"}
			return
		}
		r.DeleteState = d.State
		if d.Deleted && d.State == "deleted" {
			r.Deleted = true
			break
		}
	}
	if !r.Deleted && !r.AlreadyAbsent {
		r.Failure = &failure{"delete_incomplete"}
		return
	}
	_, err = db.Stats(ctx, targetQueue)
	if !errors.Is(err, engine.ErrQueueNotFound) {
		r.Failure = &failure{"absence_readback_failed"}
		return
	}
	r.AbsentReadback = true
	return
}

func writeReport(dir string, r report) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	// Refuse overwriting previous cleanup evidence, including a FAIL.
	f, err := os.OpenFile(filepath.Join(dir, "cleanup.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func main() { os.Exit(run()) }
func run() int {
	log.SetOutput(io.Discard)
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	flags := flag.NewFlagSet("turso-diagnostic-cleanup", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	out := flags.String("out", "", "new safe evidence directory")
	if err := flags.Parse(os.Args[1:]); err != nil || *out == "" || flags.NArg() != 0 {
		fmt.Println("cleanup refused: invalid_arguments")
		return 2
	}
	if _, err := os.Stat(filepath.Join(*out, "cleanup.json")); err == nil || !os.IsNotExist(err) {
		fmt.Println("cleanup refused: existing_evidence")
		return 2
	}
	c, f := preflight(os.LookupEnv)
	if f != nil {
		r := newReport()
		r.Failure = f
		r.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		_ = writeReport(*out, r)
		fmt.Println("cleanup refused: preflight")
		return 2
	}
	if http.DefaultTransport == nil || http.DefaultClient == nil || http.DefaultClient.Transport != nil {
		fmt.Println("cleanup refused: unexpected_default_transport")
		return 2
	}
	previous := http.DefaultTransport
	tracker := &trackedTransport{next: previous}
	http.DefaultTransport = tracker
	defer func() { http.DefaultTransport = previous }()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	r := cleanup(ctx, c, func(ctx context.Context, opts engine.Options) (store, error) {
		db, err := engine.Open(ctx, opts)
		if err != nil {
			return nil, err
		}
		return db, nil
	}, tracker, limits{10 * time.Second, 10 * time.Second, 200 * time.Millisecond, 20 * time.Millisecond})
	if err := writeReport(*out, r); err != nil {
		fmt.Println("cleanup failed: artifact_write")
		return 1
	}
	fmt.Printf("cleanup completed: passed=%t blocked=%t absent=%t\n", r.Passed, r.Blocked, r.AbsentReadback)
	if !r.Passed {
		return 1
	}
	return 0
}
