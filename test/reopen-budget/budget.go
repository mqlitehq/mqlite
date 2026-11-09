package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/mqlitehq/mqlite"
	low "github.com/mqlitehq/mqlite-go"
)

const caseBudget = 70 * time.Second
const cleanupBudget = 60 * time.Second

type caseConfig struct {
	dsn, token      string
	trace           *providerObserver
	afterFirstClose func()
	beforeRelease   func()
	budget          time.Duration
	wrapBroker      func(http.RoundTripper) http.RoundTripper
	register        func(string, string) error
	events          *safeLog
}
type caseResult struct {
	SDK                    string         `json:"sdk"`
	Queue                  string         `json:"owned_queue"`
	Passed                 bool           `json:"passed"`
	Failure                string         `json:"failure,omitempty"`
	OwnersJoined           bool           `json:"owners_joined"`
	ReceiverJoined         bool           `json:"receiver_joined"`
	CleanupAbsent          bool           `json:"cleanup_absent"`
	CleanupFailure         string         `json:"cleanup_failure,omitempty"`
	StorageEvents          []storageEvent `json:"storage_events"`
	StorageEventsDropped   int            `json:"storage_events_dropped"`
	ProviderJoined         bool           `json:"provider_joined"`
	InitialOpenMS          int64          `json:"initial_open_ms"`
	StopMS                 int64          `json:"stop_ms"`
	CloseMS                int64          `json:"close_ms"`
	ReopenMS               int64          `json:"reopen_ms"`
	ReadyMS                int64          `json:"ready_ms"`
	StopToReadyMS          int64          `json:"stop_to_ready_ms"`
	StopToSettledMS        int64          `json:"stop_to_settled_ms"`
	InitialRemainingMS     int64          `json:"initial_remaining_ms"`
	LeaseMS                int64          `json:"lease_ms"`
	MaxDelivery            int            `json:"max_delivery"`
	HandlerCalls           int32          `json:"handler_calls"`
	IdentityVerified       bool           `json:"identity_verified"`
	BackgroundMaintenance  bool           `json:"background_maintenance"`
	RenewAcknowledged      bool           `json:"renew_acknowledged"`
	RenewRequests          int            `json:"renew_requests"`
	ExplicitRenewSucceeded bool           `json:"explicit_renew_succeeded"`
	CompleteAcknowledged   bool           `json:"complete_acknowledged"`
	CompleteRequests       int            `json:"complete_requests"`
	TokenHash              string         `json:"token_sha256,omitempty"`
	Sequence               int64          `json:"sequence"`
	RemainingMessages      int64          `json:"remaining_messages"`
	DeadLettered           int64          `json:"dead_lettered"`
	ObservedErrors         []string       `json:"observed_errors"`
	InitialOpen            providerCounts `json:"initial_open"`
	Reopen                 providerCounts `json:"reopen"`
	FullWindow             providerCounts `json:"full_window"`
}
type owner struct {
	e         *mq.Embedded
	cancel    context.CancelFunc
	done      chan error
	stopDone  bool
	closeOnce sync.Once
	closeDone chan error
	closed    bool
}

func (o *owner) stop(ctx context.Context) error {
	if o.cancel == nil || o.stopDone {
		return nil
	}
	o.cancel()
	select {
	case err := <-o.done:
		o.stopDone = true
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (o *owner) close(ctx context.Context) error {
	if o.closed {
		return nil
	}
	if o.cancel != nil && !o.stopDone {
		return failure("close_before_serve_join")
	}
	o.closeOnce.Do(func() { o.closeDone = make(chan error, 1); go func() { o.closeDone <- o.e.Close() }() })
	select {
	case err := <-o.closeDone:
		o.closed = true
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func startOwner(ctx context.Context, o *owner, addr string) error {
	run, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.done = make(chan error, 1)
	ready := make(chan struct{})
	go func() { o.done <- o.e.Serve(run, addr, mq.WithReady(func() { close(ready) })) }()
	select {
	case <-ready:
		return nil
	case err := <-o.done:
		o.stopDone = true
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func openOwner(ctx context.Context, cfg caseConfig) (*owner, error) {
	e, err := mq.OpenEmbedded(ctx, cfg.dsn, mq.WithDBAuthToken(cfg.token), mq.WithBrokerMode(), mq.WithLogger(slog.New(safeHandler{log: cfg.events})))
	if err != nil {
		return nil, err
	}
	return &owner{e: e}, nil
}

func runCase(parent context.Context, cfg caseConfig, sdk string) (r caseResult) {
	cfg.events = &safeLog{}
	captureStart := cfg.trace.snapshot().InvalidCapture
	r = caseResult{SDK: sdk, LeaseMS: 30000, MaxDelivery: 1, ObservedErrors: []string{}, OwnersJoined: true, ReceiverJoined: true, CleanupAbsent: true, BackgroundMaintenance: true}
	duration := caseBudget
	if cfg.budget > 0 && cfg.budget < duration {
		duration = cfg.budget
	}
	ctx, cancel := context.WithTimeout(parent, duration)
	defer cancel()
	queue, err := uniqueQueue()
	if err != nil {
		r.Failure = safeClass(err)
		return
	}
	r.Queue = queue
	if cfg.register != nil {
		if err := cfg.register(queue, sdk); err != nil {
			r.Failure = "ownership_manifest_write"
			return
		}
	}
	var owners []*owner
	var active *owner
	var createAttempted bool
	var stopReceiver context.CancelFunc
	var runDone chan error
	var closeClient func()
	var releaseOnce sync.Once
	release := make(chan struct{})
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	var calls atomic.Int32
	var identityOK atomic.Bool
	var renewOK atomic.Bool
	var initialDeadline atomic.Int64
	var tokenHash atomic.Value
	var obsMu sync.Mutex
	var observed []string
	ack := &ackObserver{queue: queue, next: http.DefaultTransport}
	if cfg.wrapBroker != nil {
		ack.next = cfg.wrapBroker(ack.next)
	}
	defer func() {
		// Stop/join all application work before deleting only our registered queue.
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), cleanupBudget)
		defer cleanCancel()
		if stopReceiver != nil {
			stopReceiver()
			releaseHandler()
			select {
			case e := <-runDone:
				r.ReceiverJoined = true
				if e != nil && !errors.Is(e, context.Canceled) && r.Failure == "" {
					r.Failure = "receiver_exit:" + safeClass(e)
				}
			case <-cleanCtx.Done():
				r.ReceiverJoined = false
			}
		}
		if closeClient != nil {
			closeClient()
		}
		for _, o := range owners {
			if e := o.stop(cleanCtx); e != nil {
				r.OwnersJoined = false
			}
		}
		if createAttempted && r.ReceiverJoined && r.OwnersJoined {
			var cleanupErr error
			if active == nil || active.closed {
				active, cleanupErr = openOwner(cleanCtx, cfg)
				if cleanupErr == nil {
					owners = append(owners, active)
				}
			}
			if cleanupErr != nil {
				r.CleanupFailure = safeClass(cleanupErr)
				r.CleanupAbsent = false
			} else {
				r.CleanupAbsent = false
				for cleanCtx.Err() == nil {
					_, e := active.e.DeleteQueueWithOptions(cleanCtx, queue, mq.DeleteQueueOpts{Force: true})
					if e != nil && !errors.Is(e, mq.ErrQueueNotFound) {
						r.CleanupFailure = safeClass(e)
						break
					}
					qs, e := active.e.ListQueues(cleanCtx)
					if e != nil {
						r.CleanupFailure = safeClass(e)
						break
					}
					present := false
					for _, q := range qs {
						present = present || q.Name == queue
					}
					if !present {
						r.CleanupAbsent = true
						break
					}
					select {
					case <-time.After(50 * time.Millisecond):
					case <-cleanCtx.Done():
					}
				}
			}
		} else if createAttempted {
			r.CleanupAbsent = false
			r.CleanupFailure = "owner_or_receiver_not_joined"
		}
		for _, o := range owners {
			if e := o.close(cleanCtx); e != nil {
				r.OwnersJoined = false
				if r.CleanupFailure == "" {
					r.CleanupFailure = safeClass(e)
				}
			}
		}
		r.ProviderJoined = cfg.trace.join(cleanCtx.Done())
		r.StorageEvents, r.StorageEventsDropped = cfg.events.snapshot()
		r.HandlerCalls = calls.Load()
		r.IdentityVerified = identityOK.Load()
		r.ExplicitRenewSucceeded = renewOK.Load()
		r.CompleteAcknowledged, r.CompleteRequests = ack.snapshot()
		r.RenewAcknowledged, r.RenewRequests = ack.renewSnapshot()
		if v := tokenHash.Load(); v != nil {
			r.TokenHash = v.(string)
		}
		obsMu.Lock()
		r.ObservedErrors = append(r.ObservedErrors, observed...)
		obsMu.Unlock()
		r.Passed = r.Failure == "" && r.OwnersJoined && r.ReceiverJoined && r.CleanupAbsent && r.ProviderJoined && r.HandlerCalls == 1 && r.IdentityVerified && r.ExplicitRenewSucceeded && r.RenewAcknowledged && r.CompleteAcknowledged && r.RemainingMessages == 0 && r.DeadLettered == 0 && r.Reopen.SchemaDDL == 0 && r.Reopen.InvalidCapture == 0 && r.Reopen.Requests > 0 && r.Reopen.VersionWrites > 0 && cfg.trace.snapshot().InvalidCapture == captureStart
		if !r.Passed && r.Failure == "" {
			switch {
			case !r.OwnersJoined || !r.ReceiverJoined || !r.ProviderJoined:
				r.Failure = "cleanup:join_incomplete"
			case !r.CleanupAbsent:
				r.Failure = "cleanup:absence_not_confirmed"
			case cfg.trace.snapshot().InvalidCapture != captureStart:
				r.Failure = "evidence:provider_capture_incomplete"
			case r.Reopen.SchemaDDL != 0:
				r.Failure = "reopen:fast_path_not_observed"
			default:
				r.Failure = "evidence:acceptance_incomplete"
			}
		}

	}()
	fail := func(stage string, e error) { r.Failure = stage + ":" + safeClass(e) }
	beforeInitial := cfg.trace.snapshot()
	initialStart := time.Now()
	active, err = openOwner(ctx, cfg)
	r.InitialOpenMS = time.Since(initialStart).Milliseconds()
	r.InitialOpen = delta(beforeInitial, cfg.trace.snapshot())
	if err != nil {
		fail("initial_open", err)
		return
	}
	owners = append(owners, active)
	qs, err := active.e.ListQueues(ctx)
	if err != nil {
		fail("preflight_inventory", err)
		return
	}
	for _, q := range qs {
		if q.Name == queue {
			fail("identity_collision", failure("preexisting_queue"))
			return
		}
	}
	createAttempted = true
	r.CleanupAbsent = false
	if err = active.e.CreateQueue(ctx, queue, mq.QueueConfig{MaxDeliveryCount: 1}); err != nil {
		fail("create", err)
		return
	}
	qs, err = active.e.ListQueues(ctx)
	if err != nil {
		fail("verify_configuration", err)
		return
	}
	configured := false
	for _, q := range qs {
		if q.Name == queue {
			configured = q.Kind == "queue" && q.State == "active" && q.LockDurationMs == 30000 && q.MaxDeliveryCount == 1
		}
	}
	if !configured {
		fail("verify_configuration", failure("unexpected_queue_configuration"))
		return
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fail("listen", err)
		return
	}
	addr := ln.Addr().String()
	cfg.trace.allowBroker(addr)
	_ = ln.Close()
	if err = startOwner(ctx, active, addr); err != nil {
		fail("initial_serve", err)
		return
	}
	body := []byte("finite-owned-message:" + queue)
	messageID := queue + "-message"
	seq, err := active.e.SendOne(ctx, queue, mq.OutMessage{Body: body, MessageID: messageID})
	if err != nil || seq <= 0 {
		if err == nil {
			err = failure("missing_sequence")
		}
		fail("send", err)
		return
	}
	r.Sequence = seq
	entered := make(chan struct{}, 1)
	onError := func(e error) { obsMu.Lock(); observed = append(observed, safeClass(e)); obsMu.Unlock() }
	handle := func(hctx context.Context, n int64, b []byte, id, tok string, count int, until time.Time, renew func(context.Context) error) error {
		call := calls.Add(1)
		if call != 1 || n != seq || !bytes.Equal(b, body) || id != messageID || count != 1 || tok == "" {
			return failure("delivery_identity_mismatch")
		}
		identityOK.Store(true)
		initialDeadline.Store(until.UnixMilli())
		tokenHash.Store(hash(tok))
		ack.expect(seq, tok)
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-hctx.Done():
			return hctx.Err()
		}
		if e := renew(hctx); e != nil {
			return e
		}
		renewOK.Store(true)
		return nil
	}
	receiverCtx, stop := context.WithCancel(ctx)
	stopReceiver = stop
	runDone = make(chan error, 1)
	clientHTTP := &http.Client{Transport: ack}
	if sdk == "native" {
		c, e := mq.Open(ctx, "http://"+addr, mq.WithHTTPClient(clientHTTP))
		if e != nil {
			fail("client_open", e)
			runDone <- context.Canceled
			return
		}
		closeClient = func() { _ = c.Close() }
		go func() {
			runDone <- c.Receiver(queue, mq.WithAutoRenew(), mq.WithErrorHandler(onError)).Run(receiverCtx, func(hctx context.Context, m *mq.Message) error {
				return handle(hctx, m.SequenceNumber, m.Body, m.MessageID, m.LockToken(), m.DeliveryCount, m.LockedUntil, m.Renew)
			})
		}()
	} else if sdk == "lowfloor" {
		c, e := low.Open(ctx, "http://"+addr, low.WithHTTPClient(clientHTTP))
		if e != nil {
			fail("client_open", e)
			runDone <- context.Canceled
			return
		}
		closeClient = func() { _ = c.Close() }
		go func() {
			runDone <- c.Receiver(queue, low.WithAutoRenew(), low.WithErrorHandler(onError)).Run(receiverCtx, func(hctx context.Context, m *low.Message) error {
				return handle(hctx, m.SequenceNumber, m.Body, m.MessageID, m.LockToken(), m.DeliveryCount, m.LockedUntil, m.Renew)
			})
		}()
	} else {
		fail("sdk", failure("unsupported_sdk"))
		runDone <- context.Canceled
		return
	}
	select {
	case <-entered:
	case e := <-runDone:
		runDone <- e
		fail("handler_start", e)
		return
	case <-ctx.Done():
		fail("handler_start", ctx.Err())
		return
	}
	started := time.Now()
	r.InitialRemainingMS = time.Until(time.UnixMilli(initialDeadline.Load())).Milliseconds()
	beforeWindow := cfg.trace.snapshot()
	stopStart := time.Now()
	err = active.stop(ctx)
	r.StopMS = time.Since(stopStart).Milliseconds()
	if err != nil {
		fail("serve_stop", err)
		return
	}
	closeStart := time.Now()
	err = active.close(ctx)
	r.CloseMS = time.Since(closeStart).Milliseconds()
	if err != nil {
		r.OwnersJoined = false
		fail("owner_close", err)
		return
	}
	if cfg.afterFirstClose != nil {
		cfg.afterFirstClose()
	}
	beforeReopen := cfg.trace.snapshot()
	reopenStart := time.Now()
	active, err = openOwner(ctx, cfg)
	r.ReopenMS = time.Since(reopenStart).Milliseconds()
	r.Reopen = delta(beforeReopen, cfg.trace.snapshot())
	if err != nil {
		fail("reopen", err)
		return
	}
	owners = append(owners, active)
	readyStart := time.Now()
	if err = startOwner(ctx, active, addr); err != nil {
		fail("reopen_serve", err)
		return
	}
	r.ReadyMS = time.Since(readyStart).Milliseconds()
	r.StopToReadyMS = time.Since(started).Milliseconds()
	if cfg.beforeRelease != nil {
		cfg.beforeRelease()
	}
	releaseHandler()
	for ctx.Err() == nil {
		m, e := active.e.Stats(ctx, queue)
		if e != nil {
			fail("settlement_stats", e)
			return
		}
		r.RemainingMessages = m.Total
		r.DeadLettered = m.DeadLettered
		done, _ := ack.snapshot()
		if m.Total == 0 && done {
			break
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
		}
	}
	r.StopToSettledMS = time.Since(started).Milliseconds()
	r.FullWindow = delta(beforeWindow, cfg.trace.snapshot())
	if ctx.Err() != nil {
		fail("settlement_wait", ctx.Err())
		return
	}
	if r.StopToSettledMS >= r.InitialRemainingMS {
		fail("lease_budget", failure("initial_remaining_lease_exceeded"))
		return
	}
	obsMu.Lock()
	for _, c := range observed {
		if c != "network" && c != "network_timeout" {
			err = failure("unexpected_receiver_error")
		}
	}
	obsMu.Unlock()
	if err != nil {
		fail("receiver", err)
		return
	}
	return
}

// Complete evidence belongs to the exact generated queue, returned sequence and
// handler token. Only a fully read/closed HTTP 200 with explicit ok:true counts.
type ackObserver struct {
	next       http.RoundTripper
	queue      string
	mu         sync.Mutex
	seq        int64
	token      string
	ack        bool
	calls      int
	renewAck   bool
	renewCalls int
}

func (a *ackObserver) expect(seq int64, tok string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seq = seq
	a.token = tok
}
func (a *ackObserver) snapshot() (bool, int) { a.mu.Lock(); defer a.mu.Unlock(); return a.ack, a.calls }
func (a *ackObserver) renewSnapshot() (bool, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.renewAck, a.renewCalls
}
func (a *ackObserver) RoundTrip(r *http.Request) (*http.Response, error) {
	matching := false
	renew := strings.HasSuffix(r.URL.Path, "/Renew")
	if (strings.HasSuffix(r.URL.Path, "/Complete") || renew) && r.GetBody != nil {
		b, e := r.GetBody()
		if e == nil {
			data, e := io.ReadAll(io.LimitReader(b, 8193))
			_ = b.Close()
			var in struct {
				Queue string `json:"queue"`
				Seq   int64  `json:"seq_number"`
				Token string `json:"lock_token"`
			}
			if e == nil && len(data) <= 8192 && json.Unmarshal(data, &in) == nil {
				a.mu.Lock()
				matching = in.Queue == a.queue && in.Seq == a.seq && in.Token == a.token && a.token != ""
				if matching {
					if renew {
						a.renewCalls++
					} else {
						a.calls++
					}
				}
				a.mu.Unlock()
			}
		}
	}
	resp, err := a.next.RoundTrip(r)
	if err != nil || resp == nil || resp.Body == nil || !matching || resp.StatusCode != 200 {
		return resp, err
	}
	resp.Body = &ackBody{ReadCloser: resp.Body, record: func(data []byte, valid bool) {
		var out struct {
			OK *bool `json:"ok"`
		}
		if valid && json.Unmarshal(data, &out) == nil && out.OK != nil && *out.OK {
			a.mu.Lock()
			if renew {
				a.renewAck = true
			} else {
				a.ack = true
			}
			a.mu.Unlock()
		}
	}}
	return resp, nil
}

type ackBody struct {
	io.ReadCloser
	mu     sync.Mutex
	data   []byte
	eof    bool
	bad    bool
	once   sync.Once
	record func([]byte, bool)
}

func (b *ackBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data)+n <= 8192 {
		b.data = append(b.data, p[:n]...)
	} else {
		b.bad = true
	}
	if e == io.EOF {
		b.eof = true
	} else if e != nil {
		b.bad = true
	}
	return n, e
}
func (b *ackBody) Close() error {
	e := b.ReadCloser.Close()
	b.once.Do(func() {
		b.mu.Lock()
		data := append([]byte(nil), b.data...)
		valid := b.eof && !b.bad && e == nil
		b.mu.Unlock()
		b.record(data, valid)
	})
	return e
}
