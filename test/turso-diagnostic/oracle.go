//go:build mqlite_perf_diag

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	mq "github.com/mqlitehq/mqlite"
)

// These input and full-content checks intentionally match the V11 performance
// oracle. Bodies are generated, never included in artifacts.
func payloadFor(id string, n int) []byte {
	b := make([]byte, 0, n+32)
	for i := 0; len(b) < n; i++ {
		s := sha256.Sum256([]byte(fmt.Sprintf("validation/v1/%s/%d", id, i)))
		b = append(b, s[:]...)
	}
	return b[:n]
}
func inputFor(id string, n int) mq.OutMessage {
	return mq.OutMessage{Body: payloadFor(id, n), MessageID: id, GroupID: id + "-group", CorrelationID: id + "-correlation", ReplyTo: "validation-replies", Subject: "keep", ContentType: "application/octet-stream", Properties: map[string]string{"id": id, "unicode": "café/中文/🚀"}}
}
func checkDelivery(m *delivery, in mq.OutMessage, seq int64) bool {
	return m.Sequence > 0 && (seq == 0 || m.Sequence == seq) && m.ID == in.MessageID && bytes.Equal(m.Body, in.Body) && m.Group == in.GroupID && m.Correlation == in.CorrelationID && m.Reply == in.ReplyTo && m.Subject == in.Subject && m.ContentType == in.ContentType && reflect.DeepEqual(m.Properties, in.Properties) && m.Count == 1 && !m.Enqueued.IsZero() && m.Token != "" && !m.LockedUntil.IsZero()
}
func checkACK(seqs []int64, n int) error {
	if len(seqs) != n {
		return oracle("send_ack_count")
	}
	seen := map[int64]bool{}
	for _, s := range seqs {
		if s <= 0 || seen[s] {
			return oracle("send_ack_sequence")
		}
		seen[s] = true
	}
	return nil
}

type identity struct {
	ID               string `json:"id"`
	SHA256           string `json:"sha256"`
	Sequence         int64  `json:"confirmed_sequence,omitempty"`
	ObservedSequence int64  `json:"observed_sequence,omitempty"`
	Status           string `json:"send_status"`
	Received         int    `json:"received"`
	Settled          bool   `json:"settled"`
}
type accounting struct {
	Attempted         int        `json:"attempted_messages"`
	Confirmed         int        `json:"confirmed_sends"`
	SendUnknown       int        `json:"unknown_sends"`
	SendRejected      int        `json:"rejected_sends"`
	SendPending       int        `json:"pending_send_results"`
	Received          int        `json:"received_deliveries"`
	UniqueReceived    int        `json:"unique_received"`
	Settled           int        `json:"confirmed_client_settlement_items"`
	SettleUnknown     int        `json:"unknown_settlement_items"`
	SettleRejected    int        `json:"rejected_settlement_items"`
	Missing           int        `json:"missing_confirmed"`
	UnknownObserved   int        `json:"unknown_sends_observed"`
	UnknownUnobserved int        `json:"unknown_sends_unobserved"`
	Duplicates        int        `json:"duplicate_deliveries"`
	Unexpected        int        `json:"unexpected_deliveries"`
	Corrupted         int        `json:"corrupted_deliveries"`
	UnsettledObserved int        `json:"unsettled_observed_identities"`
	OperationFailures int        `json:"operation_failures"`
	Ledger            []identity `json:"identity_ledger"`
}
type identityBook struct {
	mu                                                                                          sync.Mutex
	ids                                                                                         map[string]*identity
	received, settled, unknownSettle, rejectedSettle, duplicates, unexpected, corrupt, failures int
}

func newBook() *identityBook { return &identityBook{ids: map[string]*identity{}} }
func (b *identityBook) add(ms []mq.OutMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, m := range ms {
		s := sha256.Sum256(m.Body)
		b.ids[m.MessageID] = &identity{ID: m.MessageID, SHA256: hex.EncodeToString(s[:]), Status: "pending"}
	}
}
func (b *identityBook) sent(ms []mq.OutMessage, seqs []int64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.failures++
	}
	for i, m := range ms {
		id := b.ids[m.MessageID]
		if err == nil {
			id.Status = "confirmed"
			id.Sequence = seqs[i]
		} else if mayHaveApplied(err) {
			id.Status = "unknown"
		} else {
			id.Status = "rejected"
		}
	}
}
func (b *identityBook) observe(ms []*delivery, payload int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, m := range ms {
		b.received++
		id, ok := b.ids[m.ID]
		if !ok {
			b.unexpected++
			continue
		}
		id.Received++
		if id.Received > 1 {
			b.duplicates++
		}
		bad := false
		seq := int64(0)
		if id.Status == "confirmed" {
			seq = id.Sequence
			if seq <= 0 {
				bad = true
			}
		}
		if id.ObservedSequence == 0 {
			id.ObservedSequence = m.Sequence
		} else if id.ObservedSequence != m.Sequence {
			bad = true
		}
		hash := sha256.Sum256(m.Body)
		if hex.EncodeToString(hash[:]) != id.SHA256 || !checkDelivery(m, inputFor(id.ID, payload), seq) {
			bad = true
		}
		if bad {
			b.corrupt++
		}
	}
}
func (b *identityBook) complete(ms []*delivery, mask []bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.failures++
		if mayHaveApplied(err) {
			b.unknownSettle += len(ms)
		} else {
			b.rejectedSettle += len(ms)
		}
		return
	}
	if len(mask) != len(ms) {
		b.failures++
		b.unknownSettle += len(ms)
		return
	}
	for i, ok := range mask {
		if !ok {
			b.rejectedSettle++
			continue
		}
		if id, found := b.ids[ms[i].ID]; found && !id.Settled {
			id.Settled = true
			b.settled++
		}
	}
}
func (b *identityBook) failed() { b.mu.Lock(); b.failures++; b.mu.Unlock() }
func (b *identityBook) snapshot() accounting {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := accounting{Received: b.received, Settled: b.settled, SettleUnknown: b.unknownSettle, SettleRejected: b.rejectedSettle, Duplicates: b.duplicates, Unexpected: b.unexpected, Corrupted: b.corrupt, OperationFailures: b.failures}
	for _, id := range b.ids {
		r.Attempted++
		switch id.Status {
		case "confirmed":
			r.Confirmed++
			if id.Received == 0 {
				r.Missing++
			}
		case "unknown":
			r.SendUnknown++
			if id.Received > 0 {
				r.UnknownObserved++
			} else {
				r.UnknownUnobserved++
			}
		case "rejected":
			r.SendRejected++
		default:
			r.SendPending++
		}
		if id.Received > 0 {
			r.UniqueReceived++
			if !id.Settled {
				r.UnsettledObserved++
			}
		}
		r.Ledger = append(r.Ledger, *id)
	}
	sort.Slice(r.Ledger, func(i, j int) bool { return r.Ledger[i].ID < r.Ledger[j].ID })
	return r
}
func (r accounting) valid(n int, s mq.Metrics) bool {
	return r.Attempted == n && n > 0 && r.Confirmed == n && r.SendUnknown == 0 && r.SendRejected == 0 && r.SendPending == 0 && r.Received == n && r.UniqueReceived == n && r.Settled == n && r.SettleUnknown == 0 && r.SettleRejected == 0 && r.UnsettledObserved == 0 && r.Missing == 0 && r.Duplicates == 0 && r.Unexpected == 0 && r.Corrupted == 0 && r.OperationFailures == 0 && s.Total == 0 && s.Active == 0 && s.Locked == 0 && s.Scheduled == 0 && s.Deferred == 0 && s.DeadLettered == 0
}

type latency struct {
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	MaxMS   float64 `json:"max_ms"`
	MeanMS  float64 `json:"mean_ms"`
	Method  string  `json:"method"`
}

func distribution(vs []time.Duration) latency {
	r := latency{Samples: len(vs), Method: "exact_bounded_samples_floor_rank"}
	if len(vs) == 0 {
		return r
	}
	v := append([]time.Duration(nil), vs...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	pct := func(p float64) float64 { return ms(v[int(float64(len(v)-1)*p)]) }
	for _, d := range v {
		r.MeanMS += ms(d)
	}
	r.MeanMS /= float64(len(v))
	r.P50MS = pct(.5)
	r.P95MS = pct(.95)
	r.P99MS = pct(.99)
	r.MaxMS = ms(v[len(v)-1])
	return r
}
