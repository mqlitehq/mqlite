package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"strconv"
	"time"

	mq "github.com/mqlitehq/mqlite"
)

const recipeVersion = 1

var laneNames = []string{"ordinary", "group", "strict", "retry", "scheduled", "deferred", "topics", "outbox"}

var requiredRecipes = [][]string{
	{"dedup-replay", "attempt-replay", "renew", "renew-batch", "complete-batch", "complete-replay", "receive-delete"},
	{"two-groups", "backoff-hol", "group-order"},
	{"locked-hol", "deferred-hol", "backoff-hol", "strict-order"},
	{"max-delivery-dlq", "explicit-reject", "expired-token-fenced", "redrive"},
	{"not-before", "eventual-delivery"},
	{"deferred-excluded", "pick", "expired-deferred-excluded", "ttl-dlq", "ttl-discard"},
	{"include", "exclude", "fanout"},
	{"tx-commit", "tx-rollback", "business-reconciled"},
}

type input struct {
	ID          string            `json:"id"`
	BodyHash    string            `json:"body_sha256"`
	Bytes       int               `json:"bytes"`
	Targets     []string          `json:"targets"`
	Outcome     string            `json:"outcome"`
	Group       string            `json:"group"`
	Subject     string            `json:"subject"`
	Correlation string            `json:"correlation"`
	Reply       string            `json:"reply"`
	ContentType string            `json:"content_type"`
	Properties  map[string]string `json:"properties"`
	TTLMillis   int64             `json:"ttl_ms"`
	ScheduledAt int64             `json:"scheduled_at_ms"`
	Body        []byte            `json:"-"`
}

func (m input) out() mq.OutMessage {
	return mq.OutMessage{MessageID: m.ID, Body: m.Body, GroupID: m.Group,
		Subject: m.Subject, CorrelationID: m.Correlation, ReplyTo: m.Reply,
		ContentType: m.ContentType, Properties: m.Properties, TTL: time.Duration(m.TTLMillis) * time.Millisecond}
}

type plan struct {
	Version   int     `json:"recipe_version"`
	Seed      string  `json:"seed"`
	Lane      int     `json:"lane"`
	Batch     uint64  `json:"batch"`
	CreatedAt int64   `json:"created_at_ms"`
	Inputs    []input `json:"inputs"`
}

func queue(seed, suffix string) string { return "soak-" + seed + "-" + suffix }

func bodyFor(id string, size int) []byte {
	out := make([]byte, 0, size+sha256.Size)
	for block := 0; len(out) < size; block++ {
		sum := sha256.Sum256([]byte("mqlite-soak-body-v1:" + id + ":" + strconv.Itoa(block)))
		out = append(out, sum[:]...)
	}
	return out[:size]
}

func makePlan(seed string, lane int, batch uint64, created int64) plan {
	p := plan{Version: recipeVersion, Seed: seed, Lane: lane, Batch: batch, CreatedAt: created}
	n := []int{4, 4, 2, 1, 2, 2, 2, 3}[lane]
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s/%s/%d/%d", seed, laneNames[lane], batch, i)
		m := input{ID: id, Bytes: 256 + int((batch+uint64(i))%8)*128,
			Targets: []string{queue(seed, laneNames[lane])}, Outcome: "complete",
			Group: id + "/group", Subject: "keep", Correlation: id + "/correlation", Reply: "soak-replies",
			ContentType: "application/octet-stream", Properties: map[string]string{
				"lane": laneNames[lane], "batch": strconv.FormatUint(batch, 10), "index": strconv.Itoa(i), "region": "eu", "unicode": "café"}}
		m.Body = bodyFor(id, m.Bytes)
		m.BodyHash = sumBytes(m.Body)
		if lane == 1 {
			m.Group = fmt.Sprintf("%s/group/%d/%d", seed, batch, i/2)
		}
		if lane == 4 {
			m.ScheduledAt = created + 2000
		}
		if lane == 5 && i == 1 {
			m.TTLMillis = 1500
			m.Outcome = "ttl-dlq"
			if batch%2 == 1 {
				m.Targets = []string{queue(seed, "ttl-discard")}
				m.Outcome = "ttl-discard"
			}
		}
		if lane == 6 {
			m.Targets = []string{queue(seed, "all-events"), queue(seed, "eu-events")}
			if i == 1 {
				m.Subject = "drop"
				m.Properties["region"] = "us"
				m.Targets = m.Targets[:1]
			}
		}
		if lane == 7 && i == 2 {
			m.Targets = nil
			m.Outcome = "rollback"
		}
		p.Inputs = append(p.Inputs, m)
	}
	return p
}

func sumBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func encoded(value any) ([]byte, error) { return json.Marshal(value) }

func atomicJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(data, '\n'))
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

type journal struct {
	file  *os.File
	hash  hash.Hash
	count uint64
}

func newJournal(path string) (*journal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &journal{file: f, hash: sha256.New()}, nil
}

func (j *journal) add(value any) error {
	data, err := encoded(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := j.file.Write(data); err != nil {
		return err
	}
	if err := j.file.Sync(); err != nil {
		return err
	}
	_, _ = j.hash.Write(data)
	j.count++
	return nil
}

func (j *journal) digest() string { return hex.EncodeToString(j.hash.Sum(nil)) }

type batchRecord struct {
	Version      int      `json:"recipe_version"`
	Seed         string   `json:"seed"`
	Lane         int      `json:"lane"`
	Batch        uint64   `json:"batch"`
	CreatedAt    int64    `json:"created_at_ms"`
	VerifiedAt   int64    `json:"verified_at_ms"`
	ExpectedHash string   `json:"expected_sha256"`
	AckHash      string   `json:"ack_sha256"`
	ObservedHash string   `json:"observed_sha256"`
	AckRecords   uint64   `json:"ack_records"`
	Observations uint64   `json:"observation_records"`
	LogicalSends uint64   `json:"logical_sends"`
	Deliveries   uint64   `json:"fresh_deliveries"`
	Replays      uint64   `json:"replayed_deliveries"`
	Recipes      []string `json:"recipes"`
	Previous     string   `json:"previous_sha256"`
	Hash         string   `json:"sha256"`
}

func recordHash(record batchRecord) (string, error) {
	record.Hash = ""
	data, err := encoded(record)
	return sumBytes(data), err
}

type batchLedger struct {
	plan         plan
	dir          string
	expectedHash string
	ack          *journal
	observed     *journal
}

func beginBatch(root string, p plan) (*batchLedger, error) {
	dir := filepath.Join(root, "pending", laneNames[p.Lane])
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, fmt.Errorf("pending batch directory: %w", err)
	}
	data, err := encoded(p)
	if err != nil {
		return nil, err
	}
	if err := atomicJSON(filepath.Join(dir, "expected.json"), p); err != nil {
		return nil, err
	}
	ack, err := newJournal(filepath.Join(dir, "ack.jsonl"))
	if err != nil {
		return nil, err
	}
	obs, err := newJournal(filepath.Join(dir, "observed.jsonl"))
	if err != nil {
		return nil, join(err, ack.file.Close())
	}
	return &batchLedger{plan: p, dir: dir, expectedHash: sumBytes(data), ack: ack, observed: obs}, nil
}

func (b *batchLedger) close() error { return join(b.ack.file.Close(), b.observed.file.Close()) }

// remove is called only after the verified record and its hash chain are synced.
func (b *batchLedger) remove() error {
	for _, name := range []string{"expected.json", "ack.jsonl", "observed.jsonl"} {
		if err := os.Remove(filepath.Join(b.dir, name)); err != nil {
			return err
		}
	}
	return os.Remove(b.dir)
}

func checkContent(m *mq.Message, expected input) error {
	props, err := encoded(m.Properties)
	if err != nil {
		return err
	}
	wantProps, err := encoded(expected.Properties)
	if err != nil {
		return err
	}
	if m.MessageID != expected.ID || !bytes.Equal(m.Body, expected.Body) || sumBytes(m.Body) != expected.BodyHash ||
		m.GroupID != expected.Group || m.Subject != expected.Subject || m.CorrelationID != expected.Correlation ||
		m.ReplyTo != expected.Reply || m.ContentType != expected.ContentType || !bytes.Equal(props, wantProps) {
		return fmt.Errorf("identity/full body/metadata mismatch for expected %s", expected.ID)
	}
	return nil
}
