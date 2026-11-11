package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type entry struct {
	ID, Queue string
	Body      []byte
	Metadata  messageMetadata
}

type messageMetadata struct {
	GroupID     string            `json:"group_id"`
	Subject     string            `json:"subject"`
	ContentType string            `json:"content_type"`
	Properties  map[string]string `json:"properties"`
}

func metadataFor(profile, id string) messageMetadata {
	if profile != "escaped" {
		return messageMetadata{}
	}
	return messageMetadata{GroupID: id + "-group", Subject: "receive \"qualification\"\x00", ContentType: "application/octet-stream", Properties: map[string]string{"escaped": strings.Repeat("\x00", 800), "quote": "\"\\\n<>&"}}
}

func metadataDigest(value messageMetadata) string {
	raw, err := json.Marshal(value)
	if err != nil {
		panic("fixed message metadata cannot be encoded")
	}
	return hashBytes(raw)
}

type identity struct {
	ID              string `json:"id"`
	Queue           string `json:"queue"`
	BodySHA256      string `json:"body_sha256"`
	MetadataSHA256  string `json:"metadata_sha256,omitempty"`
	MetadataProfile string `json:"metadata_profile,omitempty"`
	Sequence        int64  `json:"sequence"`
	Acknowledged    bool   `json:"acknowledged"`
	Deliveries      int    `json:"deliveries"`
	Completed       bool   `json:"completed"`
}

type resource struct {
	Name     string `json:"name"`
	Eligible bool   `json:"eligible_for_cleanup"`
	Created  bool   `json:"create_acknowledged"`
	Deleted  bool   `json:"deletion_confirmed"`
	Absent   bool   `json:"confirmed_absent"`
}

type safeFailure struct {
	Stage       string `json:"stage"`
	Category    string `json:"category"`
	Fingerprint string `json:"fingerprint"`
	HTTPStatus  int    `json:"http_status,omitempty"`
}

type operation struct {
	Stage      string `json:"stage"`
	StartedUTC string `json:"started_utc"`
	ElapsedMs  int64  `json:"elapsed_ms"`
	Finished   bool   `json:"finished"`
	Success    bool   `json:"success"`
}

type managedCheck struct {
	Queue       string `json:"queue"`
	Invocations int64  `json:"handler_invocations"`
	Joined      bool   `json:"run_joined"`
}

type report struct {
	Scope           string         `json:"scope"`
	Pass            bool           `json:"pass"`
	Prefix          string         `json:"prefix"`
	RuntimeSHA      string         `json:"runtime_sha"`
	SDKSHA          string         `json:"sdk_sha"`
	HarnessSHA      string         `json:"harness_sha"`
	ScenarioSeconds int            `json:"scenario_seconds"`
	CleanupSeconds  int            `json:"cleanup_seconds"`
	Resources       []resource     `json:"resources"`
	Identities      []identity     `json:"identities"`
	Operations      []operation    `json:"operations"`
	Failures        []safeFailure  `json:"failures"`
	Trace           []rpcEvent     `json:"rpc_trace"`
	Managed         []managedCheck `json:"managed_checks"`
}

type ledger struct {
	mu       sync.Mutex
	output   string
	report   report
	expected map[string]entry
	failAt   string
}

func hashBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (l *ledger) saveLocked() error {
	raw, err := json.MarshalIndent(l.report, "", "  ")
	if err != nil {
		return err
	}
	temporary := filepath.Join(l.output, "evidence.json.tmp")
	if err := os.WriteFile(temporary, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(l.output, "evidence.json"))
}

func (l *ledger) save() error { l.mu.Lock(); defer l.mu.Unlock(); return l.saveLocked() }

func (l *ledger) fail(stage string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	category := "operation"
	if errors.Is(err, context.DeadlineExceeded) {
		category = "deadline"
	}
	if errors.Is(err, context.Canceled) {
		category = "canceled"
	}
	var response interface{ HTTPStatus() int }
	status := 0
	if errors.As(err, &response) {
		status = response.HTTPStatus()
		if category == "operation" && status >= 500 {
			category = "http_5xx"
		}
	}
	l.report.Failures = append(l.report.Failures, safeFailure{Stage: stage, Category: category, Fingerprint: hashBytes([]byte(err.Error())), HTTPStatus: status})
	l.report.Pass = false
}

func (l *ledger) timed(stage string, fn func() error) error {
	start := time.Now()
	l.mu.Lock()
	index := len(l.report.Operations)
	l.report.Operations = append(l.report.Operations, operation{Stage: stage, StartedUTC: start.UTC().Format(time.RFC3339Nano)})
	l.mu.Unlock()
	if err := l.save(); err != nil {
		return err
	}
	err := fn()
	l.mu.Lock()
	l.report.Operations[index].ElapsedMs = time.Since(start).Milliseconds()
	l.report.Operations[index].Finished = true
	l.report.Operations[index].Success = err == nil
	l.mu.Unlock()
	if err != nil {
		l.fail(stage, err)
	}
	if writeErr := l.save(); writeErr != nil {
		return writeErr
	}
	return err
}

func (l *ledger) register(queue string, count, size int) []entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	entries := make([]entry, count)
	for i := range entries {
		id := fmt.Sprintf("%s-%d", queue, i)
		body := append([]byte(id+":"), bytes.Repeat([]byte{'x'}, size)...)
		entries[i] = entry{ID: id, Queue: queue, Body: body}
		l.expected[id] = entries[i]
		l.report.Identities = append(l.report.Identities, identity{ID: id, Queue: queue, BodySHA256: hashBytes(body)})
	}
	return entries
}

func (l *ledger) acknowledge(entries []entry, seqs []int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(entries) != len(seqs) {
		return errors.New("send acknowledgement length mismatch")
	}
	seen := map[int64]bool{}
	seenIDs := map[string]bool{}
	indexes := make([]int, len(entries))
	for i, item := range entries {
		if seqs[i] <= 0 || seen[seqs[i]] || seenIDs[item.ID] {
			return errors.New("send acknowledgement sequence invalid")
		}
		seen[seqs[i]] = true
		seenIDs[item.ID] = true
		found := false
		for j := range l.report.Identities {
			record := &l.report.Identities[j]
			if record.ID == item.ID {
				if record.Acknowledged {
					return errors.New("duplicate send acknowledgement")
				}
				indexes[i] = j
				found = true
			}
		}
		if !found {
			return errors.New("unknown send identity")
		}
	}
	for i, index := range indexes {
		l.report.Identities[index].Acknowledged = true
		l.report.Identities[index].Sequence = seqs[i]
	}
	return nil
}

func (l *ledger) delivered(message delivery) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	item, ok := l.expected[message.ID]
	if !ok || item.Queue != message.Queue || !bytes.Equal(item.Body, message.Body) {
		return errors.New("unexpected message identity or value")
	}
	for i := range l.report.Identities {
		record := &l.report.Identities[i]
		if record.ID == message.ID {
			if !record.Acknowledged || record.Sequence != message.Sequence || record.Deliveries != 0 || message.Count != 1 {
				return errors.New("sequence, delivery count or duplicate mismatch")
			}
			record.Deliveries++
			return nil
		}
	}
	return errors.New("missing expected identity")
}

func (l *ledger) completed(messages []delivery) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, message := range messages {
		found := false
		for i := range l.report.Identities {
			record := &l.report.Identities[i]
			if record.ID == message.ID {
				if !record.Acknowledged || record.Sequence != message.Sequence || record.Deliveries != 1 || record.Completed {
					return errors.New("completion without one matching delivery")
				}
				record.Completed, found = true, true
			}
		}
		if !found {
			return errors.New("unknown completion identity")
		}
	}
	return nil
}

func (l *ledger) verifyIdentities() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, record := range l.report.Identities {
		if !record.Acknowledged || record.Deliveries != 1 || !record.Completed {
			return errors.New("identity ledger incomplete")
		}
	}
	if len(l.report.Identities) != 10 {
		return errors.New("identity ledger inventory mismatch")
	}
	return nil
}
