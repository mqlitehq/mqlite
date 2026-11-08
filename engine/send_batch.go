package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unicode/utf8"
)

// A driver batch transports several parameterized statements in one HTTP call,
// inside the existing transaction. These are transport bounds, not additional
// commits: failure in any chunk still rolls back the entire Send.
const (
	remoteSendChunkCount = 64
	remoteSendChunkBytes = 512 << 10
)

func (e *Engine) canBatchDirectSend(targets []target, ms []OutMessage) bool {
	if !e.db.remote || !isHTTPHranaDSN(e.db.dsn) || len(ms) < 2 || len(targets) != 1 ||
		targets[0].fanout || targets[0].queue.expiresAtMs != 0 {
		return false
	}
	for _, m := range ms {
		// The driver's JSON encoder replaces invalid UTF-8. Keep the legacy
		// per-item lookup when a Go map key would not have the stored spelling.
		if !utf8.ValidString(m.MessageID) {
			return false
		}
	}
	return true
}

type directSendInput struct {
	message              OutMessage
	visibleAt, expiresAt int64
	key, hash            string
}

type directDedupEntry struct {
	seq     int64
	hash    sql.NullString
	pending int // zero for a committed row; otherwise an inserts index plus one
}

type directSendSlot struct {
	seq      int64
	pending  int
	conflict bool
}

type directSendPlan struct {
	slots                  []directSendSlot
	inserts                []directSendInput
	deduplicated, rejected int64
}

func prepareDirectSend(ctx context.Context, q queueRow, ms []OutMessage, atMs int64, forced State, now int64) ([]directSendInput, error) {
	inputs := make([]directSendInput, len(ms))
	for i, m := range ms {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		if q.ordering == OrderGroupFIFO && m.GroupID == "" {
			return nil, ErrGroupRequired
		}
		visible, expires, err := messageTimes(q, m, atMs, forced, now)
		if err != nil {
			return nil, err
		}
		in := directSendInput{message: m, visibleAt: visible, expiresAt: expires}
		if q.dedupWindowMs > 0 {
			in.hash = sha256hex(m.Body)
			in.key = m.MessageID
			if in.key == "" {
				in.key = in.hash
			}
		}
		inputs[i] = in
	}
	return inputs, nil
}

func planDirectSend(inputs []directSendInput, dedup bool, existing map[string]directDedupEntry) directSendPlan {
	plan := directSendPlan{slots: make([]directSendSlot, len(inputs))}
	// The snapshot belongs only to this attempt; never mutate a caller's cache
	// or carry an uncommitted sequence into a replayed transaction.
	known := make(map[string]directDedupEntry, len(existing)+len(inputs))
	for key, entry := range existing {
		known[key] = entry
	}
	for i, in := range inputs {
		if dedup {
			if old, ok := known[in.key]; ok {
				if old.hash.Valid && old.hash.String != in.hash {
					plan.slots[i].conflict = true
					plan.rejected++
				} else {
					plan.slots[i].seq = old.seq
					plan.slots[i].pending = old.pending
					plan.deduplicated++
				}
				continue
			}
		}
		next := len(plan.inserts)
		plan.inserts = append(plan.inserts, in)
		plan.slots[i].pending = next + 1
		if dedup {
			known[in.key] = directDedupEntry{hash: sql.NullString{String: in.hash, Valid: true}, pending: next + 1}
		}
	}
	return plan
}

func directSendKeys(inputs []directSendInput) []string {
	seen := make(map[string]bool, len(inputs))
	keys := make([]string, 0, len(inputs))
	for _, in := range inputs {
		if !seen[in.key] {
			seen[in.key] = true
			keys = append(keys, in.key)
		}
	}
	return keys
}

func readDirectDedup(ctx context.Context, tx *txn, q queueRow, inputs []directSendInput, now int64) (map[string]directDedupEntry, error) {
	keys := directSendKeys(inputs)
	existing := make(map[string]directDedupEntry, len(keys))
	for start := 0; start < len(keys); {
		end, bytes := start, 128
		for end < len(keys) && end-start < remoteSendChunkCount {
			next := boundedJSONTextBytes(keys[end]) + 64
			if end > start && bytes+next > remoteSendChunkBytes {
				break
			}
			bytes += next
			end++
		}
		args := make([]any, 0, end-start+2)
		args = append(args, q.name, now-q.dedupWindowMs)
		for _, key := range keys[start:end] {
			args = append(args, key)
		}
		query := `SELECT message_id,seq_number,request_hash FROM dedup WHERE queue=? AND seen_at>? AND message_id IN (` +
			strings.TrimSuffix(strings.Repeat("?,", end-start), ",") + `)`
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var key string
			var entry directDedupEntry
			if err = rows.Scan(&key, &entry.seq, &entry.hash); err != nil {
				break
			}
			if _, duplicate := existing[key]; duplicate {
				err = fmt.Errorf("mqlite: duplicate key in dedup lookup response")
				break
			}
			existing[key] = entry
		}
		if err == nil {
			err = rows.Err()
		}
		closeErr := rows.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
		start = end
	}
	return existing, nil
}

// insertDirectBatch is only for a non-expiring direct target. Fan-out and
// expiring subscriptions retain their per-target lifecycle/fence path.
func (e *Engine) insertDirectBatch(ctx context.Context, tx *txn, q queueRow, ms []OutMessage, atMs int64, forced State, now int64) ([]int64, []bool, bool, error) {
	inputs, err := prepareDirectSend(ctx, q, ms, atMs, forced, now)
	if err != nil {
		return nil, nil, false, err
	}
	var existing map[string]directDedupEntry
	if q.dedupWindowMs > 0 {
		existing, err = readDirectDedup(ctx, tx, q, inputs, now)
		if err != nil {
			return nil, nil, false, err
		}
	}
	plan := planDirectSend(inputs, q.dedupWindowMs > 0, existing)
	inserted, err := e.insertDirectMessages(ctx, tx, q.name, plan.inserts, forced, now)
	if err != nil {
		return nil, nil, false, err
	}
	if q.dedupWindowMs > 0 {
		if err = writeDirectDedup(ctx, tx, q.name, plan.inserts, inserted, now); err != nil {
			return nil, nil, false, err
		}
	}
	seqs, conflicts := make([]int64, len(ms)), make([]bool, len(ms))
	for i, slot := range plan.slots {
		conflicts[i] = slot.conflict
		seqs[i] = slot.seq
		if slot.pending > 0 {
			seqs[i] = inserted[slot.pending-1]
		}
	}
	tx.record(q.name, "deduplicated", plan.deduplicated)
	tx.record(q.name, "dedup_conflict", plan.rejected)
	return seqs, conflicts, len(inserted) != 0, nil
}

// insertArgsBytes is a conservative bound for JSON encoding of these fixed
// argument types. One oversized message keeps its existing single-statement
// path; batching must never turn an accepted payload into a new size rejection.
func insertArgsBytes(args []any) int {
	bytes := 2*len(insertMessageSQL) + 512
	for _, arg := range args {
		switch value := arg.(type) {
		case string:
			bytes += boundedJSONTextBytes(value) + 64
		case sql.NullString:
			if value.Valid {
				bytes += boundedJSONTextBytes(value.String) + 64
			}
		case []byte:
			if len(value) > remoteSendChunkBytes*3/4 {
				return remoteSendChunkBytes + 1
			}
			bytes += 4*((len(value)+2)/3) + 64
		default:
			bytes += 64
		}
		if bytes > remoteSendChunkBytes {
			return remoteSendChunkBytes + 1
		}
	}
	return bytes
}

func boundedJSONTextBytes(s string) int {
	if len(s) > remoteSendChunkBytes/6 {
		return remoteSendChunkBytes + 1
	}
	return 6 * len(s)
}

func (e *Engine) insertDirectMessages(ctx context.Context, tx *txn, queue string, inputs []directSendInput, forced State, now int64) ([]int64, error) {
	seqs := make([]int64, len(inputs))
	for start := 0; start < len(inputs); {
		end, bytes := start, 64
		var args []any
		for end < len(inputs) && end-start < remoteSendChunkCount {
			in := inputs[end]
			next, err := messageInsertArgs(queue, in.message, in.visibleAt, in.expiresAt, forced, now)
			if err != nil {
				return nil, err
			}
			size := insertArgsBytes(next)
			if end > start && bytes+size > remoteSendChunkBytes {
				break
			}
			args = append(args, next...)
			bytes += size
			end++
		}
		if end-start == 1 {
			in := inputs[start]
			seq, err := e.rawInsert(ctx, tx, queue, in.message, in.visibleAt, in.expiresAt, forced, now)
			if err != nil {
				return nil, err
			}
			seqs[start] = seq
		} else {
			// Each INSERT has its own result set. The mapping follows statement
			// order, not an assumed order of a multi-row RETURNING result.
			query := strings.Repeat(insertMessageSQL+";", end-start)
			rows, err := tx.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, err
			}
			err = scanInsertSets(rows, seqs[start:end])
			closeErr := rows.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				return nil, err
			}
			tx.record(queue, "enqueued", int64(end-start))
			if forced == StateScheduled {
				tx.record(queue, "scheduled", int64(end-start))
			}
		}
		start = end
	}
	return seqs, nil
}

func scanInsertSets(rows *sql.Rows, seqs []int64) error {
	for i := range seqs {
		if i > 0 && !rows.NextResultSet() {
			if err := rows.Err(); err != nil {
				return err
			}
			return fmt.Errorf("mqlite: missing message insert result")
		}
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return fmt.Errorf("mqlite: message insert returned no sequence")
		}
		if err := rows.Scan(&seqs[i]); err != nil {
			return err
		}
		if seqs[i] <= 0 || rows.Next() {
			return fmt.Errorf("mqlite: invalid message insert result")
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	if rows.NextResultSet() {
		return fmt.Errorf("mqlite: unexpected message insert result set")
	}
	return rows.Err()
}

func writeDirectDedup(ctx context.Context, tx *txn, queue string, inputs []directSendInput, seqs []int64, now int64) error {
	for start := 0; start < len(inputs); {
		end, bytes := start, 256
		var args []any
		for end < len(inputs) && end-start < remoteSendChunkCount {
			in := inputs[end]
			size := boundedJSONTextBytes(queue) + boundedJSONTextBytes(in.key) + boundedJSONTextBytes(in.hash) + 320
			if end > start && bytes+size > remoteSendChunkBytes {
				break
			}
			args = append(args, queue, in.key, in.hash, seqs[end], now)
			bytes += size
			end++
		}
		query := `INSERT INTO dedup (queue,message_id,request_hash,seq_number,seen_at) VALUES ` +
			strings.TrimSuffix(strings.Repeat("(?,?,?,?,?),", end-start), ",") + `
			ON CONFLICT(queue,message_id) DO UPDATE SET request_hash=excluded.request_hash,
			seq_number=excluded.seq_number, seen_at=excluded.seen_at`
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
		start = end
	}
	return nil
}
