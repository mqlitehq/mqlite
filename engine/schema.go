package engine

// schemaVersion is an opaque guard token for the current on-disk schema. mqlite keeps
// a single canonical schema (schemaStmts below) and does not migrate: on Open, a DB
// whose recorded version differs is refused with ErrSchemaVersionMismatch (see db.go)
// rather than running today's DDL against a layout it doesn't match. Change the token
// whenever the schema changes incompatibly — pre-1.0 a stale DB is simply recreated.
const schemaVersion = "3"

// schemaStmts is the mqlite SQLite/libSQL schema (design §5.2 + §11.1).
// Executed one statement at a time so it works identically on local modernc
// SQLite and on remote Turso/libSQL (Hrana wants one statement per exec).
// All times are INTEGER Unix milliseconds (epoch ms, UTC).
var schemaStmts = []string{
	// queues / subscriptions metadata. one row = one deliverable target.
	`CREATE TABLE IF NOT EXISTS queues (
	    name                  TEXT PRIMARY KEY,
	    kind                  TEXT NOT NULL DEFAULT 'queue' CHECK (kind IN ('queue','subscription')),
	    lock_duration_ms      INTEGER NOT NULL DEFAULT 30000,
	    max_delivery_count    INTEGER NOT NULL DEFAULT 10,
	    default_ttl_ms        INTEGER NOT NULL DEFAULT 0,
	    dead_letter_on_expire INTEGER NOT NULL DEFAULT 1,
	    dedup_window_ms       INTEGER NOT NULL DEFAULT 0,
	    ordering_mode         TEXT NOT NULL DEFAULT 'standard'
	                              CHECK (ordering_mode IN ('standard','group_fifo','strict_fifo')),
	    -- per-queue DLQ retention overrides (MQLITE-29). 0 = inherit the broker/engine
	    -- default; >0 = this queue's own bound; -1 = explicitly unbounded (opt out of
	    -- the default). Enforced drop-oldest by reapDLQ.
	    dlq_max_age_ms        INTEGER NOT NULL DEFAULT 0,
	    dlq_max_count         INTEGER NOT NULL DEFAULT 0,
	    dlq_max_bytes         INTEGER NOT NULL DEFAULT 0,
	    created_at            INTEGER NOT NULL,
	    updated_at            INTEGER NOT NULL
	) STRICT`,

	// topic -> subscription fan-out roster.
	`CREATE TABLE IF NOT EXISTS subscriptions (
	    topic        TEXT NOT NULL,
	    subscription TEXT NOT NULL,
	    filter_json  TEXT,
	    created_at   INTEGER NOT NULL,
	    PRIMARY KEY (topic, subscription),
	    FOREIGN KEY (subscription) REFERENCES queues(name) ON DELETE CASCADE
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS idx_subs_by_topic ON subscriptions(topic)`,

	// core message table. id (INTEGER PRIMARY KEY AUTOINCREMENT = rowid) is the
	// broker-assigned seq_number (ASB SequenceNumber analogue): strictly increasing for
	// committed messages and NEVER reused, even after the highest row is deleted — without
	// AUTOINCREMENT SQLite would recycle a freed max rowid, letting a stale seq handle alias
	// a later message (MQLITE-71). It may gap (deleting the highest row retires that id — the
	// next insert jumps past it), and it is not a durable cross-lifetime handle: once a
	// message is settled/cancelled its seq is gone for good.
	`CREATE TABLE IF NOT EXISTS messages (
	    id             INTEGER PRIMARY KEY AUTOINCREMENT,
	    queue          TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
	    state          TEXT NOT NULL DEFAULT 'active'
	                       CHECK (state IN ('active','locked','deferred','scheduled','completed','dead_lettered')),
	    visible_at     INTEGER NOT NULL DEFAULT 0,
	    locked_until   INTEGER NOT NULL DEFAULT 0,
	    lock_token     TEXT,
	    delivery_count INTEGER NOT NULL DEFAULT 0,
	    enqueued_at    INTEGER NOT NULL,
	    expires_at     INTEGER NOT NULL DEFAULT 0,
	    message_id     TEXT,
	    correlation_id TEXT,
	    reply_to       TEXT,
	    group_id       TEXT,
	    content_type   TEXT,
	    subject        TEXT,
	    properties     TEXT,
	    body           BLOB NOT NULL,
	    dead_letter_reason      TEXT,
	    dead_letter_description TEXT
	) STRICT`,

	// Hot claim path: a partial index over ACTIVE rows only, ordered by (queue,id).
	// claim seeks straight to the queue's smallest active id regardless of how many
	// locked / dead-lettered / backlogged rows exist — deep-backlog drain stays
	// O(log n) instead of degrading to a scan. Partial also lowers write
	// amplification (locked/completed rows never enter this index).
	`CREATE INDEX IF NOT EXISTS idx_msg_active ON messages(queue, id) WHERE state='active'`,
	`CREATE INDEX IF NOT EXISTS idx_msg_locked    ON messages(state, locked_until)  WHERE state='locked'`,
	`CREATE INDEX IF NOT EXISTS idx_msg_scheduled ON messages(state, visible_at)    WHERE state='scheduled'`,
	`CREATE INDEX IF NOT EXISTS idx_msg_deferred  ON messages(queue, id)            WHERE state='deferred'`,
	`CREATE INDEX IF NOT EXISTS idx_msg_expire    ON messages(expires_at)           WHERE expires_at>0`,
	`CREATE INDEX IF NOT EXISTS idx_msg_dlq       ON messages(queue, id)            WHERE state='dead_lettered'`,
	// partial index for group-ordered claim — only grouped messages pay for it (§11.1).
	// The split per-state head-of-line probe in claimSQL seeks this by its
	// (queue,group_id,state) prefix (MQLITE-22). The trailing locked_until column
	// is no longer read by any probe (MQLITE-56 made the locked probe ignore
	// expiry) but stays: dropping it would change the schema token and orphan
	// every existing DB for zero read benefit.
	`CREATE INDEX IF NOT EXISTS idx_msg_group_inflight ON messages(queue, group_id, state, locked_until)
	  WHERE group_id IS NOT NULL`,
	// Per-state head-of-line probes for the ordered-claim paths (MQLITE-22): each
	// in-flight state gets a (queue,id) partial index so claim's per-state EXISTS
	// seeks (queue, id<?) instead of a backward rowid scan. deferred already has
	// idx_msg_deferred above; scheduled and locked get matching indexes here.
	// group_fifo uses the covering idx_msg_group_inflight instead; strict_fifo has
	// no group column and needs these. A single (queue,state,id) partial over all
	// in-flight states does NOT work — SQLite won't match a `state IN(...)` partial
	// predicate to a single state= probe. And older SQLite (modernc v1.36.1, the
	// embed-compat floor) won't reuse idx_msg_locked(state,locked_until) for the
	// locked branch — hence a dedicated idx_msg_locked_head. In-flight rows are
	// sparse, so these stay tiny.
	`CREATE INDEX IF NOT EXISTS idx_msg_sched_head  ON messages(queue, id) WHERE state='scheduled'`,
	`CREATE INDEX IF NOT EXISTS idx_msg_locked_head ON messages(queue, id) WHERE state='locked'`,

	// optional dedup table (message_id + sliding window). active only when dedup_window_ms>0.
	`CREATE TABLE IF NOT EXISTS dedup (
	    queue        TEXT NOT NULL,
	    message_id   TEXT NOT NULL,
	    request_hash TEXT,
	    seq_number   INTEGER NOT NULL,
	    seen_at      INTEGER NOT NULL,
	    PRIMARY KEY (queue, message_id)
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS idx_dedup_seen ON dedup(seen_at)`,

	// settlement receipts make settle (Complete/Abandon/DeadLetter/Defer) idempotent
	// under client RPC retries: a dropped settle response that the client retries
	// finds the receipt and returns success instead of a spurious LockLost. Keyed
	// by lock_token (unique per claim); expires_at bounds growth (janitor sweeps it).
	`CREATE TABLE IF NOT EXISTS settlement_receipts (
	    lock_token  TEXT PRIMARY KEY,
	    operation   TEXT NOT NULL,
	    created_at  INTEGER NOT NULL,
	    expires_at  INTEGER NOT NULL
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS idx_settlement_expire ON settlement_receipts(expires_at)`,

	// receive attempts make Receive idempotent under client RPC retries: a dropped
	// Receive response replays the same batch (same lock tokens) instead of burning
	// delivery_count / leaving in-flight black holes. Opt-in via a client attempt id.
	`CREATE TABLE IF NOT EXISTS receive_attempts (
	    queue       TEXT NOT NULL,
	    attempt_id  TEXT NOT NULL,
	    response    TEXT NOT NULL,
	    created_at  INTEGER NOT NULL,
	    expires_at  INTEGER NOT NULL,
	    PRIMARY KEY (queue, attempt_id)
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS idx_recv_attempt_expire ON receive_attempts(expires_at)`,

	`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT`,
}
