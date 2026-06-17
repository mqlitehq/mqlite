package engine

// schemaVersion is bumped when the DDL below changes incompatibly.
const schemaVersion = "1"

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

	// core message table. id (INTEGER PRIMARY KEY = rowid) is the broker-assigned,
	// monotonic, gap-free seq_number (ASB SequenceNumber analogue).
	`CREATE TABLE IF NOT EXISTS messages (
	    id             INTEGER PRIMARY KEY,
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
	    session_id     TEXT,
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
	// partial index for session/group-ordered claim — only session queues pay for it (§11.1).
	`CREATE INDEX IF NOT EXISTS idx_msg_group_inflight ON messages(queue, session_id, state, locked_until)
	  WHERE session_id IS NOT NULL`,

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

	`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT`,
}
