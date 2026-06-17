# mqlite end-to-end test suite

Black-box tests that drive a **running broker** from three angles, so both the
**API** path (raw HTTP) and the **SDK** path (Go) are covered, across every
queue scenario: single requests, batches, topics/subscriptions, ordering,
dead-letter + redrive, defer, schedule, dedup, and fencing.

| File | Lang | What it covers |
|---|---|---|
| `api_curl.sh` | bash + curl + jq | Every HTTP endpoint is curl-able; wire format, auth (401), error codes, fan-out, one DLQ+redrive flow, defer. The **"用 API"** path. |
| `api_tests.py` | Python 3 (stdlib) | Behavioural matrix with an independent HTTP client: lifecycle, visibility timeout, abandon→redelivery, DLQ+redrive, explicit dead-letter, defer, schedule, dedup window + conflict, session ordering, receive-and-delete, fencing. |
| `sdkcheck/main.go` | Go (mqlite SDK) | The **"用 SDK"** path: remote `mqlite.Client` for all the flows above, plus embedded `OpenEmbedded` + `Tx` (same-DB transactional enqueue, commit & rollback) and `Receiver.Run`. |
| `run.sh` | bash | Builds the binary, boots an ephemeral broker, runs all three suites, aggregates pass/fail, tears down. |

## Run

```bash
# from the module root (cc/mqlite)
./test/run.sh
```

Against the live **Turso** backing store (broker stores in Turso; suites still
hit it over HTTP):

```bash
export MQLITE_DB="libsql://<db>.turso.io"
export MQLITE_DB_AUTH_TOKEN="<jwt>"     # from env only — never commit it
./test/run.sh --turso
```

Options: `--keep` (keep the temp dir + broker log), `MQLITE_TEST_PORT=NNNN` to
pin the port.

## Notes

- Each run uses a unique `RUNID` prefix for queue/topic names, so repeated runs
  against a persistent Turso DB don't collide. Local runs use a throwaway temp DB.
- Suites assert on **returned** seq numbers / lock tokens (not absolute ids),
  since `seq_number` is a global rowid — so the same suite is valid on a fresh
  local DB and on a shared remote one.
- The schedule and visibility-timeout tests include short real-time waits
  (~1–3s) because they exercise time-based transitions.
- Each suite exits non-zero on any failed assertion; `run.sh` returns non-zero
  if any suite failed.
