#!/usr/bin/env bash
# Orchestrator: build mqlite, boot an ephemeral broker, run all three suites
# (curl API / Python behaviour / Go SDK), aggregate, tear down.
#
#   ./test/run.sh            # against a fresh local SQLite file
#   ./test/run.sh --turso    # broker backs onto remote Turso (needs MQLITE_DB[+token])
#   ./test/run.sh --keep     # keep the temp dir + broker log
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)

USE_TURSO=0; KEEP=0
for a in "$@"; do case "$a" in
  --turso) USE_TURSO=1 ;;
  --keep)  KEEP=1 ;;
  -h|--help) echo "usage: run.sh [--turso] [--keep]"; exit 0 ;;
  *) echo "unknown arg: $a"; exit 2 ;;
esac; done

TMP=$(mktemp -d)
PORT=${MQLITE_TEST_PORT:-$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()' 2>/dev/null || echo 8077)}
export ENDPOINT="http://127.0.0.1:$PORT"
export TOKEN="mqk_test_$$"
export RUNID="r$(date +%s)x$$"
export MQLITE_MAX_MESSAGE_BYTES=65536   # small cap so the size-boundary test is cheap

DB="file:$TMP/mq.db"; DBTOK=""; LABEL="local SQLite"
if [ "$USE_TURSO" = 1 ]; then
  : "${MQLITE_DB:?--turso requires MQLITE_DB=libsql://...}"
  DB="$MQLITE_DB"; DBTOK="${MQLITE_DB_AUTH_TOKEN:-}"; LABEL="remote Turso"
fi

echo "==> building mqlite"
( cd "$ROOT" && go build -o "$TMP/mqlite" ./cmd/mqlite ) || { echo "build failed"; exit 1; }

echo "==> starting broker on :$PORT  (backing: $LABEL)"
MQLITE_DB="$DB" MQLITE_DB_AUTH_TOKEN="$DBTOK" MQLITE_TOKENS="$TOKEN" \
  MQLITE_MAX_MESSAGE_BYTES="$MQLITE_MAX_MESSAGE_BYTES" \
  "$TMP/mqlite" serve --addr "127.0.0.1:$PORT" > "$TMP/broker.log" 2>&1 &
BROKER=$!

cleanup() {
  kill "$BROKER" 2>/dev/null
  wait "$BROKER" 2>/dev/null
  if [ "$KEEP" = 1 ]; then echo "(kept $TMP)"; else rm -rf "$TMP"; fi
}
trap cleanup EXIT

ok=0
for _ in $(seq 1 60); do
  if curl -sf "$ENDPOINT/healthz" >/dev/null 2>&1; then ok=1; break; fi
  if ! kill -0 "$BROKER" 2>/dev/null; then echo "broker exited early:"; cat "$TMP/broker.log"; exit 1; fi
  sleep 0.25
done
[ "$ok" = 1 ] || { echo "broker did not become healthy:"; cat "$TMP/broker.log"; exit 1; }

rc=0
echo;  echo "########## 1/3  API via curl ##########"
bash "$HERE/api_curl.sh" || rc=1
echo;  echo "########## 2/3  Behaviour via Python ##########"
python3 "$HERE/api_tests.py" || rc=1
echo;  echo "########## 3/3  SDK via Go ##########"
( cd "$ROOT" && MQLITE_ENDPOINT="$ENDPOINT" MQLITE_TOKEN="$TOKEN" MQLITE_RUNID="$RUNID" go run ./test/sdkcheck ) || rc=1

echo
if [ "$rc" = 0 ]; then echo "✅ ALL SUITES PASSED"; else echo "❌ SOME SUITES FAILED (broker log: $TMP/broker.log)"; KEEP=1; fi
exit $rc
