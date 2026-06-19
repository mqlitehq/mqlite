#!/usr/bin/env bash
# API suite via raw curl — proves every endpoint is curl-able and the wire format
# / auth / error codes behave. (The "用 API" path; mirrors design §7.4.)
set -uo pipefail
cd "$(dirname "$0")"
# shellcheck source=lib.sh
. ./lib.sh

echo "API (curl) suite — endpoint=$ENDPOINT runid=$RUNID"

# ── health & auth ────────────────────────────────────────────────────────────
section "health & auth"
H=$(curl -s "$ENDPOINT/healthz"); assert_eq "ok" "$H" "GET /healthz"
_api ""      "$P_LISTQ" '{}'; assert_eq 401 "$API_STATUS" "no token -> 401"
_api "nope"  "$P_LISTQ" '{}'; assert_eq 401 "$API_STATUS" "wrong token -> 401"
api          "$P_LISTQ" '{}'; assert_eq 200 "$API_STATUS" "valid token -> 200"

# ── queue lifecycle ──────────────────────────────────────────────────────────
section "queue: create / send / receive / complete"
Q="${RUNID}_basic"
create_queue "$Q" '{"max_delivery_count":3}'; assert_eq 200 "$API_STATUS" "CreateQueue"

api "$P_SEND" "{\"queue\":\"$Q\",\"messages\":[{\"body\":\"$(b64 hello)\",\"message_id\":\"m1\"}]}"
S1=$(jqr "$API_BODY" '.seq_numbers[0]')
assert_eq 200 "$API_STATUS" "Send single -> 200"
assert_ge 1 "$S1" "Send returns a seq_number"

api "$P_SEND" "{\"queue\":\"$Q\",\"messages\":[{\"body\":\"$(b64 a)\"},{\"body\":\"$(b64 b)\"},{\"body\":\"$(b64 c)\"}]}"
assert_eq 3 "$(jqr "$API_BODY" '.seq_numbers | length')" "SendBatch returns 3 seqs"

api "$P_RECV" "{\"queue\":\"$Q\",\"max_messages\":2,\"wait_time_ms\":3000}"
assert_eq 2 "$(jqr "$API_BODY" '.messages | length')" "Receive(max=2) returns 2"
T0=$(jqr "$API_BODY" '.messages[0].lock_token'); SS0=$(jqr "$API_BODY" '.messages[0].seq_number')
B0=$(jqr "$API_BODY" '.messages[0].body')
assert_nonempty "$T0" "received message carries a lock_token"
assert_eq "$(b64 hello)" "$B0" "body round-trips as base64"

api "$P_COMP" "{\"queue\":\"$Q\",\"seq_number\":$SS0,\"lock_token\":\"$T0\"}"
assert_eq true "$(jqr "$API_BODY" '.ok')" "Complete with valid token -> ok:true"
# idempotent settle (MQLITE-8): replaying the same (seq, token) is a no-op success.
api "$P_COMP" "{\"queue\":\"$Q\",\"seq_number\":$SS0,\"lock_token\":\"$T0\"}"
assert_eq true "$(jqr "$API_BODY" '.ok')" "Complete replay (same token) -> ok:true (idempotent)"
# fencing: a wrong / never-issued token is rejected as 409 lock_lost, never silent ok.
api "$P_COMP" "{\"queue\":\"$Q\",\"seq_number\":$SS0,\"lock_token\":\"deadbeef\"}"
assert_eq 409 "$API_STATUS" "Complete with wrong token -> 409 lock_lost"

# ── error codes ──────────────────────────────────────────────────────────────
section "error handling"
api "$P_SEND" "{\"queue\":\"${RUNID}_missing\",\"messages\":[{\"body\":\"$(b64 x)\"}]}"
assert_eq 404 "$API_STATUS" "Send to missing queue -> 404"
assert_contains '"code":"not_found"' "$API_BODY" "error envelope has code not_found"

# ── peek / metrics / list ────────────────────────────────────────────────────
section "peek / metrics / list"
api "$P_PEEK" "{\"queue\":\"$Q\",\"max\":10}"
assert_contains '"state"' "$API_BODY" "Peek returns a state field"
case "$API_BODY" in *lock_token*) fail "Peek must NOT leak lock_token";; *) pass "Peek does not expose lock_token";; esac
api "$P_METRICS" "{\"queue\":\"$Q\"}"
assert_nonempty "$(jqr "$API_BODY" '.total')" "Metrics returns total"
api "$P_LISTQ" '{}'
assert_contains "$Q" "$API_BODY" "ListQueues contains our queue"

# ── topic fan-out + filter ───────────────────────────────────────────────────
section "topic: fan-out + subject-prefix filter"
T="${RUNID}_events"
api "$P_CSUB" "{\"topic\":\"$T\",\"name\":\"${T}_all\"}";  assert_eq 200 "$API_STATUS" "CreateSubscription all"
api "$P_CSUB" "{\"topic\":\"$T\",\"name\":\"${T}_paid\",\"filter\":{\"subject_prefix\":\"payment.\"}}"; assert_eq 200 "$API_STATUS" "CreateSubscription paid(filter)"
api "$P_SEND" "{\"queue\":\"$T\",\"messages\":[{\"body\":\"$(b64 o)\",\"subject\":\"order.created\"}]}"
api "$P_SEND" "{\"queue\":\"$T\",\"messages\":[{\"body\":\"$(b64 p)\",\"subject\":\"payment.captured\"}]}"
api "$P_METRICS" "{\"queue\":\"${T}_all\"}";  assert_eq 2 "$(jqr "$API_BODY" '.active')" "sub 'all' received both"
api "$P_METRICS" "{\"queue\":\"${T}_paid\"}"; assert_eq 1 "$(jqr "$API_BODY" '.active')" "sub 'paid' filtered to payment.*"

# ── dead-letter + redrive ────────────────────────────────────────────────────
section "dead-letter (max delivery) + redrive"
QD="${RUNID}_dlq"
create_queue "$QD" '{"max_delivery_count":2}'
api "$P_SEND" "{\"queue\":\"$QD\",\"messages\":[{\"body\":\"$(b64 poison)\"}]}"
for i in 1 2; do
  api "$P_RECV" "{\"queue\":\"$QD\",\"max_messages\":1,\"wait_time_ms\":2000}"
  st=$(jqr "$API_BODY" '.messages[0].lock_token'); ss=$(jqr "$API_BODY" '.messages[0].seq_number')
  api "$P_ABAN" "{\"queue\":\"$QD\",\"seq_number\":$ss,\"lock_token\":\"$st\"}"
done
api "$P_PEEK" "{\"queue\":\"$QD\",\"state\":\"dead_lettered\",\"max\":10}"
assert_ge 1 "$(jqr "$API_BODY" '.messages | length')" "message landed in DLQ after max deliveries"
api "$P_METRICS" "{\"queue\":\"$QD\"}"; assert_eq 1 "$(jqr "$API_BODY" '.dead_lettered')" "metrics show dead_lettered=1"
api "$P_REDRIVE" "{\"queue\":\"$QD\"}"; assert_ge 1 "$(jqr "$API_BODY" '.moved')" "Redrive moved >=1 back to active"
api "$P_RECV" "{\"queue\":\"$QD\",\"max_messages\":1,\"wait_time_ms\":2000}"
assert_ge 1 "$(jqr "$API_BODY" '.messages | length')" "redriven message is receivable again"

# ── defer / receive-deferred ─────────────────────────────────────────────────
section "defer / receive-deferred"
QF="${RUNID}_defer"
create_queue "$QF"
api "$P_SEND" "{\"queue\":\"$QF\",\"messages\":[{\"body\":\"$(b64 later)\"}]}"
SF=$(jqr "$API_BODY" '.seq_numbers[0]')
api "$P_RECV" "{\"queue\":\"$QF\",\"max_messages\":1,\"wait_time_ms\":2000}"
TF=$(jqr "$API_BODY" '.messages[0].lock_token')
api "$P_DEFER" "{\"queue\":\"$QF\",\"seq_number\":$SF,\"lock_token\":\"$TF\"}"
assert_eq true "$(jqr "$API_BODY" '.ok')" "Defer -> ok:true"
api "$P_RECV" "{\"queue\":\"$QF\",\"max_messages\":1,\"wait_time_ms\":500}"
assert_eq 0 "$(jqr "$API_BODY" '.messages | length')" "deferred message hidden from normal receive"
api "$P_RDEFER" "{\"queue\":\"$QF\",\"seq_numbers\":[$SF]}"
assert_eq 1 "$(jqr "$API_BODY" '.messages | length')" "ReceiveDeferred fetches it by seq"

summary "API (curl)"
