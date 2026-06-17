#!/usr/bin/env bash
# Shared helpers for the mqlite shell test suites.
# Expects ENDPOINT and TOKEN in the environment (set by run.sh).

: "${ENDPOINT:?set ENDPOINT}"
: "${TOKEN:?set TOKEN}"
: "${RUNID:=local}"

PASS=0
FAIL=0
RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; DIM=$'\033[2m'; NC=$'\033[0m'

section() { printf "\n${YEL}── %s${NC}\n" "$1"; }
pass()    { PASS=$((PASS + 1)); printf "  ${GRN}✓${NC} %s\n" "$1"; }
fail()    { FAIL=$((FAIL + 1)); printf "  ${RED}✗ %s${NC}\n" "$1"; }

assert_eq() { # expected actual label
  if [ "$1" = "$2" ]; then pass "$3"; else fail "$3 ${DIM}(expected[$1] got[$2])${NC}"; fi
}
assert_ge() { # min actual label
  if [ "$2" -ge "$1" ] 2>/dev/null; then pass "$3"; else fail "$3 ${DIM}(want >=$1 got[$2])${NC}"; fi
}
assert_contains() { # needle haystack label
  case "$2" in *"$1"*) pass "$3";; *) fail "$3 ${DIM}(no '$1' in '$2')${NC}";; esac
}
assert_nonempty() { # value label
  if [ -n "$1" ] && [ "$1" != "null" ]; then pass "$2"; else fail "$2 ${DIM}(empty/null)${NC}"; fi
}

b64() { printf '%s' "$1" | base64 | tr -d '\n'; }
jqr() { printf '%s' "$1" | jq -r "$2" 2>/dev/null; }

# _api TOKEN PATH JSON  -> sets API_STATUS, API_BODY
_api() {
  local tok="$1" path="$2" data="$3"
  local args=(-s -w $'\n%{http_code}' -H 'Content-Type: application/json' --data "$data")
  [ -n "$tok" ] && args+=(-H "Authorization: Bearer $tok")
  local resp
  resp=$(curl "${args[@]}" "$ENDPOINT$path")
  API_STATUS="${resp##*$'\n'}"
  API_BODY="${resp%$'\n'*}"
}
# api PATH JSON  (authenticated with $TOKEN)
api() { _api "$TOKEN" "$1" "$2"; }

# Connect-style endpoint paths
P_SEND=/mqlite.v1.QueueService/Send
P_SCHED=/mqlite.v1.QueueService/Schedule
P_RECV=/mqlite.v1.QueueService/Receive
P_COMP=/mqlite.v1.QueueService/Complete
P_ABAN=/mqlite.v1.QueueService/Abandon
P_DEADL=/mqlite.v1.QueueService/DeadLetter
P_DEFER=/mqlite.v1.QueueService/Defer
P_RDEFER=/mqlite.v1.QueueService/ReceiveDeferred
P_RENEW=/mqlite.v1.QueueService/RenewLock
P_PEEK=/mqlite.v1.QueueService/Peek
P_METRICS=/mqlite.v1.QueueService/GetQueueMetrics
P_CQ=/mqlite.v1.AdminService/CreateQueue
P_CSUB=/mqlite.v1.AdminService/CreateSubscription
P_LISTQ=/mqlite.v1.AdminService/ListQueues
P_REDRIVE=/mqlite.v1.AdminService/Redrive

# create_queue NAME [CONFIG_JSON]
create_queue() {
  local name="$1" cfg="${2:-}"
  [ -z "$cfg" ] && cfg='{}'
  api "$P_CQ" "{\"name\":\"$name\",\"config\":$cfg}"
}

summary() { # suite-name
  printf "\n%s: ${GRN}%d passed${NC}, ${RED}%d failed${NC}\n" "$1" "$PASS" "$FAIL"
  [ "$FAIL" -eq 0 ]
}
