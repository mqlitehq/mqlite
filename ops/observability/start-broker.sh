#!/bin/sh
set -eu
# Read secrets at runtime; Docker's configured environment contains no credentials.
MQLITE_TOKENS=$(cat /run/secrets/admin_token)
MQLITE_MONITOR_TOKENS=$(cat /run/secrets/monitor_token)
export MQLITE_TOKENS MQLITE_MONITOR_TOKENS
exec mqlite serve
