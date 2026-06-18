#!/usr/bin/env bash
# Runs inside the bench container: starts system monitors (device I/O, vmstat),
# runs the load generator (which self-attributes CPU + disk I/O per scenario via
# /proc/self), then stops monitors and records the environment.
set -u
OUT=/data
mkdir -p "$OUT"
DUR="${BENCH_DUR:-5s}"
MSG="${BENCH_MSG:-256}"

# environment facts (so the report is reproducible / honest about the host)
{
  echo "=== uname ==="; uname -a
  echo "=== nproc ==="; nproc
  echo "=== CLK_TCK ==="; getconf CLK_TCK
  echo "=== cpu model ==="; grep -m1 'model name' /proc/cpuinfo 2>/dev/null || grep -m1 'Processor' /proc/cpuinfo 2>/dev/null || echo "n/a"
  echo "=== mem ==="; grep -E 'MemTotal' /proc/meminfo
  echo "=== /data filesystem ==="; df -hT "$OUT" | tail -n +1
  echo "=== mount type ==="; stat -f -c '%T' "$OUT"
} > "$OUT/env.txt" 2>&1

# system-wide samplers (1s) for the whole run — corroborate the in-proc probe
( iostat -x 1 > "$OUT/iostat.log" 2>&1 ) & IOSTAT=$!
( vmstat 1     > "$OUT/vmstat.log" 2>&1 ) & VMSTAT=$!

echo ">>> running mqlite-bench (dur=$DUR msg=${MSG}B)"
mqlite-bench -dir "$OUT" -dur "$DUR" -msgsize "$MSG" -out "$OUT/results.json"
RC=$?

kill "$IOSTAT" "$VMSTAT" 2>/dev/null
wait "$IOSTAT" "$VMSTAT" 2>/dev/null
echo ">>> done (rc=$RC); artifacts in $OUT:"
ls -la "$OUT"
exit $RC
