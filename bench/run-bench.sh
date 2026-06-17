#!/usr/bin/env bash
# Build the bench image (native arch) and run the stress matrix in a container,
# keeping the DB on the container's own fast filesystem (NOT a bind mount), then
# copy the results + monitor logs out.
#
#   ./bench/run-bench.sh                 # 5s/scenario, 256B messages
#   BENCH_DUR=10s BENCH_MSG=1024 ./bench/run-bench.sh
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
OUT="$HERE/out"
DUR="${BENCH_DUR:-5s}"
MSG="${BENCH_MSG:-256}"
NAME="mqlite-bench-run"

rm -rf "$OUT"; mkdir -p "$OUT"

echo "==> building bench image (native arch)"
docker build -f "$HERE/Dockerfile" -t mqlite-bench:latest "$ROOT"

docker rm -f "$NAME" >/dev/null 2>&1 || true
echo "==> running matrix (dur=$DUR msg=${MSG}B) — DB on container fs"
docker run --name "$NAME" -e BENCH_DUR="$DUR" -e BENCH_MSG="$MSG" mqlite-bench:latest

echo "==> copying results out"
docker cp "$NAME:/data/." "$OUT/"
docker rm -f "$NAME" >/dev/null 2>&1 || true

echo "==> artifacts:"
ls -la "$OUT"
echo "results.json -> $OUT/results.json"
