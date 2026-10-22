#!/usr/bin/env bash
# Build the linux/amd64 bench image and run the stress matrix in a container,
# keeping the DB on the container's own fast filesystem (NOT a bind mount), then
# copy the results + monitor logs out.
#
#   ./test/bench/run-bench.sh                 # 5s/scenario, 256B messages
#   BENCH_DUR=10s BENCH_MSG=1024 ./test/bench/run-bench.sh
#   BENCH_ONLY=lifecycle_churn BENCH_DUR=2s ./test/bench/run-bench.sh
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
OUT="$HERE/out"
DUR="${BENCH_DUR:-5s}"
MSG="${BENCH_MSG:-256}"
ONLY="${BENCH_ONLY:-}"
NAME="mqlite-bench-run"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

rm -rf "$OUT"; mkdir -p "$OUT"

echo "==> building bench image (linux/amd64)"
docker build --platform linux/amd64 -f "$HERE/Dockerfile" -t mqlite-bench:latest "$ROOT"

docker rm -f "$NAME" >/dev/null 2>&1 || true
echo "==> running matrix (dur=$DUR msg=${MSG}B only=${ONLY:-default}) — DB on container fs"
set +e
docker run --platform linux/amd64 --name "$NAME" -e BENCH_DUR="$DUR" -e BENCH_MSG="$MSG" -e BENCH_ONLY="$ONLY" mqlite-bench:latest
RC=$?

echo "==> copying results out"
docker cp "$NAME:/data/." "$OUT/"
COPY_RC=$?
set -e
if [ "$RC" -eq 0 ] && [ "$COPY_RC" -ne 0 ]; then
  RC=$COPY_RC
fi

echo "==> artifacts:"
ls -la "$OUT"
if [ -f "$OUT/results.json" ]; then
  echo "results.json -> $OUT/results.json"
else
  echo "results.json was not produced"
fi
exit "$RC"
