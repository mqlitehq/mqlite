# Bench image: builds the load generator + bundles system-monitoring tools.
# Built at NATIVE arch (no --platform) so CPU/throughput reflect real hardware,
# not qemu emulation. Build context = module root:  docker build -f bench/Dockerfile .
FROM golang:1.25-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
      sysstat procps coreutils && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /usr/local/bin/mqlite-bench ./cmd/mqlite-bench
COPY bench/entry.sh /usr/local/bin/entry.sh
RUN chmod +x /usr/local/bin/entry.sh
# /data is the container's own (VM ext4) filesystem — real disk I/O, NOT a slow
# bind mount. Results are copied out by run-bench.sh via `docker cp`.
ENV BENCH_DUR=5s BENCH_MSG=256
ENTRYPOINT ["/usr/local/bin/entry.sh"]
