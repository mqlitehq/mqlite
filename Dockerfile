# mqlite broker — single static pure-Go binary (no CGO).
# Build:  docker build --platform linux/amd64 -t mqlite:dev .
# Run:    docker run --platform linux/amd64 -p 6754:6754 -e MQLITE_TOKENS=mqk_dev mqlite:dev

# golang:1.25-alpine is a rolling tag — each pull is the latest 1.25.x patch, so the
# release binary always carries the current Go stdlib security fixes. Do NOT pin to a
# specific old patch (e.g. 1.25.9), which would ship known stdlib CVEs. CI's
# govulncheck job (go-version: stable) gates the same.
#
# --platform=$BUILDPLATFORM keeps the Go toolchain on the builder's native arch (no
# QEMU); we cross-compile with GOARCH=$TARGETARCH, so one buildx run produces both
# linux/amd64 and linux/arm64 images. A plain `docker build` still works — TARGETARCH
# is auto-set by BuildKit, and falls back to amd64 if absent.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO_ENABLED=0 -> fully static binary (modernc sqlite + libsql are pure Go).
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags "-s -w" -o /out/mqlite ./cmd/mqlite

FROM alpine:3.20
# ca-certificates: TLS to a remote Turso/libSQL DSN (x509 verification).
# tzdata: named time zones for TZ / expr date(...,tz) — core mqlite is epoch-ms UTC,
# so this is only for correctness when a non-UTC zone is actually used.
RUN apk add --no-cache ca-certificates tzdata && mkdir -p /data
COPY --from=build /out/mqlite /usr/local/bin/mqlite
EXPOSE 6754
# Default to a local file DB on the /data volume. Override MQLITE_DB with a
# libsql://... URL (+ MQLITE_DB_AUTH_TOKEN) to use remote Turso instead.
ENV MQLITE_DB=file:/data/mq.db
VOLUME ["/data"]
ENTRYPOINT ["mqlite"]
# No --addr: use the built-in default :6754 (MQLITE_ADDR can still override it).
CMD ["serve"]
