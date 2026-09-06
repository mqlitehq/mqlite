# mqlite broker — single static pure-Go binary (no CGO).
# Build:  docker build --platform linux/amd64 -t mqlite:dev .
# Run:    docker run --platform linux/amd64 -p 6754:6754 -e MQLITE_TOKENS=mqk_dev mqlite:dev

# Use supported Go and Alpine branches. Scan the actual output binary during every
# build; a source scan under CI's different toolchain cannot validate this artifact.
#
# --platform=$BUILDPLATFORM keeps the Go toolchain on the builder's native arch (no
# QEMU); we cross-compile with GOARCH=$TARGETARCH, so one buildx run produces both
# linux/amd64 and linux/arm64 images. A plain `docker build` still works — TARGETARCH
# is auto-set by BuildKit, and falls back to amd64 if absent.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine3.24 AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO_ENABLED=0 -> fully static binary (modernc sqlite + libsql are pure Go).
# Keep symbols for binary vulnerability analysis; remove only DWARF debug data.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags "-w" -o /out/mqlite ./cmd/mqlite
RUN go run golang.org/x/vuln/cmd/govulncheck@latest -mode=binary /out/mqlite

FROM alpine:3.24
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/mqlitehq/mqlite" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$REVISION
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
