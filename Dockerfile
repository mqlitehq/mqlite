# mqlite broker — single static pure-Go binary (no CGO).
# Build:  docker build --platform linux/amd64 -t mqlite:0.1.0 .
# Run:    docker run --platform linux/amd64 -p 8080:8080 -e MQLITE_TOKENS=mqk_dev mqlite:0.1.0

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO_ENABLED=0 -> fully static binary (modernc sqlite + libsql are pure Go).
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/mqlite ./cmd/mqlite

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && mkdir -p /data
COPY --from=build /out/mqlite /usr/local/bin/mqlite
EXPOSE 8080
# Default to a local file DB on the /data volume. Override MQLITE_DB with a
# libsql://... URL (+ MQLITE_DB_AUTH_TOKEN) to use remote Turso instead.
ENV MQLITE_DB=file:/data/mq.db
VOLUME ["/data"]
ENTRYPOINT ["mqlite"]
CMD ["serve", "--addr", ":8080"]
