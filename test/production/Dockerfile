FROM --platform=$BUILDPLATFORM golang:1.27-alpine3.24 AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags "-w" -o /out/soak ./test/production/soak
RUN go run golang.org/x/vuln/cmd/govulncheck@latest -mode=binary /out/soak

FROM alpine:3.24 AS runtime
RUN apk upgrade --no-cache
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/mqlitehq/mqlite" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$REVISION \
      org.mqlite.role="production-acceptance-runner"
COPY --from=build /out/soak /usr/local/bin/soak
ENTRYPOINT ["soak"]
