FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY go.mod ./
COPY main.go ./
COPY src ./src
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/webdav_sync_tool .

FROM alpine:3.22

RUN apk add --no-cache curl bash ca-certificates

ARG TARGETARCH
ARG SUPERCRONIC_VERSION=0.2.51

RUN case "$TARGETARCH" in \
            amd64) SUPERCRONIC_SHA1SUM=1692066e350a88ff4d7d6474fe3c7a6c41ec5e89 ;; \
            arm64) SUPERCRONIC_SHA1SUM=9d4c2000493215a1898502b2fc2dada3e8ea8c7a ;; \
            *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
        esac \
 && SUPERCRONIC="supercronic-linux-${TARGETARCH}" \
 && SUPERCRONIC_URL="https://github.com/aptible/supercronic/releases/download/v${SUPERCRONIC_VERSION}/${SUPERCRONIC}" \
 && curl -fsSLo "$SUPERCRONIC" "$SUPERCRONIC_URL" \
 && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
 && chmod +x "$SUPERCRONIC" \
 && mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
 && ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic

COPY --from=build /out/webdav_sync_tool /app/webdav_sync_tool
COPY crontab /etc/crontab

RUN mkdir -p /vault
WORKDIR /app

CMD ["supercronic", "/etc/crontab"]
