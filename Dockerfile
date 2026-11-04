FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY main.go ./
COPY src ./src
RUN CGO_ENABLED=0 go build -o /out/yandex_disk_saver .

FROM alpine:3.22

RUN apk add --no-cache curl bash ca-certificates

COPY --from=ghcr.io/aptible/supercronic:latest \
    /usr/local/bin/supercronic /usr/local/bin/supercronic

COPY --from=build /out/yandex_disk_saver /app/yandex_disk_saver
COPY crontab /etc/crontab

RUN mkdir -p /vault
WORKDIR /app

CMD ["supercronic", "/etc/crontab"]
