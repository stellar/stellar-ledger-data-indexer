# stage 1: build stellar-ledger-data-indexer app with CGO
FROM golang:1.23.4 AS build

ENV CGO_ENABLED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential  \
    ca-certificates \
    pkg-config \
    libczmq-dev \
    libzmq3-dev \
    libsodium-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/indexer

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o /usr/local/bin ./...

# stage 2: runtime enviroment
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libczmq4 \
    libzmq5 \
    libsodium23 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /indexer

COPY --from=build /usr/local/bin/stellar-ledger-data-indexer /usr/local/bin/stellar-ledger-data-indexer
COPY --from=build /usr/src/indexer/docker docker

# changing workdir to a new path in order to use mounted empty ephemeral volumes as storage
WORKDIR /indexer/data

ENTRYPOINT []

CMD ["stellar-ledger-data-indexer"]
