# Check if we need to prepend docker commands with sudo
SUDO := $(shell docker version >/dev/null 2>&1 || echo "sudo")

# https://github.com/opencontainers/image-spec/blob/master/annotations.md
BUILD_DATE := $(shell date -u +%FT%TZ)

DEFAULT_INDEXERHASH := stellar/stellar-ledger-data-indexer:$(shell git rev-parse --short=9 HEAD)
INDEXERHASH ?= $(DEFAULT_INDEXERHASH)

docker-build:
	$(SUDO) docker build --platform linux/amd64 --pull --no-cache --label org.opencontainers.image.created="$(BUILD_DATE)" \
	-t $(INDEXERHASH) -t stellar/stellar-ledger-data-indexer:latest -f ./docker/Dockerfile .

docker-push:
	$(SUDO) docker push $(INDEXERHASH)
	$(SUDO) docker push stellar/stellar-ledger-data-indexer:latest

lint:
	pre-commit run --show-diff-on-failure --color=always --all-files
