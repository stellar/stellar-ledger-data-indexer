# stellar-ledger-data-indexer
Stellar Ledger Data Indexer indexes ledger data by transaction_hash, contract_id, etc

# Install

## **Manual Installation**

1. Clone this repo `git clone https://github.com/stellar/stellar-ledger-data-indexer`
2. Build stellar-ledger-data-indexer with `go build`

```sh
$ ./stellar-ledger-data-indexer -config-file config.test.toml --start 58762521 --dataset contract_data
## You can also use --end to specify end ledger to import
```

### Docker
1. Build the docker image locally with `make docker-build`
2. Run the docker container in interactive mode to run index commands.

```sh
$ docker run --platform linux/amd64 -it stellar/stellar-ledger-data-indexer:latest /bin/bash
```

### How it works

1. `ledgerMetaDataReader.go` reads raw XDR data from Galexie bucket.
2. `tranform` parses raw XDR data into JSON format and sends to postgres util.
3. `postgres` utils helps to write data to cloudsql instance.

### Configs

```
[datastore_config]
type = "GCS"

[datastore_config.params]
destination_bucket_path = "path/to/galexie/bucket

[datastore_config.schema]
ledgers_per_file = 1
files_per_partition = 64000

[stellar_core_config]
  network = "pubnet"

[postgres_config]
  host = "postgres"
  user = "postgres"
  database = "postgres"
  port = 5432
```
