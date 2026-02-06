## Setting up instance

The ledger data indexer can be setup using Kube clusters. A user can start an instance with following command:

```sh
$ ./stellar-ledger-data-indexer -config-file config.test.toml --start 58762521

# If max ledger sequence in DB is greater than 58762521, indexer starts from max ledger sequence. Otehrwise, it starts from 58762521, in unbounded mode. i.e. It catches up to current ledger.


$ ./stellar-ledger-data-indexer -config-file config.test.toml --start 58762521 --backfill

# If backfill flag is set,indexer disregards the max ledger in DB and reprocesses for given start/end ledger. In above case, it will start from 58762521. Note that it only overwrites records if there exists new version of given key_hash.

$ ./stellar-ledger-data-indexer -config-file config.test.toml --start 58762521 --end 58762530

# If max ledger sequence in DB is greater than 58762521, indexer starts from max ledger sequence. Otehrwise, it starts from 58762521. It runs uptil ledger 58762530

$ ./stellar-ledger-data-indexer -config-file config.test.toml --start 58762521 --end 58762530 --backfill

# If backfill flag is set, indexer disregards the max ledger in DB and reprocesses for given start/end ledger. In above case, it will process between 58762521 and 58762530. Note that it only overwrites records if there exists new version of given key_hash.
```

### Backfills

It is recommended to start parallel containers to ingest historical data. Example:

```sh
$ ./stellar-ledger-data-indexer -config-file config.test.toml --start 1000000 --end 2000000 --backfill

$ ./stellar-ledger-data-indexer -config-file config.test.toml --start 2000000 --end 3000000 --backfill

$ ./stellar-ledger-data-indexer -config-file config.test.toml --start 3000000 --end 4000000 --backfill

```