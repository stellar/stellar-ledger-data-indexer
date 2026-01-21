### Step 1: Export data from bigquery to temp table

```sh
bq query \
  --use_legacy_sql=false \
  --destination_table=project-id:dataset.table \
  --replace=true <<'SQL'
SELECT 
  contract_id,
  ledger_sequence,
  ledger_key_hash as key_hash,
  case 
    when contract_durability = 'ContractDataDurabilityPersistent' then 'persistent'
    when contract_durability = 'ContractDataDurabilityTemporary' then 'temporary'
    else contract_durability
    end as durability,
  json_value(key_decoded, '$.vec[0].symbol') as key_symbol,
  json_value(key) as key,
  json_value(val) as val,
  closed_at
FROM `crypto-stellar.crypto_stellar_dbt.contract_data_current` 
SQL
```

### Step 2: Export data from temp table to GCS files

```sh
bq extract \
  --destination_format=CSV \
  --print_header=false \
  project-id:dataset.table \
  gs://bucket_name/ledgers/temp.csv
```

### Step 3: Create a backfill table in CloudSQL studio

```
CREATE TABLE IF NOT EXISTS contract_data_backfill (
    contract_id TEXT,
    ledger_sequence INTEGER NOT NULL,
    key_hash TEXT,
    durability TEXT,
    key_symbol TEXT,
    key BYTEA,
    val BYTEA,
    closed_at TIMESTAMP WITH TIME ZONE NOT NULL
);
```

### Step 4: Import data from GCS files to postgres

```sh
  gcloud sql import csv --project project-id cloudsql-instance-name \
  gs://bucket_name/ledgers/temp.csv \
  --database=postgres \
  --table=contract_data_backfill
```
