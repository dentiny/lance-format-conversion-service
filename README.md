# Lance format conversion service

Rust service and web application for converting Parquet files, Parquet
directories, and Hugging Face datasets into Lance 2.3. Sources can be read from
NFS-mounted paths, AWS S3, or Hugging Face; destinations can be written to
NFS-mounted paths or AWS S3. Iceberg support is deferred.

The service assumes that a source dataset remains immutable after schema
validation and throughout conversion.

## Web interface

Schedule a conversion, inspect its source schema, and select blob and index
columns:

![Schedule a Lance conversion](docs/screenshots/schedule-conversion.png)

Monitor conversion status, row progress, attempts, and errors:

![Monitor conversion jobs](docs/screenshots/conversion-jobs.png)

## Why convert Parquet to Lance?

Parquet is an interoperable columnar file format, but a directory of Parquet
files does not provide dataset-level search or indexing. Lance turns those
files into a versioned dataset designed for multimodal and AI workloads:

- Scalar, full-text, and vector indexes accelerate filtering, text search, and
  nearest-neighbor search without scanning every row.
- Blob V2 stores image, audio, video, and other large payloads using inline,
  packed, or dedicated Lance-managed storage.
- Dataset metadata and transactional commits provide one consistent table
  instead of requiring callers to coordinate a directory of independent files.
- Fragment-based updates support adding and backfilling columns without
  rewriting the entire dataset or every existing data file.
- Efficient random row and column access supports interactive retrieval while
  retaining an Arrow-compatible columnar schema.

## Features

### Sources and destinations

- Convert one Parquet file or a directory of Parquet files into a Lance 2.3
  dataset.
- Read source data from NFS-mounted paths, AWS S3, or Hugging Face datasets.
- Write Lance datasets to NFS-mounted paths or AWS S3.
- Stream Hugging Face Parquet files directly over HTTP without staging them on
  local disk.
- Source datasets are read-only and are never deleted by this service.

### Schema, blobs, and indexes

- Inspect the source Arrow schema before enqueueing a conversion.
- Select URL columns for Blob V2 ingestion. Lance fetches the referenced bytes
  and stores each value inline, packed, or in a dedicated blob file according
  to the configured thresholds.
- Create scalar, full-text, and vector Lance indexes after ingestion.
- Validate unsupported Arrow types and selected blob columns before writing.
- Verify the destination row count before marking a job as successful.

### Job execution and reliability

- Persist jobs, conversion options, progress, and structured error history in
  async SQLx PostgreSQL by default, or SQLite when the `sqlite` feature is
  enabled.
- Atomically claim jobs with renewable leases and attempt-based fencing.
- Run a bounded worker pool in the reconciler and checkpoint row progress.
- Retry interrupted or failed attempts up to 16 times.
- Prevent two jobs from targeting the same destination by using the destination
  URI as the job key.

### Web application and API

- Schedule conversions from an embedded, dependency-free web interface.
- Configure blob columns and indexes from the inspected schema.
- Monitor queued, running, successful, and failed jobs on a dedicated page.
- View rows read, rows written, total rows, attempts, timestamps, and errors.
- Use the same functionality through the Axum JSON API or the `lance-convert`
  CLI.

## Workspace architecture

- `crates/core`: job models and dataset location classification
- `crates/converter`: Parquet and Hugging Face readers, schema inspection,
  validation, Lance writes, and progress accounting
- `crates/job-store`: object-safe `JobStore` interface and storage errors
- `crates/job-store-factory`: database URL dispatch and backend construction.
  `postgres` is the default Cargo feature; `sqlite` is opt-in
- `crates/job-store-sqlite`: SQLite store, embedded migrations, and store tests
- `crates/job-store-postgres`: PostgreSQL store and pglite-oxide tests.
  Schema SQL is
  [Terraform-managed](crates/job-store-postgres/migrations/README.md)
- `crates/web`: `lance-web`, the HTTP job control plane and embedded UI
- `crates/cli`: `lance-convert`, a CLI that submits and inspects jobs through
  the web API
- [`crates/reconciler`](crates/reconciler/README.md): bounded polling, lease
  maintenance, progress checkpointing, and conversion execution

There are two server deployables: `lance-web` and `lance-reconciler`. There is
no separate worker or maintenance process. The reconciler claims jobs and runs
its conversion workers in one process. `lance-convert` is a client; it talks to
`lance-web` over HTTP and does not open the job database.

## TODO

- Add parallel Lance fragment writers for large datasets. The initial
  implementation deliberately uses one sequential writer per conversion job.
- Preserve bounded end-to-end backpressure between source readers and Lance
  writers, including parallel writers, so remote reads cannot outrun writes,
  accumulate unbounded record batches, and cause an out-of-memory failure.
- Add durable conversion checkpoints and idempotent fragment commits so a
  worker that reclaims an interrupted job can resume from its last committed
  checkpoint instead of restarting the overwrite from the beginning.
- Add a reconciliation task that cleans up terminal jobs after configurable
  age and retained-count thresholds. MVP records are retained indefinitely;
  running and queuing jobs must never be removed by retention cleanup.
- In the final production-hardening milestone, add structured tracing,
  `jemalloc`, and request-triggered CPU and memory profiling.

SQLite is available behind the `sqlite` Cargo feature. SQLite deployments must
run web and reconciler in the same pod or on the same host, backed by one
shared local volume containing the database. SQLite is not suitable for
independent pods because its locking and WAL files require a shared local
filesystem. The default `postgres` feature supports separate web and
reconciler processes against one PostgreSQL database.

## Quick start

### Prerequisites

- Rust 1.97.1, installed automatically by `rustup` from
  `rust-toolchain.toml`
- PostgreSQL for the default backend, or a local filesystem path when using
  SQLite
- AWS credentials for S3, when reading or writing object storage
- `HF_TOKEN` in the environment when reading a private Hugging Face dataset

Start the web control plane in the first terminal:

```shell
cargo run -p lance-web -- \
  --listen-address 127.0.0.1:8080 \
  --database-url postgres://user:pass@127.0.0.1:5432/lance_jobs \
  --database-max-connections 8
```

Start the reconciler against the same database in a second terminal:

```shell
cargo run -p lance-reconciler -- \
  --database-url postgres://user:pass@127.0.0.1:5432/lance_jobs \
  --database-max-connections 8 \
  --worker-count 4 \
  --poll-interval-ms 1000 \
  --lease-duration-secs 900 \
  --lease-renew-interval-secs 180 \
  --progress-interval-secs 30 \
  --target-lance-file-size-mib 512 \
  --blob-inline-threshold-mib 2 \
  --blob-dedicated-threshold-mib 32
```

Both processes are required: `lance-web` accepts and displays jobs, while
`lance-reconciler` claims and executes them. Omit `--database-url` to use
`postgres://127.0.0.1:5432/lance_jobs`. Omit `--database-max-connections` to
use a PostgreSQL pool of 8 connections. Run `lance-web --help` or
`lance-reconciler --help` for the full flag list.

For same-host SQLite development, enable the `sqlite` feature:

```shell
mkdir -p data
cargo run -p lance-web --features sqlite -- --database-url sqlite://./data/service.db
cargo run -p lance-reconciler --features sqlite -- --database-url sqlite://./data/service.db
```

Open these pages:

- `http://127.0.0.1:8080/` schedules a new conversion.
- `http://127.0.0.1:8080/jobs` monitors conversions and refreshes every three
  seconds.

### Try the included fixture

The `testdata` directory contains a three-row Parquet file and an SVG referenced
by its nullable `asset_url` column.

1. Open the scheduling page.
2. Enter a creator name.
3. Use the absolute path to `testdata/sample.parquet` as the source.
4. Use an absolute destination path ending in `.lance`.
5. Select **Copy**, then choose **Inspect schema**.
6. Select `asset_url` as a blob column. Optionally select indexes for compatible
   columns.
7. Create the job. The browser redirects to the jobs page.

The job should transition from `queuing` to `running` and then `succeeded`, with
three rows read and written.

Do not use `~` in local paths because URI parsing does not perform shell
expansion. Use an absolute path instead.

## HTTP API

- `GET /` — scheduling interface
- `GET /jobs` — job monitoring interface
- `GET /healthz`
- `POST /v1/sources/inspect`
- `POST /v1/jobs`
- `GET /v1/jobs`
- `GET /v1/jobs/{destination_uri}`

Inspect a source before enqueueing:

```shell
curl -X POST http://127.0.0.1:8080/v1/sources/inspect \
  -H 'content-type: application/json' \
  -d '{"source_uri":"/absolute/path/to/parquet-directory"}'
```

Create a job:

```shell
curl -X POST http://127.0.0.1:8080/v1/jobs \
  -H 'content-type: application/json' \
  -d '{
    "creator":"test-user",
    "source_uri":"s3://source-bucket/datasets/images",
    "destination_uri":"s3://destination-bucket/datasets/images.lance",
    "blob_columns":[{"column":"image_url"}],
    "indices":[{"column":"label","index_type":"scalar"}]
  }'
```

`POST /v1/jobs` returns `201 Created` with the job JSON body and a `Location`
header pointing at `GET /v1/jobs/{destination_uri}`. Percent-encode the
destination URI in that path, because it is the job key and may contain `://`
and `/`. A second job for the same destination returns `409 Conflict`. Job
statuses are `queuing`, `running`, `succeeded`, and `failed`.

Read one job:

```shell
curl http://127.0.0.1:8080/v1/jobs/s3%3A%2F%2Fdestination-bucket%2Fdatasets%2Fimages.lance
```

Filter the newest jobs by exact creator, inclusive creation-time bounds, and
ongoing status:

```shell
curl --get http://127.0.0.1:8080/v1/jobs \
  --data-urlencode 'creator=test-user' \
  --data-urlencode 'ongoing_only=true' \
  --data-urlencode 'creation_timestamp_ms_from=1786400000000' \
  --data-urlencode 'creation_timestamp_ms_to=1786486399999' \
  --data-urlencode 'limit=100'
```

All filters are optional and combined with `AND`. Use `failed_only=true` for
terminal failures; `failed_only` and `ongoing_only` are mutually exclusive.
Use `order_by=creation|update` and `order=asc|desc` to sort results. The default
is creation timestamp from newest to oldest. The service returns at most 100
jobs.

## CLI

`lance-convert` calls the same HTTP API. Point it at the web control plane with
`--url` or `LANCE_API_URL` (default `http://127.0.0.1:8080`). Each command has
examples on its help page: `lance-convert --help`, `lance-convert submit --help`,
`lance-convert list --help`, and `lance-convert status --help`.

```shell
cargo run -p lance-convert -- submit \
  --creator test-user \
  --source testdata/sample.parquet \
  --destination /tmp/sample.lance \
  --blob-column asset_url \
  --index label:scalar

cargo run -p lance-convert -- status --destination /tmp/sample.lance
cargo run -p lance-convert -- list --creator test-user --ongoing
```

Commands print pretty JSON. Submit a job, then poll `status` or `list` until
the job is `succeeded` or `failed`. Both `lance-web` and `lance-reconciler`
must be running for a submitted job to execute.
