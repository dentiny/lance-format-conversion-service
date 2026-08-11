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
- Run either a `copy` job or a `move` job. Hugging Face sources are copy-only.

### Schema, blobs, and indexes

- Inspect the source Arrow schema before enqueueing a conversion.
- Select URL columns for Blob V2 ingestion. Lance fetches the referenced bytes
  and stores each value inline, packed, or in a dedicated blob file according
  to the configured thresholds.
- Create scalar, full-text, spatial, and vector Lance indexes after ingestion.
- Validate unsupported Arrow types and selected blob columns before writing.
- Verify the destination row count before marking a job as successful.

### Job execution and reliability

- Persist jobs, conversion options, progress, and structured error history in
  async SQLx SQLite.
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
- Use the same functionality through the Axum JSON API.

## Workspace architecture

- `crates/core`: job models and dataset location classification
- `crates/converter`: Parquet and Hugging Face readers, schema inspection,
  validation, Lance writes, progress accounting, and move-source deletion
- `crates/job-store`: object-safe `JobStore` interface and storage errors
- `crates/job-store-factory`: database URL dispatch and backend construction
- `crates/job-store-sqlite`: SQLite store, embedded migrations, and store tests
- `crates/web`: `lance-web`, the HTTP job control plane and embedded UI
- `crates/reconciler`: bounded polling, lease maintenance, progress
  checkpointing, and conversion execution

There are exactly two deployables: `lance-web` and `lance-reconciler`. There is
no separate worker or maintenance process. The reconciler claims jobs and runs
its conversion workers in one process.

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

SQLite development deployments must run web and reconciler in the same pod or
on the same host, backed by one shared local volume containing the database.
SQLite is not suitable for independent pods because its locking and WAL files
require a shared local filesystem. Production deployments with separate web
and reconciler pods require PostgreSQL.

## Quick start

### Prerequisites

- Rust 1.97.1, installed automatically by `rustup` from
  `rust-toolchain.toml`
- A local filesystem path for development, or AWS credentials for S3
- `HF_TOKEN` in the environment when reading a private Hugging Face dataset

Create the development database directory:

```shell
mkdir -p data
```

Start the web control plane in the first terminal:

```shell
cargo run -p lance-web -- \
  --listen-address 127.0.0.1:8080 \
  --database-url sqlite://./data/service.db
```

Start the reconciler against the same database in a second terminal:

```shell
cargo run -p lance-reconciler -- \
  --database-url sqlite://./data/service.db \
  --worker-count 4 \
  --poll-interval-ms 1000 \
  --lease-duration-secs 900 \
  --lease-renew-interval-secs 300 \
  --progress-interval-secs 30 \
  --target-lance-file-size-mib 512 \
  --blob-inline-threshold-mib 2 \
  --blob-dedicated-threshold-mib 32
```

Both processes are required: `lance-web` accepts and displays jobs, while
`lance-reconciler` claims and executes them.

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

### Build and test

```shell
cargo build --workspace
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo tree --duplicates
```

Runtime configuration uses command-line flags. Credentials must not be passed
as flags because process arguments are observable. AWS credentials use the
standard AWS environment or instance-provider chain.

## Location grammar

- NFS-mounted source file or directory: an absolute scheme-less filesystem path
- NFS-mounted destination: an absolute scheme-less filesystem path
- S3 source prefix or destination: `s3://bucket/non-empty-prefix`
- Hugging Face source: `hf://datasets/owner/name@revision?config=name&split=train`

Hugging Face sources are `copy`-only. A `move` job is accepted only for NFS or
S3 sources.

## HTTP API

- `GET /` — scheduling interface
- `GET /jobs` — job monitoring interface
- `GET /healthz`
- `POST /v1/sources/inspect`
- `POST /v1/jobs`
- `GET /v1/jobs`

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
    "kind":"copy",
    "destination_uri":"s3://destination-bucket/datasets/images.lance",
    "blob_columns":[{"column":"image_url"}],
    "indices":[{"columns":["label"],"index_type":"bitmap"}]
  }'
```

`POST /v1/jobs` returns `202 Accepted`. Poll `GET /v1/jobs` for status and
progress. Job statuses are `queuing`, `running`, `succeeded`, and `failed`.
