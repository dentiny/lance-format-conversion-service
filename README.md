# Lance format conversion service

Rust service for converting Parquet directories and Hugging Face datasets into
Lance 2.3 datasets on AWS S3. NFS and AWS S3 are supported as source storage.
Iceberg support is deferred.

The service assumes that a source dataset remains immutable after schema
validation and throughout conversion.

## Milestones 0 through 2

The service currently includes:

- Rust 1.97.1 pinned in `rust-toolchain.toml`
- Axum health and job API in `lance-web`
- Typed NFS, S3, and Hugging Face location classification
- `copy` and `move` job contracts
- Object-safe `JobStore` interface
- SQLite implementation with an embedded schema, WAL, and busy timeout
- Atomic lease claims, 15-minute lease representation, attempt-based fencing, and progress snapshots
- Destination URI as the permanent job primary key
- Stateless Arrow schema validation before each write
- Parquet-directory readers for NFS and AWS S3
- Hugging Face dataset Parquet discovery and direct HTTP streaming
- Lance 2.3 overwrite writes with a configurable soft file-size target
- Blob V2 inline-threshold metadata for columns already marked as Blob V2
- Bounded reconciler polling and conversion workers
- Lease renewal, 30-second progress checkpoints, and attempt fencing
- Terminal success/failure transitions and structured retries capped at 16

## Workspace architecture

- `crates/core`: job models and dataset location classification
- `crates/converter`: Parquet and Hugging Face readers, validation, Lance writes,
  progress accounting, and move-source deletion
- `crates/job-store`: object-safe `JobStore` interface and storage errors
- `crates/job-store-factory`: database URL dispatch and backend construction
- `crates/job-store-sqlite`: SQLite store, embedded migrations, and store tests
- `crates/web`: `lance-web`, the HTTP job control plane
- `crates/reconciler`: bounded polling, lease maintenance, progress
  checkpointing, and conversion execution

There are exactly two deployables: `lance-web` and `lance-reconciler`. There is
no separate worker or maintenance process. The reconciler claims jobs and runs
its conversion workers in one process.

## TODO

- In the next milestone, add the pre-enqueue schema UI and store the selected
  URL-backed blob columns as part of each `Job`. Also let users select columns
  to index and choose an index type from a dropdown. Persist and validate all
  blob and index specifications before enqueue, use the blob specifications
  during conversion, and create the requested indexes afterward.
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

## Build and test

```shell
cargo build --workspace
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo tree --duplicates
```

The conversion path uses Lance, Arrow/DataFusion, Parquet, AWS S3, and a minimal
HTTP client for Hugging Face. Leptos, tracing, `jemalloc`, and profiling remain
deferred. Runtime dependencies disable default features and opt into only
required capabilities.

## Run

Run the web control plane:

```shell
cargo run -p lance-web -- \
  --listen-address 127.0.0.1:8080 \
  --database-url sqlite://./data/service.db
```

Run the reconciler:

```shell
cargo run -p lance-reconciler -- \
  --database-url sqlite://./data/service.db \
  --worker-count 256 \
  --poll-interval-ms 1000 \
  --lease-duration-secs 900 \
  --lease-renew-interval-secs 300 \
  --progress-interval-secs 30 \
  --target-lance-file-size-mib 512 \
  --blob-inline-threshold-mib 2
```

Runtime service configuration uses command-line flags. Credentials must not be
passed as flags because process arguments are observable. AWS credentials use
the standard AWS environment/instance-provider chain; private Hugging Face
datasets use `HF_TOKEN`.

The reconciler uses these flags to bound conversion concurrency, maintain
leases, checkpoint progress, and configure Lance writes.

## Location grammar

- NFS-mounted source: any scheme-less filesystem path
- S3 source or destination: `s3://bucket/non-empty-prefix`
- Hugging Face source: `hf://datasets/owner/name@revision?config=name&split=train`

Hugging Face sources are `copy`-only. A `move` job is accepted only for NFS or
S3 sources.

## API skeleton

- `GET /healthz`
- `POST /v1/jobs`
- `GET /v1/jobs`

Example job request:

```shell
curl -X POST http://127.0.0.1:8080/v1/jobs \
  -H 'content-type: application/json' \
  -d '{
    "creator":"test-user",
    "source_uri":"s3://source-bucket/datasets/images",
    "kind":"copy",
    "destination_uri":"s3://destination-bucket/datasets/images.lance"
  }'
```
