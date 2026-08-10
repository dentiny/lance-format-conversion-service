# Lance format conversion service

Rust service for converting Parquet directories and Hugging Face datasets into
Lance 2.3 datasets on AWS S3. NFS and AWS S3 are supported as source storage.
Iceberg support is deferred.

The service assumes that a source dataset remains immutable after schema
validation and throughout conversion.

## Milestone 0

The foundational control plane currently includes:

- Rust 1.97.1 pinned in `rust-toolchain.toml`
- Axum health and job API in `lance-web`
- Typed NFS, S3, and Hugging Face location classification
- `copy` and `move` job contracts
- Object-safe `JobStore` interface
- SQLite implementation with an embedded schema, WAL, and busy timeout
- Atomic lease claims, 15-minute lease representation, attempt-based fencing, and progress snapshots
- Active destination reservation
- a separate `lance-reconciler` process, which initializes storage but does not
  claim work

Stateless dataset schema validation and conversion execution begin in Milestone
1. The reconciler explicitly does not claim jobs until Milestone 1 provides a
conversion handler.

## Workspace architecture

- `crates/core`: job models and dataset location classification
- `crates/job-store`: object-safe `JobStore` interface and storage errors
- `crates/job-store-factory`: database URL dispatch and backend construction
- `crates/job-store-sqlite`: SQLite store, embedded migrations, and store tests
- `crates/web`: `lance-web`, the HTTP job control plane
- `crates/reconciler`: `lance-reconciler`, the future conversion control loop

There are exactly two deployables: `lance-web` and `lance-reconciler`. There is
no separate worker or maintenance process. In Milestone 1 the reconciler will
own polling, bounded Tokio conversion workers, progress and lease updates, move
deletion, and orphan cleanup.

## TODO

- Add a reconciliation task that cleans up terminal jobs after configurable
  age and retained-count thresholds. MVP records are retained indefinitely;
  running and queuing jobs must never be removed by retention cleanup.
- Define a retry policy and upper bound for the job attempt counter. MVP only
  requires attempts to remain non-negative.
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

The dependency set intentionally excludes Lance, Arrow, Parquet, S3, Leptos,
tracing, `jemalloc`, and profiling crates until the milestones that use them.
Runtime dependencies disable default features and opt into only required
capabilities.

## Run

Run the web control plane:

```shell
cargo run -p lance-web -- \
  --listen-address 127.0.0.1:8080 \
  --database-url sqlite://./data/service.db
```

Run the Milestone 0 reconciler:

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
passed as flags because process arguments are observable. The reconciler opens
the selected job-store backend and waits for Ctrl-C without claiming jobs.

Milestone 1 will map these defaults to Lance `WriteParams::max_bytes_per_file`
and Blob V2 field metadata. The 512 MiB file target is a soft limit: a file may
exceed it by the final write batch and footer.

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
- `GET /v1/jobs/{id}`

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
