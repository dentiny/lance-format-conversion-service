# Lance format conversion service

Rust service for converting Parquet directories and Hugging Face datasets into
Lance 2.3 datasets on AWS S3. NFS and AWS S3 are supported as source storage.
Iceberg support is deferred.

## Milestone 0

The foundational control plane currently includes:

- Rust 1.97.1 pinned in `rust-toolchain.toml`
- Axum health, inspection, and job API skeleton
- Typed NFS, S3, and Hugging Face source-location grammar
- S3-only destination validation
- `copy` and `move` job contracts
- Backend-enforced inspection gate before enqueue
- Object-safe `JobStore` interface
- SQLite implementation with embedded migrations, WAL, foreign keys, and busy timeout
- Atomic lease claims, 15-minute lease representation, fencing tokens, and monotonic progress checkpoints
- Active destination reservation
- jemalloc as the global allocator on supported targets

Dataset inspection and conversion execution begin in Milestone 1. Inspections
therefore remain `pending` in the current binary, and conversion submission is
correctly rejected until an inspection engine marks one `ready`.

## Build and test

```shell
cargo build
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

The dependency set intentionally excludes Lance, Arrow, Parquet, S3, Leptos,
and profiling crates until the milestones that use them. Runtime dependencies
disable default features and opt into only required capabilities.

## Run

```shell
cargo run -- \
  --listen-address 127.0.0.1:8080 \
  --database-path ./data/service.db \
  --worker-count 256 \
  --poll-interval-ms 1000 \
  --lease-duration-secs 900 \
  --lease-renew-interval-secs 300 \
  --progress-interval-secs 30 \
  --target-lance-file-size-mib 512 \
  --blob-inline-threshold-mib 2
```

Runtime service configuration uses command-line flags. Credentials must not be
passed as flags because process arguments are observable.

Milestone 1 will map these defaults to Lance `WriteParams::max_bytes_per_file`
and Blob V2 field metadata. The 512 MiB file target is a soft limit: a file may
exceed it by the final write batch and footer.

## Location grammar

- NFS source: `nfs:///absolute/path`
- S3 source or destination: `s3://bucket/non-empty-prefix`
- Hugging Face source: `hf://datasets/owner/name@revision?config=name&split=train`

Hugging Face sources are `copy`-only. A `move` job is accepted only for NFS or
S3 sources.

## API skeleton

- `GET /healthz`
- `POST /v1/inspections`
- `GET /v1/inspections/{id}`
- `POST /v1/jobs`
- `GET /v1/jobs`
- `GET /v1/jobs/{id}`

Example inspection request:

```shell
curl -X POST http://127.0.0.1:8080/v1/inspections \
  -H 'content-type: application/json' \
  -d '{"source_uri":"s3://source-bucket/datasets/images"}'
```

Job submission requires the resulting inspection to have completed
successfully:

```json
{
  "inspection_id": "00000000-0000-0000-0000-000000000000",
  "kind": "copy",
  "destination_uri": "s3://destination-bucket/datasets/images.lance"
}
```
