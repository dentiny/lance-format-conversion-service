# CourtListener bulk data: file hierarchy and conversion sharding

Status: dataset design note, reviewed 2026-11-20. Upstream paths are examples or
explicit templates. The sharding and staging layout below are proposals, not
new service options or claims of a completed dataset conversion.

## Dataset and upstream hierarchy

CourtListener exports database tables as dated, bzip2-compressed CSV snapshots,
accompanied by SQL schema files. These are full exports of individual tables. Older
jurisdiction/date-partitioned JSON layouts should not be used as the current bulk-data
contract.

```text
s3://com-courtlistener-storage/
  bulk-data/
    courts-<YYYY-MM-DD>.csv.bz2
    dockets-<YYYY-MM-DD>.csv.bz2
    opinion-clusters-<YYYY-MM-DD>.csv.bz2
    opinions-<YYYY-MM-DD>.csv.bz2
    schema-<YYYY-MM-DD>.sql
  embeddings/opinions/<model>/...     # separate dataset, not CSV table shards
```

Filenames identify table and UTC generation date. Discover actual available objects
rather than presuming every table shares one date. Same-date tables are exported while
the database is live, so cross-table references can be missing. Keep schema and export
dialect with the selected files. Courts, dockets, clusters and opinions represent
different entities, not interchangeable document partitions.

## Work boundaries

| Boundary | Meaning |
| --- | --- |
| Acquisition | One dated table CSV.bz2 and its associated schema. |
| Logical grouping | Table + generation date + primary key. |
| Proposed shard | Whole files initially; parsed primary-key buckets for very large tables. |
| Unsafe boundary | Arbitrary bzip2 ranges or newline splitting of CSV containing multiline text. |

## Proposed shard plan

Choose explicit table/date pairs and freeze object digests. Stream decompression into a
parser configured for PostgreSQL COPY CSV semantics, including header, double quotes,
backslash escapes, embedded newlines and NULL versus quoted empty strings. Do not assume
a generic CSV default is sufficient. Preserve tables independently with their own
schemas and primary/foreign keys. For a large table, stage parsed records into buckets
using SHA-256 of the UTF-8 primary-key string, first eight bytes unsigned big-endian
modulo fixed N. Persist table, date, N and membership. This creates downstream shards
after sequential framing; it does not parallelize initial bzip2 decompression.

For every frozen run, persist a manifest containing source URL/key, acquisition
time, source version/date, logical record or document identity, representation,
compressed size, content digest, parser/schema version, shard algorithm and exact
membership. Record unavailable sizes/digests explicitly and compute a digest of
downloaded bytes before declaring a staged object immutable. Directory prefixes,
Arrow batch sizes and Lance output file sizes are different boundaries.

## Conversion-service integration

The current [source dispatch](../../crates/converter/src/source/mod.rs) accepts
Parquet, Hugging Face and WARC. It has no native CourtListener bulk data
discovery/parser
route. Do not submit the upstream URL or raw XML/CSV/ebook archive as though it
were a supported source. Use an explicit external preparation step, or implement
and review a new source adapter separately.

A proposed preparation layout, owned by the caller, is:

```text
<staging>/<source-snapshot>/
  manifest.json
  shard-0000/
    part-00000.parquet
    part-00001.parquet
  shard-0001/
    part-00000.parquet
<destination>/<source-snapshot>/shard-0000.lance
<destination>/<source-snapshot>/shard-0001.lance
```

Materialize one consistent, service-supported Arrow schema per source directory;
keep Parquet files directly inside each submitted shard directory. Store original
payloads/assets separately and retain their immutable references and digests.
Different table or record grains need separate schema families and destinations.
Text extraction, XML flattening, archive expansion and content retention need
explicit contracts; metadata-only staging must be labelled as such.

Schedule one existing conversion job per prepared shard with a unique destination
URI. The destination URI is the job key, so concurrent shards must never write
the same Lance dataset. A downstream catalogue/union of those datasets would
need separate orchestration; the service does not merge their commits automatically.
Bound acquisition/parser concurrency and buffered output before increasing worker
count; byte skew matters more than equal file counts.

The current [converter](../../crates/converter/src/converter.rs) uses one sequential
Lance writer per job and overwrite on retry. A target Lance file size rotates
output files; it does not split upstream data into scheduling units or enable
parallel fragment writes. Reported row progress is not a durable fragment-resume
cursor. A retry must reuse immutable staged inputs and can overwrite the shard
from its beginning. Publish a run as complete only after all manifest shards
have succeeded and their counts reconcile.

## Refresh and retry semantics

A newer dated table replaces that table's prior full snapshot; appending it duplicates
unchanged rows. Keep historical snapshots separately if required. Validate and report
cross-table orphans instead of silently dropping them. Embeddings have separate
model/version and inventory semantics and should have a separate ingestion plan.

## Validation before publishing a run

Reconcile parsed rows and primary-key uniqueness per table. Verify NULL/empty
round-trips and a multiline/escaped-text sample. Report foreign-key orphans as
snapshot-consistency findings, and confirm every selected source record is in exactly
one staged shard.

Also verify compatible schemas across every Parquet file in a shard, source and
staged checksums, disjoint manifest membership, and successful shard job counts.
Include retained, rejected and missing rows/documents in reconciliation rather
than silently interpreting row-count validation as source completeness.

## Evidence and limits

Upstream documentation, directory examples and repository source were inspected;
no corpus download, parser execution, sharding benchmark or production conversion
was performed. The service behavior above is based on the linked implementation.
The companion [Open Arrow Access provider
guide](https://github.com/dentiny/open-arrow-access/blob/main/service/courtlistener.md)
describes library acquisition support; that library is not automatically wired
into this service.

- [Bulk data documentation](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data)
- [Bulk object browser](https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/)
