# GovInfo bulk data: file hierarchy and conversion sharding

Status: dataset design note, reviewed 2026-11-20. Upstream paths are examples or
explicit templates. The sharding and staging layout below are proposals, not
new service options or claims of a completed dataset conversion.

## Dataset and upstream hierarchy

GovInfo publishes selected government collections as machine-readable bulk XML. Its
hierarchy is collection-specific. Collection folders and convenience ZIPs organize
acquisition; they do not establish a single schema or one universal record grain.

```text
https://www.govinfo.gov/bulkdata/
  FR/2026/
    FR-2026.zip
    01/
      FR-2026-01-02.xml
      FR-2026-01.zip
  CFR/2025/title-1/
    CFR-2025-title1-vol1.xml
    CFR-2025-title-1.zip
  BILLS/119/1/hr/
    BILLS-119hr1001ih.xml
    BILLS-119hr1001eh.xml
  BILLSTATUS/119/hr/...
  <collection>/resources-or-other-subdirectories/...  # discover actual names
```

FR uses year/month issue files, CFR uses edition year/title/volume, and BILLS uses
Congress/session/bill type with versioned bill XML. BILLSTATUS is a distinct collection
and its Congress/type layout differs from BILLS. The paths above are concrete directory
examples plus marked placeholders, not a complete inventory. Year/month/title ZIPs may
cover the same XML published individually. Collection resources provide relevant schemas
and guides.

## Work boundaries

| Boundary | Meaning |
| --- | --- |
| Acquisition | One listed XML or one selected archive representation. |
| Logical grouping | Collection-specific document/package identity, version and source revision. |
| Proposed shard | Disjoint groups of document keys within one collection/schema family. |
| Unsafe boundary | A universal date hierarchy, overlapping ZIP + loose XML ingestion or arbitrary XML offsets. |

## Proposed shard plan

Recursively enumerate the selected collection using the published JSON or XML directory
endpoints, recording child folders and direct files. Choose either loose XML or archive
members for a scope, then deduplicate by logical document/version and source digest.
Group whole XML documents by collection and measured bytes; retain Congress/session/type
or edition/year/title as useful partition metadata without assuming equal sizes. A large
issue or volume may need a collection-aware XML record parser before finer downstream
partitioning. Record the parent document and element position so derived article/section
rows can be traced and reconstructed.

For every frozen run, persist a manifest containing source URL/key, acquisition
time, source version/date, logical record or document identity, representation,
compressed size, content digest, parser/schema version, shard algorithm and exact
membership. Record unavailable sizes/digests explicitly and compute a digest of
downloaded bytes before declaring a staged object immutable. Directory prefixes,
Arrow batch sizes and Lance output file sizes are different boundaries.

## Conversion-service integration

The current [source dispatch](../../crates/converter/src/source/mod.rs) accepts
Parquet, Hugging Face and WARC. It has no native GovInfo bulk data discovery/parser
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

Freeze listing responses and downloaded bytes with acquisition time and content digests.
An old publication path can be corrected, so scanning only the newest date misses
changes. Compare selected-object digests on refresh. Keep BILLS text versions distinct;
do not collapse introduced and engrossed versions merely because the bill number
matches. Bulk access and authenticated package API discovery are separate routes.

## Validation before publishing a run

Check recursive traversal completeness and count unique selected document/version
identities. Reconcile extracted ZIP membership against loose-file scope to prevent
duplicates. Validate XML with the relevant collection schema where available, and report
corrections, missing documents and unsupported collection variants.

Also verify compatible schemas across every Parquet file in a shard, source and
staged checksums, disjoint manifest membership, and successful shard job counts.
Include retained, rejected and missing rows/documents in reconciliation rather
than silently interpreting row-count validation as source completeness.

## Evidence and limits

Upstream documentation, directory examples and repository source were inspected;
no corpus download, parser execution, sharding benchmark or production conversion
was performed. The service behavior above is based on the linked implementation.
The companion [Open Arrow Access provider
guide](https://github.com/dentiny/open-arrow-access/blob/main/service/govinfo.md)
describes library acquisition support; that library is not automatically wired
into this service.

- [Bulk root](https://www.govinfo.gov/bulkdata/)
- [Directory endpoint documentation](https://github.com/usgpo/bulk-data)
- [FR example](https://www.govinfo.gov/bulkdata/FR/2026/01)
- [CFR example](https://www.govinfo.gov/bulkdata/CFR/2025/title-1)
- [BILLS example](https://www.govinfo.gov/bulkdata/BILLS/119/1/hr)
- [BILLSTATUS example](https://www.govinfo.gov/bulkdata/BILLSTATUS/119)
