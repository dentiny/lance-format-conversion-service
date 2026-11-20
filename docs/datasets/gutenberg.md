# Project Gutenberg: file hierarchy and conversion sharding

Status: dataset design note, reviewed 2026-11-20. Upstream paths are examples or
explicit templates. The sharding and staging layout below are proposals, not
new service options or claims of a completed dataset conversion.

## Dataset and upstream hierarchy

Project Gutenberg distributes ebooks in multiple representations plus RDF catalogue
metadata. The main mirror and generated collection have different roots; metadata alone
does not constitute book-content ingestion.

```text
<main-module-root>/
  1/2/3/4/12345/<advertised-file>    # documented ID example, not a date
<generated-module-root>/
  epub/12345/<advertised-file>       # module-relative layout in mirroring guide
<combined-mirror>/cache/epub/<id>/    # optional local mount, not a third corpus
```

The main `gutenberg` and generated `gutenberg-epub` rsync modules are separate
inventories. Generated files may be mounted under `cache/epub/`; historical catalogue
paths can use `cache/generated/`. Preserve the original URL and resolved mirror key.
TXT, HTML with assets, EPUB and Kindle variants can describe the same ebook. Discover
advertised names and formats instead of constructing every filename from an ID.

## Work boundaries

| Boundary | Meaning |
| --- | --- |
| Acquisition | One independently advertised file, or a whole archive when offered. |
| Logical grouping | Ebook ID plus selected representations and metadata. |
| Proposed shard | A persisted set of ebook IDs, balanced by total selected bytes. |
| Unsafe boundary | Digit-directory prefixes as equal-sized shards, or arbitrary offsets inside one ebook/archive. |

## Proposed shard plan

Freeze a mirror inventory after synchronization. Assign each ebook ID once; keep every
selected representation and referenced local asset associated with that book. For a
stable initial assignment, use SHA-256 of the UTF-8 decimal ebook ID, interpret its
first eight bytes as an unsigned big-endian integer, and take modulo a fixed N. Persist
N and actual membership; optionally replace the assignment with a recorded size-balanced
plan for skewed books. A change in N creates a new plan. Treat archive expansion and
text extraction as explicit steps. Retain original payloads and the complete RDF when
mapped columns omit upstream fields.

For every frozen run, persist a manifest containing source URL/key, acquisition
time, source version/date, logical record or document identity, representation,
compressed size, content digest, parser/schema version, shard algorithm and exact
membership. Record unavailable sizes/digests explicitly and compute a digest of
downloaded bytes before declaring a staged object immutable. Directory prefixes,
Arrow batch sizes and Lance output file sizes are different boundaries.

## Conversion-service integration

The current [source dispatch](../../crates/converter/src/source/mod.rs) accepts
Parquet, Hugging Face and WARC. It has no native Project Gutenberg discovery/parser
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

Compare the next inventory by resolved key and content digest. Updated representations
replace the corresponding version; deleted files need explicit reconciliation. Ebook ID
and issued date do not identify immutable bytes. Reuse the frozen inventory for a retry.

## Validation before publishing a run

Reconcile distinct ebook IDs separately from representation/file counts. Check that
every selected file is assigned once, that assets resolve within its retained package,
and that missing or unsupported formats are reported. A catalogue row count cannot prove
full-text coverage.

Also verify compatible schemas across every Parquet file in a shard, source and
staged checksums, disjoint manifest membership, and successful shard job counts.
Include retained, rejected and missing rows/documents in reconciliation rather
than silently interpreting row-count validation as source completeness.

## Evidence and limits

Upstream documentation, directory examples and repository source were inspected;
no corpus download, parser execution, sharding benchmark or production conversion
was performed. The service behavior above is based on the linked implementation.
The companion [Open Arrow Access provider
guide](https://github.com/dentiny/open-arrow-access/blob/main/service/gutenberg.md)
describes library acquisition support; that library is not automatically wired
into this service.

- [Mirroring guide](https://www.gutenberg.org/help/mirroring.html)
- [Official mobile copy of mirroring guide](https://m.gutenberg.org/help/mirroring.html)
