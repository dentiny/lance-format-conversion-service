# HathiTrust Hathifiles: file hierarchy and conversion sharding

Status: dataset design note, reviewed 2026-11-20. Upstream paths are examples or
explicit templates. The sharding and staging layout below are proposals, not
new service options or claims of a completed dataset conversion.

## Dataset and upstream hierarchy

Hathifiles describe volumes and bibliographic/access attributes. They are metadata
exports, not OCR, page images, full-text books or HTRC Extracted Features. A catalogue
row does not grant access to the underlying volume.

```text
https://www.hathitrust.org/files/hathifiles/
  hathi_file_list.json
  hathi_full_<YYYYMMDD>.txt.gz       # full inventory
  hathi_upd_<YYYYMMDD>.txt.gz        # incremental additions/updates
# Concrete dates must come from the selected listing; this is a naming template.
```

The publisher's listing generator exposes filename, full flag, size, created/modified
timestamps and URL. It uses a rolling retention window (70 days by default in the
inspected code), so this endpoint is not a permanent archive. Save selected files
locally. Hathifiles contain tab-delimited fields; the publisher's current field
definition lists 26 columns, starting with `htid`. Preserve identifiers as strings and
pin the field/header specification instead of inferring types from numeric-looking
values.

## Work boundaries

| Boundary | Meaning |
| --- | --- |
| Acquisition | One gzip full or update file from a frozen listing. |
| Logical grouping | Volume identifier htid, retaining each update's source date. |
| Proposed shard | Stable htid buckets after TSV parsing, separately versioned full/update events. |
| Unsafe boundary | Append all listed files as disjoint rows, or split gzip at arbitrary byte offsets. |

## Proposed shard plan

Choose one full baseline and the subsequent update files through an explicit cutoff.
Save the listing, filenames, dates and digests, and check the required update sequence
before processing. Stream gzip and parse tab-separated records with the pinned column
order and explicit header policy; do not assume CSV quoting rules or that each file
carries a header. Preserve empty fields and multi-valued identifier strings. Partition
parsed records using SHA-256 of UTF-8 htid, first eight bytes unsigned big-endian modulo
fixed N. A single gzip stream remains a sequential acquisition/decompression boundary
even if downstream buckets are processed concurrently.

For every frozen run, persist a manifest containing source URL/key, acquisition
time, source version/date, logical record or document identity, representation,
compressed size, content digest, parser/schema version, shard algorithm and exact
membership. Record unavailable sizes/digests explicitly and compute a digest of
downloaded bytes before declaring a staged object immutable. Directory prefixes,
Arrow batch sizes and Lance output file sizes are different boundaries.

## Conversion-service integration

The current [source dispatch](../../crates/converter/src/source/mod.rs) accepts
Parquet, Hugging Face and WARC. It has no native HathiTrust Hathifiles discovery/parser
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

Maintain an immutable event view keyed by source file/date and htid, then derive a
current view by applying updates in source-date order with deterministic within-file
duplicate handling. Multiple full files overlap and must not be concatenated. Absence
from a daily update is not deletion. Reconcile against a later complete baseline to
detect removals; missing update files require recovery from a newer full snapshot.
Rights timestamps describe rights metadata and should not replace the source-file
sequence as a general change cursor.

## Validation before publishing a run

Reconcile input rows, distinct htids and output events separately. Verify column count
and leading zeros, report duplicate htids, and confirm chronological update application.
Record deletions found during full-baseline reconciliation and do not claim full-text
coverage from metadata counts.

Also verify compatible schemas across every Parquet file in a shard, source and
staged checksums, disjoint manifest membership, and successful shard job counts.
Include retained, rejected and missing rows/documents in reconciliation rather
than silently interpreting row-count validation as source completeness.

## Evidence and limits

Upstream documentation, directory examples and repository source were inspected;
no corpus download, parser execution, sharding benchmark or production conversion
was performed. The service behavior above is based on the linked implementation.
The companion [Open Arrow Access provider
guide](https://github.com/dentiny/open-arrow-access/blob/main/service/hathifiles.md)
describes library acquisition support; that library is not automatically wired
into this service.

- [Publisher field definitions and generation](https://github.com/hathitrust/hathifiles)
- [Publisher listing generator](https://github.com/hathitrust/hathifiles/blob/main/jobs/update_hathifile_listing.rb)
- [HathiTrust data page](https://www.hathitrust.org/hathifiles)
