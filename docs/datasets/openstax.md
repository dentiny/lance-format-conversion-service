# OpenStax textbooks: file hierarchy and conversion sharding

Status: dataset design note, reviewed 2026-11-20. Upstream paths are examples or
explicit templates. The sharding and staging layout below are proposals, not
new service options or claims of a completed dataset conversion.

## Dataset and upstream hierarchy

Use public OpenStax book source repositories as the acquisition route described here. A
source repository contains CNXML, collection structure and media; a rendered PDF or web
edition is a separate representation that must be selected and versioned explicitly.

```text
openstax/osbooks-physics @ <commit-sha>/
  collections/physics.collection.xml  # ordered collection spine
  modules/m54082/index.cnxml           # section source
  media/CNX_HSPhysics_01_00_galaxy.jpg  # referenced asset
  META-INF/
  LICENSE
<repository>.zip                       # transport snapshot, not a module shard
```

The Physics collection nests `col:subcollection` elements and references modules with
`col:module document="m54082"`. That module contains a relative image reference into
`../../media/`. Resolve paths relative to each source file. The example establishes a
real repository layout, not a promise that all OpenStax repositories contain one book or
identical directory names. Filter organization repository listings deliberately; they
include software repositories.

## Work boundaries

| Boundary | Meaning |
| --- | --- |
| Acquisition | One repository archive pinned to a resolved commit SHA. |
| Logical grouping | Collection/book, ordered module occurrences and referenced assets. |
| Proposed shard | Repository + commit + collection, optionally a group of module occurrences. |
| Unsafe boundary | ZIP response chunks, lexical module filename order or arbitrary CNXML byte ranges. |

## Proposed shard plan

Start with one repository/commit per acquisition task. Parse collection order before
planning smaller work units. Identify a module occurrence by repository, commit,
collection path and its position in the collection spine; module ID alone does not
encode reading order or distinguish reuse. Group whole module occurrences by measured
source-plus-asset bytes. Persist a module/asset manifest and keep shared media
content-addressed or explicitly referenced, so packaging it twice does not create two
textual sections. Retain XML, mathematics, cross-references, attribution and original
assets even when a derived text view is produced.

For every frozen run, persist a manifest containing source URL/key, acquisition
time, source version/date, logical record or document identity, representation,
compressed size, content digest, parser/schema version, shard algorithm and exact
membership. Record unavailable sizes/digests explicitly and compute a digest of
downloaded bytes before declaring a staged object immutable. Directory prefixes,
Arrow batch sizes and Lance output file sizes are different boundaries.

## Conversion-service integration

The current [source dispatch](../../crates/converter/src/source/mod.rs) accepts
Parquet, Hugging Face and WARC. It has no native OpenStax textbooks discovery/parser
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

Resolve a branch or tag once and record the resulting SHA. A retry uses the same SHA and
archive digest. A new commit is a new snapshot; compare collection, module and asset
digests, including dependencies of unchanged module text. Check each selected book's
license and attribution metadata rather than assigning one license to the organization.

## Validation before publishing a run

Verify every collection reference resolves, spine order survives reconstruction, and
assets exist. Count module occurrences and unique modules separately. Check that all
requested repositories/collections are represented and that archive extraction handles
its actual root directory safely.

Also verify compatible schemas across every Parquet file in a shard, source and
staged checksums, disjoint manifest membership, and successful shard job counts.
Include retained, rejected and missing rows/documents in reconciliation rather
than silently interpreting row-count validation as source completeness.

## Evidence and limits

Upstream documentation, directory examples and repository source were inspected;
no corpus download, parser execution, sharding benchmark or production conversion
was performed. The service behavior above is based on the linked implementation.
The companion [Open Arrow Access provider
guide](https://github.com/dentiny/open-arrow-access/blob/main/service/openstax.md)
describes library acquisition support; that library is not automatically wired
into this service.

- [Physics repository](https://github.com/openstax/osbooks-physics)
- [Physics collection](https://github.com/openstax/osbooks-physics/blob/main/collections/physics.collection.xml)
- [Module and image reference](https://github.com/openstax/osbooks-physics/blob/main/modules/m54082/index.cnxml)
