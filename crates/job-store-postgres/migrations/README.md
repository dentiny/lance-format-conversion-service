# PostgreSQL schema

This directory is the source of truth for the jobs-database schema.
Terraform applies these SQL files to production PostgreSQL. `lance-web` and
`lance-reconciler` only connect; they never load or execute this SQL when a
pod starts or restarts.

Tests execute the same files against PGlite so the test schema matches what
Terraform applies.

## Versioning

Files are named `{version}_{description}.sql` and applied in version order:

```text
0001_initial.sql
0002_….sql
```

Each version runs once against a given database. Later schema changes go in a
new file (`0002_…`, `0003_…`), not by editing a file that Terraform has already
applied.

That split exists because the database outlives any pod:

- A restart must not recreate types, tables, or indexes, and must not rewrite
  live data.
- `CREATE TABLE` cannot evolve an existing table. Additive changes such as
  `ALTER TYPE job_progress ADD ATTRIBUTE …` belong in a new version.
- Editing an already-applied file would make git history disagree with
  production. A new version is an explicit, ordered change Terraform can apply
  to every environment the same way.
