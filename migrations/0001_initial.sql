CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS inspections (
    id TEXT PRIMARY KEY,
    source_uri TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('nfs', 's3', 'hugging_face')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'ready', 'failed')),
    schema_fingerprint TEXT,
    error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (
        (status = 'ready' AND schema_fingerprint IS NOT NULL AND error IS NULL)
        OR (status = 'failed' AND error IS NOT NULL)
        OR status = 'pending'
    )
) STRICT;

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    inspection_id TEXT NOT NULL REFERENCES inspections(id),
    kind TEXT NOT NULL CHECK (kind IN ('copy', 'move')),
    source_uri TEXT NOT NULL,
    destination_uri TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'queued',
            'running',
            'validating',
            'publishing',
            'deleting_source',
            'succeeded',
            'failed',
            'cancelled'
        )
    ),
    submitted_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at_ms INTEGER,
    source_bytes_read INTEGER NOT NULL DEFAULT 0 CHECK (source_bytes_read >= 0),
    lance_bytes_written INTEGER NOT NULL DEFAULT 0 CHECK (lance_bytes_written >= 0),
    rows_read INTEGER NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_written INTEGER NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    work_units_completed INTEGER NOT NULL DEFAULT 0 CHECK (work_units_completed >= 0),
    work_units_total INTEGER NOT NULL DEFAULT 0 CHECK (work_units_total >= 0),
    CHECK (
        (status = 'running' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
        OR status != 'running'
    ),
    CHECK (work_units_total = 0 OR work_units_completed <= work_units_total)
) STRICT;

CREATE INDEX IF NOT EXISTS jobs_claimable_idx
    ON jobs(status, lease_expires_at_ms, submitted_at_ms);

CREATE INDEX IF NOT EXISTS jobs_source_submitted_idx
    ON jobs(source_uri, submitted_at_ms);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_destination_idx
    ON jobs(destination_uri)
    WHERE status IN ('queued', 'running', 'validating', 'publishing', 'deleting_source');

CREATE TABLE IF NOT EXISTS job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS job_events_job_id_idx
    ON job_events(job_id, id);
