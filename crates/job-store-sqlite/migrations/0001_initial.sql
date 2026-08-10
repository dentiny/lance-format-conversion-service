CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('copy', 'move')),
    source_uri TEXT NOT NULL,
    destination_uri TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'queued',
            'running',
            'succeeded',
            'failed'
        )
    ),
    submission_timestamp_ms INTEGER NOT NULL,
    update_timestamp_ms INTEGER NOT NULL,
    -- TODO: Define a retry/attempt upper bound after MVP.
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    lease_expiration_timestamp_ms INTEGER,
    source_bytes_read INTEGER NOT NULL DEFAULT 0 CHECK (source_bytes_read >= 0),
    lance_bytes_written INTEGER NOT NULL DEFAULT 0 CHECK (lance_bytes_written >= 0),
    rows_read INTEGER NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_written INTEGER NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    work_units_completed INTEGER NOT NULL DEFAULT 0 CHECK (work_units_completed >= 0),
    work_units_total INTEGER NOT NULL DEFAULT 0 CHECK (work_units_total >= 0),
    CHECK (
        (status = 'running' AND lease_expiration_timestamp_ms IS NOT NULL)
        OR status != 'running'
    ),
    CHECK (work_units_total = 0 OR work_units_completed <= work_units_total)
) STRICT;

CREATE INDEX IF NOT EXISTS jobs_queued_idx
    ON jobs(submission_timestamp_ms, id)
    WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS jobs_expired_running_idx
    ON jobs(lease_expiration_timestamp_ms, submission_timestamp_ms, id)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS jobs_source_idx
    ON jobs(source_uri);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_destination_idx
    ON jobs(destination_uri)
    WHERE status IN ('queued', 'running');
