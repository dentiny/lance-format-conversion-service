CREATE TABLE IF NOT EXISTS jobs (
    -- Job identity and conversion request.
    id TEXT PRIMARY KEY,
    creator TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('copy', 'move')),
    source_uri TEXT NOT NULL,
    destination_uri TEXT NOT NULL,

    -- Job lifecycle.
    status TEXT NOT NULL CHECK (
        status IN (
            'queuing',
            'running',
            'succeeded',
            'failed'
        )
    ),
    creation_timestamp_ms INTEGER NOT NULL,
    update_timestamp_ms INTEGER NOT NULL,

    -- Retry and lease state.
    -- TODO (next milestone): Stop retrying after 16 attempts.
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    error_reasons_json TEXT NOT NULL DEFAULT '[]' CHECK (
        json_valid(error_reasons_json)
        AND json_type(error_reasons_json) = 'array'
    ),
    lease_expiration_timestamp_ms INTEGER,

    -- Job progress.
    source_bytes_read INTEGER NOT NULL DEFAULT 0 CHECK (source_bytes_read >= 0),
    lance_bytes_written INTEGER NOT NULL DEFAULT 0 CHECK (lance_bytes_written >= 0),
    rows_read INTEGER NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_written INTEGER NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    work_units_completed INTEGER NOT NULL DEFAULT 0 CHECK (work_units_completed >= 0),
    work_units_total INTEGER NOT NULL DEFAULT 0 CHECK (work_units_total >= 0),

    -- Cross-column invariants.
    CHECK (
        (status = 'running' AND lease_expiration_timestamp_ms IS NOT NULL)
        OR status != 'running'
    ),
    CHECK (work_units_total = 0 OR work_units_completed <= work_units_total)
) STRICT;

-- Query pattern: claim the oldest queuing jobs in creation order.
CREATE INDEX IF NOT EXISTS jobs_queuing_idx
    ON jobs(creation_timestamp_ms, id)
    WHERE status = 'queuing';

-- Query pattern: reclaim running jobs whose lease has expired.
CREATE INDEX IF NOT EXISTS jobs_expired_running_idx
    ON jobs(lease_expiration_timestamp_ms, creation_timestamp_ms, id)
    WHERE status = 'running';

-- Query pattern: filter jobs by creator.
CREATE INDEX IF NOT EXISTS jobs_creator_idx
    ON jobs(creator);

-- Query pattern: list all jobs from newest to oldest.
CREATE INDEX IF NOT EXISTS jobs_creation_idx
    ON jobs(creation_timestamp_ms DESC, id DESC);

-- Query pattern: list one creator's failed jobs from newest to oldest.
CREATE INDEX IF NOT EXISTS jobs_creator_failed_idx
    ON jobs(creator, creation_timestamp_ms DESC, id DESC)
    WHERE status = 'failed';

-- Constraint pattern: prevent two active jobs from writing the same destination.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_destination_idx
    ON jobs(destination_uri)
    WHERE status IN ('queuing', 'running');
