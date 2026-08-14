CREATE TABLE IF NOT EXISTS jobs (
    -- Job identity and conversion request.
    creator TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    destination_uri TEXT PRIMARY KEY,

    -- User-selected conversion options.
    blob_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        jsonb_typeof(blob_columns_json) = 'array'
    ),
    indices_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        jsonb_typeof(indices_json) = 'array'
    ),

    -- Job lifecycle.
    status TEXT NOT NULL CHECK (
        status IN (
            'queuing',
            'running',
            'succeeded',
            'failed'
        )
    ),
    creation_timestamp_ms BIGINT NOT NULL,
    update_timestamp_ms BIGINT NOT NULL,

    -- Retry and lease state.
    attempt BIGINT NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    error_reasons_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        jsonb_typeof(error_reasons_json) = 'array'
    ),
    lease_expiration_timestamp_ms BIGINT,

    -- Job progress.
    rows_read BIGINT NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_written BIGINT NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    rows_total BIGINT NOT NULL DEFAULT 0 CHECK (rows_total >= 0),

    -- Cross-column invariants.
    CHECK (
        (status = 'running' AND lease_expiration_timestamp_ms IS NOT NULL)
        OR status != 'running'
    ),
    CHECK (
        rows_total = 0
        OR (rows_read <= rows_total AND rows_written <= rows_total)
    )
);

-- Query pattern: claim the oldest queuing jobs in creation order.
CREATE INDEX IF NOT EXISTS jobs_queuing_idx
    ON jobs(creation_timestamp_ms, destination_uri)
    WHERE status = 'queuing';

-- Query pattern: reclaim running jobs whose lease has expired.
CREATE INDEX IF NOT EXISTS jobs_expired_running_idx
    ON jobs(lease_expiration_timestamp_ms, creation_timestamp_ms, destination_uri)
    WHERE status = 'running';

-- Query pattern: filter jobs by creator.
CREATE INDEX IF NOT EXISTS jobs_creator_idx
    ON jobs(creator);

-- Query pattern: list all jobs from newest to oldest.
CREATE INDEX IF NOT EXISTS jobs_creation_idx
    ON jobs(creation_timestamp_ms DESC, destination_uri DESC);

-- Query pattern: list one creator's failed jobs from newest to oldest.
CREATE INDEX IF NOT EXISTS jobs_creator_failed_idx
    ON jobs(creator, creation_timestamp_ms DESC, destination_uri DESC)
    WHERE status = 'failed';
