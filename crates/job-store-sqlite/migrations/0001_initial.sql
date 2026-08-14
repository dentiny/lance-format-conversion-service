CREATE TABLE IF NOT EXISTS jobs (
    -- Job identity and conversion request.
    creator TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    destination_uri TEXT PRIMARY KEY,

    -- User-selected conversion options.
    blob_columns_json TEXT NOT NULL DEFAULT '[]' CHECK (
        json_valid(blob_columns_json)
        AND json_type(blob_columns_json) = 'array'
    ),
    indices_json TEXT NOT NULL DEFAULT '[]' CHECK (
        json_valid(indices_json)
        AND json_type(indices_json) = 'array'
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
    creation_timestamp_ms INTEGER NOT NULL,
    update_timestamp_ms INTEGER NOT NULL,

    -- Retry and lease state.
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    error_reasons_json TEXT NOT NULL DEFAULT '[]' CHECK (
        json_valid(error_reasons_json)
        AND json_type(error_reasons_json) = 'array'
    ),
    lease_expiration_timestamp_ms INTEGER,

    -- Job progress.
    rows_read INTEGER NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_written INTEGER NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    rows_total INTEGER NOT NULL DEFAULT 0 CHECK (rows_total >= 0),

    -- Cross-column invariants.
    CHECK (
        (status = 'running' AND lease_expiration_timestamp_ms IS NOT NULL)
        OR status != 'running'
    ),
    CHECK (
        rows_total = 0
        OR (rows_read <= rows_total AND rows_written <= rows_total)
    )
) STRICT;

-- Query pattern: claim the oldest queuing jobs in creation order.
CREATE INDEX IF NOT EXISTS jobs_queuing_index
    ON jobs(creation_timestamp_ms)
    WHERE status = 'queuing';

-- Query pattern: reclaim running jobs whose lease has expired.
CREATE INDEX IF NOT EXISTS jobs_expired_running_index
    ON jobs(lease_expiration_timestamp_ms)
    WHERE status = 'running';

-- Query pattern: filter jobs by creator.
CREATE INDEX IF NOT EXISTS jobs_creator_index
    ON jobs(creator);

-- Query pattern: list all jobs from newest to oldest.
CREATE INDEX IF NOT EXISTS jobs_creation_index
    ON jobs(creation_timestamp_ms);

-- Query pattern: list failed jobs from newest to oldest.
CREATE INDEX IF NOT EXISTS jobs_failed_index
    ON jobs(creation_timestamp_ms)
    WHERE status = 'failed';
