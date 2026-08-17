-- Applied by Terraform in production. Tests execute this file against PGlite.

CREATE TYPE blob_column_spec AS (
    "column" TEXT
);

CREATE TYPE index_spec AS (
    "column" TEXT,
    index_type TEXT
);

CREATE TYPE job_status AS ENUM (
    'queuing',
    'running',
    'succeeded',
    'failed'
);

CREATE TYPE job_error AS (
    attempt BIGINT,
    error_timestamp_ms BIGINT,
    reason TEXT
);

CREATE TYPE job_progress AS (
    rows_read BIGINT,
    rows_written BIGINT,
    rows_total BIGINT,
    rows_missing_blobs BIGINT
);

CREATE TABLE jobs (
    -- Job identity and conversion request.
    creator TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    destination_uri TEXT PRIMARY KEY,

    -- User-selected conversion options.
    blob_columns blob_column_spec[] NOT NULL DEFAULT ARRAY[]::blob_column_spec[],
    indices index_spec[] NOT NULL DEFAULT ARRAY[]::index_spec[],

    -- Job lifecycle.
    status job_status NOT NULL,
    creation_timestamp_ms BIGINT NOT NULL,
    update_timestamp_ms BIGINT NOT NULL,

    -- Retry and lease state.
    attempt BIGINT NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    error_reasons job_error[] NOT NULL DEFAULT ARRAY[]::job_error[],
    lease_expiration_timestamp_ms BIGINT,

    -- Job progress.
    progress job_progress NOT NULL DEFAULT (0, 0, 0, 0)::job_progress,

    -- Cross-column invariants.
    CHECK (
        (status = 'running' AND lease_expiration_timestamp_ms IS NOT NULL)
        OR status != 'running'
    ),
    CHECK (
        (progress).rows_read >= 0
        AND (progress).rows_written >= 0
        AND (progress).rows_total >= 0
        AND (progress).rows_missing_blobs >= 0
    ),
    CHECK (
        (progress).rows_total = 0
        OR (
            (progress).rows_read <= (progress).rows_total
            AND (progress).rows_written <= (progress).rows_total
            AND (progress).rows_missing_blobs <= (progress).rows_total
        )
    )
);

-- Query pattern: claim the oldest queuing jobs in creation order.
CREATE INDEX jobs_queuing_index
    ON jobs(creation_timestamp_ms)
    WHERE status = 'queuing';

-- Query pattern: list and reclaim running jobs in creation order.
CREATE INDEX jobs_running_index
    ON jobs(creation_timestamp_ms)
    WHERE status = 'running';

-- Query pattern: list failed jobs from newest to oldest.
CREATE INDEX jobs_failed_index
    ON jobs(creation_timestamp_ms)
    WHERE status = 'failed';

-- Query pattern: filter jobs by creator.
CREATE INDEX jobs_creator_index
    ON jobs(creator);

-- Query pattern: list all jobs from newest to oldest.
CREATE INDEX jobs_creation_index
    ON jobs(creation_timestamp_ms);

-- Query pattern: list jobs from newest to oldest by last update.
CREATE INDEX jobs_update_index
    ON jobs(update_timestamp_ms);
