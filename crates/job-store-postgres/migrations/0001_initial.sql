DO $migration$
BEGIN
    CREATE TYPE blob_column_spec AS (
        "column" TEXT
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$migration$;

DO $migration$
BEGIN
    CREATE TYPE index_spec AS (
        "column" TEXT,
        index_type TEXT
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$migration$;

DO $migration$
BEGIN
    CREATE TYPE job_status AS ENUM (
        'queuing',
        'running',
        'succeeded',
        'failed'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$migration$;

DO $migration$
BEGIN
    CREATE TYPE job_error AS (
        attempt BIGINT,
        error_timestamp_ms BIGINT,
        reason TEXT
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$migration$;

DO $migration$
BEGIN
    CREATE TYPE job_progress AS (
        rows_read BIGINT,
        rows_written BIGINT,
        rows_total BIGINT
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$migration$;

CREATE TABLE IF NOT EXISTS jobs (
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
    progress job_progress NOT NULL DEFAULT (0, 0, 0)::job_progress,

    -- Cross-column invariants.
    CHECK (
        (status = 'running' AND lease_expiration_timestamp_ms IS NOT NULL)
        OR status != 'running'
    ),
    CHECK (
        (progress).rows_read >= 0
        AND (progress).rows_written >= 0
        AND (progress).rows_total >= 0
    ),
    CHECK (
        (progress).rows_total = 0
        OR (
            (progress).rows_read <= (progress).rows_total
            AND (progress).rows_written <= (progress).rows_total
        )
    )
);

-- Query pattern: claim the oldest queuing jobs in creation order.
CREATE INDEX IF NOT EXISTS jobs_queuing_idx
    ON jobs(creation_timestamp_ms)
    WHERE status = 'queuing';

-- Query pattern: reclaim running jobs whose lease has expired.
CREATE INDEX IF NOT EXISTS jobs_expired_running_idx
    ON jobs(lease_expiration_timestamp_ms)
    WHERE status = 'running';

-- Query pattern: filter jobs by creator.
CREATE INDEX IF NOT EXISTS jobs_creator_idx
    ON jobs(creator);

-- Query pattern: list all jobs from newest to oldest.
CREATE INDEX IF NOT EXISTS jobs_creation_idx
    ON jobs(creation_timestamp_ms);

-- Query pattern: list failed jobs from newest to oldest.
CREATE INDEX IF NOT EXISTS jobs_failed_idx
    ON jobs(creation_timestamp_ms)
    WHERE status = 'failed';
