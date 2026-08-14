use lance_conversion_core::job::{BlobColumnSpec, IndexSpec, JobError, JobProgress, JobStatus};
use lance_job_store::StoreError;

use super::{i64_to_u32, i64_to_u64, parse_value, u64_as_i64};

#[derive(sqlx::Type)]
#[sqlx(type_name = "blob_column_spec")]
pub(super) struct PgBlobColumnSpec {
    column: String,
}

impl From<BlobColumnSpec> for PgBlobColumnSpec {
    fn from(spec: BlobColumnSpec) -> Self {
        Self {
            column: spec.column,
        }
    }
}

impl From<PgBlobColumnSpec> for BlobColumnSpec {
    fn from(spec: PgBlobColumnSpec) -> Self {
        Self {
            column: spec.column,
        }
    }
}

#[derive(sqlx::Type)]
#[sqlx(type_name = "index_spec")]
pub(super) struct PgIndexSpec {
    column: String,
    index_type: String,
}

impl From<IndexSpec> for PgIndexSpec {
    fn from(spec: IndexSpec) -> Self {
        Self {
            column: spec.column,
            index_type: spec.index_type.to_string(),
        }
    }
}

impl TryFrom<PgIndexSpec> for IndexSpec {
    type Error = StoreError;

    fn try_from(spec: PgIndexSpec) -> Result<Self, Self::Error> {
        Ok(Self {
            column: spec.column,
            index_type: parse_value(&spec.index_type)?,
        })
    }
}

#[derive(Clone, Copy, sqlx::Type)]
#[sqlx(type_name = "job_status", rename_all = "snake_case")]
pub(super) enum PgJobStatus {
    Queuing,
    Running,
    Succeeded,
    Failed,
}

impl From<JobStatus> for PgJobStatus {
    fn from(status: JobStatus) -> Self {
        match status {
            JobStatus::Queuing => Self::Queuing,
            JobStatus::Running => Self::Running,
            JobStatus::Succeeded => Self::Succeeded,
            JobStatus::Failed => Self::Failed,
        }
    }
}

impl From<PgJobStatus> for JobStatus {
    fn from(status: PgJobStatus) -> Self {
        match status {
            PgJobStatus::Queuing => Self::Queuing,
            PgJobStatus::Running => Self::Running,
            PgJobStatus::Succeeded => Self::Succeeded,
            PgJobStatus::Failed => Self::Failed,
        }
    }
}

#[derive(sqlx::Type)]
#[sqlx(type_name = "job_error")]
pub(super) struct PgJobError {
    attempt: i64,
    error_timestamp_ms: i64,
    reason: String,
}

impl From<JobError> for PgJobError {
    fn from(error: JobError) -> Self {
        Self {
            attempt: i64::from(error.attempt),
            error_timestamp_ms: error.error_timestamp_ms,
            reason: error.reason,
        }
    }
}

impl TryFrom<PgJobError> for JobError {
    type Error = StoreError;

    fn try_from(error: PgJobError) -> Result<Self, Self::Error> {
        Ok(Self {
            attempt: i64_to_u32(error.attempt)?,
            error_timestamp_ms: error.error_timestamp_ms,
            reason: error.reason,
        })
    }
}

#[derive(sqlx::Type)]
#[sqlx(type_name = "job_progress")]
#[allow(clippy::struct_field_names)]
pub(super) struct PgJobProgress {
    rows_read: i64,
    rows_written: i64,
    rows_total: i64,
}

impl TryFrom<JobProgress> for PgJobProgress {
    type Error = StoreError;

    fn try_from(progress: JobProgress) -> Result<Self, Self::Error> {
        Ok(Self {
            rows_read: u64_as_i64(progress.rows_read)?,
            rows_written: u64_as_i64(progress.rows_written)?,
            rows_total: u64_as_i64(progress.rows_total)?,
        })
    }
}

impl TryFrom<PgJobProgress> for JobProgress {
    type Error = StoreError;

    fn try_from(progress: PgJobProgress) -> Result<Self, Self::Error> {
        Ok(Self {
            rows_read: i64_to_u64(progress.rows_read)?,
            rows_written: i64_to_u64(progress.rows_written)?,
            rows_total: i64_to_u64(progress.rows_total)?,
        })
    }
}
