use sqlx::{PgConnection, Row, postgres::PgRow};

use lance_conversion_core::job::{
    BlobColumnSpec, IndexSpec, Job, JobError, JobProgress, JobStatus,
};
use lance_job_store::StoreError;

use super::{
    database_error, i64_to_u32,
    types::{PgBlobColumnSpec, PgIndexSpec, PgJobError, PgJobProgress, PgJobStatus},
};

pub(super) const SELECT_JOBS_SQL: &str = "SELECT creator, source_uri, destination_uri,
    status, creation_timestamp_ms, update_timestamp_ms, attempt,
    error_reasons, lease_expiration_timestamp_ms, progress,
    blob_columns, indices
    FROM jobs";
const LOAD_JOB_SQL: &str = "SELECT creator, source_uri, destination_uri,
    status, creation_timestamp_ms, update_timestamp_ms, attempt,
    error_reasons, lease_expiration_timestamp_ms, progress,
    blob_columns, indices
    FROM jobs
    WHERE destination_uri = $1";

pub(super) async fn load_job(
    connection: &mut PgConnection,
    destination_uri: &str,
) -> Result<Job, StoreError> {
    let row = sqlx::query(LOAD_JOB_SQL)
        .bind(destination_uri)
        .fetch_optional(connection)
        .await
        .map_err(database_error)?
        .ok_or(StoreError::NotFound)?;
    row_to_job(&row)
}

pub(super) fn row_to_job(row: &PgRow) -> Result<Job, StoreError> {
    let error_reasons = row
        .try_get::<Vec<PgJobError>, _>("error_reasons")
        .map_err(database_error)?
        .into_iter()
        .map(JobError::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let blob_columns = row
        .try_get::<Vec<PgBlobColumnSpec>, _>("blob_columns")
        .map_err(database_error)?
        .into_iter()
        .map(BlobColumnSpec::from)
        .collect();
    let indices = row
        .try_get::<Vec<PgIndexSpec>, _>("indices")
        .map_err(database_error)?
        .into_iter()
        .map(IndexSpec::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Job {
        creator: row.try_get("creator").map_err(database_error)?,
        source_uri: row.try_get("source_uri").map_err(database_error)?,
        destination_uri: row.try_get("destination_uri").map_err(database_error)?,
        blob_columns,
        indices,
        status: JobStatus::from(
            row.try_get::<PgJobStatus, _>("status")
                .map_err(database_error)?,
        ),
        creation_timestamp_ms: row
            .try_get("creation_timestamp_ms")
            .map_err(database_error)?,
        update_timestamp_ms: row.try_get("update_timestamp_ms").map_err(database_error)?,
        attempt: i64_to_u32(row.try_get("attempt").map_err(database_error)?)?,
        error_reasons,
        lease_expiration_timestamp_ms: row
            .try_get("lease_expiration_timestamp_ms")
            .map_err(database_error)?,
        progress: JobProgress::try_from(
            row.try_get::<PgJobProgress, _>("progress")
                .map_err(database_error)?,
        )?,
    })
}
