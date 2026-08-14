use std::{error::Error, path::Path};

use arrow::array::RecordBatch;
use lance_conversion_core::{
    job::{Job, JobProgress, JobStatus, NewJob},
    location::{DatasetLocation, LocationError},
};
use parquet::arrow::ArrowWriter;

pub type FixtureResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

/// Writes one record batch to a Parquet file.
///
/// # Errors
///
/// Returns an error when Arrow cannot encode the batch or the file cannot be written.
pub async fn write_parquet(path: impl AsRef<Path>, batch: &RecordBatch) -> FixtureResult {
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None)?;
    writer.write(batch)?;
    tokio::fs::write(path, writer.into_inner()?).await?;
    Ok(())
}

/// Creates a canonical running conversion job for converter tests.
#[must_use]
pub fn running_job(source: &Path, destination: &Path) -> Job {
    Job {
        creator: "test-user".to_owned(),
        source_uri: source.to_string_lossy().into_owned(),
        destination_uri: destination.to_string_lossy().into_owned(),
        blob_columns: Vec::new(),
        indices: Vec::new(),
        status: JobStatus::Running,
        creation_timestamp_ms: 1,
        update_timestamp_ms: 1,
        attempt: 1,
        error_reasons: Vec::new(),
        lease_expiration_timestamp_ms: Some(i64::MAX),
        progress: JobProgress::default(),
    }
}

/// Creates a canonical new job with empty conversion options.
///
/// # Errors
///
/// Returns an error when either dataset location uses an unsupported scheme.
pub fn new_job(
    creator: impl Into<String>,
    source_uri: impl Into<String>,
    destination_uri: impl Into<String>,
    creation_timestamp_ms: i64,
) -> Result<NewJob, LocationError> {
    Ok(NewJob {
        creator: creator.into(),
        source: DatasetLocation::parse_location(source_uri)?,
        destination: DatasetLocation::parse_location(destination_uri)?,
        blob_columns: Vec::new(),
        indices: Vec::new(),
        creation_timestamp_ms,
    })
}
