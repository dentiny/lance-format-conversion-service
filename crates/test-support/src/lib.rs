use std::{path::Path, sync::Arc};

use arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use lance_conversion_core::{
    job::{Job, JobProgress, JobStatus, NewJob},
    location::{DatasetLocation, LocationError},
};
use parquet::arrow::async_writer::AsyncArrowWriter;

/// Schema used by shared conversion fixtures: nullable `text` and required `number`.
#[must_use]
pub fn get_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("number", DataType::Int64, false),
    ]))
}

/// Writes `part-0.parquet` with one test row into `dir`.
#[allow(clippy::missing_panics_doc)]
pub async fn write_test_parquet(dir: impl AsRef<Path>) {
    let batch = RecordBatch::try_new(
        get_test_schema(),
        vec![
            Arc::new(StringArray::from(vec![Some("row")])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .expect("test batch matches test schema");
    write_parquet(dir.as_ref().join("part-0.parquet"), &batch).await;
}

/// Writes one record batch to a Parquet file.
#[allow(clippy::missing_panics_doc)]
pub async fn write_parquet(path: impl AsRef<Path>, batch: &RecordBatch) {
    let file = tokio::fs::File::create(path).await.unwrap();
    let mut writer = AsyncArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).await.unwrap();
    writer.close().await.unwrap();
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
