use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use datafusion::{
    arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    },
    parquet::arrow::ArrowWriter,
};
use lance::Dataset;
use lance_conversion_core::job::{Job, JobKind, JobProgress, JobStatus};

use crate::{ConversionProgress, Converter, ConverterConfig};

#[tokio::test]
async fn converts_local_parquet_directory() {
    let source = temporary_directory("source").await;
    let destination_parent = temporary_directory("destination").await;
    let destination = destination_parent.join("dataset.lance");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    let parquet = writer.into_inner().unwrap();
    tokio::fs::write(source.join("part.parquet"), parquet)
        .await
        .unwrap();

    let job = Job {
        creator: "test-user".to_owned(),
        kind: JobKind::Copy,
        source_uri: source.to_string_lossy().into_owned(),
        destination_uri: destination.to_string_lossy().into_owned(),
        status: JobStatus::Running,
        creation_timestamp_ms: 1,
        update_timestamp_ms: 1,
        attempt: 1,
        error_reasons: Vec::new(),
        lease_expiration_timestamp_ms: Some(i64::MAX),
        progress: JobProgress::default(),
    };
    let converter = Converter::new(ConverterConfig {
        target_lance_file_size_mib: 512,
        blob_inline_threshold_mib: 2,
    });
    let progress = converter
        .convert(&job, Arc::new(ConversionProgress::default()))
        .await
        .unwrap();

    assert_eq!(progress.rows_read, 3);
    assert_eq!(progress.rows_written, 3);
    assert_eq!(progress.rows_total, 3);
    assert!(
        Dataset::open(destination.to_string_lossy().as_ref())
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap()
            == 3
    );
    tokio::fs::remove_dir_all(source).await.unwrap();
    tokio::fs::remove_dir_all(destination_parent).await.unwrap();
}

async fn temporary_directory(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "lance-converter-test-{label}-{}-{timestamp}",
        std::process::id()
    ));
    tokio::fs::create_dir(&path).await.unwrap();
    path
}
