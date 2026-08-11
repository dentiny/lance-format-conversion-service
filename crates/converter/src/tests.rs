use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{
            Array, Int64Array, RecordBatch,
            builder::{Int64Builder, ListBuilder},
        },
        datatypes::{DataType, Field, Schema},
    },
    parquet::arrow::ArrowWriter,
};
use futures::TryStreamExt;
use lance::Dataset;
use lance_conversion_core::job::{Job, JobKind, JobProgress, JobStatus};
use tempfile::TempDir;

use crate::{ConversionProgress, Converter, ConverterConfig};

#[tokio::test]
async fn converts_local_parquet_directory() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let destination = temp_dir.path().join("dataset.lance");
    tokio::fs::create_dir(&source).await.unwrap();
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
}

#[tokio::test]
async fn converts_local_nested_list_columns() {
    let mut list = ListBuilder::new(Int64Builder::new());
    list.append_value([Some(1), Some(2)]);
    list.append_value([Some(3)]);
    let list = Arc::new(list.finish());

    let mut nested = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    nested.append_value(vec![
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(3)]),
    ]);
    nested.append_value(vec![None, Some(Vec::<Option<i64>>::new())]);
    let nested = Arc::new(nested.finish());

    let schema = Arc::new(Schema::new(vec![
        Field::new("list", list.data_type().clone(), false),
        Field::new("nested_list", nested.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![list, nested]).unwrap();
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let destination = temp_dir.path().join("dataset.lance");
    tokio::fs::create_dir(&source).await.unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    tokio::fs::write(
        source.join("part.parquet"),
        writer.into_inner().unwrap(),
    )
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
    Converter::new(ConverterConfig {
        target_lance_file_size_mib: 512,
        blob_inline_threshold_mib: 2,
    })
    .convert(&job, Arc::new(ConversionProgress::default()))
    .await
    .unwrap();

    let dataset = Dataset::open(destination.to_string_lossy().as_ref())
        .await
        .unwrap();
    let output = dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(
        output[0].schema().field(0).data_type(),
        batch.schema().field(0).data_type()
    );
    assert_eq!(
        output[0].schema().field(1).data_type(),
        batch.schema().field(1).data_type()
    );

}
