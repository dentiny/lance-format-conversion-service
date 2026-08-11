use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{
            Array, Int64Array, RecordBatch, StringArray,
            builder::{Int64Builder, ListBuilder},
        },
        datatypes::{DataType, Field, Schema},
    },
    parquet::arrow::ArrowWriter,
};
use futures::TryStreamExt;
use lance::{Dataset, index::DatasetIndexExt};
use lance_conversion_core::job::{
    BlobColumnSpec, IndexSpec, IndexType, Job, JobKind, JobProgress, JobStatus,
};
use tempfile::TempDir;

use crate::{ConversionProgress, Converter, ConverterConfig};

const TARGET_FILE_SIZE_MIB: u64 = 512;
const BLOB_INLINE_THRESHOLD_MIB: u64 = 2;
const BLOB_DEDICATED_THRESHOLD_MIB: u64 = 4;
const MIB: u64 = 1024 * 1024;
const INLINE_PAYLOAD_BYTES: usize = 1024 * 1024;
const PACKED_PAYLOAD_BYTES: usize = 3 * 1024 * 1024;
const DEDICATED_PAYLOAD_BYTES: usize = 5 * 1024 * 1024;
const BLOB_ROW_INDICES: [u64; 3] = [0, 1, 2];
const EXPECTED_BLOB_KINDS: [&str; 3] = ["Inline", "Packed", "Dedicated"];

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
        blob_columns: Vec::new(),
        indices: Vec::new(),
        status: JobStatus::Running,
        creation_timestamp_ms: 1,
        update_timestamp_ms: 1,
        attempt: 1,
        error_reasons: Vec::new(),
        lease_expiration_timestamp_ms: Some(i64::MAX),
        progress: JobProgress::default(),
    };
    let converter = Converter::new(ConverterConfig {
        target_lance_file_size_mib: TARGET_FILE_SIZE_MIB,
        blob_inline_threshold_mib: BLOB_INLINE_THRESHOLD_MIB,
        blob_dedicated_threshold_mib: BLOB_DEDICATED_THRESHOLD_MIB,
    });
    let progress = converter
        .convert(&job, Arc::new(ConversionProgress::default()))
        .await
        .unwrap();

    assert_eq!(progress.rows_read, 3);
    assert_eq!(progress.rows_written, 3);
    assert_eq!(progress.rows_total, 3);
    let dataset = Dataset::open(destination.to_string_lossy().as_ref())
        .await
        .unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
    assert_eq!(
        dataset.schema().field("value").unwrap().data_type(),
        DataType::Int64
    );
}

#[tokio::test]
async fn converts_local_nested_list_columns() {
    let mut list = ListBuilder::new(Int64Builder::new());
    list.append_value([Some(1), Some(2)]);
    list.append_value([Some(3)]);
    let list = Arc::new(list.finish());

    let mut nested = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    nested.append_value(vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3)])]);
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
    tokio::fs::write(source.join("part.parquet"), writer.into_inner().unwrap())
        .await
        .unwrap();

    let job = Job {
        creator: "test-user".to_owned(),
        kind: JobKind::Copy,
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
    };
    Converter::new(ConverterConfig {
        target_lance_file_size_mib: TARGET_FILE_SIZE_MIB,
        blob_inline_threshold_mib: BLOB_INLINE_THRESHOLD_MIB,
        blob_dedicated_threshold_mib: BLOB_DEDICATED_THRESHOLD_MIB,
    })
    .convert(&job, Arc::new(ConversionProgress::default()))
    .await
    .unwrap();

    let dataset = Arc::new(
        Dataset::open(destination.to_string_lossy().as_ref())
            .await
            .unwrap(),
    );
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

#[tokio::test]
async fn ingests_nullable_file_url_blob() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let destination = temp_dir.path().join("dataset.lance");
    let blob_path = temp_dir.path().join("payload.bin");
    let payload = b"blob payload";
    tokio::fs::create_dir(&source).await.unwrap();
    tokio::fs::write(&blob_path, payload).await.unwrap();
    let blob_url = reqwest::Url::from_file_path(&blob_path)
        .unwrap()
        .to_string();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("asset", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some(blob_url), None])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    tokio::fs::write(source.join("part.parquet"), writer.into_inner().unwrap())
        .await
        .unwrap();

    let mut job = test_job(&source, &destination);
    job.blob_columns = vec![BlobColumnSpec {
        column: "asset".to_owned(),
    }];
    Converter::new(test_config())
        .convert(&job, Arc::new(ConversionProgress::default()))
        .await
        .unwrap();
    tokio::fs::remove_file(blob_path).await.unwrap();

    let dataset = Arc::new(
        Dataset::open(destination.to_string_lossy().as_ref())
            .await
            .unwrap(),
    );
    let blobs = dataset
        .take_blobs_by_indices(&[0, 1], "asset")
        .await
        .unwrap();
    assert_eq!(blobs.len(), 2);
    assert_eq!(
        blobs[0].as_ref().unwrap().read().await.unwrap().as_ref(),
        payload
    );
    assert!(blobs[1].is_none());
    let fields = dataset.schema().fields.iter().collect::<Vec<_>>();
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[1].name, "asset");
    assert_eq!(
        fields[1]
            .metadata
            .get("lance-encoding:blob-inline-size-threshold"),
        Some(&(BLOB_INLINE_THRESHOLD_MIB * MIB).to_string())
    );
    assert_eq!(
        fields[1]
            .metadata
            .get("lance-encoding:blob-dedicated-size-threshold"),
        Some(&(BLOB_DEDICATED_THRESHOLD_MIB * MIB).to_string())
    );
}

#[tokio::test]
async fn places_ingested_blobs_by_threshold() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let destination = temp_dir.path().join("dataset.lance");
    tokio::fs::create_dir(&source).await.unwrap();

    let payload_sizes = [
        INLINE_PAYLOAD_BYTES,
        PACKED_PAYLOAD_BYTES,
        DEDICATED_PAYLOAD_BYTES,
    ];
    let mut blob_paths = Vec::with_capacity(payload_sizes.len());
    let mut blob_urls = Vec::with_capacity(payload_sizes.len());
    for (position, size) in payload_sizes.into_iter().enumerate() {
        let path = temp_dir.path().join(format!("payload-{position}.bin"));
        tokio::fs::write(&path, vec![0_u8; size]).await.unwrap();
        blob_urls.push(reqwest::Url::from_file_path(&path).unwrap().to_string());
        blob_paths.push(path);
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "asset",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(blob_urls))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    tokio::fs::write(source.join("part.parquet"), writer.into_inner().unwrap())
        .await
        .unwrap();

    let mut job = test_job(&source, &destination);
    job.blob_columns = vec![BlobColumnSpec {
        column: "asset".to_owned(),
    }];
    Converter::new(test_config())
        .convert(&job, Arc::new(ConversionProgress::default()))
        .await
        .unwrap();
    for path in blob_paths {
        tokio::fs::remove_file(path).await.unwrap();
    }

    let dataset = Arc::new(
        Dataset::open(destination.to_string_lossy().as_ref())
            .await
            .unwrap(),
    );
    let blobs = dataset
        .take_blobs_by_indices(&BLOB_ROW_INDICES, "asset")
        .await
        .unwrap();
    let actual_kinds = blobs
        .iter()
        .map(|blob| format!("{:?}", blob.as_ref().unwrap().kind()))
        .collect::<Vec<_>>();
    assert_eq!(actual_kinds, EXPECTED_BLOB_KINDS);
}

#[tokio::test]
async fn creates_requested_scalar_indexes() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let destination = temp_dir.path().join("dataset.lance");
    tokio::fs::create_dir(&source).await.unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    tokio::fs::write(source.join("part.parquet"), writer.into_inner().unwrap())
        .await
        .unwrap();

    let mut job = test_job(&source, &destination);
    job.indices = vec![
        IndexSpec {
            columns: vec!["id".to_owned()],
            index_type: IndexType::BTree,
        },
        IndexSpec {
            columns: vec!["text".to_owned()],
            index_type: IndexType::NGram,
        },
    ];
    Converter::new(test_config())
        .convert(&job, Arc::new(ConversionProgress::default()))
        .await
        .unwrap();

    let dataset = Dataset::open(destination.to_string_lossy().as_ref())
        .await
        .unwrap();
    let indexes = dataset.load_indices().await.unwrap();
    assert_eq!(indexes.len(), 2);
    assert!(
        indexes
            .iter()
            .any(|index| index.name == "conversion_0_b_tree_idx")
    );
    assert!(
        indexes
            .iter()
            .any(|index| index.name == "conversion_1_n_gram_idx")
    );
}

#[tokio::test]
async fn rejects_incompatible_index_type_before_index_build() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let destination = temp_dir.path().join("dataset.lance");
    tokio::fs::create_dir(&source).await.unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    tokio::fs::write(source.join("part.parquet"), writer.into_inner().unwrap())
        .await
        .unwrap();

    let mut job = test_job(&source, &destination);
    job.indices = vec![IndexSpec {
        columns: vec!["value".to_owned()],
        index_type: IndexType::Inverted,
    }];
    let error = Converter::new(test_config())
        .convert(&job, Arc::new(ConversionProgress::default()))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("inverted index is incompatible with column 'value'")
    );
}

fn test_config() -> ConverterConfig {
    ConverterConfig {
        target_lance_file_size_mib: TARGET_FILE_SIZE_MIB,
        blob_inline_threshold_mib: BLOB_INLINE_THRESHOLD_MIB,
        blob_dedicated_threshold_mib: BLOB_DEDICATED_THRESHOLD_MIB,
    }
}

fn test_job(source: &std::path::Path, destination: &std::path::Path) -> Job {
    Job {
        creator: "test-user".to_owned(),
        kind: JobKind::Copy,
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
