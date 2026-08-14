use std::sync::Arc;

use arrow::{
    array::{
        Array, Int64Array, RecordBatch, StringArray,
        builder::{Int64Builder, ListBuilder},
    },
    datatypes::{DataType, Field, Schema},
};
use futures::TryStreamExt;
use lance::{Dataset, index::DatasetIndexExt};
use lance_conversion_core::job::{BlobColumnSpec, IndexSpec, IndexType};
use lance_test_support::{running_job, write_parquet};
use tempfile::TempDir;

use crate::{ConversionError, ConversionProgress, Converter, ConverterConfig};

const TARGET_FILE_SIZE_MIB: u64 = 512;
const BLOB_INLINE_THRESHOLD_MIB: u64 = 2;
const BLOB_DEDICATED_THRESHOLD_MIB: u64 = 4;
const INLINE_PAYLOAD_BYTES: usize = 1024 * 1024;
const PACKED_PAYLOAD_BYTES: usize = 3 * 1024 * 1024;
const DEDICATED_PAYLOAD_BYTES: usize = 5 * 1024 * 1024;
const BLOB_ROW_INDEX: [u64; 1] = [0];

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
    write_parquet(source.join("part.parquet"), &batch)
        .await
        .unwrap();

    let job = running_job(&source, &destination);
    let converter = Converter::new(ConverterConfig {
        target_lance_file_size_mib: TARGET_FILE_SIZE_MIB,
        blob_inline_threshold_mib: BLOB_INLINE_THRESHOLD_MIB,
        blob_dedicated_threshold_mib: BLOB_DEDICATED_THRESHOLD_MIB,
    })
    .unwrap();
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
    assert!(!dataset.schema().field("value").unwrap().nullable);
}

#[tokio::test]
async fn converts_matching_parquet_files_in_sorted_order() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let destination = temp_dir.path().join("dataset.lance");
    tokio::fs::create_dir(&source).await.unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    for (file_name, value) in [("part-b.parquet", 2), ("part-a.parquet", 1)] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        write_parquet(source.join(file_name), &batch).await.unwrap();
    }

    let progress = Converter::new(test_config())
        .unwrap()
        .convert(
            &running_job(&source, &destination),
            Arc::new(ConversionProgress::default()),
        )
        .await
        .unwrap();

    assert_eq!(progress.rows_total, 2);
    let dataset = Dataset::open(destination.to_string_lossy().as_ref())
        .await
        .unwrap();
    let values = dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, [1, 2]);
}

#[tokio::test]
async fn rejects_mismatched_parquet_file_schemas() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let destination = temp_dir.path().join("dataset.lance");
    tokio::fs::create_dir(&source).await.unwrap();
    let integers =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1])) as _)]).unwrap();
    let strings =
        RecordBatch::try_from_iter([("value", Arc::new(StringArray::from(vec!["one"])) as _)])
            .unwrap();
    write_parquet(source.join("part-a.parquet"), &integers)
        .await
        .unwrap();
    write_parquet(source.join("part-b.parquet"), &strings)
        .await
        .unwrap();

    let error = Converter::new(test_config())
        .unwrap()
        .convert(
            &running_job(&source, &destination),
            Arc::new(ConversionProgress::default()),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, ConversionError::Validation(message) if message.contains("does not match"))
    );
    assert!(!destination.exists());
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
    write_parquet(source.join("part.parquet"), &batch)
        .await
        .unwrap();

    let job = running_job(&source, &destination);
    Converter::new(ConverterConfig {
        target_lance_file_size_mib: TARGET_FILE_SIZE_MIB,
        blob_inline_threshold_mib: BLOB_INLINE_THRESHOLD_MIB,
        blob_dedicated_threshold_mib: BLOB_DEDICATED_THRESHOLD_MIB,
    })
    .unwrap()
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
async fn ingests_inline_blob_storage() {
    assert_blob_storage(Some(INLINE_PAYLOAD_BYTES), Some("Inline")).await;
}

#[tokio::test]
async fn ingests_packed_blob_storage() {
    assert_blob_storage(Some(PACKED_PAYLOAD_BYTES), Some("Packed")).await;
}

#[tokio::test]
async fn ingests_external_blob_storage() {
    assert_blob_storage(Some(DEDICATED_PAYLOAD_BYTES), Some("Dedicated")).await;
}

#[tokio::test]
async fn ingests_null_blob() {
    assert_blob_storage(None, None).await;
}

async fn assert_blob_storage(payload_size: Option<usize>, expected_kind: Option<&str>) {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let destination = temp_dir.path().join("dataset.lance");
    tokio::fs::create_dir(&source).await.unwrap();
    let blob_path = if let Some(size) = payload_size {
        let path = temp_dir.path().join("payload.bin");
        tokio::fs::write(&path, vec![0_u8; size]).await.unwrap();
        Some(path)
    } else {
        None
    };
    let blob_url = blob_path
        .as_ref()
        .map(|path| reqwest::Url::from_file_path(path).unwrap().to_string());
    let schema = Arc::new(Schema::new(vec![Field::new("asset", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![blob_url]))],
    )
    .unwrap();
    write_parquet(source.join("part.parquet"), &batch)
        .await
        .unwrap();

    let mut job = running_job(&source, &destination);
    job.blob_columns = vec![BlobColumnSpec {
        column: "asset".to_owned(),
    }];
    Converter::new(test_config())
        .unwrap()
        .convert(&job, Arc::new(ConversionProgress::default()))
        .await
        .unwrap();
    if let Some(path) = blob_path {
        tokio::fs::remove_file(path).await.unwrap();
    }

    let dataset = Arc::new(
        Dataset::open(destination.to_string_lossy().as_ref())
            .await
            .unwrap(),
    );
    let blobs = dataset
        .take_blobs_by_indices(&BLOB_ROW_INDEX, "asset")
        .await
        .unwrap();
    match (blobs[0].as_ref(), expected_kind, payload_size) {
        (Some(blob), Some(kind), Some(size)) => {
            assert_eq!(format!("{:?}", blob.kind()), kind);
            assert_eq!(blob.size(), u64::try_from(size).unwrap());
            assert_eq!(blob.read().await.unwrap().len(), size);
        }
        (None, None, None) => {}
        _ => panic!("blob storage did not match the expected kind"),
    }
}

#[tokio::test]
async fn creates_requested_scalar_and_text_indexes() {
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
    write_parquet(source.join("part.parquet"), &batch)
        .await
        .unwrap();

    let mut job = running_job(&source, &destination);
    job.indices = vec![
        IndexSpec {
            columns: vec!["id".to_owned()],
            index_type: IndexType::Scalar,
        },
        IndexSpec {
            columns: vec!["text".to_owned()],
            index_type: IndexType::Text,
        },
    ];
    Converter::new(test_config())
        .unwrap()
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
            .any(|index| index.name == "conversion_0_scalar_idx")
    );
    assert!(
        indexes
            .iter()
            .any(|index| index.name == "conversion_1_text_idx")
    );
}

fn test_config() -> ConverterConfig {
    ConverterConfig {
        target_lance_file_size_mib: TARGET_FILE_SIZE_MIB,
        blob_inline_threshold_mib: BLOB_INLINE_THRESHOLD_MIB,
        blob_dedicated_threshold_mib: BLOB_DEDICATED_THRESHOLD_MIB,
    }
}
