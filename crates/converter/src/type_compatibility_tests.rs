use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{ArrayRef, RecordBatch, StringDictionaryBuilder, new_null_array},
        datatypes::{DataType, Field, Fields, Int32Type, Schema, TimeUnit},
    },
    parquet::arrow::ArrowWriter,
};
use lance::Dataset;
use lance_conversion_core::job::{Job, JobKind, JobProgress, JobStatus};
use tempfile::TempDir;

use crate::{ConversionProgress, Converter, ConverterConfig};

const TARGET_FILE_SIZE_MIB: u64 = 512;
const BLOB_INLINE_THRESHOLD_MIB: u64 = 2;
const BLOB_DEDICATED_THRESHOLD_MIB: u64 = 4;

#[tokio::test]
async fn converts_supported_parquet_types_to_lance() {
    let fields = get_test_fields();
    let temp_dir = TempDir::new().unwrap();
    for field in &fields {
        convert_field(temp_dir.path(), field.as_ref())
            .await
            .unwrap_or_else(|error| panic!("type '{}' failed: {error}", field.name()));
    }
}

async fn convert_field(root: &std::path::Path, field: &Field) -> Result<(), String> {
    let source = root.join(format!("{}-source", field.name()));
    let destination = root.join(format!("{}.lance", field.name()));
    tokio::fs::create_dir(&source)
        .await
        .map_err(|error| error.to_string())?;
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![get_test_array(field)])
        .map_err(|error| error.to_string())?;
    let mut writer =
        ArrowWriter::try_new(Vec::new(), schema, None).map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    tokio::fs::write(
        source.join("data.parquet"),
        writer.into_inner().map_err(|error| error.to_string())?,
    )
    .await
    .map_err(|error| error.to_string())?;

    Converter::new(ConverterConfig {
        target_lance_file_size_mib: TARGET_FILE_SIZE_MIB,
        blob_inline_threshold_mib: BLOB_INLINE_THRESHOLD_MIB,
        blob_dedicated_threshold_mib: BLOB_DEDICATED_THRESHOLD_MIB,
    })
    .convert(
        &get_test_job(&source, &destination),
        Arc::new(ConversionProgress::default()),
    )
    .await
    .map_err(|error| error.to_string())?;
    let dataset = Dataset::open(destination.to_string_lossy().as_ref())
        .await
        .map_err(|error| error.to_string())?;
    if dataset
        .count_rows(None)
        .await
        .map_err(|error| error.to_string())?
        != 1
    {
        return Err("row count differs after conversion".to_owned());
    }
    Ok(())
}

fn get_test_array(field: &Field) -> ArrayRef {
    if field.name() == "dictionary" {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append("value").unwrap();
        Arc::new(builder.finish())
    } else {
        new_null_array(field.data_type(), 1)
    }
}

/// Canonical Arrow types that both the Parquet reader and Lance 2.3 support.
fn get_test_fields() -> Fields {
    let item = || Arc::new(Field::new("item", DataType::Int64, true));
    let nested_list = DataType::List(Arc::new(Field::new("item", DataType::List(item()), true)));
    let struct_type = DataType::Struct(
        vec![
            Field::new("number", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
        ]
        .into(),
    );
    let map_entries = DataType::Struct(
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]
        .into(),
    );

    vec![
        Field::new("null", DataType::Null, true),
        Field::new("boolean", DataType::Boolean, true),
        Field::new("int8", DataType::Int8, true),
        Field::new("int16", DataType::Int16, true),
        Field::new("int32", DataType::Int32, true),
        Field::new("int64", DataType::Int64, true),
        Field::new("uint8", DataType::UInt8, true),
        Field::new("uint16", DataType::UInt16, true),
        Field::new("uint32", DataType::UInt32, true),
        Field::new("uint64", DataType::UInt64, true),
        Field::new("float16", DataType::Float16, true),
        Field::new("float32", DataType::Float32, true),
        Field::new("float64", DataType::Float64, true),
        Field::new("decimal128", DataType::Decimal128(38, 4), true),
        Field::new("decimal256", DataType::Decimal256(76, 4), true),
        Field::new("utf8", DataType::Utf8, true),
        Field::new("utf8_view", DataType::Utf8View, true),
        Field::new("large_utf8", DataType::LargeUtf8, true),
        Field::new("binary", DataType::Binary, true),
        Field::new("binary_view", DataType::BinaryView, true),
        Field::new("large_binary", DataType::LargeBinary, true),
        Field::new("fixed_binary", DataType::FixedSizeBinary(16), true),
        Field::new("date32", DataType::Date32, true),
        Field::new("date64", DataType::Date64, true),
        Field::new("time32", DataType::Time32(TimeUnit::Millisecond), true),
        Field::new("time64", DataType::Time64(TimeUnit::Microsecond), true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("duration", DataType::Duration(TimeUnit::Microsecond), true),
        Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("list", DataType::List(item()), true),
        Field::new("large_list", DataType::LargeList(item()), true),
        Field::new("fixed_size_list", DataType::FixedSizeList(item(), 3), true),
        Field::new("nested_list", nested_list, true),
        Field::new("struct", struct_type, true),
        Field::new(
            "map",
            DataType::Map(Arc::new(Field::new("entries", map_entries, false)), false),
            true,
        ),
    ]
    .into()
}

fn get_test_job(source: &std::path::Path, destination: &std::path::Path) -> Job {
    Job {
        creator: "type-compatibility-test".to_owned(),
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
