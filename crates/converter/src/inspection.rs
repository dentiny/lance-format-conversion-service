use datafusion::{arrow::datatypes::DataType, prelude::SessionContext};
use lance_conversion_core::location::DatasetLocation;
use serde::Serialize;

use crate::{ConversionError, source, validation};

/// A validated source schema ready for conversion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SourceSchemaInspection {
    pub columns: Vec<SourceColumn>,
}

/// One top-level column reported by a source schema inspection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SourceColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub blob_eligible: bool,
}

/// Inspects the validated `DataFusion` union schema for a conversion source.
///
/// # Errors
///
/// Returns an error when the URI is invalid, the source cannot be prepared or
/// read, its Parquet schemas cannot be unioned, or its schema is unsupported.
pub async fn inspect_source_schema(
    source_uri: &str,
) -> Result<SourceSchemaInspection, ConversionError> {
    let location = DatasetLocation::parse_location(source_uri)
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    let source = <dyn source::SourceDataset>::open(location);
    let context = SessionContext::new();
    let dataframe = source::prepare_dataframe(source.as_ref(), &context).await?;
    let fields = dataframe.schema().fields();
    validation::validate_schema(fields)?;

    let columns = fields
        .iter()
        .map(|field| SourceColumn {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
            blob_eligible: is_blob_eligible(field.data_type()),
        })
        .collect();
    Ok(SourceSchemaInspection { columns })
}

/// Returns whether a `DataFusion` column can be selected for blob ingestion.
const fn is_blob_eligible(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use datafusion::{
        arrow::{
            array::{Int64Array, RecordBatch, StringArray, new_empty_array},
            datatypes::{DataType, Field, Schema},
        },
        parquet::arrow::ArrowWriter,
    };
    use tempfile::TempDir;

    use super::inspect_source_schema;

    const FIRST_PARQUET_FILE: &str = "part-0.parquet";
    const SECOND_PARQUET_FILE: &str = "part-1.parquet";

    /// Verifies local preparation, union schema semantics, and blob eligibility.
    #[tokio::test]
    async fn inspects_local_parquet_columns_and_blob_eligibility() {
        let temp_dir = TempDir::new().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("number", DataType::Int64, false),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("first")])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("second")])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();
        write_parquet(&temp_dir.path().join(FIRST_PARQUET_FILE), &first).await;
        write_parquet(&temp_dir.path().join(SECOND_PARQUET_FILE), &second).await;

        let inspection = inspect_source_schema(temp_dir.path().to_string_lossy().as_ref())
            .await
            .unwrap();

        assert_eq!(inspection.columns.len(), 2);
        assert_eq!(inspection.columns[0].name, "text");
        assert!(inspection.columns[0].blob_eligible);
        assert!(inspection.columns[0].nullable);
        assert_eq!(inspection.columns[1].name, "number");
        assert!(!inspection.columns[1].blob_eligible);
        assert!(!inspection.columns[1].nullable);
    }

    /// Verifies nested Arrow types use their normal display representation.
    #[tokio::test]
    async fn displays_nested_local_parquet_type() {
        let temp_dir = TempDir::new().unwrap();
        let nested_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![Field::new("value", DataType::Int64, true)]
                    .into_iter()
                    .collect(),
            ),
            true,
        )));
        let schema = Arc::new(Schema::new(vec![Field::new("nested", nested_type, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![new_empty_array(schema.field(0).data_type())],
        )
        .unwrap();
        write_parquet(&temp_dir.path().join(FIRST_PARQUET_FILE), &batch).await;

        let inspection = inspect_source_schema(temp_dir.path().to_string_lossy().as_ref())
            .await
            .unwrap();

        let nested = &inspection.columns[0];
        assert_eq!(nested.name, "nested");
        assert!(nested.data_type.contains("List"));
        assert!(nested.data_type.contains("Struct"));
        assert!(nested.data_type.contains("Int64"));
        assert!(!nested.blob_eligible);
    }

    /// Writes one record batch to a local Parquet file for inspection tests.
    async fn write_parquet(path: &Path, batch: &RecordBatch) {
        let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        tokio::fs::write(path, writer.into_inner().unwrap())
            .await
            .unwrap();
    }
}
