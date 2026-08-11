use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray},
        datatypes::{DataType, Field, Schema},
    },
    error::DataFusionError,
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;
use lance::{BlobArrayBuilder, BlobFieldOptions, blob_field_with_options};
use lance_conversion_core::job::BlobColumnSpec;

use crate::ConversionError;

const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
const BLOB_V2_EXTENSION_NAME: &str = "lance.blob.v2";
const BLOB_INLINE_THRESHOLD_KEY: &str = "lance-encoding:blob-inline-size-threshold";
const BLOB_DEDICATED_THRESHOLD_KEY: &str = "lance-encoding:blob-dedicated-size-threshold";

pub(crate) fn apply_blob_columns(
    stream: SendableRecordBatchStream,
    blob_columns: &[BlobColumnSpec],
    inline_threshold: usize,
    dedicated_threshold: NonZeroUsize,
) -> Result<SendableRecordBatchStream, ConversionError> {
    let source_schema = stream.schema();
    let selected = validate_blob_columns(source_schema.fields(), blob_columns)?;
    let fields = source_schema
        .fields()
        .iter()
        .map(|field| {
            if selected.contains(field.name()) {
                return Arc::new(blob_field_with_options(
                    field.name(),
                    field.is_nullable(),
                    BlobFieldOptions::default()
                        .with_inline_size_threshold(inline_threshold)
                        .with_dedicated_size_threshold(dedicated_threshold),
                ));
            }
            let mut output_field = field.as_ref().clone();
            if output_field
                .metadata()
                .get(ARROW_EXTENSION_NAME_KEY)
                .is_some_and(|name| name == BLOB_V2_EXTENSION_NAME)
            {
                let mut metadata = output_field.metadata().clone();
                metadata.insert(
                    BLOB_INLINE_THRESHOLD_KEY.to_owned(),
                    inline_threshold.to_string(),
                );
                metadata.insert(
                    BLOB_DEDICATED_THRESHOLD_KEY.to_owned(),
                    dedicated_threshold.to_string(),
                );
                output_field = output_field.with_metadata(metadata);
            }
            Arc::new(output_field)
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    let batch_schema = Arc::clone(&schema);
    let batches = stream.map(move |batch| {
        batch.and_then(|batch| transform_batch(&batch, &batch_schema, &selected))
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

fn validate_blob_columns(
    fields: &[Arc<Field>],
    blob_columns: &[BlobColumnSpec],
) -> Result<HashSet<String>, ConversionError> {
    let mut selected = HashSet::with_capacity(blob_columns.len());
    for spec in blob_columns {
        if !selected.insert(spec.column.clone()) {
            return Err(ConversionError::InvalidBlobSpec(format!(
                "column '{}' is selected more than once",
                spec.column
            )));
        }
        let matches = fields
            .iter()
            .filter(|field| field.name() == &spec.column)
            .collect::<Vec<_>>();
        let field = match matches.as_slice() {
            [] => {
                return Err(ConversionError::InvalidBlobSpec(format!(
                    "selected column '{}' does not exist",
                    spec.column
                )));
            }
            [field] => *field,
            _ => {
                return Err(ConversionError::InvalidBlobSpec(format!(
                    "selected column '{}' is duplicated in the source schema",
                    spec.column
                )));
            }
        };
        // DataFusion presents Parquet Utf8 as Utf8View by default. It is the
        // zero-copy execution representation of the same logical source type.
        if !matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Err(ConversionError::InvalidBlobSpec(format!(
                "selected column '{}' must have type Utf8 or LargeUtf8, found {}",
                spec.column,
                field.data_type()
            )));
        }
    }
    Ok(selected)
}

fn transform_batch(
    batch: &RecordBatch,
    schema: &Arc<Schema>,
    selected: &HashSet<String>,
) -> Result<RecordBatch, DataFusionError> {
    let columns = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            if selected.contains(field.name()) {
                uri_array(column, field.name())
            } else {
                Ok(Arc::clone(column))
            }
        })
        .collect::<Result<Vec<ArrayRef>, DataFusionError>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

fn uri_array(column: &ArrayRef, column_name: &str) -> Result<ArrayRef, DataFusionError> {
    let mut builder = BlobArrayBuilder::new(column.len());
    match column.data_type() {
        DataType::Utf8 => {
            let strings = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 columns use StringArray");
            for row in 0..strings.len() {
                let value = (!strings.is_null(row)).then(|| strings.value(row));
                push_uri(&mut builder, value, row, column_name)?;
            }
        }
        DataType::LargeUtf8 => {
            let strings = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("LargeUtf8 columns use LargeStringArray");
            for row in 0..strings.len() {
                let value = (!strings.is_null(row)).then(|| strings.value(row));
                push_uri(&mut builder, value, row, column_name)?;
            }
        }
        DataType::Utf8View => {
            let strings = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Utf8View columns use StringViewArray");
            for row in 0..strings.len() {
                let value = (!strings.is_null(row)).then(|| strings.value(row));
                push_uri(&mut builder, value, row, column_name)?;
            }
        }
        data_type => {
            return Err(DataFusionError::Execution(format!(
                "blob column '{column_name}' changed to incompatible type {data_type}"
            )));
        }
    }
    builder
        .finish()
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

fn push_uri(
    builder: &mut BlobArrayBuilder,
    value: Option<&str>,
    row: usize,
    column_name: &str,
) -> Result<(), DataFusionError> {
    let Some(value) = value else {
        return builder
            .push_null()
            .map_err(|error| DataFusionError::Execution(error.to_string()));
    };
    validate_blob_uri(value, column_name, row)?;
    builder
        .push_uri(value)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

fn validate_blob_uri(value: &str, column_name: &str, row: usize) -> Result<(), DataFusionError> {
    let url = reqwest::Url::parse(value).map_err(|error| {
        DataFusionError::Execution(format!(
            "blob column '{column_name}' row {row} is not a valid absolute URL: {error}"
        ))
    })?;
    match url.scheme() {
        "file" if url.path().starts_with('/') => Ok(()),
        "s3" | "http" | "https" if url.host_str().is_some() => Ok(()),
        "file" => Err(DataFusionError::Execution(format!(
            "blob column '{column_name}' row {row} must use an absolute file URL"
        ))),
        scheme => Err(DataFusionError::Execution(format!(
            "blob column '{column_name}' row {row} uses unsupported URL scheme '{scheme}'; supported schemes are file, s3, http, and https"
        ))),
    }
}

pub(crate) fn validate(fields: &[Arc<Field>]) -> Result<(), ConversionError> {
    for field in fields {
        if field.metadata().iter().any(|(key, value)| {
            key.to_lowercase().contains("variant") || value.to_lowercase().contains("variant")
        }) {
            return Err(ConversionError::UnsupportedType(format!(
                "column '{}' uses unsupported variant metadata",
                field.name()
            )));
        }
        validate_type(field.name(), field.data_type())?;
    }
    Ok(())
}

fn validate_type(column: &str, data_type: &DataType) -> Result<(), ConversionError> {
    match data_type {
        DataType::Union(_, _) => Err(ConversionError::UnsupportedType(format!(
            "column '{column}' uses unsupported union/variant data"
        ))),
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => validate_type(column, field.data_type()),
        DataType::Struct(fields) => {
            for field in fields {
                validate_type(field.name(), field.data_type())?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use datafusion::arrow::datatypes::{DataType, Field};
    use lance_conversion_core::job::BlobColumnSpec;

    use super::{validate, validate_blob_columns};
    use crate::ConversionError;

    #[test]
    fn accepts_nested_lists() {
        let nested = Field::new(
            "nested",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ))),
            true,
        );
        validate(&[Arc::new(nested)]).unwrap();
    }

    #[test]
    fn rejects_variant_metadata() {
        let variant =
            Field::new("variant", DataType::Binary, true).with_metadata(HashMap::from([(
                "PARQUET:logical_type".to_owned(),
                "VARIANT".to_owned(),
            )]));
        assert!(matches!(
            validate(&[Arc::new(variant)]),
            Err(ConversionError::UnsupportedType(_))
        ));
    }

    #[test]
    fn rejects_invalid_blob_column_specs() {
        let fields = vec![
            Arc::new(Field::new("url", DataType::Utf8, true)),
            Arc::new(Field::new("number", DataType::Int64, true)),
        ];
        let spec = |column: &str| BlobColumnSpec {
            column: column.to_owned(),
        };

        assert!(matches!(
            validate_blob_columns(&fields, &[spec("missing")]),
            Err(ConversionError::InvalidBlobSpec(message)) if message.contains("does not exist")
        ));
        assert!(matches!(
            validate_blob_columns(&fields, &[spec("url"), spec("url")]),
            Err(ConversionError::InvalidBlobSpec(message)) if message.contains("more than once")
        ));
        assert!(matches!(
            validate_blob_columns(&fields, &[spec("number")]),
            Err(ConversionError::InvalidBlobSpec(message)) if message.contains("Utf8 or LargeUtf8")
        ));
    }
}
