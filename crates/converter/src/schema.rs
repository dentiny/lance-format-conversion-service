use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray},
        datatypes::{DataType, Schema},
    },
    error::DataFusionError,
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;
use lance::{BlobArrayBuilder, BlobFieldOptions, blob_field_with_options};
use lance_conversion_core::job::BlobColumnSpec;

use crate::{ConversionError, validation};

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
    let selected = validation::validate_blob_columns(source_schema.fields(), blob_columns)?;
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
    validation::validate_blob_uri(value, column_name, row)?;
    builder
        .push_uri(value)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}
