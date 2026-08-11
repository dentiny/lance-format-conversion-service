use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;

use crate::ConversionError;

const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
const BLOB_V2_EXTENSION_NAME: &str = "lance.blob.v2";
const BLOB_INLINE_THRESHOLD_KEY: &str = "lance-encoding:blob-inline-size-threshold";

pub(crate) fn apply_blob_inline_threshold(
    stream: SendableRecordBatchStream,
    inline_threshold: usize,
) -> SendableRecordBatchStream {
    let source_schema = stream.schema();
    let fields = source_schema
        .fields()
        .iter()
        .map(|field| {
            let mut field = field.as_ref().clone();
            if field
                .metadata()
                .get(ARROW_EXTENSION_NAME_KEY)
                .is_some_and(|name| name == BLOB_V2_EXTENSION_NAME)
            {
                let mut metadata = field.metadata().clone();
                metadata.insert(
                    BLOB_INLINE_THRESHOLD_KEY.to_owned(),
                    inline_threshold.to_string(),
                );
                field = field.with_metadata(metadata);
            }
            Arc::new(field)
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    let batch_schema = Arc::clone(&schema);
    let batches = stream.map(move |batch| {
        batch.and_then(|batch| {
            batch
                .with_schema(Arc::clone(&batch_schema))
                .map_err(Into::into)
        })
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, batches))
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
        | DataType::FixedSizeList(field, _) => validate_type(column, field.data_type()),
        DataType::Struct(fields) => {
            for field in fields {
                validate_type(field.name(), field.data_type())?;
            }
            Ok(())
        }
        DataType::Map(field, _) => validate_type(column, field.data_type()),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use datafusion::arrow::datatypes::{DataType, Field};

    use super::validate;
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
        let variant = Field::new("variant", DataType::Binary, true).with_metadata(HashMap::from([(
            "PARQUET:logical_type".to_owned(),
            "VARIANT".to_owned(),
        )]));
        assert!(matches!(
            validate(&[Arc::new(variant)]),
            Err(ConversionError::UnsupportedType(_))
        ));
    }
}
