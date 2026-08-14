use std::{collections::HashSet, sync::Arc};

use datafusion::{
    arrow::datatypes::{DataType, Field},
    error::DataFusionError,
};
use lance::Dataset;
use lance_conversion_core::job::BlobColumnSpec;

use crate::ConversionError;

/// Returns whether an Arrow column can be selected for Blob V2 ingestion.
pub(crate) const fn is_blob_eligible(data_type: &DataType) -> bool {
    // DataFusion presents Parquet Utf8 as Utf8View by default.
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub(crate) fn validate_schema(fields: &[Arc<Field>]) -> Result<(), ConversionError> {
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

pub(crate) fn validate_blob_columns(
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
        if !is_blob_eligible(field.data_type()) {
            return Err(ConversionError::InvalidBlobSpec(format!(
                "selected column '{}' must have type Utf8 or LargeUtf8, found {}",
                spec.column,
                field.data_type()
            )));
        }
    }
    Ok(selected)
}

pub(crate) fn validate_blob_uri(
    value: &str,
    column_name: &str,
    row: usize,
) -> Result<(), DataFusionError> {
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

pub(crate) async fn validate_row_count(
    destination: &Dataset,
    expected_rows: u64,
) -> Result<(), ConversionError> {
    let actual_rows = destination
        .count_rows(None)
        .await
        .map_err(|error| ConversionError::Validation(error.to_string()))?;
    let actual_rows = u64::try_from(actual_rows)
        .map_err(|error| ConversionError::Validation(error.to_string()))?;
    (actual_rows == expected_rows).then_some(()).ok_or_else(|| {
        ConversionError::Validation(format!(
            "source has {expected_rows} rows but destination has {actual_rows}"
        ))
    })
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

    use super::{validate_blob_columns, validate_schema};
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
        validate_schema(&[Arc::new(nested)]).unwrap();
    }

    #[test]
    fn rejects_variant_metadata() {
        let variant =
            Field::new("variant", DataType::Binary, true).with_metadata(HashMap::from([(
                "PARQUET:logical_type".to_owned(),
                "VARIANT".to_owned(),
            )]));
        assert!(matches!(
            validate_schema(&[Arc::new(variant)]),
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
