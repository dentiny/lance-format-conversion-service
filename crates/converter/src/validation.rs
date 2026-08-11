use lance::Dataset;

use crate::ConversionError;

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
