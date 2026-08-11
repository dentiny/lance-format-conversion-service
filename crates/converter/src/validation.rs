use lance::Dataset;

use crate::ConversionError;

pub(crate) async fn validate_row_count(
    destination_uri: &str,
    expected_rows: u64,
) -> Result<(), ConversionError> {
    let destination = Dataset::open(destination_uri)
        .await
        .map_err(|error| ConversionError::Validation(error.to_string()))?;
    let actual_rows = destination
        .count_rows(None)
        .await
        .map_err(|error| ConversionError::Validation(error.to_string()))?;
    let actual_rows = u64::try_from(actual_rows)
        .map_err(|error| ConversionError::Validation(error.to_string()))?;
    compare_row_counts(expected_rows, actual_rows)
}

fn compare_row_counts(expected_rows: u64, actual_rows: u64) -> Result<(), ConversionError> {
    (actual_rows == expected_rows).then_some(()).ok_or_else(|| {
        ConversionError::Validation(format!(
            "source has {expected_rows} rows but destination has {actual_rows}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::compare_row_counts;
    use crate::ConversionError;

    const SOURCE_ROWS: u64 = 3;
    const DESTINATION_ROWS: u64 = 2;

    #[test]
    fn rejects_different_row_counts() {
        let error = compare_row_counts(SOURCE_ROWS, DESTINATION_ROWS).unwrap_err();
        assert!(matches!(error, ConversionError::Validation(_)));
        assert_eq!(
            error.to_string(),
            "conversion validation failed: source has 3 rows but destination has 2"
        );
    }
}
