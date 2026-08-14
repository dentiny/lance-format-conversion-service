use arrow::datatypes::DataType;
use lance::{Dataset, index::DatasetIndexExt, index::vector::VectorIndexParams};
use lance_conversion_core::job::{IndexSpec, IndexType as JobIndexType};
use lance_index::{
    IndexParams, IndexType,
    scalar::{BuiltinIndexType, ScalarIndexParams},
    vector::{ivf::IvfBuildParams, pq::PQBuildParams},
};
use lance_linalg::distance::DistanceType;

use crate::ConversionError;

const MAX_PRODUCT_SUBVECTORS: usize = 16;

pub(crate) async fn create(
    dataset: &mut Dataset,
    specs: &[IndexSpec],
) -> Result<(), ConversionError> {
    for (position, spec) in specs.iter().enumerate() {
        let column = spec.column.as_str();
        if column.is_empty() {
            return Err(ConversionError::InvalidIndexSpec(format!(
                "{} index must specify a column",
                spec.index_type
            )));
        }
        let field = dataset.schema().field(column).ok_or_else(|| {
            ConversionError::InvalidIndexSpec(format!(
                "selected index column '{column}' does not exist"
            ))
        })?;
        let (index_type, params) = mapping(spec.index_type, &field.data_type())?;
        dataset
            .create_index(
                &[column],
                index_type,
                Some(format!("conversion_{position}_{}_idx", spec.index_type)),
                params.as_ref(),
                true,
            )
            .await
            .map_err(|error| {
                ConversionError::Index(format!(
                    "{} index on column '{}': {error}",
                    spec.index_type, column
                ))
            })?;
    }
    Ok(())
}

fn vector_dimension(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::FixedSizeList(item, dimension)
            if is_vector_element(item.data_type()) && *dimension > 0 =>
        {
            usize::try_from(*dimension).ok()
        }
        DataType::List(item) | DataType::LargeList(item) => vector_dimension(item.data_type()),
        _ => None,
    }
}

fn is_vector_element(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::UInt8
    )
}

fn mapping(
    index_type: JobIndexType,
    data_type: &DataType,
) -> Result<(IndexType, Box<dyn IndexParams>), ConversionError> {
    let mapped = match index_type {
        JobIndexType::Scalar => (IndexType::BTree, scalar_parameters(BuiltinIndexType::BTree)),
        JobIndexType::Text => (
            IndexType::Inverted,
            scalar_parameters(BuiltinIndexType::Inverted),
        ),
        JobIndexType::Vector => (IndexType::Vector, vector_parameters(data_type)?),
    };
    Ok(mapped)
}

fn scalar_parameters(index_type: BuiltinIndexType) -> Box<dyn IndexParams> {
    Box::new(ScalarIndexParams::for_builtin(index_type))
}

fn vector_parameters(data_type: &DataType) -> Result<Box<dyn IndexParams>, ConversionError> {
    let dimension = vector_dimension(data_type).ok_or_else(|| {
        ConversionError::InvalidIndexSpec(
            "vector index requires a positive-dimensional fixed-size vector column".to_owned(),
        )
    })?;
    let ivf = IvfBuildParams::default();
    let pq = PQBuildParams {
        num_sub_vectors: product_subvectors(dimension),
        ..PQBuildParams::default()
    };
    let params = VectorIndexParams::with_ivf_pq_params(DistanceType::L2, ivf, pq);
    Ok(Box::new(params))
}

fn product_subvectors(dimension: usize) -> usize {
    (1..=MAX_PRODUCT_SUBVECTORS.min(dimension))
        .rev()
        .find(|candidate| dimension.is_multiple_of(*candidate))
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use lance_conversion_core::job::IndexType as JobIndexType;
    use lance_index::IndexType;

    use super::{mapping, product_subvectors};

    fn scalar_field() -> Field {
        Field::new("value", DataType::Int64, false)
    }

    fn string_field() -> Field {
        Field::new("value", DataType::Utf8, false)
    }

    fn vector_field() -> Field {
        Field::new(
            "value",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 12),
            false,
        )
    }

    #[test]
    fn maps_every_public_job_index_type() {
        let cases = [
            (JobIndexType::Scalar, IndexType::BTree),
            (JobIndexType::Text, IndexType::Inverted),
            (JobIndexType::Vector, IndexType::Vector),
        ];

        for (job_type, expected) in cases {
            let field = match job_type {
                JobIndexType::Scalar => scalar_field(),
                JobIndexType::Text => string_field(),
                JobIndexType::Vector => vector_field(),
            };
            let (actual, _params) = mapping(job_type, field.data_type()).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn chooses_largest_small_divisor_for_product_quantization() {
        assert_eq!(product_subvectors(128), 16);
        assert_eq!(product_subvectors(12), 12);
        assert_eq!(product_subvectors(17), 1);
    }
}
