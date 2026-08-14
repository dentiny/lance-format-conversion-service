use arrow::datatypes::DataType;
use lance::{Dataset, index::DatasetIndexExt, index::vector::VectorIndexParams};
use lance_conversion_core::job::{IndexSpec, IndexType as JobIndexType};
use lance_index::{
    IndexParams, IndexType,
    scalar::{BuiltinIndexType, ScalarIndexParams},
    vector::{
        bq::RQBuildParams, hnsw::builder::HnswBuildParams, ivf::IvfBuildParams, pq::PQBuildParams,
        sq::builder::SQBuildParams,
    },
};
use lance_linalg::distance::DistanceType;

use crate::ConversionError;

const MAX_PRODUCT_SUBVECTORS: usize = 16;

pub(crate) async fn create(
    dataset: &mut Dataset,
    specs: &[IndexSpec],
) -> Result<(), ConversionError> {
    for (position, spec) in specs.iter().enumerate() {
        let column = spec.columns.first().ok_or_else(|| {
            ConversionError::InvalidIndexSpec(format!(
                "{} index must specify at least one column",
                spec.index_type
            ))
        })?;
        let field = dataset.schema().field(column).ok_or_else(|| {
            ConversionError::InvalidIndexSpec(format!(
                "selected index column '{column}' does not exist"
            ))
        })?;
        let (index_type, params) = mapping(spec.index_type, &field.data_type())?;
        let columns = spec.columns.iter().map(String::as_str).collect::<Vec<_>>();
        dataset
            .create_index(
                &columns,
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
        JobIndexType::Scalar => (
            IndexType::Scalar,
            scalar_parameters(BuiltinIndexType::BTree),
        ),
        JobIndexType::BTree => (IndexType::BTree, scalar_parameters(BuiltinIndexType::BTree)),
        JobIndexType::Bitmap => (
            IndexType::Bitmap,
            scalar_parameters(BuiltinIndexType::Bitmap),
        ),
        JobIndexType::LabelList => (
            IndexType::LabelList,
            scalar_parameters(BuiltinIndexType::LabelList),
        ),
        JobIndexType::Inverted => (
            IndexType::Inverted,
            scalar_parameters(BuiltinIndexType::Inverted),
        ),
        JobIndexType::NGram => (IndexType::NGram, scalar_parameters(BuiltinIndexType::NGram)),
        JobIndexType::ZoneMap => (
            IndexType::ZoneMap,
            scalar_parameters(BuiltinIndexType::ZoneMap),
        ),
        JobIndexType::BloomFilter => (
            IndexType::BloomFilter,
            scalar_parameters(BuiltinIndexType::BloomFilter),
        ),
        JobIndexType::RTree => (IndexType::RTree, scalar_parameters(BuiltinIndexType::RTree)),
        JobIndexType::Fm => (IndexType::Fm, scalar_parameters(BuiltinIndexType::Fm)),
        JobIndexType::Vector => (
            IndexType::Vector,
            vector_parameters(JobIndexType::Vector, data_type)?,
        ),
        JobIndexType::IvfFlat => (
            IndexType::IvfFlat,
            vector_parameters(JobIndexType::IvfFlat, data_type)?,
        ),
        JobIndexType::IvfSq => (
            IndexType::IvfSq,
            vector_parameters(JobIndexType::IvfSq, data_type)?,
        ),
        JobIndexType::IvfPq => (
            IndexType::IvfPq,
            vector_parameters(JobIndexType::IvfPq, data_type)?,
        ),
        JobIndexType::IvfHnswSq => (
            IndexType::IvfHnswSq,
            vector_parameters(JobIndexType::IvfHnswSq, data_type)?,
        ),
        JobIndexType::IvfHnswPq => (
            IndexType::IvfHnswPq,
            vector_parameters(JobIndexType::IvfHnswPq, data_type)?,
        ),
        JobIndexType::IvfHnswFlat => (
            IndexType::IvfHnswFlat,
            vector_parameters(JobIndexType::IvfHnswFlat, data_type)?,
        ),
        JobIndexType::IvfRq => (
            IndexType::IvfRq,
            vector_parameters(JobIndexType::IvfRq, data_type)?,
        ),
    };
    Ok(mapped)
}

fn scalar_parameters(index_type: BuiltinIndexType) -> Box<dyn IndexParams> {
    Box::new(ScalarIndexParams::for_builtin(index_type))
}

fn vector_parameters(
    index_type: JobIndexType,
    data_type: &DataType,
) -> Result<Box<dyn IndexParams>, ConversionError> {
    let dimension = vector_dimension(data_type).ok_or_else(|| {
        ConversionError::InvalidIndexSpec(format!(
            "{index_type} index requires a positive-dimensional fixed-size vector column"
        ))
    })?;
    let ivf = IvfBuildParams::default();
    let hnsw = HnswBuildParams::default();
    let pq = PQBuildParams {
        num_sub_vectors: product_subvectors(dimension),
        ..PQBuildParams::default()
    };
    let params = match index_type {
        JobIndexType::Vector | JobIndexType::IvfPq => {
            VectorIndexParams::with_ivf_pq_params(DistanceType::L2, ivf, pq)
        }
        JobIndexType::IvfFlat => VectorIndexParams::with_ivf_flat_params(DistanceType::L2, ivf),
        JobIndexType::IvfSq => {
            VectorIndexParams::with_ivf_sq_params(DistanceType::L2, ivf, SQBuildParams::default())
        }
        JobIndexType::IvfHnswFlat => VectorIndexParams::ivf_hnsw(DistanceType::L2, ivf, hnsw),
        JobIndexType::IvfHnswPq => {
            VectorIndexParams::with_ivf_hnsw_pq_params(DistanceType::L2, ivf, hnsw, pq)
        }
        JobIndexType::IvfHnswSq => VectorIndexParams::with_ivf_hnsw_sq_params(
            DistanceType::L2,
            ivf,
            hnsw,
            SQBuildParams::default(),
        ),
        JobIndexType::IvfRq => {
            VectorIndexParams::with_ivf_rq_params(DistanceType::L2, ivf, RQBuildParams::default())
        }
        _ => unreachable!("called only for vector index types"),
    };
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

    fn label_field() -> Field {
        Field::new(
            "value",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        )
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
            (JobIndexType::Scalar, IndexType::Scalar),
            (JobIndexType::BTree, IndexType::BTree),
            (JobIndexType::Bitmap, IndexType::Bitmap),
            (JobIndexType::LabelList, IndexType::LabelList),
            (JobIndexType::Inverted, IndexType::Inverted),
            (JobIndexType::NGram, IndexType::NGram),
            (JobIndexType::ZoneMap, IndexType::ZoneMap),
            (JobIndexType::BloomFilter, IndexType::BloomFilter),
            (JobIndexType::RTree, IndexType::RTree),
            (JobIndexType::Fm, IndexType::Fm),
            (JobIndexType::Vector, IndexType::Vector),
            (JobIndexType::IvfFlat, IndexType::IvfFlat),
            (JobIndexType::IvfSq, IndexType::IvfSq),
            (JobIndexType::IvfPq, IndexType::IvfPq),
            (JobIndexType::IvfHnswSq, IndexType::IvfHnswSq),
            (JobIndexType::IvfHnswPq, IndexType::IvfHnswPq),
            (JobIndexType::IvfHnswFlat, IndexType::IvfHnswFlat),
            (JobIndexType::IvfRq, IndexType::IvfRq),
        ];

        for (job_type, expected) in cases {
            let field = match job_type {
                JobIndexType::LabelList => label_field(),
                JobIndexType::Inverted | JobIndexType::NGram | JobIndexType::Fm => string_field(),
                JobIndexType::Vector
                | JobIndexType::IvfFlat
                | JobIndexType::IvfSq
                | JobIndexType::IvfPq
                | JobIndexType::IvfHnswSq
                | JobIndexType::IvfHnswPq
                | JobIndexType::IvfHnswFlat
                | JobIndexType::IvfRq => vector_field(),
                _ => scalar_field(),
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
