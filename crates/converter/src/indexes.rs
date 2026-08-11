use std::collections::HashSet;

use datafusion::arrow::datatypes::DataType;
use lance::{Dataset, datatypes::Field, index::DatasetIndexExt, index::vector::VectorIndexParams};
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

const PRODUCT_QUANTIZATION_BITS: usize = 8;
const RABIT_QUANTIZATION_BITS: u8 = 1;
const MAX_PRODUCT_SUBVECTORS: usize = 16;
const GEOARROW_EXTENSION_PREFIX: &str = "geoarrow.";
const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";

enum Parameters {
    Scalar(ScalarIndexParams),
    Vector(VectorIndexParams),
}

impl Parameters {
    fn as_index_params(&self) -> &dyn IndexParams {
        match self {
            Self::Scalar(params) => params,
            Self::Vector(params) => params,
        }
    }
}

pub(crate) async fn create(
    dataset: &mut Dataset,
    specs: &[IndexSpec],
) -> Result<(), ConversionError> {
    validate_specs(dataset, specs)?;
    for (position, spec) in specs.iter().enumerate() {
        let column = &spec.columns[0];
        let field = dataset
            .schema()
            .field(column)
            .expect("index columns were validated");
        let (index_type, params) = mapping(spec.index_type, &field.data_type())?;
        let columns = [column.as_str()];
        dataset
            .create_index(
                &columns,
                index_type,
                Some(index_name(position, spec.index_type)),
                params.as_index_params(),
                true,
            )
            .await
            .map_err(|error| {
                ConversionError::Index(format!(
                    "{} index on column '{}': {error}",
                    index_type_name(spec.index_type),
                    column
                ))
            })?;
    }
    Ok(())
}

fn validate_specs(dataset: &Dataset, specs: &[IndexSpec]) -> Result<(), ConversionError> {
    let mut unique_specs = HashSet::with_capacity(specs.len());
    for spec in specs {
        if spec.columns.is_empty() {
            return Err(ConversionError::InvalidIndexSpec(format!(
                "{} index must specify one column",
                index_type_name(spec.index_type)
            )));
        }
        let mut unique_columns = HashSet::with_capacity(spec.columns.len());
        for column in &spec.columns {
            if column.trim().is_empty() {
                return Err(ConversionError::InvalidIndexSpec(
                    "index column names must not be empty".to_owned(),
                ));
            }
            if !unique_columns.insert(column) {
                return Err(ConversionError::InvalidIndexSpec(format!(
                    "column '{column}' is selected more than once in one index"
                )));
            }
        }
        if spec.columns.len() != 1 {
            return Err(ConversionError::InvalidIndexSpec(format!(
                "{} index specifies {} columns, but Lance 10 supports creating an index on exactly one column",
                index_type_name(spec.index_type),
                spec.columns.len()
            )));
        }
        let column = &spec.columns[0];
        let Some(field) = dataset.schema().field(column) else {
            return Err(ConversionError::InvalidIndexSpec(format!(
                "selected index column '{column}' does not exist"
            )));
        };
        let unique_key = format!("{:?}\0{column}", spec.index_type);
        if !unique_specs.insert(unique_key) {
            return Err(ConversionError::InvalidIndexSpec(format!(
                "{} index on column '{column}' is specified more than once",
                index_type_name(spec.index_type)
            )));
        }
        validate_type(spec.index_type, field)?;
    }
    Ok(())
}

fn validate_type(index_type: JobIndexType, field: &Field) -> Result<(), ConversionError> {
    let valid = match index_type {
        JobIndexType::Inverted | JobIndexType::NGram | JobIndexType::Fm => {
            is_string(&field.data_type())
        }
        JobIndexType::LabelList => match field.data_type() {
            DataType::List(item) | DataType::LargeList(item) => is_scalar(item.data_type()),
            _ => false,
        },
        JobIndexType::RTree => field
            .metadata
            .get(ARROW_EXTENSION_NAME_KEY)
            .is_some_and(|name| name.starts_with(GEOARROW_EXTENSION_PREFIX)),
        JobIndexType::Vector
        | JobIndexType::IvfFlat
        | JobIndexType::IvfSq
        | JobIndexType::IvfPq
        | JobIndexType::IvfHnswSq
        | JobIndexType::IvfHnswPq
        | JobIndexType::IvfHnswFlat
        | JobIndexType::IvfRq => vector_dimension(&field.data_type()).is_some(),
        JobIndexType::Scalar
        | JobIndexType::BTree
        | JobIndexType::Bitmap
        | JobIndexType::ZoneMap
        | JobIndexType::BloomFilter => is_scalar(&field.data_type()),
    };
    if valid {
        Ok(())
    } else {
        Err(ConversionError::InvalidIndexSpec(format!(
            "{} index is incompatible with column '{}' of type {}",
            index_type_name(index_type),
            field.name,
            field.data_type()
        )))
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_scalar(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Null
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::Map(_, _)
            | DataType::RunEndEncoded(_, _)
    )
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
) -> Result<(IndexType, Parameters), ConversionError> {
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

fn scalar_parameters(index_type: BuiltinIndexType) -> Parameters {
    Parameters::Scalar(ScalarIndexParams::for_builtin(index_type))
}

fn vector_parameters(
    index_type: JobIndexType,
    data_type: &DataType,
) -> Result<Parameters, ConversionError> {
    let dimension = vector_dimension(data_type).ok_or_else(|| {
        ConversionError::InvalidIndexSpec(format!(
            "{} index requires a positive-dimensional fixed-size vector column",
            index_type_name(index_type)
        ))
    })?;
    let ivf = IvfBuildParams::default();
    let hnsw = HnswBuildParams::default();
    let pq = PQBuildParams::new(product_subvectors(dimension), PRODUCT_QUANTIZATION_BITS);
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
        JobIndexType::IvfRq => VectorIndexParams::with_ivf_rq_params(
            DistanceType::L2,
            ivf,
            RQBuildParams::new(RABIT_QUANTIZATION_BITS),
        ),
        _ => unreachable!("called only for vector index types"),
    };
    Ok(Parameters::Vector(params))
}

fn product_subvectors(dimension: usize) -> usize {
    (1..=MAX_PRODUCT_SUBVECTORS.min(dimension))
        .rev()
        .find(|candidate| dimension.is_multiple_of(*candidate))
        .unwrap_or(1)
}

fn index_name(position: usize, index_type: JobIndexType) -> String {
    format!(
        "conversion_{}_{}_idx",
        position,
        index_type_name(index_type)
    )
}

const fn index_type_name(index_type: JobIndexType) -> &'static str {
    match index_type {
        JobIndexType::Scalar => "scalar",
        JobIndexType::BTree => "b_tree",
        JobIndexType::Bitmap => "bitmap",
        JobIndexType::LabelList => "label_list",
        JobIndexType::Inverted => "inverted",
        JobIndexType::NGram => "n_gram",
        JobIndexType::ZoneMap => "zone_map",
        JobIndexType::BloomFilter => "bloom_filter",
        JobIndexType::RTree => "r_tree",
        JobIndexType::Fm => "fm",
        JobIndexType::Vector => "vector",
        JobIndexType::IvfFlat => "ivf_flat",
        JobIndexType::IvfSq => "ivf_sq",
        JobIndexType::IvfPq => "ivf_pq",
        JobIndexType::IvfHnswSq => "ivf_hnsw_sq",
        JobIndexType::IvfHnswPq => "ivf_hnsw_pq",
        JobIndexType::IvfHnswFlat => "ivf_hnsw_flat",
        JobIndexType::IvfRq => "ivf_rq",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use lance_conversion_core::job::IndexType as JobIndexType;
    use lance_index::IndexType;

    use super::{Parameters, mapping, product_subvectors};

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
            let (actual, params) = mapping(job_type, field.data_type()).unwrap();
            assert_eq!(actual, expected);
            match params {
                Parameters::Scalar(params) => assert!(!params.index_type.is_empty()),
                Parameters::Vector(params) => {
                    let expected_params_type = if job_type == JobIndexType::Vector {
                        IndexType::IvfPq
                    } else {
                        expected
                    };
                    assert_eq!(params.index_type(), expected_params_type);
                }
            }
        }
    }

    #[test]
    fn chooses_largest_small_divisor_for_product_quantization() {
        assert_eq!(product_subvectors(128), 16);
        assert_eq!(product_subvectors(12), 12);
        assert_eq!(product_subvectors(17), 1);
    }
}
