use std::{num::NonZeroUsize, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, LargeStringArray, RecordBatch},
    compute::{cast, filter_record_batch},
    datatypes::{DataType, Schema},
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use lance::{
    BlobArrayBuilder, BlobFieldOptions, blob_field_with_options,
    deps::datafusion::{
        error::DataFusionError,
        physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
    },
};
use lance_conversion_core::job::BlobColumnSpec;
use reqwest::{Client, StatusCode, Url};

use crate::{ConversionError, ConversionProgress, validation};

const FETCH_CONCURRENCY: usize = 8;

enum BlobValue {
    Null,
    Bytes(Bytes),
    Uri(String),
    Missing,
}

/// Converts selected Parquet string columns into Blob V2 arrays.
pub(crate) fn apply_blob_columns(
    stream: SendableRecordBatchStream,
    blob_columns: &[BlobColumnSpec],
    inline_threshold: usize,
    dedicated_threshold: NonZeroUsize,
    client: Client,
    progress: Arc<ConversionProgress>,
) -> Result<SendableRecordBatchStream, ConversionError> {
    let source_schema = stream.schema();
    let plan = Arc::new(BlobPlan::new(
        source_schema.as_ref(),
        blob_columns,
        inline_threshold,
        dedicated_threshold,
    )?);
    let schema = Arc::clone(&plan.schema);
    let batches = stream.then(move |batch| {
        let plan = Arc::clone(&plan);
        let client = client.clone();
        let progress = Arc::clone(&progress);
        async move {
            let batch = batch?;
            progress.record_read(batch.num_rows());
            let (batch, missing_rows) = plan.transform(batch, &client).await?;
            progress.record_missing_blobs(missing_rows);
            Ok(batch)
        }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

/// Batch-independent mapping from source URL columns to Lance Blob V2 columns.
struct BlobPlan {
    /// For each source column, the index of its temporary blob-value vector.
    /// Non-blob columns contain `None`.
    ///
    /// For example, given source columns
    /// `[id, image_url, caption, thumbnail_url]`, with `image_url` and
    /// `thumbnail_url` selected as blobs, this is
    /// `[None, Some(0), None, Some(1)]`. During batch conversion,
    /// `values[0]` holds every `image_url` result and `values[1]` holds every
    /// `thumbnail_url` result.
    slots: Vec<Option<usize>>,
    /// Output schema with selected string fields replaced by Lance Blob V2 fields.
    schema: Arc<Schema>,
}

impl BlobPlan {
    fn new(
        source_schema: &Schema,
        blob_columns: &[BlobColumnSpec],
        inline_threshold: usize,
        dedicated_threshold: NonZeroUsize,
    ) -> Result<Self, ConversionError> {
        let names = validation::validate_blob_columns(source_schema.fields(), blob_columns)?;
        let mut next_slot = 0;
        let slots = source_schema
            .fields()
            .iter()
            .map(|field| {
                names.contains(field.name()).then(|| {
                    let slot = next_slot;
                    next_slot += 1;
                    slot
                })
            })
            .collect::<Vec<_>>();
        let fields = source_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if slots[index].is_some() {
                    Arc::new(blob_field_with_options(
                        field.name(),
                        field.is_nullable(),
                        BlobFieldOptions::default()
                            .with_inline_size_threshold(inline_threshold)
                            .with_dedicated_size_threshold(dedicated_threshold),
                    ))
                } else {
                    Arc::clone(field)
                }
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            source_schema.metadata().clone(),
        ));
        Ok(Self { slots, schema })
    }

    async fn transform(
        &self,
        batch: RecordBatch,
        client: &Client,
    ) -> Result<(RecordBatch, usize), DataFusionError> {
        let rows = batch.num_rows();
        let mut values = (0..self.slots.iter().flatten().count())
            .map(|_| Vec::with_capacity(rows))
            .collect::<Vec<_>>();
        let mut requests = Vec::new();
        let batch_schema = batch.schema();

        for (column, slot) in self.slots.iter().enumerate() {
            let Some(slot) = slot else { continue };
            let field = batch_schema.field(column);
            let strings = cast(batch.column(column), &DataType::LargeUtf8)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let strings = strings
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("cast to LargeUtf8 returns LargeStringArray");

            for row in 0..rows {
                if strings.is_null(row) {
                    values[*slot].push(BlobValue::Null);
                    continue;
                }
                let value = strings.value(row);
                let url = validation::validate_blob_uri(value, field.name(), row)?;
                if matches!(url.scheme(), "http" | "https") {
                    values[*slot].push(BlobValue::Missing);
                    requests.push((*slot, row, url));
                } else {
                    values[*slot].push(BlobValue::Uri(value.to_owned()));
                }
            }
        }

        for (slot, row, bytes) in fetch_all(client, requests).await? {
            if let Some(bytes) = bytes {
                values[slot][row] = BlobValue::Bytes(bytes);
            }
        }

        let keep = (0..rows)
            .map(|row| {
                values
                    .iter()
                    .all(|column| !matches!(column[row], BlobValue::Missing))
            })
            .collect::<Vec<_>>();
        let missing_rows = keep.iter().filter(|keep| !**keep).count();

        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| {
                self.slots[index].map_or_else(
                    || Ok(Arc::clone(column)),
                    |slot| build_blob_array(&values[slot]),
                )
            })
            .collect::<Result<Vec<ArrayRef>, DataFusionError>>()?;
        let mut batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        if missing_rows != 0 {
            batch = filter_record_batch(&batch, &BooleanArray::from(keep))
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        }
        Ok((batch, missing_rows))
    }
}

fn build_blob_array(values: &[BlobValue]) -> Result<ArrayRef, DataFusionError> {
    let mut builder = BlobArrayBuilder::new(values.len());
    for value in values {
        let result = match value {
            BlobValue::Null => builder.push_null(),
            BlobValue::Bytes(bytes) => builder.push_bytes(bytes),
            BlobValue::Uri(uri) => builder.push_uri(uri.clone()),
            BlobValue::Missing => builder.push_empty(),
        };
        result.map_err(|error| DataFusionError::Execution(error.to_string()))?;
    }
    builder
        .finish()
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

async fn fetch_all(
    client: &Client,
    requests: Vec<(usize, usize, Url)>,
) -> Result<Vec<(usize, usize, Option<Bytes>)>, DataFusionError> {
    stream::iter(requests)
        .map(|(column, row, url)| {
            let client = client.clone();
            async move {
                let response = client
                    .get(url.clone())
                    .send()
                    .await
                    .map_err(|error| format!("GET {url} failed: {error}"))?;
                if matches!(response.status(), StatusCode::NOT_FOUND | StatusCode::GONE) {
                    return Ok((column, row, None));
                }
                if !response.status().is_success() {
                    return Err(format!("GET {url} returned {}", response.status()));
                }
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|error| format!("GET {url} failed while reading response: {error}"))?;
                Ok((column, row, Some(bytes)))
            }
        })
        .buffer_unordered(FETCH_CONCURRENCY)
        .map_err(DataFusionError::Execution)
        .try_collect()
        .await
}
