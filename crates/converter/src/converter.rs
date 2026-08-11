use std::sync::Arc;

use datafusion::prelude::{ParquetReadOptions, SessionContext};
use lance::dataset::{InsertBuilder, WriteMode, WriteParams};
use lance_conversion_core::{
    job::{Job, JobKind, JobProgress},
    location::DatasetLocation,
};
use lance_file::version::LanceFileVersion;

use crate::{
    ConversionError, ConversionProgress, ConverterConfig, destination::Destination, schema, source,
    validation,
};

const MIB: u64 = 1024 * 1024;

pub struct Converter {
    config: ConverterConfig,
}

impl Converter {
    #[must_use]
    pub const fn new(config: ConverterConfig) -> Self {
        Self { config }
    }

    /// Converts one immutable Parquet source into a Lance 2.3 dataset.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot be read, its schema is
    /// unsupported, the Lance commit fails, or a move source cannot be deleted.
    pub async fn convert(
        &self,
        job: &Job,
        progress: Arc<ConversionProgress>,
    ) -> Result<JobProgress, ConversionError> {
        let source = DatasetLocation::parse_location(&job.source_uri)
            .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
        let source = <dyn source::SourceDataset>::open(source);
        if job.kind == JobKind::Move && source.copy_only() {
            return Err(ConversionError::InvalidSource(
                "Hugging Face datasets are copy-only".to_owned(),
            ));
        }

        let context = SessionContext::new();
        let prepared = source.prepare(&context).await?;
        let mut locations = prepared.parquet_files.into_iter();
        let first_location = locations.next().ok_or_else(|| {
            ConversionError::InvalidSource("source contains no Parquet locations".to_owned())
        })?;
        let mut dataframe = context
            .read_parquet(first_location, ParquetReadOptions::default())
            .await
            .map_err(|error| ConversionError::Read(error.to_string()))?;
        for location in locations {
            let next = context
                .read_parquet(location, ParquetReadOptions::default())
                .await
                .map_err(|error| ConversionError::Read(error.to_string()))?;
            dataframe = dataframe
                .union(next)
                .map_err(|error| ConversionError::Read(error.to_string()))?;
        }
        schema::validate(dataframe.schema().fields())?;
        let stream = dataframe
            .execute_stream()
            .await
            .map_err(|error| ConversionError::Read(error.to_string()))?;

        let max_bytes_per_file = self
            .config
            .target_lance_file_size_mib
            .checked_mul(MIB)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or_else(|| {
                ConversionError::InvalidConfiguration(
                    "target Lance file size does not fit usize".to_owned(),
                )
            })?;
        let inline_threshold = self
            .config
            .blob_inline_threshold_mib
            .checked_mul(MIB)
            .ok_or_else(|| {
                ConversionError::InvalidConfiguration("blob inline threshold overflow".to_owned())
            })?;
        if inline_threshold > u64::try_from(max_bytes_per_file).unwrap_or(u64::MAX) {
            return Err(ConversionError::InvalidConfiguration(
                "blob inline threshold exceeds target Lance file size".to_owned(),
            ));
        }
        let inline_threshold = usize::try_from(inline_threshold)
            .map_err(|error| ConversionError::InvalidConfiguration(error.to_string()))?;
        let stream = schema::apply_blob_inline_threshold(stream, inline_threshold);
        let stream = progress.track_reads(stream);

        let mut params = WriteParams::with_storage_version(LanceFileVersion::V2_3);
        // Overwrite prevents a full-job retry from appending duplicate rows.
        // Resuming partial work still requires durable fragment checkpoints.
        params.mode = WriteMode::Overwrite;
        params.max_bytes_per_file = max_bytes_per_file;
        Destination::new(&job.destination_uri).configure(&mut params)?;
        let callback_progress = Arc::clone(&progress);
        // One conversion uses one sequential Lance writer. It may rotate files
        // at the configured size, but never writes fragments in parallel.
        let write_result = InsertBuilder::new(job.destination_uri.as_str())
            .with_params(&params)
            .progress(move |stats| {
                callback_progress.record_write(stats.rows_written);
            })
            .execute_stream(stream)
            .await;
        write_result.map_err(|error| ConversionError::Write(error.to_string()))?;

        let source_rows = progress.snapshot().rows_read;
        validation::validate_row_count(&job.destination_uri, source_rows).await?;
        progress.finish();
        if job.kind == JobKind::Move {
            source.delete().await?;
        }
        Ok(progress.snapshot())
    }
}
