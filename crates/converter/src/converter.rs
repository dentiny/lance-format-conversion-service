use std::{num::NonZeroUsize, sync::Arc};

use datafusion::prelude::{ParquetReadOptions, SessionContext};
use lance::dataset::{ExternalBlobMode, InsertBuilder, WriteMode, WriteParams};
use lance_conversion_core::{
    job::{Job, JobKind, JobProgress},
    location::DatasetLocation,
};
use lance_file::version::LanceFileVersion;

use crate::{
    ConversionError, ConversionProgress, ConverterConfig, destination::Destination, indexes,
    schema, source, validation,
};

const MIB: u64 = 1024 * 1024;

struct ByteConfig {
    max_bytes_per_file: usize,
    inline_threshold: usize,
    dedicated_threshold: NonZeroUsize,
}

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
        let byte_config = validate_config(self.config)?;

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

        let stream = schema::apply_blob_columns(
            stream,
            &job.blob_columns,
            byte_config.inline_threshold,
            byte_config.dedicated_threshold,
        )?;
        let stream = progress.track_reads(stream);

        let mut params = WriteParams::with_storage_version(LanceFileVersion::V2_3);
        // Overwrite prevents a full-job retry from appending duplicate rows.
        // Resuming partial work still requires durable fragment checkpoints.
        params.mode = WriteMode::Overwrite;
        params.max_bytes_per_file = byte_config.max_bytes_per_file;
        params.external_blob_mode = ExternalBlobMode::Ingest;
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
        let mut destination = write_result.map_err(|error| {
            if job.blob_columns.is_empty() {
                ConversionError::Write(error.to_string())
            } else {
                ConversionError::Write(format!("blob URL/store ingestion failed: {error}"))
            }
        })?;

        let source_rows = progress.snapshot().rows_read;
        validation::validate_row_count(&destination, source_rows).await?;
        indexes::create(&mut destination, &job.indices).await?;
        progress.finish();
        if job.kind == JobKind::Move {
            source.delete().await?;
        }
        Ok(progress.snapshot())
    }
}

fn validate_config(config: ConverterConfig) -> Result<ByteConfig, ConversionError> {
    let max_bytes_per_file =
        mib_to_usize(config.target_lance_file_size_mib, "target Lance file size")?;
    let inline_threshold = mib_to_usize(config.blob_inline_threshold_mib, "blob inline threshold")?;
    let dedicated_threshold = mib_to_usize(
        config.blob_dedicated_threshold_mib,
        "blob dedicated threshold",
    )?;
    if inline_threshold >= dedicated_threshold {
        return Err(ConversionError::InvalidConfiguration(
            "blob inline threshold must be smaller than blob dedicated threshold".to_owned(),
        ));
    }
    if inline_threshold > max_bytes_per_file {
        return Err(ConversionError::InvalidConfiguration(
            "blob inline threshold exceeds target Lance file size".to_owned(),
        ));
    }
    let dedicated_threshold = NonZeroUsize::new(dedicated_threshold).ok_or_else(|| {
        ConversionError::InvalidConfiguration(
            "blob dedicated threshold must be greater than zero".to_owned(),
        )
    })?;
    Ok(ByteConfig {
        max_bytes_per_file,
        inline_threshold,
        dedicated_threshold,
    })
}

fn mib_to_usize(value: u64, setting: &str) -> Result<usize, ConversionError> {
    value
        .checked_mul(MIB)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or_else(|| {
            ConversionError::InvalidConfiguration(format!("{setting} does not fit usize bytes"))
        })
}
