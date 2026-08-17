use std::sync::Arc;

use lance::dataset::{ExternalBlobMode, InsertBuilder, WriteMode, WriteParams};
use lance::session::Session;
use lance_conversion_core::job::{Job, JobProgress};
use lance_file::version::LanceFileVersion;
use reqwest::Client;

use crate::{
    ConversionError, ConversionProgress, ConverterConfig, blob, config::ByteConfig, indexes,
    source, validation,
};

pub struct Converter {
    config: ByteConfig,
    session: Arc<Session>,
    http_client: Client,
}

impl Converter {
    /// Builds a converter with validated, byte-normalized settings.
    ///
    /// # Errors
    ///
    /// Returns an error when file-size or blob thresholds are inconsistent or
    /// cannot be represented as platform byte counts.
    pub fn new(config: ConverterConfig) -> Result<Self, ConversionError> {
        let session = Session::default();
        Ok(Self {
            config: config.validate()?,
            session: Arc::new(session),
            http_client: Client::new(),
        })
    }

    /// Converts one immutable Parquet source into a Lance 2.3 dataset.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot be read, its schema is
    /// unsupported, or the Lance commit fails.
    pub async fn convert(
        &self,
        job: &Job,
        progress: Arc<ConversionProgress>,
    ) -> Result<JobProgress, ConversionError> {
        let destination = source::open_backend(&job.destination_uri)?;
        let prepared = source::open_validated_source(&job.source_uri).await?;
        let stream = prepared.into_stream();

        let stream = blob::apply_blob_columns(
            stream,
            &job.blob_columns,
            self.config.inline_threshold,
            self.config.dedicated_threshold,
            self.http_client.clone(),
            Arc::clone(&progress),
        )?;

        let mut params = WriteParams::with_storage_version(LanceFileVersion::V2_3);
        // Overwrite prevents a full-job retry from appending duplicate rows.
        // Resuming partial work still requires durable fragment checkpoints.
        params.mode = WriteMode::Overwrite;
        params.max_bytes_per_file = self.config.max_bytes_per_file;
        params.external_blob_mode = ExternalBlobMode::Ingest;
        params.store_params = destination.lance_storage_options()?;
        params.session = Some(Arc::clone(&self.session));
        let callback_progress = Arc::clone(&progress);
        // One conversion uses one sequential Lance writer. It may rotate files
        // at the configured size, but never writes fragments in parallel.
        let write_result = InsertBuilder::new(job.destination_uri.as_str())
            .with_params(&params)
            .progress(move |stats| {
                callback_progress.record_rows_written_total(stats.rows_written);
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

        let snapshot = progress.snapshot();
        let expected_rows = snapshot
            .rows_read
            .saturating_sub(snapshot.rows_missing_blobs);
        validation::validate_row_count(&destination, expected_rows).await?;
        indexes::create(&mut destination, &job.indices).await?;
        progress.finish();
        Ok(progress.snapshot())
    }
}
