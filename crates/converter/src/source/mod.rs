mod hugging_face;
mod parquet;
mod warc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use lance::deps::datafusion::physical_plan::SendableRecordBatchStream;
use lance_conversion_core::location::SourceKind;

use self::{hugging_face::HuggingFaceSource, parquet::ParquetSource, warc::WarcSource};
use crate::ConversionError;

/// A logical input format that produces validated Arrow record batches.
#[async_trait]
trait Source: Send {
    async fn into_stream(self: Box<Self>) -> Result<SendableRecordBatchStream, ConversionError>;
}

/// Returns a validated source schema without preparing every source file.
pub(crate) async fn get_source_schema(source_uri: &str) -> Result<SchemaRef, ConversionError> {
    match source_kind(source_uri)? {
        SourceKind::Parquet => ParquetSource::schema(source_uri).await,
        SourceKind::HuggingFace => HuggingFaceSource::schema(source_uri).await,
        SourceKind::Warc => WarcSource::schema(source_uri).await,
    }
}

/// Opens a format-specific source independently of its physical storage.
pub(crate) async fn open(source_uri: &str) -> Result<SendableRecordBatchStream, ConversionError> {
    let source: Box<dyn Source> = match source_kind(source_uri)? {
        SourceKind::Parquet => Box::new(ParquetSource::open(source_uri).await?),
        SourceKind::HuggingFace => Box::new(HuggingFaceSource::new(source_uri)),
        SourceKind::Warc => Box::new(WarcSource::new(source_uri)),
    };
    source.into_stream().await
}

fn source_kind(source_uri: &str) -> Result<SourceKind, ConversionError> {
    SourceKind::from_uri(source_uri)
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))
}
