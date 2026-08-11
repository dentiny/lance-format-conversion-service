mod directory;
mod hugging_face;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use lance_conversion_core::location::{DatasetLocation, LocationKind};

use crate::ConversionError;

pub(crate) struct PreparedSource {
    pub(crate) parquet_locations: Vec<String>,
}

/// Provides a uniform interface for preparing and deleting source datasets.
///
/// Implementations translate their native location into a Parquet URI that
/// DataFusion can read and own source-specific deletion behavior for move jobs.
#[async_trait]
pub(crate) trait SourceDataset: Send + Sync {
    /// Returns whether this source supports copy jobs only.
    ///
    /// Sources that return `true` must not have [`Self::delete`] called.
    fn copy_only(&self) -> bool;

    /// Makes the source's Parquet files available to the conversion reader.
    ///
    /// Directory sources return their existing URI and may register an object
    /// store with `context`. Remote catalog sources return all resolved
    /// Parquet URLs and register the stores needed to stream them directly.
    ///
    /// # Errors
    ///
    /// Returns an error if the location is invalid or its Parquet files cannot
    /// be made available.
    async fn prepare(&self, context: &SessionContext) -> Result<PreparedSource, ConversionError>;

    /// Deletes the source after a move conversion commits successfully.
    ///
    /// # Errors
    ///
    /// Returns an error if any source file or object cannot be deleted.
    async fn delete(&self) -> Result<(), ConversionError>;
}

pub(crate) fn open(source: DatasetLocation) -> Box<dyn SourceDataset> {
    match source.kind() {
        LocationKind::Nfs | LocationKind::S3 => Box::new(directory::DirectorySource::new(source)),
        LocationKind::HuggingFace => Box::new(hugging_face::HuggingFaceDataset::new(source)),
    }
}
