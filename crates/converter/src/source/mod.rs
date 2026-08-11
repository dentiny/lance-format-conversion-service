mod directory;
mod hugging_face;

use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use lance_conversion_core::location::{DatasetLocation, LocationKind};

use crate::ConversionError;

pub(crate) struct PreparedSource {
    pub(crate) parquet_uri: String,
    pub(crate) temporary_directory: Option<PathBuf>,
}

pub(crate) async fn cleanup(prepared: &PreparedSource) -> Result<(), ConversionError> {
    if let Some(path) = &prepared.temporary_directory {
        tokio::fs::remove_dir_all(path)
            .await
            .map_err(|error| ConversionError::Read(error.to_string()))?;
    }
    Ok(())
}

pub(crate) async fn prepare(
    context: &SessionContext,
    source: &DatasetLocation,
) -> Result<PreparedSource, ConversionError> {
    match source.kind() {
        LocationKind::Nfs | LocationKind::S3 => directory::prepare(context, source).await,
        LocationKind::HuggingFace => hugging_face::prepare(source.uri()).await,
    }
}

pub(crate) async fn delete(source: &DatasetLocation) -> Result<(), ConversionError> {
    match source.kind() {
        LocationKind::Nfs | LocationKind::S3 => directory::delete(source).await,
        LocationKind::HuggingFace => Err(ConversionError::InvalidSource(
            "Hugging Face datasets are copy-only".to_owned(),
        )),
    }
}
