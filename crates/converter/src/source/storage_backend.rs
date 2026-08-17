use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use lance::io::ObjectStoreParams;
use lance_conversion_core::location::{DatasetLocation, LocationKind};

use super::{
    PreparedParquetFile, hugging_face::HuggingFaceBackend, nfs::NfsBackend,
    object_storage::ObjectStorageBackend,
};
use crate::{ConversionError, validation};

/// Storage backend for a dataset location.
#[async_trait]
pub(crate) trait StorageBackend: Send + Sync {
    /// Lists Parquet files, stopping after `limit` files when it is `Some`.
    ///
    /// `None` lists every file. The default schema path uses `Some(1)`.
    ///
    /// # Errors
    ///
    /// Returns an error if the location cannot be listed or its files cannot be
    /// opened.
    async fn list_files(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<PreparedParquetFile>, ConversionError>;

    /// Reads the Arrow schema from one representative Parquet file.
    ///
    /// # Errors
    ///
    /// Returns an error if the location has no Parquet files or the schema is
    /// unsupported.
    async fn get_schema(&self) -> Result<SchemaRef, ConversionError> {
        let files = self.list_files(Some(1)).await?;
        if files.len() != 1 {
            return Err(ConversionError::InvalidSource(
                "source contains no Parquet files".to_owned(),
            ));
        }
        let schema = files[0].read_schema().await?;
        validation::validate_schema(schema.fields())?;
        Ok(schema)
    }

    /// Lance object-store params for this location, for the writer SDK.
    fn lance_storage_options(&self) -> Result<Option<ObjectStoreParams>, ConversionError>;
}

/// Opens the backend for `uri`.
pub(crate) fn open_backend(uri: &str) -> Result<Box<dyn StorageBackend>, ConversionError> {
    let location = DatasetLocation::parse_location(uri)
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    match location.kind() {
        LocationKind::Nfs => Ok(Box::new(NfsBackend::new(location))),
        LocationKind::S3 => Ok(Box::new(ObjectStorageBackend::new(location))),
        LocationKind::HuggingFace => Ok(Box::new(HuggingFaceBackend::new(location))),
    }
}
