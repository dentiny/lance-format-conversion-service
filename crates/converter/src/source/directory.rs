use async_trait::async_trait;
use lance_conversion_core::location::{DatasetLocation, LocationKind};

use super::{PreparedSource, SourceDataset, nfs, s3};
use crate::ConversionError;

pub(super) struct DirectorySource {
    location: DatasetLocation,
}

impl DirectorySource {
    pub(super) const fn new(location: DatasetLocation) -> Self {
        Self { location }
    }
}

#[async_trait]
impl SourceDataset for DirectorySource {
    fn copy_only(&self) -> bool {
        false
    }

    async fn prepare(&self) -> Result<PreparedSource, ConversionError> {
        match self.location.kind() {
            LocationKind::Nfs => nfs::prepare(self.location.uri()).await,
            LocationKind::S3 => s3::prepare(self.location.uri()).await,
            LocationKind::HuggingFace => Err(ConversionError::InvalidSource(
                "expected a directory-based source".to_owned(),
            )),
        }
    }

    async fn delete(&self) -> Result<(), ConversionError> {
        match self.location.kind() {
            LocationKind::Nfs => nfs::delete(self.location.uri()).await,
            LocationKind::S3 => s3::delete(self.location.uri()).await,
            LocationKind::HuggingFace => Err(ConversionError::InvalidSource(
                "expected a directory-based source".to_owned(),
            )),
        }
    }
}
