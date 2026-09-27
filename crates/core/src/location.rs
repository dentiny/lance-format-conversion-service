use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Logical format exposed by a conversion source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Parquet,
    HuggingFace,
    Warc,
}

/// Physical backend that stores a dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Nfs,
    S3,
    HuggingFace,
}

impl SourceKind {
    /// Classifies source format independently from its storage backend.
    ///
    /// # Errors
    ///
    /// Returns an error when the URI uses an unsupported scheme.
    pub fn from_uri(uri: &str) -> Result<Self, LocationError> {
        let location = DatasetLocation::parse_location(uri)?;
        Ok(if location.storage_kind() == StorageKind::HuggingFace {
            Self::HuggingFace
        } else if is_warc_source_uri(uri) {
            Self::Warc
        } else {
            Self::Parquet
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLocation {
    uri: String,
}

impl DatasetLocation {
    /// Parses an NFS-mounted path, S3 URI, or Hugging Face dataset URI.
    ///
    /// # Errors
    ///
    /// Returns an error when an explicit scheme is unsupported.
    pub fn parse_location(uri: impl Into<String>) -> Result<Self, LocationError> {
        let uri = uri.into();
        validate_scheme(&uri)?;
        Ok(Self { uri })
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    #[must_use]
    pub fn storage_kind(&self) -> StorageKind {
        if self.uri.starts_with("s3://") {
            StorageKind::S3
        } else if self.uri.starts_with("hf://") {
            StorageKind::HuggingFace
        } else {
            StorageKind::Nfs
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LocationError {
    #[error("unsupported dataset location scheme: {0}")]
    UnsupportedScheme(String),
}

/// Returns whether a source URI names a WARC or gzip-compressed WARC file.
#[must_use]
pub fn is_warc_source_uri(uri: &str) -> bool {
    let path = uri.split(['?', '#']).next().unwrap_or(uri);
    let path = std::path::Path::new(path);
    if path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("warc"))
    {
        return true;
    }
    path.extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("gz"))
        && path
            .file_stem()
            .map(std::path::Path::new)
            .and_then(std::path::Path::extension)
            .is_some_and(|extension| extension.eq_ignore_ascii_case("warc"))
}

fn validate_scheme(uri: &str) -> Result<(), LocationError> {
    if uri.starts_with("s3://") || uri.starts_with("hf://") {
        Ok(())
    } else if let Some((scheme, _)) = uri.split_once("://") {
        Err(LocationError::UnsupportedScheme(scheme.to_owned()))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{DatasetLocation, LocationError, SourceKind, StorageKind};

    #[test]
    fn accepts_supported_locations() {
        assert_eq!(
            DatasetLocation::parse_location("/datasets/images")
                .unwrap()
                .storage_kind(),
            StorageKind::Nfs
        );
        assert_eq!(
            DatasetLocation::parse_location("s3://example-bucket/datasets/images")
                .unwrap()
                .storage_kind(),
            StorageKind::S3
        );
        assert_eq!(
            DatasetLocation::parse_location(
                "hf://datasets/owner/name@main?config=default&split=train"
            )
            .unwrap()
            .storage_kind(),
            StorageKind::HuggingFace
        );
    }

    #[test]
    fn classifies_source_format_independently_from_storage() {
        assert_eq!(
            SourceKind::from_uri("/datasets/images").unwrap(),
            SourceKind::Parquet
        );
        assert_eq!(
            SourceKind::from_uri("s3://bucket/archive.warc.gz").unwrap(),
            SourceKind::Warc
        );
        assert_eq!(
            SourceKind::from_uri("hf://datasets/owner/name").unwrap(),
            SourceKind::HuggingFace
        );
    }

    #[test]
    fn rejects_unknown_schemes() {
        assert_eq!(
            DatasetLocation::parse_location("gs://bucket/key").unwrap_err(),
            LocationError::UnsupportedScheme("gs".to_owned())
        );
    }
}
