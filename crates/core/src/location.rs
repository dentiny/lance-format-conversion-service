use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocationKind {
    Nfs,
    S3,
    HuggingFace,
}

impl fmt::Display for LocationKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Nfs => "nfs",
            Self::S3 => "s3",
            Self::HuggingFace => "hugging_face",
        })
    }
}

impl FromStr for LocationKind {
    type Err = LocationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "nfs" => Ok(Self::Nfs),
            "s3" => Ok(Self::S3),
            "hugging_face" => Ok(Self::HuggingFace),
            _ => Err(LocationError::UnsupportedScheme(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLocation {
    uri: String,
    kind: LocationKind,
}

impl DatasetLocation {
    /// Parses an NFS-mounted path, S3 URI, or Hugging Face dataset URI.
    ///
    /// # Errors
    ///
    /// Returns an error when an explicit scheme is unsupported.
    pub fn parse_location(uri: impl Into<String>) -> Result<Self, LocationError> {
        let uri = uri.into();
        let kind = parse_kind(&uri)?;
        Ok(Self { uri, kind })
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    #[must_use]
    pub const fn kind(&self) -> LocationKind {
        self.kind
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LocationError {
    #[error("unsupported dataset location scheme: {0}")]
    UnsupportedScheme(String),
}

fn parse_kind(uri: &str) -> Result<LocationKind, LocationError> {
    if uri.starts_with("s3://") {
        Ok(LocationKind::S3)
    } else if uri.starts_with("hf://") {
        Ok(LocationKind::HuggingFace)
    } else if let Some((scheme, _)) = uri.split_once("://") {
        Err(LocationError::UnsupportedScheme(scheme.to_owned()))
    } else {
        Ok(LocationKind::Nfs)
    }
}

#[cfg(test)]
mod tests {
    use super::{DatasetLocation, LocationError, LocationKind};

    #[test]
    fn accepts_supported_locations() {
        assert_eq!(
            DatasetLocation::parse_location("/datasets/images")
                .unwrap()
                .kind(),
            LocationKind::Nfs
        );
        assert_eq!(
            DatasetLocation::parse_location("s3://example-bucket/datasets/images")
                .unwrap()
                .kind(),
            LocationKind::S3
        );
        assert_eq!(
            DatasetLocation::parse_location(
                "hf://datasets/owner/name@main?config=default&split=train"
            )
            .unwrap()
            .kind(),
            LocationKind::HuggingFace
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
