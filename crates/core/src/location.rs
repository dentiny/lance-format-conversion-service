use std::{fmt, net::Ipv4Addr, str::FromStr};

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
    /// Parses a supported NFS, S3, or Hugging Face source URI.
    ///
    /// # Errors
    ///
    /// Returns an error when the scheme is unsupported or the URI does not
    /// follow the service's canonical location grammar.
    pub fn parse_source(uri: impl Into<String>) -> Result<Self, LocationError> {
        let uri = uri.into();
        let kind = parse_kind(&uri)?;

        match kind {
            LocationKind::Nfs => validate_nfs(&uri)?,
            LocationKind::S3 => validate_s3(&uri)?,
            LocationKind::HuggingFace => validate_hugging_face(&uri)?,
        }

        Ok(Self { uri, kind })
    }

    /// Parses an S3 destination URI.
    ///
    /// # Errors
    ///
    /// Returns an error when the URI is invalid or does not identify S3.
    pub fn parse_destination(uri: impl Into<String>) -> Result<Self, LocationError> {
        let location = Self::parse_source(uri)?;
        if location.kind != LocationKind::S3 {
            return Err(LocationError::DestinationMustBeS3);
        }
        Ok(location)
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    #[must_use]
    pub const fn kind(&self) -> LocationKind {
        self.kind
    }

    /// Returns whether two locations identify overlapping storage prefixes.
    ///
    /// Only S3 locations can overlap because destinations are always on S3.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        if self.kind != other.kind {
            return false;
        }
        match self.kind {
            LocationKind::S3 => {
                let Some((left_bucket, left_prefix)) = s3_parts(&self.uri) else {
                    return false;
                };
                let Some((right_bucket, right_prefix)) = s3_parts(&other.uri) else {
                    return false;
                };
                left_bucket == right_bucket
                    && (prefix_contains(left_prefix, right_prefix)
                        || prefix_contains(right_prefix, left_prefix))
            }
            LocationKind::Nfs => {
                let Some(left_path) = self.uri.strip_prefix("nfs://") else {
                    return false;
                };
                let Some(right_path) = other.uri.strip_prefix("nfs://") else {
                    return false;
                };
                prefix_contains(left_path, right_path) || prefix_contains(right_path, left_path)
            }
            LocationKind::HuggingFace => self.uri == other.uri,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LocationError {
    #[error("unsupported dataset location scheme: {0}")]
    UnsupportedScheme(String),
    #[error("NFS locations must use nfs:///absolute/path")]
    InvalidNfs,
    #[error("S3 locations must use s3://bucket/non-empty-prefix")]
    InvalidS3,
    #[error("Hugging Face locations must use hf://datasets/owner/name with an optional @revision")]
    InvalidHuggingFace,
    #[error("Lance destinations must use S3")]
    DestinationMustBeS3,
}

fn parse_kind(uri: &str) -> Result<LocationKind, LocationError> {
    if uri.starts_with("nfs://") {
        Ok(LocationKind::Nfs)
    } else if uri.starts_with("s3://") {
        Ok(LocationKind::S3)
    } else if uri.starts_with("hf://") {
        Ok(LocationKind::HuggingFace)
    } else {
        let scheme = uri.split_once("://").map_or(uri, |(scheme, _)| scheme);
        Err(LocationError::UnsupportedScheme(scheme.to_owned()))
    }
}

fn validate_nfs(uri: &str) -> Result<(), LocationError> {
    let path = uri
        .strip_prefix("nfs://")
        .ok_or(LocationError::InvalidNfs)?;
    if !path.starts_with('/')
        || path == "/"
        || path.contains('\0')
        || path.chars().any(char::is_whitespace)
        || path.split('/').any(|segment| matches!(segment, "." | ".."))
        || path.trim_start_matches('/').contains("//")
    {
        return Err(LocationError::InvalidNfs);
    }
    Ok(())
}

fn validate_s3(uri: &str) -> Result<(), LocationError> {
    let (bucket, prefix) = s3_parts(uri).ok_or(LocationError::InvalidS3)?;
    let starts_and_ends_with_alphanumeric = bucket
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && bucket
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    let bucket_valid = (3..=63).contains(&bucket.len())
        && bucket.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
        && starts_and_ends_with_alphanumeric
        && !bucket.contains("..")
        && !bucket.contains(".-")
        && !bucket.contains("-.")
        && bucket.parse::<Ipv4Addr>().is_err()
        && !["xn--", "sthree-", "amzn-s3-demo-"]
            .iter()
            .any(|prefix| bucket.starts_with(prefix))
        && !["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"]
            .iter()
            .any(|suffix| bucket.ends_with(suffix));

    if !bucket_valid || prefix.trim_matches('/').is_empty() || uri.chars().any(char::is_whitespace)
    {
        return Err(LocationError::InvalidS3);
    }
    Ok(())
}

fn validate_hugging_face(uri: &str) -> Result<(), LocationError> {
    let remainder = uri
        .strip_prefix("hf://datasets/")
        .ok_or(LocationError::InvalidHuggingFace)?;
    let (repository_and_revision, query) = remainder
        .split_once('?')
        .map_or((remainder, None), |(path, query)| (path, Some(query)));
    if query.is_some_and(|value| value.is_empty() || value.contains('?')) {
        return Err(LocationError::InvalidHuggingFace);
    }
    let (repository, revision) = repository_and_revision
        .split_once('@')
        .map_or((repository_and_revision, None), |(repository, revision)| {
            (repository, Some(revision))
        });
    if revision.is_some_and(|value| {
        value.is_empty() || value.contains('@') || !valid_uri_component(value, true)
    }) {
        return Err(LocationError::InvalidHuggingFace);
    }
    let mut segments = repository.split('/');
    let owner = segments.next().unwrap_or_default();
    let name = segments.next().unwrap_or_default();

    if owner.is_empty()
        || name.is_empty()
        || segments.next().is_some()
        || repository.len() > 96
        || !valid_hugging_face_identifier(owner)
        || !valid_hugging_face_identifier(name)
        || name
            .get(name.len().saturating_sub(4)..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(".git"))
        || uri.chars().any(char::is_whitespace)
    {
        return Err(LocationError::InvalidHuggingFace);
    }
    validate_hugging_face_query(query)?;
    Ok(())
}

fn s3_parts(uri: &str) -> Option<(&str, &str)> {
    uri.strip_prefix("s3://")?.split_once('/')
}

fn prefix_contains(parent: &str, child: &str) -> bool {
    let parent = parent.trim_matches('/');
    let child = child.trim_matches('/');
    child == parent
        || child
            .strip_prefix(parent)
            .is_some_and(|remainder| remainder.starts_with('/'))
}

fn validate_hugging_face_query(query: Option<&str>) -> Result<(), LocationError> {
    let Some(query) = query else {
        return Ok(());
    };
    let mut config_seen = false;
    let mut split_seen = false;
    for pair in query.split('&') {
        let (key, value) = pair
            .split_once('=')
            .ok_or(LocationError::InvalidHuggingFace)?;
        if value.is_empty() || !valid_uri_component(value, false) {
            return Err(LocationError::InvalidHuggingFace);
        }
        match key {
            "config" if !config_seen => config_seen = true,
            "split" if !split_seen => split_seen = true,
            _ => return Err(LocationError::InvalidHuggingFace),
        }
    }
    Ok(())
}

fn valid_uri_component(value: &str, allow_slash: bool) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return false;
            }
            index += 3;
            continue;
        }
        if !(byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.')
            || (allow_slash && byte == b'/'))
        {
            return false;
        }
        index += 1;
    }
    !value.is_empty()
}

fn valid_hugging_face_identifier(value: &str) -> bool {
    let starts_or_ends_with_forbidden =
        value.starts_with(['-', '.']) || value.ends_with(['-', '.']);
    !value.is_empty()
        && !starts_or_ends_with_forbidden
        && !value.contains("--")
        && !value.contains("..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::{DatasetLocation, LocationError, LocationKind};

    #[test]
    fn accepts_supported_sources() {
        assert_eq!(
            DatasetLocation::parse_source("nfs:///datasets/images")
                .unwrap()
                .kind(),
            LocationKind::Nfs
        );
        assert_eq!(
            DatasetLocation::parse_source("s3://example-bucket/datasets/images")
                .unwrap()
                .kind(),
            LocationKind::S3
        );
        assert_eq!(
            DatasetLocation::parse_source(
                "hf://datasets/owner/name@main?config=default&split=train"
            )
            .unwrap()
            .kind(),
            LocationKind::HuggingFace
        );
    }

    #[test]
    fn destination_is_s3_only() {
        assert_eq!(
            DatasetLocation::parse_destination("nfs:///datasets/output").unwrap_err(),
            LocationError::DestinationMustBeS3
        );
    }

    #[test]
    fn rejects_unknown_schemes() {
        assert_eq!(
            DatasetLocation::parse_source("gs://bucket/key").unwrap_err(),
            LocationError::UnsupportedScheme("gs".to_owned())
        );
    }

    #[test]
    fn rejects_invalid_s3_bucket_names() {
        for uri in [
            "s3://192.168.0.1/data",
            "s3://invalid..bucket/data",
            "s3://invalid.-bucket/data",
            "s3://xn--reserved/data",
            "s3://reserved-s3alias/data",
        ] {
            assert_eq!(
                DatasetLocation::parse_source(uri).unwrap_err(),
                LocationError::InvalidS3,
                "{uri}"
            );
        }
    }

    #[test]
    fn validates_hugging_face_suffixes() {
        for uri in [
            "hf://datasets/owner/name@",
            "hf://datasets/owner/name?unknown=value",
            "hf://datasets/owner/name?split=",
            "hf://datasets/owner/name?split=train&split=test",
            "hf://datasets/owner/name@main?config=bad value",
            "hf://datasets/.owner/name",
            "hf://datasets/owner/name.git",
            "hf://datasets/owner/bad--name",
            "hf://datasets/owner/bad..name",
            "hf://datasets/owner/name%20encoded",
        ] {
            assert_eq!(
                DatasetLocation::parse_source(uri).unwrap_err(),
                LocationError::InvalidHuggingFace,
                "{uri}"
            );
        }
    }

    #[test]
    fn detects_overlapping_s3_prefixes() {
        let source = DatasetLocation::parse_source("s3://example-bucket/data").unwrap();
        let nested =
            DatasetLocation::parse_destination("s3://example-bucket/data/output.lance").unwrap();
        let sibling =
            DatasetLocation::parse_destination("s3://example-bucket/data-output").unwrap();
        assert!(source.overlaps(&nested));
        assert!(!source.overlaps(&sibling));
    }

    #[test]
    fn detects_and_rejects_aliased_nfs_prefixes() {
        let parent = DatasetLocation::parse_source("nfs:///datasets/source/").unwrap();
        let child = DatasetLocation::parse_source("nfs:///datasets/source/child").unwrap();
        assert!(parent.overlaps(&child));
        assert_eq!(
            DatasetLocation::parse_source("nfs:///datasets/source/../other").unwrap_err(),
            LocationError::InvalidNfs
        );
    }
}
