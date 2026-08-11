use std::num::NonZeroUsize;

use crate::ConversionError;

const MIB: u64 = 1024 * 1024;

/// Controls how the converter lays out the destination Lance dataset.
#[derive(Debug, Clone, Copy)]
pub struct ConverterConfig {
    /// Soft maximum size, in MiB, for each generated Lance data file.
    pub target_lance_file_size_mib: u64,
    /// Maximum Blob V2 value size, in MiB, stored inline in a Lance data file.
    ///
    /// Larger values are stored in Lance-managed external blob files.
    pub blob_inline_threshold_mib: u64,
    /// Maximum Blob V2 value size, in MiB, stored in packed blob storage.
    ///
    /// Larger values are stored in dedicated Lance-managed blob files.
    pub blob_dedicated_threshold_mib: u64,
}

pub(crate) struct ByteConfig {
    pub(crate) max_bytes_per_file: usize,
    pub(crate) inline_threshold: usize,
    pub(crate) dedicated_threshold: NonZeroUsize,
}

impl ConverterConfig {
    pub(crate) fn validate(self) -> Result<ByteConfig, ConversionError> {
        let max_bytes_per_file =
            mib_to_usize(self.target_lance_file_size_mib, "target Lance file size")?;
        let inline_threshold =
            mib_to_usize(self.blob_inline_threshold_mib, "blob inline threshold")?;
        let dedicated_threshold = mib_to_usize(
            self.blob_dedicated_threshold_mib,
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
}

fn mib_to_usize(value: u64, setting: &str) -> Result<usize, ConversionError> {
    value
        .checked_mul(MIB)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or_else(|| {
            ConversionError::InvalidConfiguration(format!("{setting} does not fit usize bytes"))
        })
}
