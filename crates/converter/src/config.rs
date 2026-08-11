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
