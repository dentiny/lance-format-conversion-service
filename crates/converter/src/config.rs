#[derive(Debug, Clone, Copy)]
pub struct ConverterConfig {
    pub target_lance_file_size_mib: u64,
    pub blob_inline_threshold_mib: u64,
}
