use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("invalid converter configuration: {0}")]
    InvalidConfiguration(String),
    #[error("invalid source: {0}")]
    InvalidSource(String),
    #[error("invalid destination: {0}")]
    InvalidDestination(String),
    #[error("unsupported source schema: {0}")]
    UnsupportedType(String),
    #[error("source read failed: {0}")]
    Read(String),
    #[error("Lance write failed: {0}")]
    Write(String),
    #[error("conversion validation failed: {0}")]
    Validation(String),
    #[error("move source deletion failed after conversion: {0}")]
    Delete(String),
}
