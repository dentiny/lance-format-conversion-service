use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
};

use lance::deps::datafusion::error::DataFusionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("invalid converter configuration: {0}")]
    InvalidConfiguration(String),
    #[error("invalid source: {0}")]
    InvalidSource(String),
    #[error("invalid destination: {0}")]
    InvalidDestination(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("unsupported source schema: {0}")]
    UnsupportedType(String),
    #[error("invalid blob column specification: {0}")]
    InvalidBlobSpec(String),
    #[error("invalid index specification: {0}")]
    InvalidIndexSpec(String),
    #[error("source read failed: {0}")]
    Read(String),
    #[error("Lance write failed: {0}")]
    Write(String),
    #[error("Lance index creation failed: {0}")]
    Index(String),
    #[error("conversion validation failed: {0}")]
    Validation(String),
}

impl ConversionError {
    pub(crate) fn catch_panic<T>(
        context: &str,
        operation: impl FnOnce() -> Result<T, Self>,
    ) -> Result<T, Self> {
        catch_unwind(AssertUnwindSafe(operation))
            .map_err(|payload| Self::from_panic(context, payload.as_ref()))?
    }

    fn from_panic(context: &str, payload: &(dyn Any + Send)) -> Self {
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("unknown panic");
        Self::Read(format!("panic in {context}: {message}"))
    }
}

impl From<ConversionError> for DataFusionError {
    fn from(error: ConversionError) -> Self {
        Self::Execution(error.to_string())
    }
}
