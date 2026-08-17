mod blob;
mod config;
mod converter;
mod error;
mod http_store;
mod indexes;
mod inspection;
mod progress;
mod source;
mod validation;

pub use config::ConverterConfig;
pub use converter::Converter;
pub use error::ConversionError;
pub use inspection::{SourceColumn, SourceSchemaInspection, inspect_source_schema};
pub use progress::ConversionProgress;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod type_compatibility_tests;
