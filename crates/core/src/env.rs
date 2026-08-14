use std::{env::VarError, fmt::Display, str::FromStr};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnvError {
    #[error("environment variable {name} is not valid Unicode")]
    NotUnicode { name: &'static str },
    #[error("environment variable {name} has invalid value '{value}': {reason}")]
    Invalid {
        name: &'static str,
        value: String,
        reason: String,
    },
}

/// Returns a string environment variable or its default when unset.
///
/// # Errors
///
/// Returns an error when the configured value is not valid Unicode.
pub fn string_or(name: &'static str, default: &str) -> Result<String, EnvError> {
    match std::env::var(name) {
        Ok(value) => Ok(value),
        Err(VarError::NotPresent) => Ok(default.to_owned()),
        Err(VarError::NotUnicode(_)) => Err(EnvError::NotUnicode { name }),
    }
}

/// Parses an environment variable or returns its default when unset.
///
/// # Errors
///
/// Returns an error when the configured value is not valid Unicode or cannot
/// be parsed as `T`.
pub fn parse_or<T>(name: &'static str, default: T) -> Result<T, EnvError>
where
    T: FromStr,
    T::Err: Display,
{
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(VarError::NotPresent) => return Ok(default),
        Err(VarError::NotUnicode(_)) => return Err(EnvError::NotUnicode { name }),
    };
    value.parse().map_err(|error: T::Err| EnvError::Invalid {
        name,
        value,
        reason: error.to_string(),
    })
}
