use std::str::FromStr;

use clap::{Parser, Subcommand};

use lance_conversion_core::job::{IndexSpec, IndexType};

const DEFAULT_API_URL: &str = "http://127.0.0.1:8080";

/// Submit and query Lance format conversion jobs.
#[derive(Debug, Parser)]
#[command(name = "lance-convert", version = env!("CARGO_PKG_VERSION"), about)]
pub struct Cli {
    /// Base URL of the conversion control plane.
    #[arg(long, env = "LANCE_API_URL", default_value = DEFAULT_API_URL)]
    pub url: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Submit a format conversion job.
    Submit {
        /// Job creator identity recorded with the job.
        #[arg(long)]
        creator: String,
        /// Source dataset URI or filesystem path.
        #[arg(long)]
        source: String,
        /// Destination Lance dataset URI or filesystem path.
        #[arg(long)]
        destination: String,
        /// Source column ingested as a Lance blob. Repeatable.
        #[arg(long = "blob-column")]
        blob_columns: Vec<String>,
        /// Index to build after conversion, as `column:scalar|text|vector`. Repeatable.
        #[arg(long = "index", value_parser = parse_index_spec)]
        indices: Vec<IndexSpec>,
    },
    /// List conversion jobs.
    List {
        /// Restrict results to this creator.
        #[arg(long)]
        creator: Option<String>,
        /// Return only failed jobs.
        #[arg(long)]
        failed: bool,
        /// Return only queuing and running jobs.
        #[arg(long)]
        ongoing: bool,
        /// Maximum number of jobs to return.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show one job by destination URI.
    Status {
        /// Destination Lance dataset URI or filesystem path.
        #[arg(long)]
        destination: String,
    },
}

/// Parses `column:index_type` CLI index specifications.
///
/// # Errors
///
/// Returns an error when the value is missing a colon, the column is empty, or
/// the index type is not `scalar`, `text`, or `vector`.
pub fn parse_index_spec(value: &str) -> Result<IndexSpec, String> {
    let (column, index_type) = value.split_once(':').ok_or_else(|| {
        format!("expected COLUMN:TYPE, got '{value}'; type is scalar, text, or vector")
    })?;
    if column.is_empty() {
        return Err("index column must not be empty".to_owned());
    }
    Ok(IndexSpec {
        column: column.to_owned(),
        index_type: IndexType::from_str(index_type)?,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_index_spec;
    use lance_conversion_core::job::IndexType;

    #[test]
    fn parses_vector_index_spec() {
        let spec = parse_index_spec("embedding:vector").unwrap();
        assert_eq!(spec.column, "embedding");
        assert_eq!(spec.index_type, IndexType::Vector);
    }

    #[test]
    fn rejects_index_spec_without_type() {
        assert!(parse_index_spec("embedding").is_err());
    }
}
