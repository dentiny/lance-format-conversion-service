mod args;
mod client;

use clap::Parser;

pub use args::{Cli, Command, parse_index_spec};
pub use client::{Client, ClientError};

/// Parses CLI arguments and executes the selected command.
///
/// # Errors
///
/// Returns an error when arguments are invalid, the API is unreachable, or the
/// service rejects the request.
pub async fn run() -> Result<(), ClientError> {
    execute(Cli::parse()).await
}

async fn execute(cli: Cli) -> Result<(), ClientError> {
    let client = Client::new(&cli.url)?;
    match cli.command {
        Command::Submit {
            creator,
            source,
            destination,
            blob_columns,
            indices,
        } => print_json(
            &client
                .submit_job(&creator, &source, &destination, blob_columns, &indices)
                .await?,
        )?,
        Command::List {
            creator,
            failed,
            ongoing,
            limit,
        } => print_json(
            &client
                .list_jobs(creator.as_deref(), failed, ongoing, limit)
                .await?,
        )?,
        Command::Status { destination } => print_json(&client.get_job(&destination).await?)?,
    }
    Ok(())
}

fn print_json(value: &impl serde::Serialize) -> Result<(), ClientError> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
