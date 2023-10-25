use anyhow::Result;
use clap::{Parser, Subcommand};
mod backfill;
mod notebook;

/// A CLI for interacting with AWS Glue jobs
#[derive(Parser)]
struct Cli {
    /// Optional AWS profile name to use
    #[arg(short, long)]
    profile: Option<String>,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run a backfill for a given date range
    Backfill,
    /// Create a new jupyter notebook configured for a Glue interactive session
    Notebook,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Cli::parse();
    if let Some(profile) = cmd.profile {
        std::env::set_var("AWS_PROFILE", profile);
    }
    match &cmd.command {
        Commands::Backfill => backfill::run().await,
        Commands::Notebook => notebook::run().await,
    }
}
