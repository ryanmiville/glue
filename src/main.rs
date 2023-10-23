use anyhow::Result;
use clap::{Parser, Subcommand};
mod backfill;
mod notebook;

/// A CLI for interacting with AWS Glue jobs
#[derive(Parser)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// run a backfill for a given date range
    Backfill,
    /// create a new jupyter notebook connected to a Glue interactive session
    Notebook,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Cli::parse();
    match &cmd.command {
        Commands::Backfill => backfill::run().await,
        Commands::Notebook => notebook::run().await,
    }
}
