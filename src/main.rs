use anyhow::Result;
use backfill::Backfill;
use chrono::{Duration, NaiveDate};
use clap::{Parser, Subcommand};
mod backfill;
mod tui;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Cli::parse();
    match &cmd.command {
        Commands::Backfill => backfill().await,
    }
}

async fn backfill() -> Result<()> {
    let args = tui::start()?;
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_glue::Client::new(&config);

    let dates = dates(args.start_date, args.end_date);
    let backfill = Backfill::new(client, args.clone());
    for date in dates {
        let spinner = tui::spinner(args.message(&date));
        backfill.run(&date).await?;
        spinner.finish_with_message(format!("{date} backfill successful"));
    }
    Ok(())
}

fn dates(start: NaiveDate, end: NaiveDate) -> Vec<String> {
    let num_days = end.signed_duration_since(start).num_days();
    let dates: Vec<String> = (0..=num_days)
        .map(|days| start + Duration::days(days))
        .map(|date| date.format("%Y-%m-%d").to_string())
        .collect();
    dates
}
