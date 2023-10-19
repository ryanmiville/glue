use anyhow::{anyhow, bail, Result};
use async_recursion::async_recursion;
use aws_sdk_glue::operation::start_job_run::StartJobRunOutput;
use aws_sdk_glue::types::{JobRun, JobRunState};

use std::time::Duration;
use tokio::time::sleep;

use crate::tui;

pub struct Backfill {
    client: aws_sdk_glue::Client,
    args: tui::Args,
}

impl Backfill {
    pub fn new(client: aws_sdk_glue::Client, args: tui::Args) -> Self {
        Self { client, args }
    }

    pub async fn run(&self, run_date: &str) -> Result<()> {
        let job_run_id = self
            .start_job(run_date)
            .await?
            .job_run_id
            .ok_or(anyhow!("Job run ID is missing"))?;
        self.poll_job_status(&job_run_id).await
    }

    async fn start_job(&self, run_date: &str) -> Result<StartJobRunOutput> {
        let mut builder = self
            .client
            .start_job_run()
            .job_name(&self.args.name)
            .arguments(&self.args.date_arg_name, run_date);

        for chunk in self.args.additional_args.chunks(2) {
            if let [arg, value] = chunk {
                builder = builder.arguments(arg, value);
            }
        }
        let out = builder.send().await?;
        Ok(out)
    }

    async fn get_job_run(&self, job_run_id: &str) -> Result<Option<JobRun>> {
        Ok(self
            .client
            .get_job_run()
            .job_name(&self.args.name)
            .run_id(job_run_id)
            .send()
            .await?
            .job_run)
    }

    #[async_recursion(?Send)]
    async fn poll_job_status(&self, job_run_id: &str) -> Result<()> {
        let state = self
            .get_job_run(job_run_id)
            .await?
            .and_then(|r| r.job_run_state);

        match state {
            Some(JobRunState::Running) => {
                sleep(Duration::from_secs(60)).await;
                return self.poll_job_status(job_run_id).await;
            }
            Some(JobRunState::Failed) => {
                let error_message = self
                    .get_job_run(job_run_id)
                    .await?
                    .and_then(|r| r.error_message)
                    .unwrap_or("Unknown error".to_string());
                bail!(error_message);
            }
            _ => return Ok(()),
        }
    }
}
