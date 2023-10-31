use anyhow::{anyhow, bail, Result};
use async_recursion::async_recursion;
use aws_sdk_glue::operation::start_job_run::StartJobRunOutput;
use aws_sdk_glue::types::{JobRun, JobRunState};

use std::time::Duration;
use tokio::time::sleep;

mod tui;

pub async fn run() -> Result<()> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_glue::Client::new(&config);
    let jobs = get_all_job_names(&client).await?;
    let args = tui::start(jobs)?;
    let run = Run::new(client, args.clone());
    let spinner = tui::spinner(args.message());
    run.run().await?;
    spinner.finish_with_message("run successful");
    Ok(())
}

struct Run {
    client: aws_sdk_glue::Client,
    args: tui::Args,
}

impl Run {
    fn new(client: aws_sdk_glue::Client, args: tui::Args) -> Self {
        Self { client, args }
    }

    async fn run(&self) -> Result<()> {
        let job_run_id = self
            .start_job()
            .await?
            .job_run_id
            .ok_or(anyhow!("Job run ID is missing"))?;
        self.poll_job_status(&job_run_id).await
    }

    async fn start_job(&self) -> Result<StartJobRunOutput> {
        let mut builder = self.client.start_job_run().job_name(&self.args.name);

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

async fn get_all_job_names(client: &aws_sdk_glue::Client) -> Result<Vec<String>> {
    let jobs = get_all_jobs(client).await?;
    let names = jobs
        .iter()
        .flat_map(|job| job.name.clone())
        .collect::<Vec<_>>();
    Ok(names)
}

async fn get_all_jobs(
    client: &aws_sdk_glue::Client,
) -> Result<Vec<aws_sdk_glue::types::Job>, aws_sdk_glue::Error> {
    let mut jobs = Vec::new();
    let mut next_token = None;

    loop {
        let mut request = client.get_jobs();
        if let Some(token) = next_token {
            request = request.next_token(token);
        }
        let response = request.send().await?;
        jobs.extend(response.jobs.unwrap_or_default());

        if let Some(token) = response.next_token {
            next_token = Some(token);
        } else {
            break;
        }
    }

    Ok(jobs)
}
