use std::time::Duration;

use anyhow::Result;
use chrono::NaiveDate;
use indicatif::{ProgressBar, ProgressStyle};
use inquire::validator::Validation;
use inquire::{CustomUserError, DateSelect, Text};

#[derive(Debug, Clone)]
pub struct Args {
    pub name: String,
    pub date_arg_name: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub additional_args: Vec<String>,
}

impl Args {
    pub fn message(&self, date: &String) -> String {
        let mut msg = format!("Running {} with arguments:\n", self.name);
        msg.push_str(&format!(" {}: {}\n", self.date_arg_name, date));
        for chunk in self.additional_args.chunks(2) {
            if let [arg, value] = chunk {
                msg.push_str(&format!(" {}: {}\n", arg, value));
            }
        }
        return msg;
    }
}
pub fn start() -> Result<Args> {
    let start = DateSelect::new("Start date:").prompt()?;
    let end = DateSelect::new("End date:").with_min_date(start).prompt()?;
    let name = Text::new("Job name:").prompt()?;
    let date_arg_name = Text::new("Name of date arg:")
        .with_default("--date")
        .with_validator(arg_name_validator)
        .prompt()?;

    let mut additional = Vec::new();
    loop {
        let arg = Text::new("Enter additional arguments (press esc when finished):")
            .with_placeholder("--name value")
            .with_validator(arg_name_validator)
            .with_validator(contains_space_validator)
            .prompt_skippable()?;
        match arg {
            Some(arg) => {
                if let Some((arg, value)) = arg.split_once(' ') {
                    additional.push(arg.into());
                    additional.push(value.into());
                }
            }
            None => break,
        }
    }
    let args = Args {
        name,
        date_arg_name,
        start_date: start,
        end_date: end,
        additional_args: additional,
    };
    return Ok(args);
}

pub fn spinner(message: String) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{msg} [{elapsed_precise:.blue}] {spinner:.green}").unwrap(),
    );
    spinner.set_message(message);
    spinner.enable_steady_tick(Duration::from_millis(100));
    return spinner;
}

fn arg_name_validator(input: &str) -> Result<Validation, CustomUserError> {
    if input.starts_with("--") {
        Ok(Validation::Valid)
    } else {
        Ok(Validation::Invalid("args must start with '--'".into()))
    }
}

fn contains_space_validator(input: &str) -> Result<Validation, CustomUserError> {
    if input.contains(" ") {
        Ok(Validation::Valid)
    } else {
        Ok(Validation::Invalid("missing arg value".into()))
    }
}
