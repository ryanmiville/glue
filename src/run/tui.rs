use std::time::Duration;

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use inquire::validator::Validation;
use inquire::{CustomUserError, Select, Text};

#[derive(Debug, Clone)]
pub struct Args {
    pub name: String,
    pub additional_args: Vec<String>,
}

impl Args {
    pub fn message(&self) -> String {
        let mut msg = format!("Running {} with arguments:\n", self.name);
        for chunk in self.additional_args.chunks(2) {
            if let [arg, value] = chunk {
                msg.push_str(&format!(" {}: {}\n", arg, value));
            }
        }
        msg
    }
}

pub fn start(jobs: Vec<String>) -> Result<Args> {
    let name = Select::new("Select a Glue job:", jobs).prompt()?;

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
        additional_args: additional,
    };
    Ok(args)
}

pub fn spinner(message: String) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{msg} [{elapsed_precise:.blue}] {spinner:.green}").unwrap(),
    );
    spinner.set_message(message);
    spinner.enable_steady_tick(Duration::from_millis(100));
    spinner
}

fn arg_name_validator(input: &str) -> Result<Validation, CustomUserError> {
    if input.starts_with("--") {
        Ok(Validation::Valid)
    } else {
        Ok(Validation::Invalid("args must start with '--'".into()))
    }
}

fn contains_space_validator(input: &str) -> Result<Validation, CustomUserError> {
    if input.contains(' ') {
        Ok(Validation::Valid)
    } else {
        Ok(Validation::Invalid("missing arg value".into()))
    }
}
