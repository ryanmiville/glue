use anyhow::Result;
use inquire::{Select, Text};

#[derive(Debug, Clone)]
pub struct Args {
    pub name: String,
    pub role: String,
}

pub fn start(roles: Vec<String>) -> Result<Args> {
    let dt = chrono::Local::now().format("%F_%T").to_string();
    let name = Text::new("Enter a notebook name:")
        .with_default(dt.as_str())
        .prompt()?;
    let role = Select::new("Select a role ARN:", roles).prompt()?;
    Ok(Args { name, role })
}
