use anyhow::Result;
use std::fs::File;
use std::io::Write;

mod tui;

const TEMPLATE: &str = include_str!("notebook/template.ipynb");

pub async fn run() -> Result<()> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_iam::Client::new(&config);
    let roles = list_roles(client).await?;
    let args = tui::start(roles)?;
    create_notebook(&args).await?;
    Ok(())
}

async fn list_roles(client: aws_sdk_iam::Client) -> Result<Vec<String>> {
    let mut roles = Vec::new();
    let resp = client.list_roles().send().await?;
    for role in resp.roles.unwrap_or_default() {
        let arn = role.arn().filter(|n| n.contains("glue"));
        if let Some(arn) = arn {
            roles.push(arn.into());
        }
    }
    if !resp.is_truncated {
        return Ok(roles);
    }
    let mut marker = resp.marker.unwrap();
    loop {
        let resp = client.list_roles().marker(marker).send().await?;
        for role in resp.roles.unwrap_or_default() {
            let arn = role.arn().filter(|n| n.contains("glue"));
            if let Some(arn) = arn {
                roles.push(arn.into());
            }
        }
        if !resp.is_truncated {
            break;
        }
        marker = resp.marker.unwrap();
    }
    Ok(roles)
}

async fn create_notebook(args: &tui::Args) -> Result<()> {
    let contents = TEMPLATE.replace("IAM_ROLE_ARN", args.role.as_str());
    let fname = format!("{}.ipynb", args.name);
    let mut file = File::create(fname)?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}
