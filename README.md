# Glue

This is a simple CLI to interact with AWS Glue services.

## Prerequisites

You have AWS credentials configured in your environment. See [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html) for more information.

## Usage

```
glue --help
A CLI for interacting with AWS Glue jobs

Usage: glue [OPTIONS] <COMMAND>

Commands:
  run       Run a Glue job
  backfill  Run a backfill for a given date range
  notebook  Create a new jupyter notebook configured for a Glue interactive session
  help      Print this message or the help of the given subcommand(s)

Options:
  -p, --profile <PROFILE>  Optional AWS profile name to use
  -h, --help               Print help
```
