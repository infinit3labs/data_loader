# Running as a Databricks Job

The `data_loader.job_runner` module provides an entry point for running the
loader on a Databricks cluster. Parameters are supplied via Databricks widgets
or environment variables so the same script can be executed locally.

## Required Widgets / Environment Variables

- `config` / `DATALOADER_CONFIG_FILE` – path to the JSON configuration file
- `log_level` / `DATALOADER_LOG_LEVEL` – log level (default: `INFO`)
- `optimize` / `DATALOADER_OPTIMIZE` – set to `true` to run table optimization
- `vacuum` / `DATALOADER_VACUUM` – set to `true` to run table vacuum

## Usage

```bash
# On Databricks, create widgets in a notebook and run
%run /path/to/job
```

Locally the job can be executed by setting environment variables:

```bash
export DATALOADER_CONFIG_FILE=./config.json
python -m data_loader.job_runner
```
