# Running the Data Loader as a Databricks Job

The module now includes a helper script `data_loader.databricks_job` that makes it
simple to execute the loader inside a Databricks job. Parameters are read from
Databricks widgets when available and otherwise from environment variables.

## Supported Parameters

- `config_path` *(required)* – Path to the JSON configuration file (DBFS or local path).
- `tables` – Comma separated list of tables to process.
- `dry_run` – Set to `true` to perform a dry run.
- `optimize` – Run OPTIMIZE after processing.
- `vacuum` – Run VACUUM after processing.
- `cleanup_days` – Days to keep records in the tracking table (default `30`).

## Example Job Configuration

1. Upload the project package built with Poetry to DBFS or the workspace.
2. Create a new job with a task running `data_loader/databricks_job.py`.
3. Add widgets for the parameters above or set them as environment variables.

Example notebook invocation:

```python
# dbutils.notebook.run example
args = {
    "config_path": "/dbfs/config/data_loader.json",
    "optimize": "true",
    "vacuum": "true"
}
dbutils.notebook.run("databricks_job.py", 0, args)
```

See `demo/run_job_demo.py` for a local demonstration of preparing a configuration
file and environment variables.
