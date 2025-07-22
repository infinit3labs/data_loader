"""Demonstration of running the job runner locally."""
import os
from data_loader.job_runner import run_job

# Point to the example configuration for demonstration
os.environ.setdefault("DATALOADER_CONFIG_FILE", "./example_config.yaml")

if __name__ == "__main__":
    try:
        run_job()
    except Exception as exc:
        print(f"Job run failed: {exc}")
