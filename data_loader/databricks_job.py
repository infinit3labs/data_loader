"""Databricks job runner for the data loader.

This module allows executing the data loader inside a Databricks job. It reads
parameters from Databricks widgets when available and falls back to environment
variables. The following parameters are supported:

- ``config_path``: Path to the JSON configuration file.
- ``tables``: Optional comma separated list of tables to process.
- ``dry_run``: ``true`` to perform a dry run without loading data.
- ``optimize``: ``true`` to run OPTIMIZE on tables after loading.
- ``vacuum``: ``true`` to run VACUUM on tables after loading.
- ``cleanup_days``: Number of days to retain records in the tracking table.

The script can be used as the entry point for a Databricks job.
"""

from __future__ import annotations

import os
from typing import Optional
from loguru import logger

try:
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
except Exception:  # pragma: no cover - pyspark not available during unit tests
    SparkSession = None
    DBUtils = None

from .main import load_configuration, print_processing_summary
from .core.processor import DataProcessor
from .utils.logger import setup_logging


def _get_widget_or_env(dbutils: Optional[object], name: str, default: str = "") -> str:
    """Retrieve a parameter from Databricks widgets or environment variables."""
    if dbutils is not None:
        try:  # pragma: no cover - depends on Databricks runtime
            return dbutils.widgets.get(name) or default
        except Exception:
            pass
    return os.getenv(name.upper(), default)


def run_job() -> None:
    """Execute the data loader using parameters from the Databricks environment."""
    if SparkSession is None:
        raise RuntimeError("PySpark is required to run the Databricks job runner")

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark) if DBUtils is not None else None

    config_path = _get_widget_or_env(dbutils, "config_path")
    if not config_path:
        raise ValueError(
            "Configuration path must be provided via widget 'config_path' or env variable CONFIG_PATH"
        )

    tables = _get_widget_or_env(dbutils, "tables")
    dry_run = _get_widget_or_env(dbutils, "dry_run").lower() == "true"
    optimize = _get_widget_or_env(dbutils, "optimize").lower() == "true"
    vacuum = _get_widget_or_env(dbutils, "vacuum").lower() == "true"
    cleanup_days = int(_get_widget_or_env(dbutils, "cleanup_days", "30"))

    setup_logging()
    config = load_configuration(config_path, None)

    if tables:
        table_list = [t.strip() for t in tables.split(",")]
        config.tables = [t for t in config.tables if t.table_name in table_list]
        logger.info(f"Processing only tables: {table_list}")

    processor = DataProcessor(config)

    if dry_run:
        for table_conf in config.tables:
            discovered = processor.discover_files(table_conf)
            logger.info(
                f"Table {table_conf.table_name}: {len(discovered)} files would be processed"
            )
        return

    results = processor.process_all_tables()
    print_processing_summary(results)

    if optimize:
        processor.optimize_all_tables()
    if vacuum:
        processor.vacuum_all_tables()
    if cleanup_days > 0:
        processor.cleanup_old_records(cleanup_days)

    logger.info("Job completed successfully")


if __name__ == "__main__":  # pragma: no cover - entry point
    run_job()
