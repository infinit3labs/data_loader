"""Databricks Job Runner integration.

This module provides utilities to run the data loader inside a
Databricks job. Parameters can be provided either via Databricks
widgets or environment variables so that the same code works when
executed locally for testing.
"""

from __future__ import annotations

import os
from typing import Dict, Any

from loguru import logger

try:
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
except (ImportError, ModuleNotFoundError):  # pragma: no cover - Spark not available in tests
    SparkSession = None
    DBUtils = None

from .config.table_config import DataLoaderConfig
from .main import load_configuration, print_processing_summary
from .utils.logger import setup_logging


def _get_dbutils():
    """Return DBUtils instance if running on Databricks."""
    if SparkSession is None or DBUtils is None:
        return None
    try:
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except (ImportError, RuntimeError):
        return None


def load_job_params(defaults: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Load job parameters from widgets or environment variables."""
    params = defaults.copy() if defaults else {}
    dbutils = _get_dbutils()

    def _get_param(name: str, env_name: str, default: str | None = None) -> str | None:
        if dbutils:
            try:
                return dbutils.widgets.get(name)
            except Exception:
                pass
        return os.getenv(env_name, default)

    params["config_file"] = _get_param("config", "DATALOADER_CONFIG_FILE")
    params["log_level"] = _get_param("log_level", "DATALOADER_LOG_LEVEL", "INFO")
    params["optimize"] = _get_param("optimize", "DATALOADER_OPTIMIZE", "false")
    params["vacuum"] = _get_param("vacuum", "DATALOADER_VACUUM", "false")

    return params


def run_job() -> Dict[str, Any]:
    """Entry point used by Databricks jobs."""
    job_params = load_job_params()
    config_file = job_params.get("config_file")
    log_level = job_params.get("log_level", "INFO")
    optimize = str(job_params.get("optimize", "false")).lower() == "true"
    vacuum = str(job_params.get("vacuum", "false")).lower() == "true"

    setup_logging(log_level=log_level)

    if not config_file:
        raise ValueError("Configuration file must be provided via widget or env var")

    config: DataLoaderConfig = load_configuration(config_file=config_file)
    from .core.processor import DataProcessor  # imported lazily

    processor = DataProcessor(config)
    results = processor.process_all_tables()
    print_processing_summary(results)

    if optimize:
        processor.optimize_all_tables()
    if vacuum:
        processor.vacuum_all_tables()

    return results


__all__ = ["run_job", "load_job_params"]
