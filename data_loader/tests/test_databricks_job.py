import os
from types import SimpleNamespace
from unittest import mock

import pytest

from data_loader import databricks_job


def test_get_widget_or_env_prefers_widget():
    dbutils = SimpleNamespace(widgets=SimpleNamespace(get=lambda name: "widget"))
    os.environ["VALUE"] = "env"
    assert databricks_job._get_widget_or_env(dbutils, "value") == "widget"


def test_get_widget_or_env_env_fallback():
    if "VALUE" in os.environ:
        del os.environ["VALUE"]
    os.environ["VALUE"] = "env"
    assert databricks_job._get_widget_or_env(None, "value") == "env"


def test_run_job_invokes_processor(monkeypatch):
    monkeypatch.setenv("CONFIG_PATH", "cfg.json")
    monkeypatch.setenv("DATALOADER_DISABLE_SPARK", "1")

    fake_spark = mock.MagicMock()
    monkeypatch.setattr(databricks_job, "SparkSession", SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: fake_spark)))
    monkeypatch.setattr(databricks_job, "DBUtils", None)

    fake_config = mock.MagicMock()
    fake_config.tables = []
    monkeypatch.setattr(databricks_job, "load_configuration", lambda path, _: fake_config)

    processor_instance = mock.MagicMock()
    processor_instance.process_all_tables.return_value = {
        "tables_processed": 0,
        "total_files_discovered": 0,
        "total_files_processed": 0,
        "successful_files": 0,
        "failed_files": 0,
        "total_processing_time": 0.0,
        "table_results": {},
    }
    monkeypatch.setattr(databricks_job, "DataProcessor", lambda cfg: processor_instance)

    databricks_job.run_job()
    processor_instance.process_all_tables.assert_called_once()
