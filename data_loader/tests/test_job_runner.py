import os
from unittest.mock import MagicMock, patch

import pytest

from data_loader.job_runner import load_job_params


def test_load_job_params_env(monkeypatch):
    monkeypatch.setenv("DATALOADER_CONFIG_FILE", "/tmp/config.json")
    monkeypatch.setenv("DATALOADER_LOG_LEVEL", "DEBUG")
    params = load_job_params()
    assert params["config_file"] == "/tmp/config.json"
    assert params["log_level"] == "DEBUG"


def test_load_job_params_widgets(monkeypatch):
    fake_dbutils = MagicMock()
    fake_dbutils.widgets.get.side_effect = lambda n: {
        "config": "/dbfs/config.json",
        "log_level": "INFO",
        "optimize": "true",
        "vacuum": "false",
    }[n]
    with patch("data_loader.job_runner._get_dbutils", return_value=fake_dbutils):
        params = load_job_params()
    assert params["config_file"] == "/dbfs/config.json"
    assert params["optimize"] == "true"
