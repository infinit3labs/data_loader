from pathlib import Path
from data_loader.config import (
    load_config_from_file,
    load_runtime_config,
    DataLoaderConfig,
)


def test_load_config_from_yaml(tmp_path):
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        "raw_data_path: ./raw\nprocessed_data_path: ./proc\ncheckpoint_path: ./chk\ntables: []\n"
    )

    cfg = load_config_from_file(str(cfg_path))
    assert cfg.processed_data_path == "./proc"


def test_env_override(tmp_path, monkeypatch):
    cfg_path = tmp_path / "base.yaml"
    cfg_path.write_text(
        "raw_data_path: ./raw\nprocessed_data_path: ./proc\ncheckpoint_path: ./chk\ntables: []\n"
    )
    monkeypatch.setenv("DATALOADER_RAW_DATA_PATH", "./env_raw")
    cfg = load_runtime_config(str(cfg_path))
    assert cfg.raw_data_path == "./env_raw"
