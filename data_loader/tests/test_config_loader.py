from pathlib import Path
from data_loader.config import load_config_from_file, DataLoaderConfig


def test_load_config_from_json(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(
        '{"raw_data_path": "./raw", "processed_data_path": "./proc", "checkpoint_path": "./chk", "tables": []}'
    )

    cfg = load_config_from_file(str(cfg_path))
    assert isinstance(cfg, DataLoaderConfig)
    assert cfg.raw_data_path == "./raw"


def test_load_config_from_yaml(tmp_path):
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        "raw_data_path: ./raw\nprocessed_data_path: ./proc\ncheckpoint_path: ./chk\ntables: []\n"
    )

    cfg = load_config_from_file(str(cfg_path))
    assert cfg.processed_data_path == "./proc"
