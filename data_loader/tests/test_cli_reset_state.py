from typer.testing import CliRunner
from data_loader.main import app
from data_loader.config import EXAMPLE_CONFIG
import yaml
from unittest.mock import patch


def test_reset_state_command(tmp_path):
    cfg_file = tmp_path / "cfg.yaml"
    with open(cfg_file, "w") as fh:
        yaml.safe_dump(EXAMPLE_CONFIG, fh)

    runner = CliRunner()
    with patch("data_loader.core.processor.DataProcessor") as mock_proc:
        result = runner.invoke(app, ["reset-state", "--config", str(cfg_file)])
        assert result.exit_code == 0
        mock_proc.return_value.reset_pipeline_state.assert_called_once()
