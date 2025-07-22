from unittest.mock import Mock, patch
from data_loader.core.state_manager import PipelineStateManager, PipelineStageStatus
from data_loader.config.table_config import DataLoaderConfig, EXAMPLE_CONFIG
from data_loader.core.processor import DataProcessor
from data_loader.config.databricks_config import DatabricksConfig


def test_pipeline_state_manager(tmp_path):
    manager = PipelineStateManager(tmp_path)
    manager.reset()
    assert manager.get_table_status("t1") == PipelineStageStatus.NOT_STARTED
    manager.update_table_status("t1", PipelineStageStatus.IN_PROGRESS)
    assert manager.get_table_status("t1") == PipelineStageStatus.IN_PROGRESS
    manager.update_table_status("t1", PipelineStageStatus.COMPLETED)
    assert manager.get_table_status("t1") == PipelineStageStatus.COMPLETED

    # Reload
    manager2 = PipelineStateManager(tmp_path)
    assert manager2.get_table_status("t1") == PipelineStageStatus.COMPLETED


class DummyFileTracker:
    def __init__(self, *a, **k):
        pass


@patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
@patch("data_loader.core.processor.FileTracker", DummyFileTracker)
def test_processor_skips_completed(tmp_path, monkeypatch):
    cfg = EXAMPLE_CONFIG.copy()
    cfg["checkpoint_path"] = str(tmp_path)
    config = DataLoaderConfig(**cfg)

    processor = DataProcessor(config)
    monkeypatch.setattr(
        processor,
        "process_table",
        lambda tc: {
            "files_discovered": 0,
            "files_processed": 0,
            "successful_files": 0,
            "failed_files": 0,
        },
    )

    # First run processes all tables
    result1 = processor.process_all_tables()
    assert result1["tables_processed"] == len(config.tables)

    # Second run should skip because state marks tables as completed
    processor2 = DataProcessor(config)
    monkeypatch.setattr(processor2, "process_table", lambda tc: {"files_discovered": 0})
    result2 = processor2.process_all_tables()
    assert result2["tables_processed"] == 0
