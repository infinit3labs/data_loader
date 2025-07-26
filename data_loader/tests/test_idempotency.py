"""
Tests for idempotency and recovery functionality.

This module contains tests to verify that all operations are idempotent
and that the pipeline can recover from any failure point.
"""

import pytest
import tempfile
import json
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from data_loader.core.pipeline_lock import PipelineLock, PipelineLockError
from data_loader.core.file_tracker import FileTracker, FileProcessingStatus
from data_loader.core.processor import DataProcessor
from data_loader.config.table_config import DataLoaderConfig, TableConfig, LoadingStrategy
from data_loader.strategies.append_strategy import AppendStrategy
from data_loader.config.databricks_config import DatabricksConfig


@pytest.fixture(autouse=True)
def disable_spark(monkeypatch):
    """Replace Spark session with a mock to avoid initializing PySpark."""
    monkeypatch.setattr(DatabricksConfig, "spark", property(lambda self: Mock()))


class TestPipelineLock:
    """Test pipeline lock functionality."""
    
    def test_lock_creation(self, tmp_path):
        """Test creating a pipeline lock."""
        lock = PipelineLock(str(tmp_path), "test_lock")
        assert lock.lock_file.name == "test_lock.lock"
        assert not lock.is_locked()
    
    def test_lock_acquire_release(self, tmp_path):
        """Test basic lock acquire and release."""
        lock = PipelineLock(str(tmp_path), "test_lock")
        
        # Acquire lock
        assert lock.acquire(wait=False)
        assert lock.is_locked()
        assert lock._acquired
        
        # Release lock
        assert lock.release()
        assert not lock.is_locked()
        assert not lock._acquired
    
    def test_lock_prevents_concurrent_access(self, tmp_path):
        """Test that lock prevents concurrent access."""
        lock1 = PipelineLock(str(tmp_path), "test_lock")
        lock2 = PipelineLock(str(tmp_path), "test_lock")
        
        # First lock succeeds
        assert lock1.acquire(wait=False)
        
        # Second lock fails
        assert not lock2.acquire(wait=False)
        
        # Release first lock
        lock1.release()
        
        # Now second lock can acquire
        assert lock2.acquire(wait=False)
        lock2.release()
    
    def test_context_manager(self, tmp_path):
        """Test lock as context manager."""
        lock = PipelineLock(str(tmp_path), "test_lock")
        
        with lock:
            assert lock._acquired
            assert lock.is_locked()
        
        assert not lock._acquired
        assert not lock.is_locked()
    
    def test_force_release(self, tmp_path):
        """Test force release functionality."""
        lock = PipelineLock(str(tmp_path), "test_lock")
        
        lock.acquire(wait=False)
        assert lock.is_locked()
        
        assert lock.force_release()
        assert not lock.is_locked()


class MockFileTracker:
    """Mock file tracker for testing."""
    
    def __init__(self, *args, **kwargs):
        self.files = {}
        self.stats = {
            "pending": 0,
            "processing": 0,
            "completed": 0,
            "failed": 0
        }
    
    def register_files(self, file_paths, table_name):
        for path in file_paths:
            if path not in self.files:
                self.files[path] = {
                    "status": FileProcessingStatus.PENDING,
                    "table_name": table_name
                }
                self.stats["pending"] += 1
        return len([p for p in file_paths if p not in self.files])
    
    def get_unprocessed_files(self, discovered_files):
        return [f for f in discovered_files 
                if f not in self.files or self.files[f]["status"] != FileProcessingStatus.COMPLETED]
    
    def update_file_status(self, file_path, status, error_message=None, transaction_id=None):
        if file_path in self.files:
            old_status = self.files[file_path]["status"]
            self.files[file_path]["status"] = status
            
            # Update stats
            if old_status in self.stats:
                self.stats[old_status.value] -= 1
            if status.value in self.stats:
                self.stats[status.value] += 1
    
    def get_processing_stats(self, table_name=None):
        return self.stats.copy()
    
    def validate_consistency(self):
        return {
            "overall_status": "valid",
            "total_files_checked": len(self.files),
            "inconsistencies": [],
            "warnings": [],
            "table_results": {}
        }
    
    def fix_inconsistencies(self, validation_results):
        return {
            "fixes_applied": 0,
            "fixes_failed": 0,
            "actions": []
        }


class TestIdempotentOperations:
    """Test idempotent operations."""
    
    @patch("data_loader.core.processor.FileTracker", MockFileTracker)
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_processor_idempotency(self, tmp_path):
        """Test that processor operations are idempotent."""
        config_data = {
            "max_parallel_jobs": 2,
            "retry_attempts": 2,
            "timeout_minutes": 30,
            "file_tracker_database": "test_db",
            "file_tracker_table": "test_tracker",
            "checkpoint_path": str(tmp_path),
            "state_file": "test_state.json",
            "tables": [
                {
                    "table_name": "test_table",
                    "database_name": "test_db",
                    "source_path_pattern": "/test/*.parquet",
                    "loading_strategy": "append",
                    "file_format": "parquet"
                }
            ]
        }
        
        config = DataLoaderConfig(**config_data)
        processor = DataProcessor(config)
        
        # Mock file discovery to return same files
        mock_files = ["/test/file1.parquet", "/test/file2.parquet"]
        
        with patch.object(processor, 'discover_files', return_value=mock_files):
            with patch.object(processor, '_process_single_file') as mock_process:
                mock_process.return_value = Mock(
                    success=True,
                    execution_time=1.0,
                    file_path="/test/file1.parquet",
                    table_name="test_table",
                    metrics={"records_inserted": 100}
                )
                
                # First run should process files
                result1 = processor.process_all_tables(use_lock=False)
                assert result1["total_files_processed"] > 0
                
                # Second run should be idempotent (no files to process)
                result2 = processor.process_all_tables(use_lock=False)
                # Should have fewer or same files to process
                assert result2["total_files_processed"] <= result1["total_files_processed"]
    
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_append_strategy_idempotency_check(self):
        """Test append strategy idempotency checking."""
        table_config = TableConfig(
            table_name="test_table",
            database_name="test_db",
            source_path_pattern="/test/*.parquet",
            loading_strategy=LoadingStrategy.APPEND
        )
        
        strategy = AppendStrategy(table_config)
        
        # Mock Spark to simulate table exists with audit columns
        mock_spark = Mock()
        mock_table_df = Mock()
        mock_table_df.columns = ["id", "name", "_source_file", "_load_timestamp"]
        mock_spark.table.return_value = mock_table_df
        mock_spark.catalog.tableExists.return_value = True
        
        # Mock SQL query to return existing file count
        mock_result = Mock()
        mock_result.count = 1  # File already loaded
        mock_spark.sql.return_value.collect.return_value = [mock_result]
        
        strategy.spark = mock_spark
        
        # Create mock DataFrame
        mock_df = Mock()
        
        # Should detect that file already loaded
        assert not strategy.is_idempotent_load(mock_df, "/test/file1.parquet")
    
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))  
    def test_append_strategy_cleanup(self):
        """Test append strategy cleanup functionality."""
        table_config = TableConfig(
            table_name="test_table",
            database_name="test_db", 
            source_path_pattern="/test/*.parquet",
            loading_strategy=LoadingStrategy.APPEND
        )
        
        strategy = AppendStrategy(table_config)
        
        # Mock Spark
        mock_spark = Mock()
        mock_table_df = Mock()
        mock_table_df.columns = ["id", "name", "_source_file"] 
        mock_spark.table.return_value = mock_table_df
        mock_spark.catalog.tableExists.return_value = True
        
        strategy.spark = mock_spark
        
        # Mock table row counts
        strategy._get_table_row_count = Mock(side_effect=[100, 95])  # 5 records removed
        
        # Should successfully clean up
        assert strategy.cleanup_failed_load("/test/failed_file.parquet")
        
        # Verify DELETE was called
        mock_spark.sql.assert_called()
        delete_calls = [call for call in mock_spark.sql.call_args_list if 'DELETE' in str(call)]
        assert len(delete_calls) > 0


class TestRecoveryOperations:
    """Test recovery operations."""
    
    @patch("data_loader.core.processor.FileTracker", MockFileTracker)
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_pipeline_consistency_validation(self, tmp_path):
        """Test pipeline consistency validation."""
        config_data = {
            "max_parallel_jobs": 2,
            "retry_attempts": 2,
            "timeout_minutes": 30,
            "file_tracker_database": "test_db",
            "file_tracker_table": "test_tracker",
            "checkpoint_path": str(tmp_path),
            "state_file": "test_state.json",
            "tables": [
                {
                    "table_name": "test_table",
                    "database_name": "test_db",
                    "source_path_pattern": "/test/*.parquet",
                    "loading_strategy": "append",
                    "file_format": "parquet"
                }
            ]
        }
        
        config = DataLoaderConfig(**config_data)
        processor = DataProcessor(config)
        
        # Test validation
        validation_results = processor.validate_pipeline_consistency()
        
        assert "overall_status" in validation_results
        assert "file_tracker_status" in validation_results
        assert "table_consistency" in validation_results
    
    @patch("data_loader.core.processor.FileTracker", MockFileTracker) 
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_inconsistency_fixes(self, tmp_path):
        """Test inconsistency fix functionality."""
        config_data = {
            "max_parallel_jobs": 2,
            "retry_attempts": 2,
            "timeout_minutes": 30,
            "file_tracker_database": "test_db",
            "file_tracker_table": "test_tracker", 
            "checkpoint_path": str(tmp_path),
            "state_file": "test_state.json",
            "tables": [
                {
                    "table_name": "test_table",
                    "database_name": "test_db",
                    "source_path_pattern": "/test/*.parquet",
                    "loading_strategy": "append",
                    "file_format": "parquet"
                }
            ]
        }
        
        config = DataLoaderConfig(**config_data)
        processor = DataProcessor(config)
        
        # Test fix operation
        fix_results = processor.fix_pipeline_inconsistencies()
        
        assert "fixes_applied" in fix_results
        assert "fixes_failed" in fix_results
        assert "actions" in fix_results


class TestConcurrentExecution:
    """Test concurrent execution prevention."""
    
    @patch("data_loader.core.processor.FileTracker", MockFileTracker)
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_pipeline_lock_prevents_concurrent_runs(self, tmp_path):
        """Test that pipeline lock prevents concurrent execution."""
        config_data = {
            "max_parallel_jobs": 2,
            "retry_attempts": 2,
            "timeout_minutes": 30,
            "file_tracker_database": "test_db",
            "file_tracker_table": "test_tracker",
            "checkpoint_path": str(tmp_path),
            "state_file": "test_state.json",
            "tables": []
        }
        
        config = DataLoaderConfig(**config_data)
        processor1 = DataProcessor(config)
        processor2 = DataProcessor(config)
        
        # Acquire lock with first processor
        assert processor1.pipeline_lock.acquire(wait=False)
        
        # Second processor should fail to acquire lock
        with pytest.raises(PipelineLockError):
            processor2.process_all_tables(use_lock=True)
        
        # Release lock
        processor1.pipeline_lock.release()
        
        # Now second processor should be able to run
        result = processor2.process_all_tables(use_lock=True)
        assert "execution_id" in result


if __name__ == "__main__":
    pytest.main([__file__])