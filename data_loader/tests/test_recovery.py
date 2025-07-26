"""
Comprehensive recovery and failure simulation tests.

This module contains tests that simulate various failure scenarios
and validate that the pipeline can recover correctly.
"""

import pytest
import tempfile
import time
import threading
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from data_loader.core.pipeline_lock import PipelineLock, PipelineLockError
from data_loader.core.file_tracker import FileTracker, FileProcessingStatus
from data_loader.core.processor import DataProcessor
from data_loader.config.table_config import DataLoaderConfig, TableConfig, LoadingStrategy
from data_loader.strategies.append_strategy import AppendStrategy
from data_loader.strategies.scd2_strategy import SCD2Strategy
from data_loader.config.databricks_config import DatabricksConfig


@pytest.fixture(autouse=True)
def disable_spark(monkeypatch):
    """Replace Spark session with a mock to avoid initializing PySpark."""
    monkeypatch.setattr(DatabricksConfig, "spark", property(lambda self: Mock()))


class MockFileTrackerWithFailures:
    """Mock file tracker that can simulate various failure scenarios."""
    
    def __init__(self, *args, **kwargs):
        self.files = {}
        self.stats = {
            "pending": 0,
            "processing": 0, 
            "completed": 0,
            "failed": 0
        }
        self.fail_on_update = False
        self.orphaned_processing_files = []
    
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
        if self.fail_on_update:
            raise Exception("Simulated file tracker failure")
        
        if file_path in self.files:
            old_status = self.files[file_path]["status"]
            self.files[file_path]["status"] = status
            
            # Update stats
            if old_status.value in self.stats:
                self.stats[old_status.value] -= 1
            if status.value in self.stats:
                self.stats[status.value] += 1
    
    def get_processing_stats(self, table_name=None):
        return self.stats.copy()
    
    def validate_consistency(self):
        inconsistencies = []
        
        # Simulate orphaned processing records
        for file_path in self.orphaned_processing_files:
            inconsistencies.append(f"Orphaned processing record: {file_path}")
        
        return {
            "overall_status": "inconsistent" if inconsistencies else "valid",
            "total_files_checked": len(self.files),
            "inconsistencies": inconsistencies,
            "warnings": [],
            "table_results": {}
        }
    
    def fix_inconsistencies(self, validation_results):
        fixes_applied = 0
        actions = []
        
        # Simulate fixing orphaned records
        for file_path in self.orphaned_processing_files:
            if file_path in self.files:
                self.files[file_path]["status"] = FileProcessingStatus.FAILED
                fixes_applied += 1
                actions.append(f"Fixed orphaned processing record: {file_path}")
        
        self.orphaned_processing_files.clear()
        
        return {
            "fixes_applied": fixes_applied,
            "fixes_failed": 0,
            "actions": actions
        }
    
    def add_orphaned_processing_file(self, file_path):
        """Simulate an orphaned processing file."""
        self.orphaned_processing_files.append(file_path)
        if file_path in self.files:
            self.files[file_path]["status"] = FileProcessingStatus.PROCESSING


class TestRecoveryScenarios:
    """Test various recovery scenarios."""
    
    @patch("data_loader.core.processor.FileTracker", MockFileTrackerWithFailures)
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_recovery_from_file_tracker_failure(self, tmp_path):
        """Test recovery when file tracker fails during processing."""
        config_data = {
            "max_parallel_jobs": 1,
            "retry_attempts": 2,
            "timeout_minutes": 5,
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
        
        # Make file tracker fail on status updates
        processor.file_tracker.fail_on_update = True
        
        mock_files = ["/test/file1.parquet"]
        
        with patch.object(processor, 'discover_files', return_value=mock_files):
            # This should fail due to file tracker failure
            result = processor.process_all_tables(use_lock=False)
            
            # Should have errors but not crash
            assert "errors" in result
            assert len(result["errors"]) > 0
    
    @patch("data_loader.core.processor.FileTracker", MockFileTrackerWithFailures)
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_recovery_from_orphaned_processing_records(self, tmp_path):
        """Test recovery from orphaned processing records."""
        config_data = {
            "max_parallel_jobs": 1,
            "retry_attempts": 2,
            "timeout_minutes": 5,
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
        
        # Simulate orphaned processing records
        processor.file_tracker.add_orphaned_processing_file("/test/orphaned.parquet")
        
        # Validate consistency - should detect orphaned record
        validation_results = processor.validate_pipeline_consistency()
        assert validation_results["overall_status"] == "inconsistent"
        assert len(validation_results["file_tracker_status"]["inconsistencies"]) > 0
        
        # Fix inconsistencies
        fix_results = processor.fix_pipeline_inconsistencies(validation_results)
        assert fix_results["fixes_applied"] > 0
        
        # Validate again - should be clean
        post_fix_validation = processor.validate_pipeline_consistency()
        assert post_fix_validation["overall_status"] == "valid"
    
    def test_concurrent_execution_prevention(self, tmp_path):
        """Test that concurrent executions are properly prevented."""
        config_data = {
            "max_parallel_jobs": 1,
            "retry_attempts": 1,
            "timeout_minutes": 5,
            "file_tracker_database": "test_db",
            "file_tracker_table": "test_tracker",
            "checkpoint_path": str(tmp_path),
            "state_file": "test_state.json",
            "tables": []
        }
        
        config = DataLoaderConfig(**config_data)
        
        # Create two processors sharing the same lock directory
        processor1 = DataProcessor(config)
        processor2 = DataProcessor(config)
        
        # Start first processor in a thread
        def run_processor1():
            with processor1.pipeline_lock:
                time.sleep(1)  # Hold lock for a bit
                return "processor1_done"
        
        thread1 = threading.Thread(target=run_processor1)
        thread1.start()
        
        # Give thread1 time to acquire lock
        time.sleep(0.1)
        
        # Second processor should not be able to acquire lock
        assert not processor2.pipeline_lock.acquire(wait=False)
        
        # Wait for first processor to finish
        thread1.join()
        
        # Now second processor should be able to acquire lock
        assert processor2.pipeline_lock.acquire(wait=False)
        processor2.pipeline_lock.release()
    
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_strategy_idempotency_and_cleanup(self):
        """Test strategy-level idempotency and cleanup."""
        
        # Test Append Strategy
        table_config = TableConfig(
            table_name="test_table",
            database_name="test_db",
            source_path_pattern="/test/*.parquet",
            loading_strategy=LoadingStrategy.APPEND
        )
        
        append_strategy = AppendStrategy(table_config)
        
        # Mock Spark for append strategy
        mock_spark = Mock()
        mock_spark.catalog.tableExists.return_value = False
        append_strategy.spark = mock_spark
        
        # New table should be idempotent
        mock_df = Mock()
        assert append_strategy.is_idempotent_load(mock_df, "/test/new_file.parquet")
        
        # Cleanup should work even with no table
        assert append_strategy.cleanup_failed_load("/test/failed_file.parquet")
        
        # Test SCD2 Strategy
        scd2_table_config = TableConfig(
            table_name="scd2_table",
            database_name="test_db",
            source_path_pattern="/test/*.parquet",
            loading_strategy=LoadingStrategy.SCD2,
            primary_keys=["id"],
            tracking_columns=["name", "value"],
            scd2_effective_date_column="effective_date",
            scd2_end_date_column="end_date",
            scd2_current_flag_column="is_current"
        )
        
        scd2_strategy = SCD2Strategy(scd2_table_config)
        
        # Mock Spark for SCD2 strategy
        scd2_strategy.spark = mock_spark
        
        # New table should be idempotent
        assert scd2_strategy.is_idempotent_load(mock_df, "/test/new_scd2_file.parquet")
        
        # Cleanup should work even with no table
        assert scd2_strategy.cleanup_failed_load("/test/failed_scd2_file.parquet")


class TestFailureSimulation:
    """Test various failure simulation scenarios."""
    
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_strategy_failure_and_recovery(self):
        """Test strategy failure and recovery mechanisms."""
        table_config = TableConfig(
            table_name="test_table",
            database_name="test_db",
            source_path_pattern="/test/*.parquet",
            loading_strategy=LoadingStrategy.APPEND
        )
        
        strategy = AppendStrategy(table_config)
        
        # Mock Spark to simulate failures
        mock_spark = Mock()
        mock_spark.catalog.tableExists.return_value = True
        
        # Simulate table with audit columns
        mock_table_df = Mock()
        mock_table_df.columns = ["id", "name", "_source_file"]
        mock_spark.table.return_value = mock_table_df
        
        # Simulate that file was already loaded
        mock_result = Mock()
        mock_result.count = 1
        mock_spark.sql.return_value.collect.return_value = [mock_result]
        
        strategy.spark = mock_spark
        
        # Create mock DataFrame
        mock_df = Mock()
        
        # Should detect duplicate load
        assert not strategy.is_idempotent_load(mock_df, "/test/duplicate_file.parquet")
        
        # Cleanup should attempt to remove duplicates
        strategy._get_table_row_count = Mock(side_effect=[100, 95])  # Simulate removal
        assert strategy.cleanup_failed_load("/test/duplicate_file.parquet")
    
    def test_lock_timeout_and_stale_detection(self, tmp_path):
        """Test lock timeout and stale lock detection."""
        # Create a lock with very short timeout
        lock = PipelineLock(
            str(tmp_path), 
            "test_lock", 
            timeout_minutes=0.01  # 0.6 seconds
        )
        
        # Acquire lock
        assert lock.acquire(wait=False)
        
        # Wait for timeout
        time.sleep(1)
        
        # Create second lock instance
        lock2 = PipelineLock(str(tmp_path), "test_lock")
        
        # Should detect stale lock and be able to acquire
        assert lock2.acquire(wait=False)
        lock2.release()
    
    @patch("data_loader.core.processor.FileTracker", MockFileTrackerWithFailures)
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_full_recovery_workflow(self, tmp_path):
        """Test complete recovery workflow from detection to fix."""
        config_data = {
            "max_parallel_jobs": 2,
            "retry_attempts": 2,
            "timeout_minutes": 5,
            "file_tracker_database": "test_db",
            "file_tracker_table": "test_tracker",
            "checkpoint_path": str(tmp_path),
            "state_file": "test_state.json",
            "tables": [
                {
                    "table_name": "customers",
                    "database_name": "test_db",
                    "source_path_pattern": "/test/customers/*.parquet",
                    "loading_strategy": "scd2",
                    "file_format": "parquet",
                    "primary_keys": ["customer_id"],
                    "tracking_columns": ["name", "email"],
                    "scd2_effective_date_column": "effective_date",
                    "scd2_end_date_column": "end_date", 
                    "scd2_current_flag_column": "is_current"
                },
                {
                    "table_name": "transactions",
                    "database_name": "test_db",
                    "source_path_pattern": "/test/transactions/*.parquet",
                    "loading_strategy": "append",
                    "file_format": "parquet"
                }
            ]
        }
        
        config = DataLoaderConfig(**config_data)
        processor = DataProcessor(config)
        
        # Simulate various inconsistencies
        processor.file_tracker.add_orphaned_processing_file("/test/customers/orphaned1.parquet")
        processor.file_tracker.add_orphaned_processing_file("/test/transactions/orphaned2.parquet")
        
        # Mark a table as completed but with failures
        processor.state_manager.update_table_status("customers", processor.state_manager.PipelineStageStatus.COMPLETED)
        processor.file_tracker.files["/test/customers/failed.parquet"] = {
            "status": FileProcessingStatus.FAILED,
            "table_name": "customers"
        }
        processor.file_tracker.stats["failed"] = 1
        
        # Step 1: Detect inconsistencies
        validation_results = processor.validate_pipeline_consistency()
        assert validation_results["overall_status"] == "inconsistent"
        
        # Should detect file tracker issues
        assert len(validation_results["file_tracker_status"]["inconsistencies"]) > 0
        
        # Should detect table-level inconsistencies
        assert "customers" in validation_results["table_consistency"]
        assert not validation_results["table_consistency"]["customers"]["consistent"]
        
        # Step 2: Fix inconsistencies
        fix_results = processor.fix_pipeline_inconsistencies(validation_results)
        assert fix_results["fixes_applied"] > 0
        
        # Step 3: Validate fixes
        post_fix_validation = processor.validate_pipeline_consistency()
        assert post_fix_validation["overall_status"] == "valid"
        
        # Should have no more inconsistencies
        assert len(post_fix_validation["file_tracker_status"]["inconsistencies"]) == 0


class TestEdgeCases:
    """Test edge cases and corner scenarios."""
    
    def test_lock_on_nonexistent_directory(self):
        """Test lock creation when directory doesn't exist."""
        lock = PipelineLock("/tmp/nonexistent/path", "test_lock")
        
        # Should create directory and work
        assert lock.acquire(wait=False)
        assert lock.is_locked()
        lock.release()
    
    def test_lock_file_corruption(self, tmp_path):
        """Test handling of corrupted lock files."""
        lock_file = tmp_path / "corrupted.lock"
        
        # Create corrupted lock file
        with open(lock_file, 'w') as f:
            f.write("invalid json content")
        
        lock = PipelineLock(str(tmp_path), "corrupted")
        
        # Should handle corruption gracefully
        assert lock.acquire(wait=False)
        lock.release()
    
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_strategy_with_missing_config(self):
        """Test strategy behavior with missing configuration."""
        # Test append strategy with minimal config
        minimal_config = TableConfig(
            table_name="",  # Invalid
            database_name="test_db",
            source_path_pattern="/test/*.parquet",
            loading_strategy=LoadingStrategy.APPEND
        )
        
        strategy = AppendStrategy(minimal_config)
        assert not strategy.validate_config()  # Should fail validation
    
    @patch("data_loader.core.processor.FileTracker", MockFileTrackerWithFailures)
    @patch.object(DatabricksConfig, "spark", property(lambda self: Mock()))
    def test_processor_with_no_tables(self, tmp_path):
        """Test processor behavior with no tables configured."""
        config_data = {
            "max_parallel_jobs": 1,
            "retry_attempts": 1,
            "timeout_minutes": 5,
            "file_tracker_database": "test_db",
            "file_tracker_table": "test_tracker",
            "checkpoint_path": str(tmp_path),
            "state_file": "test_state.json",
            "tables": []  # No tables
        }
        
        config = DataLoaderConfig(**config_data)
        processor = DataProcessor(config)
        
        # Should complete successfully with no work
        result = processor.process_all_tables(use_lock=False)
        assert result["tables_processed"] == 0
        assert result["total_files_processed"] == 0


if __name__ == "__main__":
    pytest.main([__file__])