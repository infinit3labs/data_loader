"""
Basic tests for the data loader functionality.
"""

import pytest
from unittest.mock import Mock, patch
from data_loader.config.table_config import (
    DataLoaderConfig, TableConfig, LoadingStrategy, EXAMPLE_CONFIG
)
from data_loader.strategies.base_strategy import BaseLoadingStrategy
from data_loader.strategies.append_strategy import AppendStrategy
from data_loader.strategies.scd2_strategy import SCD2Strategy
from data_loader.config.databricks_config import databricks_config


class TestTableConfig:
    """Test table configuration functionality."""
    
    def test_loading_strategy_enum(self):
        """Test loading strategy enumeration."""
        assert LoadingStrategy.SCD2 == "scd2"
        assert LoadingStrategy.APPEND == "append"
        assert LoadingStrategy.OVERWRITE == "overwrite"
        assert LoadingStrategy.MERGE == "merge"
    
    def test_table_config_creation(self):
        """Test table configuration creation."""
        config = TableConfig(
            table_name="test_table",
            database_name="test_db",
            source_path_pattern="/data/test/*.parquet",
            loading_strategy=LoadingStrategy.APPEND
        )
        
        assert config.table_name == "test_table"
        assert config.database_name == "test_db"
        assert config.loading_strategy == LoadingStrategy.APPEND
        assert config.file_format == "parquet"  # default value
    
    def test_data_loader_config_creation(self):
        """Test data loader configuration creation."""
        config = DataLoaderConfig(**EXAMPLE_CONFIG)
        
        assert len(config.tables) == 2
        assert config.max_parallel_jobs == 4
        assert config.retry_attempts == 3
    
    def test_get_table_config(self):
        """Test getting specific table configuration."""
        config = DataLoaderConfig(**EXAMPLE_CONFIG)
        
        customers_config = config.get_table_config("customers")
        assert customers_config is not None
        assert customers_config.loading_strategy == LoadingStrategy.SCD2
        
        nonexistent_config = config.get_table_config("nonexistent")
        assert nonexistent_config is None
    
    def test_get_tables_by_strategy(self):
        """Test filtering tables by strategy."""
        config = DataLoaderConfig(**EXAMPLE_CONFIG)
        
        scd2_tables = config.get_tables_by_strategy(LoadingStrategy.SCD2)
        assert len(scd2_tables) == 1
        assert scd2_tables[0].table_name == "customers"
        
        append_tables = config.get_tables_by_strategy(LoadingStrategy.APPEND)
        assert len(append_tables) == 1
        assert append_tables[0].table_name == "transactions"


class TestAppendStrategy:
    """Test append loading strategy."""
    
    @patch.object(databricks_config, "_create_spark_session", return_value=Mock())
    def test_strategy_creation(self, _mock_spark):
        """Test append strategy creation."""
        table_config = TableConfig(
            table_name="test_table",
            database_name="test_db",
            source_path_pattern="/data/test/*.parquet",
            loading_strategy=LoadingStrategy.APPEND
        )
        
        strategy = AppendStrategy(table_config)
        assert strategy.table_config == table_config
        assert strategy.full_table_name == "test_db.test_table"
    
    @patch.object(databricks_config, "_create_spark_session", return_value=Mock())
    def test_config_validation(self, _mock_spark):
        """Test configuration validation."""
        table_config = TableConfig(
            table_name="test_table",
            database_name="test_db",
            source_path_pattern="/data/test/*.parquet",
            loading_strategy=LoadingStrategy.APPEND
        )
        
        strategy = AppendStrategy(table_config)
        assert strategy.validate_config() is True
        
        # Test with missing table name
        invalid_config = TableConfig(
            table_name="",
            database_name="test_db",
            source_path_pattern="/data/test/*.parquet",
            loading_strategy=LoadingStrategy.APPEND
        )
        
        invalid_strategy = AppendStrategy(invalid_config)
        assert invalid_strategy.validate_config() is False


class TestSCD2Strategy:
    """Test SCD2 loading strategy."""
    
    @patch.object(databricks_config, "_create_spark_session", return_value=Mock())
    def test_strategy_creation(self, _mock_spark):
        """Test SCD2 strategy creation with valid configuration."""
        table_config = TableConfig(
            table_name="test_table",
            database_name="test_db",
            source_path_pattern="/data/test/*.parquet",
            loading_strategy=LoadingStrategy.SCD2,
            primary_keys=["id"],
            tracking_columns=["name", "value"]
        )
        
        strategy = SCD2Strategy(table_config)
        assert strategy.table_config == table_config
        assert strategy.full_table_name == "test_db.test_table"
    
    @patch.object(databricks_config, "_create_spark_session", return_value=Mock())
    def test_config_validation_success(self, _mock_spark):
        """Test successful SCD2 configuration validation."""
        table_config = TableConfig(
            table_name="test_table",
            database_name="test_db",
            source_path_pattern="/data/test/*.parquet",
            loading_strategy=LoadingStrategy.SCD2,
            primary_keys=["id"],
            tracking_columns=["name", "value"],
            scd2_effective_date_column="effective_date",
            scd2_end_date_column="end_date",
            scd2_current_flag_column="is_current"
        )
        
        strategy = SCD2Strategy(table_config)
        assert strategy.validate_config() is True
    
    @patch.object(databricks_config, "_create_spark_session", return_value=Mock())
    def test_config_validation_failure(self, _mock_spark):
        """Test SCD2 configuration validation failures."""
        # Missing primary keys
        with pytest.raises(ValueError):
            table_config = TableConfig(
                table_name="test_table",
                database_name="test_db",
                source_path_pattern="/data/test/*.parquet",
                loading_strategy=LoadingStrategy.SCD2,
                tracking_columns=["name", "value"]
            )
            SCD2Strategy(table_config)
        
        # Missing tracking columns
        with pytest.raises(ValueError):
            table_config = TableConfig(
                table_name="test_table",
                database_name="test_db",
                source_path_pattern="/data/test/*.parquet",
                loading_strategy=LoadingStrategy.SCD2,
                primary_keys=["id"]
            )
            SCD2Strategy(table_config)


class TestDataQualityHelpers:
    """Test data quality helper functions."""
    
    def test_validate_file_path(self):
        """Test file path validation."""
        from data_loader.utils.helpers import validate_file_path
        
        # Test with this test file (should exist)
        assert validate_file_path(__file__) is True
        
        # Test with non-existent file
        assert validate_file_path("/nonexistent/file.txt") is False
    
    def test_format_duration(self):
        """Test duration formatting."""
        from data_loader.utils.helpers import format_duration
        
        assert format_duration(0.5) == "500ms"
        assert format_duration(1.5) == "1.5s"
        assert format_duration(90) == "1.5m"
        assert format_duration(3600) == "1.0h"
    
    def test_safe_divide(self):
        """Test safe division."""
        from data_loader.utils.helpers import safe_divide
        
        assert safe_divide(10, 2) == 5.0
        assert safe_divide(10, 0) == 0.0
        assert safe_divide(0, 5) == 0.0
    
    def test_calculate_success_rate(self):
        """Test success rate calculation."""
        from data_loader.utils.helpers import calculate_success_rate
        
        assert calculate_success_rate(8, 10) == 80.0
        assert calculate_success_rate(0, 10) == 0.0
        assert calculate_success_rate(10, 0) == 0.0
    
    def test_create_batch_id(self):
        """Test batch ID creation."""
        from data_loader.utils.helpers import create_batch_id
        from datetime import datetime
        
        test_time = datetime(2024, 1, 15, 10, 30, 45)
        batch_id = create_batch_id("/data/test_file.parquet", test_time)
        
        assert "test_file" in batch_id
        assert "20240115_103045" in batch_id
    
    def test_flatten_dict(self):
        """Test dictionary flattening."""
        from data_loader.utils.helpers import flatten_dict
        
        nested_dict = {
            "level1": {
                "level2": {
                    "value": 42
                },
                "other": "test"
            },
            "simple": "value"
        }
        
        flattened = flatten_dict(nested_dict)
        
        assert "level1.level2.value" in flattened
        assert flattened["level1.level2.value"] == 42
        assert flattened["level1.other"] == "test"
        assert flattened["simple"] == "value"


if __name__ == "__main__":
    pytest.main([__file__])