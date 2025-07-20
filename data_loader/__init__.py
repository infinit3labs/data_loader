"""
Databricks Data Loader

A comprehensive data loading module for Databricks that provides:
- File monitoring and processing status tracking
- Parallel processing capabilities
- Multiple loading strategies (SCD2, Append, etc.)
- Robust error handling and logging
"""

__version__ = "0.1.0"
__author__ = "Infinit3Labs"

# Import only non-Spark dependent modules by default
# Spark-dependent modules should be imported on-demand
from .config.table_config import (
    DataLoaderConfig,
    TableConfig,
    LoadingStrategy,
    EXAMPLE_CONFIG,
    load_config_from_file,
)

__all__ = [
    "DataLoaderConfig",
    "TableConfig", 
    "LoadingStrategy",
    "EXAMPLE_CONFIG",
    "load_config_from_file",
]

# Provide easy access to main components when PySpark is available
def get_data_processor(config):
    """Get DataProcessor instance (requires PySpark)."""
    from .core.processor import DataProcessor
    return DataProcessor(config)

def get_file_tracker(database_name, table_name):
    """Get FileTracker instance (requires PySpark)."""
    from .core.file_tracker import FileTracker
    return FileTracker(database_name, table_name)

def get_scd2_strategy(table_config):
    """Get SCD2Strategy instance (requires PySpark)."""
    from .strategies.scd2_strategy import SCD2Strategy
    return SCD2Strategy(table_config)

def get_append_strategy(table_config):
    """Get AppendStrategy instance (requires PySpark)."""
    from .strategies.append_strategy import AppendStrategy
    return AppendStrategy(table_config)
