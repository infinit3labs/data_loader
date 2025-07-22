"""
Table configuration management for data loader.

Defines table schemas, loading strategies, and processing rules.
"""

from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - optional dependency
    yaml = None


class LoadingStrategy(str, Enum):
    """Supported loading strategies."""
    SCD2 = "scd2"
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"


class TableConfig(BaseModel):
    """Configuration for a single table."""
    
    table_name: str = Field(..., description="Name of the target table")
    database_name: str = Field(..., description="Target database/schema name")
    source_path_pattern: str = Field(..., description="File path pattern to match source files")
    loading_strategy: LoadingStrategy = Field(..., description="Loading strategy to use")
    
    # SCD2 specific configurations
    primary_keys: Optional[List[str]] = Field(None, description="Primary key columns for SCD2")
    tracking_columns: Optional[List[str]] = Field(None, description="Columns to track for changes in SCD2")
    scd2_effective_date_column: Optional[str] = Field("effective_date", description="Effective date column for SCD2")
    scd2_end_date_column: Optional[str] = Field("end_date", description="End date column for SCD2")
    scd2_current_flag_column: Optional[str] = Field("is_current", description="Current flag column for SCD2")
    
    # File processing options
    file_format: str = Field("parquet", description="Source file format (parquet, csv, json, etc.)")
    schema_evolution: bool = Field(True, description="Allow schema evolution")
    partition_columns: Optional[List[str]] = Field(None, description="Partition columns for target table")
    
    # Processing options
    parallel_processing: bool = Field(True, description="Enable parallel processing")
    batch_size: int = Field(1000000, description="Batch size for processing")
    
    # Custom transformations
    transformations: Optional[Dict[str, Any]] = Field(None, description="Custom transformation rules")
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True


class DataLoaderConfig(BaseModel):
    """Main configuration for the data loader."""
    
    # Databricks settings
    raw_data_path: str = Field(..., description="Path to raw data location")
    processed_data_path: str = Field(..., description="Path to processed data location")
    checkpoint_path: str = Field(..., description="Path for checkpoints and metadata")
    
    # File tracking settings
    file_tracker_table: str = Field("file_processing_tracker", description="Table name for file tracking")
    file_tracker_database: str = Field("metadata", description="Database for file tracking table")
    
    # Processing settings
    max_parallel_jobs: int = Field(4, description="Maximum number of parallel jobs")
    retry_attempts: int = Field(3, description="Number of retry attempts for failed files")
    timeout_minutes: int = Field(60, description="Timeout for processing a single file")
    
    # Monitoring and logging
    log_level: str = Field("INFO", description="Logging level")
    enable_metrics: bool = Field(True, description="Enable metrics collection")
    
    # Table configurations
    tables: List[TableConfig] = Field(..., description="List of table configurations")
    
    def get_table_config(self, table_name: str) -> Optional[TableConfig]:
        """Get configuration for a specific table."""
        for table in self.tables:
            if table.table_name == table_name:
                return table
        return None
    
    def get_tables_by_strategy(self, strategy: LoadingStrategy) -> List[TableConfig]:
        """Get all tables using a specific loading strategy."""
        return [table for table in self.tables if table.loading_strategy == strategy]


# Environment settings that allow overriding top-level options via env vars
class DataLoaderEnvSettings(BaseSettings):
    """Configuration loaded from environment variables."""

    raw_data_path: Optional[str] = None
    processed_data_path: Optional[str] = None
    checkpoint_path: Optional[str] = None
    file_tracker_table: Optional[str] = None
    file_tracker_database: Optional[str] = None
    max_parallel_jobs: Optional[int] = None
    retry_attempts: Optional[int] = None
    timeout_minutes: Optional[int] = None
    log_level: Optional[str] = None
    enable_metrics: Optional[bool] = None

    model_config = SettingsConfigDict(
        env_prefix="DATALOADER_",
        env_nested_delimiter="__",
        extra="ignore",
    )


# Example configuration
EXAMPLE_CONFIG = {
    "raw_data_path": "/mnt/raw/",
    "processed_data_path": "/mnt/processed/",
    "checkpoint_path": "/mnt/checkpoints/",
    "file_tracker_table": "file_processing_tracker",
    "file_tracker_database": "metadata",
    "max_parallel_jobs": 4,
    "retry_attempts": 3,
    "timeout_minutes": 60,
    "log_level": "INFO",
    "enable_metrics": True,
    "tables": [
        {
            "table_name": "customers",
            "database_name": "analytics",
            "source_path_pattern": "/mnt/raw/customers/*.parquet",
            "loading_strategy": "scd2",
            "primary_keys": ["customer_id"],
            "tracking_columns": ["name", "email", "address"],
            "file_format": "parquet",
            "schema_evolution": True,
            "partition_columns": ["date_partition"]
        },
        {
            "table_name": "transactions",
            "database_name": "analytics", 
            "source_path_pattern": "/mnt/raw/transactions/*.parquet",
            "loading_strategy": "append",
            "file_format": "parquet",
            "schema_evolution": True,
            "partition_columns": ["transaction_date"]
        }
    ]
}


def load_config_from_file(path: str) -> DataLoaderConfig:
    """Load a :class:`DataLoaderConfig` from a YAML file."""
    file_path = Path(path)
    if file_path.suffix.lower() not in {".yml", ".yaml"}:
        raise ValueError("Configuration files must be YAML format (.yml or .yaml extension)")

    if yaml is None:  # pragma: no cover - dependency optional
        raise ImportError("PyYAML is required to load YAML configuration files. Install it using: pip install pyyaml")

    with open(file_path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    return DataLoaderConfig(**data)


def load_runtime_config(
    config_file: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> DataLoaderConfig:
    """Load configuration from YAML, environment variables, and overrides."""

    base_data: Dict[str, Any] = {}

    if config_file:
        base_cfg = load_config_from_file(config_file)
        base_data.update(base_cfg.model_dump())

    env_data = DataLoaderEnvSettings().model_dump(exclude_none=True)
    base_data.update(env_data)

    if overrides:
        for key, value in overrides.items():
            if key in DataLoaderConfig.model_fields and value is not None:
                base_data[key] = value

    return DataLoaderConfig(**base_data)