"""
Base class for data loading strategies.

This module defines the interface that all loading strategies must implement.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from loguru import logger

from ..config.table_config import TableConfig
from ..config.databricks_config import databricks_config


class BaseLoadingStrategy(ABC):
    """
    Abstract base class for data loading strategies.
    
    All loading strategies must inherit from this class and implement
    the required methods.
    """
    
    def __init__(self, table_config: TableConfig):
        """
        Initialize the loading strategy.
        
        Args:
            table_config: Configuration for the target table
        """
        self.table_config = table_config
        self.spark = databricks_config.spark
        self.full_table_name = f"{table_config.database_name}.{table_config.table_name}"
    
    @abstractmethod
    def load_data(self, source_df: DataFrame, file_path: str) -> Dict[str, Any]:
        """
        Load data using the specific strategy.
        
        Args:
            source_df: Source DataFrame to load
            file_path: Path of the source file
            
        Returns:
            Dictionary with loading results and metrics
        """
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """
        Validate that the table configuration is compatible with this strategy.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        pass
    
    def is_idempotent_load(self, source_df: DataFrame, file_path: str) -> bool:
        """
        Check if loading this data would be idempotent (safe to retry).
        
        Args:
            source_df: Source DataFrame to load
            file_path: Path of the source file
            
        Returns:
            True if load operation is idempotent, False otherwise
        """
        # Default implementation - subclasses should override for strategy-specific logic
        return True
    
    def cleanup_failed_load(self, file_path: str) -> bool:
        """
        Clean up any partial data from a failed load operation.
        
        Args:
            file_path: Path of the file that failed to load
            
        Returns:
            True if cleanup successful, False otherwise
        """
        # Default implementation - subclasses should override if needed
        logger.debug(f"No cleanup required for failed load: {file_path}")
        return True
    
    def prepare_target_table(self):
        """
        Prepare the target table (create if doesn't exist, ensure schema, etc.).
        
        This method should be called before loading data.
        """
        # Ensure database exists
        databricks_config.create_database_if_not_exists(self.table_config.database_name)
        
        # Strategy-specific table preparation will be implemented in subclasses
        logger.info(f"Prepared target table: {self.full_table_name}")
    
    def read_source_data(self, file_path: str) -> DataFrame:
        """
        Read source data from file.
        
        Args:
            file_path: Path to the source file
            
        Returns:
            DataFrame containing the source data
        """
        reader = self.spark.read
        
        # Configure reader based on file format
        if self.table_config.file_format.lower() == "parquet":
            reader = reader.format("parquet")
        elif self.table_config.file_format.lower() == "csv":
            reader = reader.format("csv").option("header", "true").option("inferSchema", "true")
        elif self.table_config.file_format.lower() == "json":
            reader = reader.format("json")
        elif self.table_config.file_format.lower() == "delta":
            reader = reader.format("delta")
        else:
            raise ValueError(f"Unsupported file format: {self.table_config.file_format}")
        
        # Enable schema evolution if configured
        if self.table_config.schema_evolution:
            reader = reader.option("mergeSchema", "true")
        
        df = reader.load(file_path)
        logger.info(f"Read {df.count()} rows from {file_path}")
        
        return df
    
    def apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply custom transformations to the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        if not self.table_config.transformations:
            return df
        
        # Apply transformations based on configuration
        # This is a simplified implementation - in practice, you might want
        # to support more complex transformation definitions
        for transformation_name, transformation_config in self.table_config.transformations.items():
            if transformation_name == "add_columns":
                for col_name, col_value in transformation_config.items():
                    df = df.withColumn(col_name, col_value)
            elif transformation_name == "filter":
                df = df.filter(transformation_config)
            elif transformation_name == "select":
                df = df.select(*transformation_config)
        
        logger.info(f"Applied transformations: {list(self.table_config.transformations.keys())}")
        return df
    
    def get_table_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the target table.
        
        Returns:
            Dictionary containing table statistics
        """
        try:
            if not databricks_config.table_exists(
                self.table_config.database_name, 
                self.table_config.table_name
            ):
                return {"exists": False}
            
            # Get row count
            row_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.full_table_name}").collect()[0].count
            
            # Get table size (approximate)
            describe_result = self.spark.sql(f"DESCRIBE EXTENDED {self.full_table_name}").collect()
            
            stats = {
                "exists": True,
                "row_count": row_count,
                "table_name": self.full_table_name
            }
            
            # Extract additional properties from DESCRIBE EXTENDED
            for row in describe_result:
                if row.col_name == "Statistics":
                    stats["size_info"] = row.data_type
                elif row.col_name == "Location":
                    stats["location"] = row.data_type
            
            return stats
            
        except Exception as e:
            logger.warning(f"Could not get statistics for table {self.full_table_name}: {e}")
            return {"exists": False, "error": str(e)}
    
    def optimize_table(self):
        """Run optimization on the target table."""
        try:
            databricks_config.optimize_table(
                self.table_config.database_name,
                self.table_config.table_name,
                self.table_config.partition_columns
            )
        except Exception as e:
            logger.warning(f"Could not optimize table {self.full_table_name}: {e}")
    
    def vacuum_table(self, retention_hours: int = 168):
        """
        Run VACUUM on the target table.
        
        Args:
            retention_hours: Retention period in hours (default 7 days)
        """
        try:
            databricks_config.vacuum_table(
                self.table_config.database_name,
                self.table_config.table_name,
                retention_hours
            )
        except Exception as e:
            logger.warning(f"Could not vacuum table {self.full_table_name}: {e}")
    
    def __str__(self) -> str:
        """String representation of the strategy."""
        return f"{self.__class__.__name__}(table={self.full_table_name})"