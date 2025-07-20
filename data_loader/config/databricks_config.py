"""
Databricks-specific configuration and utilities.
"""

import os
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from loguru import logger


class DatabricksConfig:
    """Databricks-specific configuration and session management."""
    
    def __init__(self, app_name: str = "DataLoader"):
        self.app_name = app_name
        self._spark: Optional[SparkSession] = None
        
    @property
    def spark(self) -> SparkSession:
        """Get or create Spark session."""
        if self._spark is None:
            self._spark = self._create_spark_session()
        return self._spark
    
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for Databricks."""
        builder = SparkSession.builder.appName(self.app_name)
        
        # Databricks-specific optimizations
        spark_configs = {
            # Delta Lake optimizations
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            
            # Performance optimizations
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            
            # Memory optimizations
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            
            # Delta optimizations
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
        }
        
        # Apply configurations
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
            
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Created Spark session with app name: {self.app_name}")
        return spark
    
    def get_database_location(self, database_name: str) -> str:
        """Get the location path for a database."""
        return f"/mnt/processed/{database_name}"
    
    def create_database_if_not_exists(self, database_name: str, location: Optional[str] = None):
        """Create database if it doesn't exist."""
        if location is None:
            location = self.get_database_location(database_name)
            
        sql = f"""
        CREATE DATABASE IF NOT EXISTS {database_name}
        LOCATION '{location}'
        """
        
        self.spark.sql(sql)
        logger.info(f"Ensured database exists: {database_name} at {location}")
    
    def optimize_table(self, database_name: str, table_name: str, 
                      partition_columns: Optional[list] = None):
        """Run OPTIMIZE on a Delta table."""
        full_table_name = f"{database_name}.{table_name}"
        
        if partition_columns:
            partition_clause = " AND ".join([f"{col} IS NOT NULL" for col in partition_columns])
            sql = f"OPTIMIZE {full_table_name} WHERE {partition_clause}"
        else:
            sql = f"OPTIMIZE {full_table_name}"
            
        self.spark.sql(sql)
        logger.info(f"Optimized table: {full_table_name}")
    
    def vacuum_table(self, database_name: str, table_name: str, retention_hours: int = 168):
        """Run VACUUM on a Delta table (default 7 days retention)."""
        full_table_name = f"{database_name}.{table_name}"
        sql = f"VACUUM {full_table_name} RETAIN {retention_hours} HOURS"
        
        self.spark.sql(sql)
        logger.info(f"Vacuumed table: {full_table_name} with {retention_hours}h retention")
    
    def get_table_properties(self, database_name: str, table_name: str) -> Dict[str, Any]:
        """Get table properties and metadata."""
        full_table_name = f"{database_name}.{table_name}"
        
        try:
            # Get table details
            describe_result = self.spark.sql(f"DESCRIBE EXTENDED {full_table_name}")
            properties = {}
            
            for row in describe_result.collect():
                if row.col_name and row.data_type:
                    properties[row.col_name] = row.data_type
                    
            return properties
        except Exception as e:
            logger.warning(f"Could not get properties for table {full_table_name}: {e}")
            return {}
    
    def table_exists(self, database_name: str, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            self.spark.sql(f"DESCRIBE TABLE {database_name}.{table_name}")
            return True
        except Exception:
            return False
    
    def get_file_info(self, path: str) -> list:
        """Get file information from a path using dbutils."""
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            return dbutils.fs.ls(path)
        except Exception as e:
            logger.warning(f"Could not list files at {path}: {e}")
            return []
    
    def close(self):
        """Close Spark session."""
        if self._spark:
            self._spark.stop()
            self._spark = None
            logger.info("Closed Spark session")


# Global instance
databricks_config = DatabricksConfig()