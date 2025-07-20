"""
Append loading strategy.

This strategy simply appends new data to the target table without
checking for duplicates or performing any complex logic. Suitable
for tables without primary keys or when all incoming data should
be preserved as-is.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from loguru import logger

from .base_strategy import BaseLoadingStrategy
from ..config.table_config import TableConfig


class AppendStrategy(BaseLoadingStrategy):
    """
    Append loading strategy.
    
    This strategy:
    1. Reads source data
    2. Applies any configured transformations
    3. Appends data to target table
    4. Optionally adds audit columns (created_at, etc.)
    """
    
    def __init__(self, table_config: TableConfig):
        """
        Initialize Append strategy.
        
        Args:
            table_config: Configuration for the target table
        """
        super().__init__(table_config)
    
    def validate_config(self) -> bool:
        """
        Validate that the table configuration is compatible with Append strategy.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        # Append strategy is very flexible and works with most configurations
        # Just validate basic requirements
        
        if not self.table_config.table_name:
            logger.error("Table name is required")
            return False
        
        if not self.table_config.database_name:
            logger.error("Database name is required")
            return False
        
        return True
    
    def prepare_target_table(self):
        """Prepare the target table for append operations."""
        super().prepare_target_table()
        
        # For append strategy, we don't need to create the table upfront
        # It will be created automatically when we first write data
        if not self.spark.catalog.tableExists(self.full_table_name):
            logger.info(f"Target table {self.full_table_name} will be created on first append")
    
    def load_data(self, source_df: DataFrame, file_path: str) -> Dict[str, Any]:
        """
        Load data using Append strategy.
        
        Args:
            source_df: Source DataFrame to load
            file_path: Path of the source file
            
        Returns:
            Dictionary with loading results and metrics
        """
        logger.info(f"Starting append load for {self.full_table_name} from {file_path}")
        
        # Apply transformations to source data
        source_df = self.apply_transformations(source_df)
        
        # Add audit columns if configured
        source_df_with_audit = self._add_audit_columns(source_df, file_path)
        
        # Check if target table exists
        table_exists = self.spark.catalog.tableExists(self.full_table_name)
        
        # Get row count before loading
        rows_to_insert = source_df_with_audit.count()
        
        if not table_exists:
            # First load - create table
            result = self._initial_load(source_df_with_audit)
        else:
            # Subsequent loads - append data
            result = self._append_load(source_df_with_audit)
        
        # Optimize table after load if configured
        if hasattr(self.table_config, 'auto_optimize') and self.table_config.auto_optimize:
            self.optimize_table()
        
        result.update({
            "file_path": file_path,
            "strategy": "Append",
            "table_name": self.full_table_name,
            "total_records_processed": rows_to_insert
        })
        
        logger.info(f"Completed append load for {self.full_table_name}: {result}")
        return result
    
    def _add_audit_columns(self, df: DataFrame, file_path: str) -> DataFrame:
        """
        Add audit columns to track data lineage.
        
        Args:
            df: Input DataFrame
            file_path: Source file path
            
        Returns:
            DataFrame with audit columns added
        """
        # Add standard audit columns
        df_with_audit = df \
            .withColumn("_load_timestamp", current_timestamp()) \
            .withColumn("_source_file", lit(file_path))
        
        # Add batch ID (could be timestamp-based or UUID)
        batch_id = file_path.split("/")[-1]  # Use filename as batch ID
        df_with_audit = df_with_audit.withColumn("_batch_id", lit(batch_id))
        
        return df_with_audit
    
    def _initial_load(self, source_df: DataFrame) -> Dict[str, Any]:
        """
        Perform initial load (create table and insert data).
        
        Args:
            source_df: Source DataFrame to load
            
        Returns:
            Dictionary with load results
        """
        logger.info(f"Performing initial append load for {self.full_table_name}")
        
        # Configure the writer
        writer = source_df.write.format("delta").mode("overwrite")
        
        # Add Delta Lake optimizations
        writer = writer \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true")
        
        # Add partitioning if configured
        if self.table_config.partition_columns:
            writer = writer.partitionBy(*self.table_config.partition_columns)
            logger.info(f"Partitioning by: {self.table_config.partition_columns}")
        
        # Write the data to create the table
        writer.saveAsTable(self.full_table_name)
        
        row_count = source_df.count()
        
        return {
            "operation": "initial_load",
            "records_inserted": row_count,
            "records_updated": 0,
            "records_deleted": 0
        }
    
    def _append_load(self, source_df: DataFrame) -> Dict[str, Any]:
        """
        Perform append load (add data to existing table).
        
        Args:
            source_df: Source DataFrame to append
            
        Returns:
            Dictionary with load results
        """
        logger.info(f"Performing append load for {self.full_table_name}")
        
        # Get initial row count for metrics
        initial_count = self._get_table_row_count()
        
        # Configure the writer for append
        writer = source_df.write.format("delta").mode("append")
        
        # Add Delta Lake optimizations
        writer = writer \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true")
        
        # Handle schema evolution if enabled
        if self.table_config.schema_evolution:
            writer = writer.option("mergeSchema", "true")
            logger.info("Schema evolution enabled for append operation")
        
        # Write the data
        writer.saveAsTable(self.full_table_name)
        
        # Get final row count for verification
        final_count = self._get_table_row_count()
        records_inserted = final_count - initial_count
        
        return {
            "operation": "append_load",
            "records_inserted": records_inserted,
            "records_updated": 0,
            "records_deleted": 0,
            "initial_table_count": initial_count,
            "final_table_count": final_count
        }
    
    def _get_table_row_count(self) -> int:
        """
        Get the current row count of the target table.
        
        Returns:
            Current row count
        """
        try:
            if self.spark.catalog.tableExists(self.full_table_name):
                return self.spark.sql(f"SELECT COUNT(*) as count FROM {self.full_table_name}").collect()[0].count
            else:
                return 0
        except Exception as e:
            logger.warning(f"Could not get row count for {self.full_table_name}: {e}")
            return 0
    
    def deduplicate_if_needed(self, df: DataFrame, dedup_columns: list = None) -> DataFrame:
        """
        Optionally deduplicate data before appending.
        
        This method can be called if you want to remove duplicates
        even in an append strategy.
        
        Args:
            df: Input DataFrame
            dedup_columns: Columns to use for deduplication (if None, use all columns)
            
        Returns:
            Deduplicated DataFrame
        """
        if dedup_columns is None:
            # Deduplicate based on all columns
            deduplicated_df = df.distinct()
        else:
            # Deduplicate based on specific columns
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number
            
            window_spec = Window.partitionBy(*dedup_columns).orderBy(df.columns[0])  # Order by first column
            deduplicated_df = df.withColumn("row_num", row_number().over(window_spec)) \
                              .filter(col("row_num") == 1) \
                              .drop("row_num")
        
        original_count = df.count()
        dedup_count = deduplicated_df.count()
        
        if original_count != dedup_count:
            logger.info(f"Removed {original_count - dedup_count} duplicate records")
        
        return deduplicated_df
    
    def handle_late_arriving_data(self, df: DataFrame, 
                                 date_column: str, 
                                 max_delay_days: int = 7) -> DataFrame:
        """
        Handle late-arriving data by filtering out records that are too old.
        
        Args:
            df: Input DataFrame
            date_column: Column containing the date/timestamp
            max_delay_days: Maximum allowed delay in days
            
        Returns:
            Filtered DataFrame
        """
        from pyspark.sql.functions import datediff, current_date
        
        # Filter out records that are older than max_delay_days
        filtered_df = df.filter(
            datediff(current_date(), col(date_column)) <= max_delay_days
        )
        
        original_count = df.count()
        filtered_count = filtered_df.count()
        
        if original_count != filtered_count:
            logger.info(f"Filtered out {original_count - filtered_count} late-arriving records")
        
        return filtered_df