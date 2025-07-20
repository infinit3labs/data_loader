"""
SCD2 (Slowly Changing Dimension Type 2) loading strategy.

This strategy maintains historical records by tracking changes over time.
When a record changes, the old record is marked as inactive and a new
record is inserted with the current data.
"""

from datetime import datetime
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, max as spark_max, 
    row_number, lag, coalesce, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from loguru import logger

from .base_strategy import BaseLoadingStrategy
from ..config.table_config import TableConfig


class SCD2Strategy(BaseLoadingStrategy):
    """
    Slowly Changing Dimension Type 2 (SCD2) loading strategy.
    
    This strategy:
    1. Identifies new and changed records
    2. Marks old records as inactive (sets end_date, is_current=false)
    3. Inserts new/changed records as active (sets effective_date, is_current=true)
    4. Maintains full history of changes
    """
    
    def __init__(self, table_config: TableConfig):
        """
        Initialize SCD2 strategy.
        
        Args:
            table_config: Configuration for the target table
        """
        super().__init__(table_config)
        
        # Validate SCD2-specific configuration
        if not self.validate_config():
            raise ValueError("Invalid configuration for SCD2 strategy")
    
    def validate_config(self) -> bool:
        """
        Validate that the table configuration is compatible with SCD2.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        if not self.table_config.primary_keys:
            logger.error("SCD2 strategy requires primary_keys to be defined")
            return False
        
        if not self.table_config.tracking_columns:
            logger.error("SCD2 strategy requires tracking_columns to be defined")
            return False
        
        # Check that SCD2 columns are defined
        required_scd2_columns = [
            "scd2_effective_date_column",
            "scd2_end_date_column", 
            "scd2_current_flag_column"
        ]
        
        for column_config in required_scd2_columns:
            if not getattr(self.table_config, column_config):
                logger.error(f"SCD2 strategy requires {column_config} to be defined")
                return False
        
        return True
    
    def prepare_target_table(self):
        """Prepare the target table with SCD2 schema."""
        super().prepare_target_table()
        
        # If table doesn't exist, we'll create it when we first write data
        # The schema will be inferred from the first DataFrame with SCD2 columns added
        if not self.spark.catalog.tableExists(self.full_table_name):
            logger.info(f"Target table {self.full_table_name} will be created on first load")
    
    def load_data(self, source_df: DataFrame, file_path: str) -> Dict[str, Any]:
        """
        Load data using SCD2 strategy.
        
        Args:
            source_df: Source DataFrame to load
            file_path: Path of the source file
            
        Returns:
            Dictionary with loading results and metrics
        """
        logger.info(f"Starting SCD2 load for {self.full_table_name} from {file_path}")
        
        # Apply transformations to source data
        source_df = self.apply_transformations(source_df)
        
        # Add SCD2 metadata columns to source data
        source_df_with_scd2 = self._add_scd2_columns(source_df)
        
        # Check if target table exists
        table_exists = self.spark.catalog.tableExists(self.full_table_name)
        
        if not table_exists:
            # First load - just insert all records as current
            result = self._initial_load(source_df_with_scd2)
        else:
            # Incremental load - perform SCD2 merge
            result = self._incremental_load(source_df_with_scd2)
        
        # Optimize table after load
        self.optimize_table()
        
        result.update({
            "file_path": file_path,
            "strategy": "SCD2",
            "table_name": self.full_table_name
        })
        
        logger.info(f"Completed SCD2 load for {self.full_table_name}: {result}")
        return result
    
    def _add_scd2_columns(self, df: DataFrame) -> DataFrame:
        """
        Add SCD2 metadata columns to the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with SCD2 columns added
        """
        current_time = current_timestamp()
        
        df_with_scd2 = df \
            .withColumn(self.table_config.scd2_effective_date_column, current_time) \
            .withColumn(self.table_config.scd2_end_date_column, lit(None).cast(TimestampType())) \
            .withColumn(self.table_config.scd2_current_flag_column, lit(True))
        
        return df_with_scd2
    
    def _initial_load(self, source_df: DataFrame) -> Dict[str, Any]:
        """
        Perform initial load (table doesn't exist yet).
        
        Args:
            source_df: Source DataFrame with SCD2 columns
            
        Returns:
            Dictionary with load results
        """
        logger.info(f"Performing initial SCD2 load for {self.full_table_name}")
        
        # Write the data to create the table
        write_options = {
            "format": "delta",
            "mode": "overwrite",
            "option": [
                ("delta.autoOptimize.optimizeWrite", "true"),
                ("delta.autoOptimize.autoCompact", "true")
            ]
        }
        
        writer = source_df.write.format("delta").mode("overwrite")
        
        # Apply write options
        for option_key, option_value in [
            ("delta.autoOptimize.optimizeWrite", "true"),
            ("delta.autoOptimize.autoCompact", "true")
        ]:
            writer = writer.option(option_key, option_value)
        
        # Add partitioning if configured
        if self.table_config.partition_columns:
            writer = writer.partitionBy(*self.table_config.partition_columns)
        
        writer.saveAsTable(self.full_table_name)
        
        row_count = source_df.count()
        
        return {
            "operation": "initial_load",
            "records_inserted": row_count,
            "records_updated": 0,
            "records_deleted": 0,
            "total_records_processed": row_count
        }
    
    def _incremental_load(self, source_df: DataFrame) -> Dict[str, Any]:
        """
        Perform incremental SCD2 load.
        
        Args:
            source_df: Source DataFrame with SCD2 columns
            
        Returns:
            Dictionary with load results
        """
        logger.info(f"Performing incremental SCD2 load for {self.full_table_name}")
        
        # Read current table data
        target_df = self.spark.read.format("delta").table(self.full_table_name)
        
        # Get only current records from target
        current_target_df = target_df.filter(
            col(self.table_config.scd2_current_flag_column) == True
        )
        
        # Identify changes
        changes_result = self._identify_changes(source_df, current_target_df)
        
        # Process the changes
        records_inserted = 0
        records_updated = 0
        
        if changes_result["new_records"].count() > 0:
            records_inserted = self._insert_new_records(changes_result["new_records"])
        
        if changes_result["changed_records"].count() > 0:
            records_updated = self._update_changed_records(
                changes_result["changed_records"],
                changes_result["updated_source_records"]
            )
        
        total_processed = source_df.count()
        
        return {
            "operation": "incremental_load",
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_deleted": 0,
            "total_records_processed": total_processed
        }
    
    def _identify_changes(self, source_df: DataFrame, current_target_df: DataFrame) -> Dict[str, DataFrame]:
        """
        Identify new and changed records between source and target.
        
        Args:
            source_df: Source DataFrame
            current_target_df: Current records from target table
            
        Returns:
            Dictionary containing new_records and changed_records DataFrames
        """
        # Create join condition on primary keys
        join_condition = [
            source_df[pk] == current_target_df[pk] 
            for pk in self.table_config.primary_keys
        ]
        join_condition = join_condition[0] if len(join_condition) == 1 else join_condition
        
        # Left join to identify new and existing records
        joined_df = source_df.alias("source").join(
            current_target_df.alias("target"),
            join_condition,
            "left"
        )
        
        # New records (no match in target)
        new_records = joined_df.filter(
            current_target_df[self.table_config.primary_keys[0]].isNull()
        ).select("source.*")
        
        # Existing records that might have changed
        existing_records = joined_df.filter(
            current_target_df[self.table_config.primary_keys[0]].isNotNull()
        )
        
        # Identify changed records by comparing tracking columns
        change_conditions = []
        for track_col in self.table_config.tracking_columns:
            change_conditions.append(
                (col(f"source.{track_col}") != col(f"target.{track_col}")) |
                (col(f"source.{track_col}").isNull() & col(f"target.{track_col}").isNotNull()) |
                (col(f"source.{track_col}").isNotNull() & col(f"target.{track_col}").isNull())
            )
        
        # Combine all change conditions with OR
        if change_conditions:
            overall_change_condition = change_conditions[0]
            for condition in change_conditions[1:]:
                overall_change_condition = overall_change_condition | condition
            
            changed_records = existing_records.filter(overall_change_condition)
        else:
            # No tracking columns defined, consider all as unchanged
            changed_records = self.spark.createDataFrame([], existing_records.schema)
        
        # Get the source records for changed records
        if changed_records.count() > 0:
            changed_pks = changed_records.select(*[f"source.{pk}" for pk in self.table_config.primary_keys])
            updated_source_records = source_df.join(
                changed_pks,
                self.table_config.primary_keys,
                "inner"
            )
        else:
            updated_source_records = self.spark.createDataFrame([], source_df.schema)
        
        logger.info(f"Identified {new_records.count()} new records and {changed_records.count()} changed records")
        
        return {
            "new_records": new_records,
            "changed_records": changed_records,
            "updated_source_records": updated_source_records
        }
    
    def _insert_new_records(self, new_records_df: DataFrame) -> int:
        """
        Insert new records into the target table.
        
        Args:
            new_records_df: DataFrame containing new records
            
        Returns:
            Number of records inserted
        """
        if new_records_df.count() == 0:
            return 0
        
        new_records_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.full_table_name)
        
        count = new_records_df.count()
        logger.info(f"Inserted {count} new records")
        return count
    
    def _update_changed_records(self, changed_records_df: DataFrame, 
                               updated_source_records_df: DataFrame) -> int:
        """
        Update changed records using SCD2 approach.
        
        Args:
            changed_records_df: DataFrame containing changed records from target
            updated_source_records_df: DataFrame containing updated source records
            
        Returns:
            Number of records updated
        """
        if changed_records_df.count() == 0:
            return 0
        
        # Step 1: Close existing records (set end_date and is_current=false)
        pks_to_update = changed_records_df.select(*[f"source.{pk}" for pk in self.table_config.primary_keys])
        
        # Create condition for updating existing records
        pk_conditions = []
        for pk in self.table_config.primary_keys:
            pk_conditions.append(f"{pk} IN (SELECT {pk} FROM temp_pks_to_update)")
        pk_condition = " AND ".join(pk_conditions)
        
        # Create temporary view for the primary keys to update
        pks_to_update.createOrReplaceTempView("temp_pks_to_update")
        
        # Update existing records to close them
        update_sql = f"""
        UPDATE {self.full_table_name}
        SET {self.table_config.scd2_end_date_column} = current_timestamp(),
            {self.table_config.scd2_current_flag_column} = false
        WHERE {pk_condition}
        AND {self.table_config.scd2_current_flag_column} = true
        """
        
        self.spark.sql(update_sql)
        
        # Step 2: Insert new versions of the changed records
        updated_source_records_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.full_table_name)
        
        count = updated_source_records_df.count()
        logger.info(f"Updated {count} changed records using SCD2")
        return count