"""
File processing status tracking system using Delta tables.

This module provides functionality to track which files have been processed,
their status, and prevent reprocessing of the same files.
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from loguru import logger

from ..config.databricks_config import databricks_config


class FileProcessingStatus(str, Enum):
    """File processing status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class FileTracker:
    """
    Manages file processing status tracking using Delta tables.
    
    This class provides methods to:
    - Track which files have been processed
    - Update file processing status
    - Retrieve files that need processing
    - Prevent duplicate processing
    """
    
    def __init__(self, database_name: str, table_name: str):
        self.database_name = database_name
        self.table_name = table_name
        self.full_table_name = f"{database_name}.{table_name}"
        self.spark = databricks_config.spark
        
        # Initialize the tracking table
        self._create_tracking_table()
    
    def _create_tracking_table(self):
        """Create the file tracking table if it doesn't exist."""
        # Ensure database exists
        databricks_config.create_database_if_not_exists(self.database_name)
        
        # Define schema for the tracking table
        schema = StructType([
            StructField("file_path", StringType(), nullable=False),
            StructField("file_size", IntegerType(), nullable=True),
            StructField("file_modified_time", TimestampType(), nullable=True),
            StructField("table_name", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("processing_start_time", TimestampType(), nullable=True),
            StructField("processing_end_time", TimestampType(), nullable=True),
            StructField("error_message", StringType(), nullable=True),
            StructField("retry_count", IntegerType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=False),
        ])
        
        # Create table if it doesn't exist
        if not databricks_config.table_exists(self.database_name, self.table_name):
            empty_df = self.spark.createDataFrame([], schema)
            
            empty_df.write \
                .format("delta") \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true") \
                .saveAsTable(self.full_table_name)
            
            logger.info(f"Created file tracking table: {self.full_table_name}")
        else:
            logger.info(f"File tracking table already exists: {self.full_table_name}")
    
    def register_files(self, file_paths: List[str], table_name: str) -> int:
        """
        Register new files for processing.
        
        Args:
            file_paths: List of file paths to register
            table_name: Target table name for these files
            
        Returns:
            Number of new files registered
        """
        if not file_paths:
            return 0
        
        # Get existing files to avoid duplicates
        existing_files = self._get_existing_files(file_paths)
        new_files = [f for f in file_paths if f not in existing_files]
        
        if not new_files:
            logger.info(f"No new files to register for table {table_name}")
            return 0
        
        # Create DataFrame for new files
        current_time = datetime.now()
        new_files_data = []
        
        for file_path in new_files:
            # Get file metadata (size, modified time) if available
            file_size, file_modified_time = self._get_file_metadata(file_path)
            
            new_files_data.append({
                "file_path": file_path,
                "file_size": file_size,
                "file_modified_time": file_modified_time,
                "table_name": table_name,
                "status": FileProcessingStatus.PENDING.value,
                "processing_start_time": None,
                "processing_end_time": None,
                "error_message": None,
                "retry_count": 0,
                "created_at": current_time,
                "updated_at": current_time
            })
        
        # Insert new files
        new_files_df = self.spark.createDataFrame(new_files_data)
        
        new_files_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.full_table_name)
        
        logger.info(f"Registered {len(new_files)} new files for table {table_name}")
        return len(new_files)
    
    def get_pending_files(self, table_name: Optional[str] = None, 
                         limit: Optional[int] = None) -> List[str]:
        """
        Get list of files pending processing.
        
        Args:
            table_name: Filter by specific table name (optional)
            limit: Maximum number of files to return (optional)
            
        Returns:
            List of file paths pending processing
        """
        query = f"""
        SELECT file_path 
        FROM {self.full_table_name}
        WHERE status = '{FileProcessingStatus.PENDING.value}'
        """
        
        if table_name:
            query += f" AND table_name = '{table_name}'"
        
        query += " ORDER BY created_at"
        
        if limit:
            query += f" LIMIT {limit}"
        
        result = self.spark.sql(query)
        return [row.file_path for row in result.collect()]
    
    def update_file_status(self, file_path: str, status: FileProcessingStatus,
                          error_message: Optional[str] = None, transaction_id: Optional[str] = None):
        """
        Update the processing status of a file atomically.
        
        Args:
            file_path: Path of the file to update
            status: New processing status
            error_message: Error message if status is FAILED
            transaction_id: Optional transaction ID for correlation
        """
        current_time = datetime.now()
        
        # Prepare update values
        update_values = {
            "status": status.value,
            "updated_at": current_time
        }
        
        if status == FileProcessingStatus.PROCESSING:
            update_values["processing_start_time"] = current_time
        elif status in [FileProcessingStatus.COMPLETED, FileProcessingStatus.FAILED]:
            update_values["processing_end_time"] = current_time
        
        if error_message:
            update_values["error_message"] = error_message
        
        # Use transaction for atomic updates
        try:
            if status == FileProcessingStatus.FAILED:
                # Increment retry count and update status in single transaction
                update_sql = f"""
                UPDATE {self.full_table_name}
                SET 
                    retry_count = retry_count + 1,
                    status = '{status.value}',
                    processing_end_time = TIMESTAMP '{current_time}',
                    error_message = {f"'{error_message}'" if error_message else "NULL"},
                    updated_at = TIMESTAMP '{current_time}'
                WHERE file_path = '{file_path}'
                """
            else:
                # Build update SQL for other statuses
                set_clause = ", ".join([f"{k} = '{v}'" if isinstance(v, str) 
                                       else f"{k} = TIMESTAMP '{v}'" if isinstance(v, datetime)
                                       else f"{k} = {v}" for k, v in update_values.items()])
                
                update_sql = f"""
                UPDATE {self.full_table_name}
                SET {set_clause}
                WHERE file_path = '{file_path}'
                """
            
            self.spark.sql(update_sql)
            
            # Log the status update
            log_msg = f"Updated file status: {file_path} -> {status.value}"
            if transaction_id:
                log_msg += f" (txn: {transaction_id})"
            logger.debug(log_msg)
            
        except Exception as e:
            logger.error(f"Failed to update file status for {file_path}: {e}")
            raise
    
    def get_failed_files(self, max_retries: int = 3) -> List[str]:
        """
        Get files that failed processing and haven't exceeded max retries.
        
        Args:
            max_retries: Maximum number of retry attempts
            
        Returns:
            List of file paths that can be retried
        """
        query = f"""
        SELECT file_path
        FROM {self.full_table_name}
        WHERE status = '{FileProcessingStatus.FAILED.value}'
        AND retry_count < {max_retries}
        ORDER BY updated_at
        """
        
        result = self.spark.sql(query)
        return [row.file_path for row in result.collect()]
    
    def get_processing_stats(self, table_name: Optional[str] = None) -> Dict[str, int]:
        """
        Get processing statistics.
        
        Args:
            table_name: Filter by specific table name (optional)
            
        Returns:
            Dictionary with counts by status
        """
        query = f"""
        SELECT status, COUNT(*) as count
        FROM {self.full_table_name}
        """
        
        if table_name:
            query += f" WHERE table_name = '{table_name}'"
        
        query += " GROUP BY status"
        
        result = self.spark.sql(query)
        stats = {row.status: row.count for row in result.collect()}
        
        # Ensure all statuses are present
        for status in FileProcessingStatus:
            if status.value not in stats:
                stats[status.value] = 0
        
        return stats
    
    def cleanup_old_records(self, days_to_keep: int = 30):
        """
        Clean up old completed/failed records.
        
        Args:
            days_to_keep: Number of days to keep records
        """
        cleanup_sql = f"""
        DELETE FROM {self.full_table_name}
        WHERE status IN ('{FileProcessingStatus.COMPLETED.value}', '{FileProcessingStatus.FAILED.value}')
        AND updated_at < CURRENT_TIMESTAMP() - INTERVAL {days_to_keep} DAYS
        """
        
        self.spark.sql(cleanup_sql)
        logger.info(f"Cleaned up old records older than {days_to_keep} days")
    
    def _get_existing_files(self, file_paths: List[str]) -> List[str]:
        """Get list of files that already exist in tracking table."""
        if not file_paths:
            return []
        
        file_paths_str = "', '".join(file_paths)
        query = f"""
        SELECT DISTINCT file_path
        FROM {self.full_table_name}
        WHERE file_path IN ('{file_paths_str}')
        """
        
        result = self.spark.sql(query)
        return [row.file_path for row in result.collect()]
    
    def _get_file_metadata(self, file_path: str) -> tuple:
        """Get file metadata (size, modified time)."""
        try:
            # In a real Databricks environment, we would use dbutils
            # For now, return None values
            return None, None
        except Exception as e:
            logger.warning(f"Could not get metadata for file {file_path}: {e}")
            return None, None
    
    def validate_consistency(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate consistency between file tracker and actual data state.
        
        Args:
            table_name: Specific table to validate (optional)
            
        Returns:
            Dictionary with validation results
        """
        logger.info("Starting file tracker consistency validation")
        
        validation_results = {
            "overall_status": "valid",
            "total_files_checked": 0,
            "inconsistencies": [],
            "warnings": [],
            "table_results": {}
        }
        
        try:
            # Get all completed files
            query = f"""
            SELECT file_path, table_name, status, processing_end_time
            FROM {self.full_table_name}
            WHERE status = '{FileProcessingStatus.COMPLETED.value}'
            """
            
            if table_name:
                query += f" AND table_name = '{table_name}'"
            
            completed_files = self.spark.sql(query).collect()
            validation_results["total_files_checked"] = len(completed_files)
            
            for row in completed_files:
                file_path = row.file_path
                table_name = row.table_name
                
                # Check if file still exists
                try:
                    # In production, this would check DBFS/cloud storage
                    file_exists = True  # Placeholder
                except:
                    file_exists = False
                
                if not file_exists:
                    validation_results["warnings"].append(
                        f"Completed file no longer exists: {file_path}"
                    )
                
                # Track table-level results
                if table_name not in validation_results["table_results"]:
                    validation_results["table_results"][table_name] = {
                        "files_checked": 0,
                        "inconsistencies": 0
                    }
                
                validation_results["table_results"][table_name]["files_checked"] += 1
            
            # Check for orphaned processing records
            orphaned_processing = self.spark.sql(f"""
            SELECT file_path, processing_start_time
            FROM {self.full_table_name}
            WHERE status = '{FileProcessingStatus.PROCESSING.value}'
            AND processing_start_time < CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
            """).collect()
            
            for row in orphaned_processing:
                validation_results["inconsistencies"].append(
                    f"Orphaned processing record: {row.file_path} (started {row.processing_start_time})"
                )
                validation_results["overall_status"] = "inconsistent"
            
            logger.info(f"Consistency validation completed: {validation_results['overall_status']}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error during consistency validation: {e}")
            validation_results["overall_status"] = "error"
            validation_results["error"] = str(e)
            return validation_results
    
    def fix_inconsistencies(self, validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fix identified inconsistencies in file tracking.
        
        Args:
            validation_results: Results from validate_consistency()
            
        Returns:
            Dictionary with fix results
        """
        logger.info("Starting inconsistency fixes")
        
        fix_results = {
            "fixes_applied": 0,
            "fixes_failed": 0,
            "actions": []
        }
        
        try:
            # Fix orphaned processing records
            orphaned_count = self.spark.sql(f"""
            UPDATE {self.full_table_name}
            SET 
                status = '{FileProcessingStatus.FAILED.value}',
                error_message = 'Orphaned processing record - marked as failed for retry',
                processing_end_time = CURRENT_TIMESTAMP(),
                updated_at = CURRENT_TIMESTAMP()
            WHERE status = '{FileProcessingStatus.PROCESSING.value}'
            AND processing_start_time < CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
            """)
            
            if orphaned_count > 0:
                fix_results["fixes_applied"] += orphaned_count
                fix_results["actions"].append(f"Fixed {orphaned_count} orphaned processing records")
            
            logger.info(f"Applied {fix_results['fixes_applied']} consistency fixes")
            return fix_results
            
        except Exception as e:
            logger.error(f"Error fixing inconsistencies: {e}")
            fix_results["fixes_failed"] += 1
            fix_results["error"] = str(e)
            return fix_results
    
    def get_unprocessed_files(self, discovered_files: List[str]) -> List[str]:
        """
        Get files from discovered list that haven't been successfully processed.
        
        Args:
            discovered_files: List of discovered file paths
            
        Returns:
            List of file paths that need processing
        """
        if not discovered_files:
            return []
        
        # Get files that are either not tracked or not completed
        file_paths_str = "', '".join(discovered_files)
        query = f"""
        SELECT file_path 
        FROM {self.full_table_name}
        WHERE file_path IN ('{file_paths_str}')
        AND status = '{FileProcessingStatus.COMPLETED.value}'
        """
        
        completed_files = {row.file_path for row in self.spark.sql(query).collect()}
        
        # Return files that are not completed
        unprocessed_files = [f for f in discovered_files if f not in completed_files]
        
        logger.debug(f"Found {len(unprocessed_files)} unprocessed files out of {len(discovered_files)} discovered")
        return unprocessed_files