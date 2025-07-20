"""
Main data processor orchestrating the entire data loading pipeline.

This module coordinates file discovery, processing strategy selection,
parallel execution, and status tracking.
"""

import glob
import time
from typing import List, Dict, Any, Optional
from pathlib import Path
from loguru import logger

from ..config.table_config import DataLoaderConfig, TableConfig, LoadingStrategy
from ..config.databricks_config import databricks_config
from ..core.file_tracker import FileTracker, FileProcessingStatus
from ..core.parallel_executor import ParallelExecutor, ProcessingTask, ProcessingResult
from ..strategies.base_strategy import BaseLoadingStrategy
from ..strategies.scd2_strategy import SCD2Strategy
from ..strategies.append_strategy import AppendStrategy


class DataProcessor:
    """
    Main orchestrator for the data loading pipeline.
    
    Responsibilities:
    - Discover new files based on configuration patterns
    - Track file processing status
    - Select appropriate loading strategy for each table
    - Coordinate parallel processing
    - Handle errors and retries
    - Provide monitoring and metrics
    """
    
    def __init__(self, config: DataLoaderConfig):
        """
        Initialize the data processor.
        
        Args:
            config: Configuration for the data loader
        """
        self.config = config
        
        # Initialize components
        self.file_tracker = FileTracker(
            database_name=config.file_tracker_database,
            table_name=config.file_tracker_table
        )
        
        self.parallel_executor = ParallelExecutor(
            max_workers=config.max_parallel_jobs,
            timeout_minutes=config.timeout_minutes
        )
        self.parallel_executor.set_file_tracker(self.file_tracker)
        
        # Cache for loading strategies
        self._strategy_cache: Dict[str, BaseLoadingStrategy] = {}
        
        logger.info(f"Initialized DataProcessor with {len(config.tables)} table configurations")
    
    def process_all_tables(self) -> Dict[str, Any]:
        """
        Process all configured tables by discovering and loading new files.
        
        Returns:
            Dictionary with overall processing results and metrics
        """
        logger.info("Starting data processing for all configured tables")
        start_time = time.time()
        
        overall_results = {
            "processing_start_time": start_time,
            "tables_processed": 0,
            "total_files_discovered": 0,
            "total_files_processed": 0,
            "successful_files": 0,
            "failed_files": 0,
            "table_results": {},
            "errors": []
        }
        
        try:
            # Process each table configuration
            for table_config in self.config.tables:
                logger.info(f"Processing table: {table_config.table_name}")
                
                try:
                    table_result = self.process_table(table_config)
                    overall_results["table_results"][table_config.table_name] = table_result
                    overall_results["tables_processed"] += 1
                    overall_results["total_files_discovered"] += table_result.get("files_discovered", 0)
                    overall_results["total_files_processed"] += table_result.get("files_processed", 0)
                    overall_results["successful_files"] += table_result.get("successful_files", 0)
                    overall_results["failed_files"] += table_result.get("failed_files", 0)
                    
                except Exception as e:
                    error_msg = f"Error processing table {table_config.table_name}: {str(e)}"
                    logger.error(error_msg)
                    overall_results["errors"].append(error_msg)
                    overall_results["table_results"][table_config.table_name] = {
                        "success": False,
                        "error": error_msg
                    }
            
            # Handle retries for failed files
            if overall_results["failed_files"] > 0:
                logger.info("Processing retries for failed files")
                retry_results = self.retry_failed_files()
                overall_results["retry_results"] = retry_results
        
        except Exception as e:
            error_msg = f"Critical error in process_all_tables: {str(e)}"
            logger.error(error_msg)
            overall_results["errors"].append(error_msg)
        
        finally:
            overall_results["total_processing_time"] = time.time() - start_time
            
        logger.info(f"Completed processing all tables in {overall_results['total_processing_time']:.2f}s")
        return overall_results
    
    def process_table(self, table_config: TableConfig) -> Dict[str, Any]:
        """
        Process a single table by discovering and loading new files.
        
        Args:
            table_config: Configuration for the table to process
            
        Returns:
            Dictionary with processing results for this table
        """
        logger.info(f"Processing table {table_config.table_name} with strategy {table_config.loading_strategy}")
        
        # Discover new files
        discovered_files = self.discover_files(table_config)
        logger.info(f"Discovered {len(discovered_files)} files for table {table_config.table_name}")
        
        if not discovered_files:
            return {
                "table_name": table_config.table_name,
                "files_discovered": 0,
                "files_processed": 0,
                "successful_files": 0,
                "failed_files": 0,
                "processing_time": 0.0
            }
        
        # Register new files with file tracker
        new_files_count = self.file_tracker.register_files(discovered_files, table_config.table_name)
        
        # Get files pending processing
        pending_files = self.file_tracker.get_pending_files(table_config.table_name)
        
        if not pending_files:
            logger.info(f"No pending files to process for table {table_config.table_name}")
            return {
                "table_name": table_config.table_name,
                "files_discovered": len(discovered_files),
                "files_processed": 0,
                "successful_files": 0,
                "failed_files": 0,
                "processing_time": 0.0
            }
        
        # Create processing tasks
        tasks = [
            ProcessingTask(
                file_path=file_path,
                table_name=table_config.table_name,
                strategy_name=table_config.loading_strategy.value,
                max_retries=self.config.retry_attempts
            )
            for file_path in pending_files
        ]
        
        # Execute tasks in parallel
        start_time = time.time()
        execution_results = self.parallel_executor.execute_tasks(
            tasks, 
            self._process_single_file
        )
        processing_time = time.time() - start_time
        
        # Compile results
        result = {
            "table_name": table_config.table_name,
            "files_discovered": len(discovered_files),
            "new_files_registered": new_files_count,
            "files_processed": execution_results["total_tasks"],
            "successful_files": execution_results["successful_tasks"],
            "failed_files": execution_results["failed_tasks"],
            "processing_time": processing_time,
            "execution_details": execution_results
        }
        
        return result
    
    def discover_files(self, table_config: TableConfig) -> List[str]:
        """
        Discover files matching the table's source path pattern.
        
        Args:
            table_config: Configuration for the table
            
        Returns:
            List of file paths that match the pattern
        """
        try:
            # Use glob to find files matching the pattern
            pattern = table_config.source_path_pattern
            discovered_files = glob.glob(pattern, recursive=True)
            
            # Filter out directories and ensure we only get files
            file_paths = [f for f in discovered_files if Path(f).is_file()]
            
            logger.debug(f"Pattern '{pattern}' matched {len(file_paths)} files")
            return file_paths
            
        except Exception as e:
            logger.error(f"Error discovering files for pattern {table_config.source_path_pattern}: {e}")
            return []
    
    def _process_single_file(self, task: ProcessingTask) -> ProcessingResult:
        """
        Process a single file using the appropriate loading strategy.
        
        Args:
            task: Processing task containing file and table information
            
        Returns:
            Processing result
        """
        start_time = time.time()
        
        try:
            # Get table configuration
            table_config = self.config.get_table_config(task.table_name)
            if not table_config:
                raise ValueError(f"No configuration found for table {task.table_name}")
            
            # Get loading strategy
            strategy = self._get_loading_strategy(table_config)
            
            # Prepare target table
            strategy.prepare_target_table()
            
            # Read source data
            source_df = strategy.read_source_data(task.file_path)
            
            # Load data using the strategy
            load_result = strategy.load_data(source_df, task.file_path)
            
            execution_time = time.time() - start_time
            
            return ProcessingResult(
                file_path=task.file_path,
                table_name=task.table_name,
                success=True,
                execution_time=execution_time,
                metrics=load_result
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Error processing file {task.file_path}: {str(e)}"
            
            return ProcessingResult(
                file_path=task.file_path,
                table_name=task.table_name,
                success=False,
                execution_time=execution_time,
                error_message=error_msg
            )
    
    def _get_loading_strategy(self, table_config: TableConfig) -> BaseLoadingStrategy:
        """
        Get the appropriate loading strategy for a table configuration.
        
        Args:
            table_config: Table configuration
            
        Returns:
            Loading strategy instance
        """
        # Check cache first
        cache_key = f"{table_config.table_name}_{table_config.loading_strategy.value}"
        if cache_key in self._strategy_cache:
            return self._strategy_cache[cache_key]
        
        # Create new strategy based on configuration
        if table_config.loading_strategy == LoadingStrategy.SCD2:
            strategy = SCD2Strategy(table_config)
        elif table_config.loading_strategy == LoadingStrategy.APPEND:
            strategy = AppendStrategy(table_config)
        else:
            raise ValueError(f"Unsupported loading strategy: {table_config.loading_strategy}")
        
        # Validate strategy configuration
        if not strategy.validate_config():
            raise ValueError(f"Invalid configuration for {table_config.loading_strategy} strategy")
        
        # Cache the strategy
        self._strategy_cache[cache_key] = strategy
        
        return strategy
    
    def retry_failed_files(self) -> Dict[str, Any]:
        """
        Retry processing files that failed but haven't exceeded retry limits.
        
        Returns:
            Dictionary with retry results
        """
        logger.info("Starting retry processing for failed files")
        
        # Get failed files that can be retried
        failed_files = self.file_tracker.get_failed_files(self.config.retry_attempts)
        
        if not failed_files:
            logger.info("No failed files eligible for retry")
            return {"retry_tasks": 0, "successful_retries": 0, "failed_retries": 0}
        
        # Create retry tasks
        retry_tasks = []
        for file_path in failed_files:
            # Find which table this file belongs to
            table_name = self._find_table_for_file(file_path)
            if table_name:
                retry_tasks.append(ProcessingTask(
                    file_path=file_path,
                    table_name=table_name,
                    strategy_name="",  # Will be determined during processing
                    retry_count=1  # This would be properly tracked in real implementation
                ))
        
        # Execute retry tasks
        if retry_tasks:
            retry_results = self.parallel_executor.execute_tasks(retry_tasks, self._process_single_file)
            logger.info(f"Retry processing completed: {retry_results['successful_tasks']}/{len(retry_tasks)} successful")
            return retry_results
        else:
            return {"retry_tasks": 0, "successful_retries": 0, "failed_retries": 0}
    
    def _find_table_for_file(self, file_path: str) -> Optional[str]:
        """
        Find which table configuration matches a given file path.
        
        Args:
            file_path: Path of the file
            
        Returns:
            Table name if found, None otherwise
        """
        for table_config in self.config.tables:
            # Simple pattern matching - in practice you might want more sophisticated logic
            pattern = table_config.source_path_pattern.replace("*", "")
            if pattern in file_path:
                return table_config.table_name
        return None
    
    def get_processing_status(self) -> Dict[str, Any]:
        """
        Get current processing status and metrics.
        
        Returns:
            Dictionary with processing status information
        """
        overall_stats = self.file_tracker.get_processing_stats()
        
        # Get stats per table
        table_stats = {}
        for table_config in self.config.tables:
            table_stats[table_config.table_name] = self.file_tracker.get_processing_stats(
                table_config.table_name
            )
        
        return {
            "overall_statistics": overall_stats,
            "table_statistics": table_stats,
            "configuration": {
                "max_parallel_jobs": self.config.max_parallel_jobs,
                "retry_attempts": self.config.retry_attempts,
                "timeout_minutes": self.config.timeout_minutes,
                "total_tables": len(self.config.tables)
            }
        }
    
    def cleanup_old_records(self, days_to_keep: int = 30):
        """
        Clean up old file tracking records.
        
        Args:
            days_to_keep: Number of days to keep records
        """
        logger.info(f"Cleaning up file tracking records older than {days_to_keep} days")
        self.file_tracker.cleanup_old_records(days_to_keep)
    
    def optimize_all_tables(self):
        """Run optimization on all configured tables."""
        logger.info("Running optimization on all configured tables")
        
        for table_config in self.config.tables:
            try:
                strategy = self._get_loading_strategy(table_config)
                strategy.optimize_table()
                logger.info(f"Optimized table {table_config.table_name}")
            except Exception as e:
                logger.warning(f"Could not optimize table {table_config.table_name}: {e}")
    
    def vacuum_all_tables(self, retention_hours: int = 168):
        """
        Run VACUUM on all configured tables.
        
        Args:
            retention_hours: Retention period in hours
        """
        logger.info(f"Running VACUUM on all configured tables with {retention_hours}h retention")
        
        for table_config in self.config.tables:
            try:
                strategy = self._get_loading_strategy(table_config)
                strategy.vacuum_table(retention_hours)
                logger.info(f"Vacuumed table {table_config.table_name}")
            except Exception as e:
                logger.warning(f"Could not vacuum table {table_config.table_name}: {e}")