"""
Parallel execution framework for processing multiple files concurrently.

This module provides utilities for parallel processing of files
while managing resources and handling errors gracefully.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import List, Dict, Any, Callable, Optional
from dataclasses import dataclass
from loguru import logger

from ..core.file_tracker import FileTracker, FileProcessingStatus


@dataclass
class ProcessingTask:
    """Represents a single file processing task."""
    file_path: str
    table_name: str
    strategy_name: str
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class ProcessingResult:
    """Result of processing a single file."""
    file_path: str
    table_name: str
    success: bool
    execution_time: float
    error_message: Optional[str] = None
    metrics: Optional[Dict[str, Any]] = None


class ParallelExecutor:
    """
    Manages parallel execution of file processing tasks.
    
    Features:
    - Configurable number of concurrent workers
    - Automatic retry logic for failed tasks
    - Progress tracking and metrics collection
    - Resource management and cleanup
    """
    
    def __init__(self, max_workers: int = 4, timeout_minutes: int = 60):
        """
        Initialize the parallel executor.
        
        Args:
            max_workers: Maximum number of concurrent workers
            timeout_minutes: Timeout for each task in minutes
        """
        self.max_workers = max_workers
        self.timeout_seconds = timeout_minutes * 60
        self.file_tracker: Optional[FileTracker] = None
        
    def set_file_tracker(self, file_tracker: FileTracker):
        """Set the file tracker for status updates."""
        self.file_tracker = file_tracker
    
    def execute_tasks(self, tasks: List[ProcessingTask], 
                     processing_function: Callable[[ProcessingTask], ProcessingResult]) -> Dict[str, Any]:
        """
        Execute a list of processing tasks in parallel.
        
        Args:
            tasks: List of tasks to execute
            processing_function: Function to process each task
            
        Returns:
            Dictionary with execution summary and results
        """
        if not tasks:
            return {
                "total_tasks": 0,
                "successful_tasks": 0,
                "failed_tasks": 0,
                "results": []
            }
        
        logger.info(f"Starting parallel execution of {len(tasks)} tasks with {self.max_workers} workers")
        
        start_time = time.time()
        results = []
        successful_tasks = 0
        failed_tasks = 0
        
        # Update file status to PROCESSING
        if self.file_tracker:
            for task in tasks:
                self.file_tracker.update_file_status(
                    task.file_path, 
                    FileProcessingStatus.PROCESSING
                )
        
        # Execute tasks in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._execute_single_task, task, processing_function): task
                for task in tasks
            }
            
            # Process completed tasks
            for future in as_completed(future_to_task, timeout=self.timeout_seconds):
                task = future_to_task[future]
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result.success:
                        successful_tasks += 1
                        if self.file_tracker:
                            self.file_tracker.update_file_status(
                                task.file_path,
                                FileProcessingStatus.COMPLETED
                            )
                        logger.info(f"Successfully processed {task.file_path}")
                    else:
                        failed_tasks += 1
                        if self.file_tracker:
                            self.file_tracker.update_file_status(
                                task.file_path,
                                FileProcessingStatus.FAILED,
                                result.error_message
                            )
                        logger.error(f"Failed to process {task.file_path}: {result.error_message}")
                        
                except Exception as e:
                    failed_tasks += 1
                    error_msg = f"Unexpected error processing {task.file_path}: {str(e)}"
                    
                    result = ProcessingResult(
                        file_path=task.file_path,
                        table_name=task.table_name,
                        success=False,
                        execution_time=0.0,
                        error_message=error_msg
                    )
                    results.append(result)
                    
                    if self.file_tracker:
                        self.file_tracker.update_file_status(
                            task.file_path,
                            FileProcessingStatus.FAILED,
                            error_msg
                        )
                    
                    logger.error(error_msg)
        
        total_time = time.time() - start_time
        
        execution_summary = {
            "total_tasks": len(tasks),
            "successful_tasks": successful_tasks,
            "failed_tasks": failed_tasks,
            "total_execution_time": total_time,
            "average_task_time": total_time / len(tasks) if tasks else 0,
            "results": results
        }
        
        logger.info(f"Parallel execution completed: {successful_tasks}/{len(tasks)} successful, "
                   f"total time: {total_time:.2f}s")
        
        return execution_summary
    
    def _execute_single_task(self, task: ProcessingTask, 
                            processing_function: Callable[[ProcessingTask], ProcessingResult]) -> ProcessingResult:
        """
        Execute a single processing task with error handling.
        
        Args:
            task: Task to execute
            processing_function: Function to process the task
            
        Returns:
            Processing result
        """
        start_time = time.time()
        
        try:
            # Execute the processing function
            result = processing_function(task)
            result.execution_time = time.time() - start_time
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Error processing {task.file_path}: {str(e)}"
            
            return ProcessingResult(
                file_path=task.file_path,
                table_name=task.table_name,
                success=False,
                execution_time=execution_time,
                error_message=error_msg
            )
    
    def retry_failed_tasks(self, failed_results: List[ProcessingResult],
                          processing_function: Callable[[ProcessingTask], ProcessingResult],
                          max_retries: int = 3) -> Dict[str, Any]:
        """
        Retry failed tasks up to the maximum retry limit.
        
        Args:
            failed_results: List of failed processing results to retry
            processing_function: Function to process each task
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dictionary with retry execution summary
        """
        retry_tasks = []
        
        for failed_result in failed_results:
            # Check if we can retry this task
            current_retries = 0
            if self.file_tracker:
                # Get current retry count from file tracker
                stats = self.file_tracker.get_processing_stats()
                # This is simplified - in practice you'd query the specific file
                current_retries = 0  # Placeholder
            
            if current_retries < max_retries:
                retry_task = ProcessingTask(
                    file_path=failed_result.file_path,
                    table_name=failed_result.table_name,
                    strategy_name="",  # Will be determined by processor
                    retry_count=current_retries + 1,
                    max_retries=max_retries
                )
                retry_tasks.append(retry_task)
        
        if not retry_tasks:
            logger.info("No tasks eligible for retry")
            return {
                "total_retry_tasks": 0,
                "successful_retries": 0,
                "failed_retries": 0,
                "results": []
            }
        
        logger.info(f"Retrying {len(retry_tasks)} failed tasks")
        return self.execute_tasks(retry_tasks, processing_function)
    
    def get_processing_metrics(self, results: List[ProcessingResult]) -> Dict[str, Any]:
        """
        Calculate processing metrics from results.
        
        Args:
            results: List of processing results
            
        Returns:
            Dictionary with processing metrics
        """
        if not results:
            return {}
        
        total_files = len(results)
        successful_files = sum(1 for r in results if r.success)
        failed_files = total_files - successful_files
        
        execution_times = [r.execution_time for r in results if r.execution_time > 0]
        avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0
        max_execution_time = max(execution_times) if execution_times else 0
        min_execution_time = min(execution_times) if execution_times else 0
        
        # Group by table
        table_stats = {}
        for result in results:
            table_name = result.table_name
            if table_name not in table_stats:
                table_stats[table_name] = {"successful": 0, "failed": 0, "total": 0}
            
            table_stats[table_name]["total"] += 1
            if result.success:
                table_stats[table_name]["successful"] += 1
            else:
                table_stats[table_name]["failed"] += 1
        
        return {
            "total_files_processed": total_files,
            "successful_files": successful_files,
            "failed_files": failed_files,
            "success_rate": (successful_files / total_files * 100) if total_files > 0 else 0,
            "average_execution_time": avg_execution_time,
            "min_execution_time": min_execution_time,
            "max_execution_time": max_execution_time,
            "table_statistics": table_stats
        }
    
    def monitor_resource_usage(self) -> Dict[str, Any]:
        """
        Monitor resource usage during processing.
        
        Returns:
            Dictionary with resource usage metrics
        """
        import psutil
        
        try:
            # Get CPU and memory usage
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_available_gb": memory.available / (1024**3),
                "disk_percent": disk.percent,
                "disk_free_gb": disk.free / (1024**3)
            }
        except ImportError:
            logger.warning("psutil not available, cannot monitor resource usage")
            return {}
        except Exception as e:
            logger.warning(f"Error monitoring resource usage: {e}")
            return {}