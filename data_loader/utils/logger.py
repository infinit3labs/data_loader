"""
Logging configuration and utilities for the data loader.
"""

import sys
from typing import Optional
from loguru import logger


def setup_logging(log_level: str = "INFO", 
                 log_file: Optional[str] = None,
                 enable_json_format: bool = False) -> None:
    """
    Configure logging for the data loader.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
        enable_json_format: Whether to use JSON format for structured logging
    """
    # Remove default logger
    logger.remove()
    
    # Configure format
    if enable_json_format:
        log_format = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}"
    else:
        log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <level>{message}</level>"
    
    # Add console handler
    logger.add(
        sys.stdout,
        format=log_format,
        level=log_level,
        colorize=not enable_json_format,
        backtrace=True,
        diagnose=True
    )
    
    # Add file handler if specified
    if log_file:
        logger.add(
            log_file,
            format=log_format,
            level=log_level,
            rotation="100 MB",
            retention="7 days",
            compression="gz",
            backtrace=True,
            diagnose=True
        )
    
    logger.info(f"Logging configured with level: {log_level}")


def get_logger(name: str):
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logger.bind(name=name)


class DataLoaderLogger:
    """
    Specialized logger for data loader operations with structured logging.
    """
    
    def __init__(self, component_name: str):
        self.component_name = component_name
        self.logger = logger.bind(component=component_name)
    
    def log_file_processing_start(self, file_path: str, table_name: str, strategy: str):
        """Log the start of file processing."""
        self.logger.info(
            "File processing started",
            extra={
                "event": "file_processing_start",
                "file_path": file_path,
                "table_name": table_name,
                "strategy": strategy
            }
        )
    
    def log_file_processing_complete(self, file_path: str, table_name: str, 
                                   records_processed: int, execution_time: float):
        """Log successful file processing completion."""
        self.logger.info(
            "File processing completed",
            extra={
                "event": "file_processing_complete",
                "file_path": file_path,
                "table_name": table_name,
                "records_processed": records_processed,
                "execution_time": execution_time
            }
        )
    
    def log_file_processing_error(self, file_path: str, table_name: str, 
                                error_message: str, execution_time: float):
        """Log file processing errors."""
        self.logger.error(
            "File processing failed",
            extra={
                "event": "file_processing_error",
                "file_path": file_path,
                "table_name": table_name,
                "error_message": error_message,
                "execution_time": execution_time
            }
        )
    
    def log_table_stats(self, table_name: str, operation: str, stats: dict):
        """Log table operation statistics."""
        self.logger.info(
            f"Table {operation} statistics",
            extra={
                "event": f"table_{operation}_stats",
                "table_name": table_name,
                **stats
            }
        )
    
    def log_discovery_results(self, table_name: str, pattern: str, files_found: int):
        """Log file discovery results."""
        self.logger.info(
            "File discovery completed",
            extra={
                "event": "file_discovery",
                "table_name": table_name,
                "pattern": pattern,
                "files_found": files_found
            }
        )
    
    def log_parallel_execution_start(self, total_tasks: int, max_workers: int):
        """Log start of parallel execution."""
        self.logger.info(
            "Parallel execution started",
            extra={
                "event": "parallel_execution_start",
                "total_tasks": total_tasks,
                "max_workers": max_workers
            }
        )
    
    def log_parallel_execution_complete(self, total_tasks: int, successful: int, 
                                      failed: int, execution_time: float):
        """Log completion of parallel execution."""
        self.logger.info(
            "Parallel execution completed",
            extra={
                "event": "parallel_execution_complete",
                "total_tasks": total_tasks,
                "successful_tasks": successful,
                "failed_tasks": failed,
                "execution_time": execution_time,
                "success_rate": (successful / total_tasks * 100) if total_tasks > 0 else 0
            }
        )
    
    def log_retry_attempt(self, file_path: str, table_name: str, retry_count: int):
        """Log retry attempts."""
        self.logger.warning(
            "Retrying file processing",
            extra={
                "event": "retry_attempt",
                "file_path": file_path,
                "table_name": table_name,
                "retry_count": retry_count
            }
        )
    
    def log_data_quality_check(self, table_name: str, check_name: str, 
                             result: bool, details: dict = None):
        """Log data quality check results."""
        level = "info" if result else "warning"
        getattr(self.logger, level)(
            f"Data quality check: {check_name}",
            extra={
                "event": "data_quality_check",
                "table_name": table_name,
                "check_name": check_name,
                "result": result,
                "details": details or {}
            }
        )
    
    def log_performance_metrics(self, operation: str, metrics: dict):
        """Log performance metrics."""
        self.logger.info(
            f"Performance metrics: {operation}",
            extra={
                "event": "performance_metrics",
                "operation": operation,
                **metrics
            }
        )