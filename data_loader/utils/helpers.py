"""
Helper functions and utilities for the data loader.
"""

import os
import re
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
from loguru import logger

# Make PySpark imports optional
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col, lit, when, isnan, isnull
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = None


def validate_file_path(file_path: str) -> bool:
    """
    Validate that a file path exists and is accessible.
    
    Args:
        file_path: Path to validate
        
    Returns:
        True if valid, False otherwise
    """
    try:
        path = Path(file_path)
        return path.exists() and path.is_file()
    except Exception:
        return False


def extract_table_name_from_path(file_path: str, pattern: str = None) -> Optional[str]:
    """
    Extract table name from file path using pattern matching.
    
    Args:
        file_path: Path to extract table name from
        pattern: Optional regex pattern to use for extraction
        
    Returns:
        Extracted table name or None
    """
    if pattern:
        match = re.search(pattern, file_path)
        return match.group(1) if match else None
    
    # Default extraction: use parent directory name
    try:
        return Path(file_path).parent.name
    except Exception:
        return None


def get_file_metadata(file_path: str) -> Dict[str, Any]:
    """
    Get metadata about a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Dictionary with file metadata
    """
    try:
        path = Path(file_path)
        stat = path.stat()
        
        return {
            "file_name": path.name,
            "file_size": stat.st_size,
            "file_size_mb": round(stat.st_size / (1024 * 1024), 2),
            "modified_time": datetime.fromtimestamp(stat.st_mtime),
            "created_time": datetime.fromtimestamp(stat.st_ctime),
            "file_extension": path.suffix,
            "parent_directory": str(path.parent)
        }
    except Exception as e:
        logger.warning(f"Could not get metadata for file {file_path}: {e}")
        return {}


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def safe_divide(numerator: Union[int, float], denominator: Union[int, float]) -> float:
    """
    Safely divide two numbers, returning 0 if denominator is 0.
    
    Args:
        numerator: Numerator
        denominator: Denominator
        
    Returns:
        Result of division or 0 if denominator is 0
    """
    return numerator / denominator if denominator != 0 else 0.0


def calculate_success_rate(successful: int, total: int) -> float:
    """
    Calculate success rate as percentage.
    
    Args:
        successful: Number of successful items
        total: Total number of items
        
    Returns:
        Success rate as percentage (0-100)
    """
    return safe_divide(successful, total) * 100


def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """
    Flatten a nested dictionary.
    
    Args:
        d: Dictionary to flatten
        parent_key: Parent key prefix
        sep: Separator for keys
        
    Returns:
        Flattened dictionary
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def create_batch_id(file_path: str, timestamp: datetime = None) -> str:
    """
    Create a unique batch ID for a file processing operation.
    
    Args:
        file_path: Path of the file being processed
        timestamp: Optional timestamp (uses current time if not provided)
        
    Returns:
        Unique batch ID
    """
    if timestamp is None:
        timestamp = datetime.now()
    
    file_name = Path(file_path).stem
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    
    return f"{file_name}_{timestamp_str}"


def validate_dataframe_schema(df, required_columns: List[str]) -> bool:
    """
    Validate that a DataFrame contains all required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        True if all required columns are present
    """
    if not PYSPARK_AVAILABLE:
        logger.warning("PySpark not available, skipping DataFrame validation")
        return True
        
    df_columns = set(df.columns)
    required_columns_set = set(required_columns)
    
    missing_columns = required_columns_set - df_columns
    
    if missing_columns:
        logger.warning(f"Missing required columns: {missing_columns}")
        return False
    
    return True


def check_data_quality(df, checks: Dict[str, Any]) -> Dict[str, bool]:
    """
    Perform basic data quality checks on a DataFrame.
    
    Args:
        df: DataFrame to check
        checks: Dictionary of checks to perform
        
    Returns:
        Dictionary with check results
    """
    if not PYSPARK_AVAILABLE:
        logger.warning("PySpark not available, skipping data quality checks")
        return {"skipped": True}
        
    results = {}
    
    try:
        total_rows = df.count()
        
        # Check for empty DataFrame
        if 'not_empty' in checks:
            results['not_empty'] = total_rows > 0
        
        # Check for null values in specific columns
        if 'no_nulls' in checks:
            no_null_columns = checks['no_nulls']
            if isinstance(no_null_columns, str):
                no_null_columns = [no_null_columns]
            
            for column in no_null_columns:
                if column in df.columns:
                    null_count = df.filter(col(column).isNull()).count()
                    results[f'no_nulls_{column}'] = null_count == 0
        
        # Check for duplicate rows
        if 'no_duplicates' in checks:
            distinct_rows = df.distinct().count()
            results['no_duplicates'] = distinct_rows == total_rows
        
        # Check minimum row count
        if 'min_rows' in checks:
            min_rows = checks['min_rows']
            results['min_rows'] = total_rows >= min_rows
        
        # Check maximum row count
        if 'max_rows' in checks:
            max_rows = checks['max_rows']
            results['max_rows'] = total_rows <= max_rows
            
    except Exception as e:
        logger.error(f"Error performing data quality checks: {e}")
        results['error'] = str(e)
    
    return results


def sanitize_column_names(df, strategy: str = 'lowercase'):
    """
    Sanitize DataFrame column names.
    
    Args:
        df: DataFrame to sanitize
        strategy: Sanitization strategy ('lowercase', 'snake_case', 'camel_case')
        
    Returns:
        DataFrame with sanitized column names
    """
    if not PYSPARK_AVAILABLE:
        logger.warning("PySpark not available, returning original DataFrame")
        return df
        
    if strategy == 'lowercase':
        new_columns = [col.lower() for col in df.columns]
    elif strategy == 'snake_case':
        new_columns = []
        for col in df.columns:
            # Convert to snake_case
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col)
            s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
            new_columns.append(s2)
    elif strategy == 'camel_case':
        new_columns = []
        for col in df.columns:
            # Convert to camelCase
            components = col.split('_')
            camel_case = components[0].lower() + ''.join(x.capitalize() for x in components[1:])
            new_columns.append(camel_case)
    else:
        raise ValueError(f"Unsupported sanitization strategy: {strategy}")
    
    # Rename columns
    for old_name, new_name in zip(df.columns, new_columns):
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    
    return df


def add_row_hash(df, columns: List[str] = None, hash_column: str = 'row_hash'):
    """
    Add a hash column to the DataFrame for change detection.
    
    Args:
        df: DataFrame to add hash to
        columns: Columns to include in hash (all columns if None)
        hash_column: Name of the hash column
        
    Returns:
        DataFrame with hash column added
    """
    if not PYSPARK_AVAILABLE:
        logger.warning("PySpark not available, returning original DataFrame")
        return df
        
    from pyspark.sql.functions import hash as spark_hash, concat_ws
    
    if columns is None:
        columns = df.columns
    
    # Create hash from specified columns
    hash_expr = spark_hash(concat_ws('|', *[col(c) for c in columns]))
    
    return df.withColumn(hash_column, hash_expr)


def estimate_dataframe_size(df) -> Dict[str, Any]:
    """
    Estimate the size and characteristics of a DataFrame.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with size estimates
    """
    if not PYSPARK_AVAILABLE:
        logger.warning("PySpark not available, returning empty estimates")
        return {"error": "PySpark not available"}
        
    try:
        row_count = df.count()
        column_count = len(df.columns)
        
        # Sample a small portion to estimate row size
        sample_df = df.sample(0.01, seed=42).limit(1000)
        sample_count = sample_df.count()
        
        if sample_count > 0:
            # This is a rough estimation
            estimated_row_size = 100  # Rough estimate in bytes
            estimated_total_size = row_count * estimated_row_size
        else:
            estimated_total_size = 0
        
        return {
            "row_count": row_count,
            "column_count": column_count,
            "estimated_size_bytes": estimated_total_size,
            "estimated_size_mb": round(estimated_total_size / (1024 * 1024), 2),
            "sample_count": sample_count
        }
        
    except Exception as e:
        logger.warning(f"Could not estimate DataFrame size: {e}")
        return {"error": str(e)}


def create_processing_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create a summary of processing results.
    
    Args:
        results: List of processing result dictionaries
        
    Returns:
        Summary dictionary
    """
    if not results:
        return {
            "total_files": 0,
            "successful_files": 0,
            "failed_files": 0,
            "success_rate": 0.0,
            "total_records_processed": 0,
            "total_execution_time": 0.0
        }
    
    total_files = len(results)
    successful_files = sum(1 for r in results if r.get('success', False))
    failed_files = total_files - successful_files
    
    total_records = sum(r.get('metrics', {}).get('total_records_processed', 0) for r in results)
    total_time = sum(r.get('execution_time', 0) for r in results)
    
    return {
        "total_files": total_files,
        "successful_files": successful_files,
        "failed_files": failed_files,
        "success_rate": calculate_success_rate(successful_files, total_files),
        "total_records_processed": total_records,
        "total_execution_time": total_time,
        "average_execution_time": safe_divide(total_time, total_files),
        "records_per_second": safe_divide(total_records, total_time) if total_time > 0 else 0
    }