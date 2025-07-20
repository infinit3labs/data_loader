#!/usr/bin/env python3
"""
Example script demonstrating how to use the Databricks Data Loader.

This script shows how to:
1. Create a configuration
2. Initialize the data processor
3. Process files
4. Monitor progress
"""

import json
from pathlib import Path
from data_loader.config.table_config import DataLoaderConfig
from data_loader.utils.logger import setup_logging


def create_example_configuration():
    """Create an example configuration for demonstration."""
    
    config = {
        "raw_data_path": "/tmp/raw_data/",
        "processed_data_path": "/tmp/processed_data/", 
        "checkpoint_path": "/tmp/checkpoints/",
        "file_tracker_table": "file_processing_tracker",
        "file_tracker_database": "metadata",
        "max_parallel_jobs": 2,  # Reduced for example
        "retry_attempts": 2,
        "timeout_minutes": 30,
        "log_level": "INFO",
        "enable_metrics": True,
        "tables": [
            {
                "table_name": "sample_customers",
                "database_name": "demo",
                "source_path_pattern": "/tmp/raw_data/customers/*.parquet",
                "loading_strategy": "scd2",
                "primary_keys": ["customer_id"],
                "tracking_columns": ["name", "email", "city"],
                "scd2_effective_date_column": "effective_date",
                "scd2_end_date_column": "end_date", 
                "scd2_current_flag_column": "is_current",
                "file_format": "parquet",
                "schema_evolution": True,
                "partition_columns": ["country"]
            },
            {
                "table_name": "sample_events",
                "database_name": "demo",
                "source_path_pattern": "/tmp/raw_data/events/*.json",
                "loading_strategy": "append",
                "file_format": "json",
                "schema_evolution": True,
                "partition_columns": ["event_date"]
            }
        ]
    }
    
    return config


def demonstrate_data_loading():
    """Demonstrate the data loading process."""
    
    print("=== Databricks Data Loader Example ===")
    
    # Setup logging
    setup_logging(log_level="INFO")
    print("✓ Logging configured")
    
    # Create example configuration
    config_dict = create_example_configuration()
    print("✓ Configuration created")
    
    # Load configuration using Pydantic
    try:
        config = DataLoaderConfig(**config_dict)
        print(f"✓ Configuration loaded: {len(config.tables)} tables configured")
    except Exception as e:
        print(f"✗ Configuration error: {e}")
        return
    
    # Initialize the data processor
    try:
        from data_loader.core.processor import DataProcessor
        processor = DataProcessor(config)
        print("✓ Data processor initialized")
    except Exception as e:
        print(f"✗ Processor initialization error (expected without PySpark): {e}")
        processor = None
    
    # Create sample directories (in real Databricks, these would be DBFS paths)
    sample_dirs = [
        "/tmp/raw_data/customers",
        "/tmp/raw_data/events",
        "/tmp/processed_data",
        "/tmp/checkpoints"
    ]
    
    for dir_path in sample_dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    print("✓ Sample directories created")
    
    # Display configuration summary
    print("\n=== Configuration Summary ===")
    print(f"Raw data path: {config.raw_data_path}")
    print(f"Processed data path: {config.processed_data_path}")
    print(f"Max parallel jobs: {config.max_parallel_jobs}")
    print(f"Retry attempts: {config.retry_attempts}")
    
    print("\nConfigured tables:")
    for table in config.tables:
        print(f"  - {table.table_name} ({table.loading_strategy})")
        print(f"    Source: {table.source_path_pattern}")
        print(f"    Target: {table.database_name}.{table.table_name}")
    
    # Get processing status
    print("\n=== Processing Status ===")
    if processor:
        try:
            status = processor.get_processing_status()
            print("Overall statistics:")
            for status_type, count in status["overall_statistics"].items():
                print(f"  {status_type}: {count}")
            
            print("\nTable statistics:")
            for table_name, stats in status["table_statistics"].items():
                print(f"  {table_name}:")
                for stat_type, count in stats.items():
                    print(f"    {stat_type}: {count}")
                    
        except Exception as e:
            print(f"Status check completed (requires PySpark): {e}")
    else:
        print("Processor not available (requires PySpark environment)")
    
    # Demonstrate file discovery (without actual processing in this example)
    print("\n=== File Discovery Demo ===")
    if processor:
        for table_config in config.tables:
            try:
                discovered_files = processor.discover_files(table_config)
                print(f"{table_config.table_name}: {len(discovered_files)} files found")
                
                # Show first few files if any found
                for i, file_path in enumerate(discovered_files[:3]):
                    print(f"  - {file_path}")
                
                if len(discovered_files) > 3:
                    print(f"  ... and {len(discovered_files) - 3} more files")
                    
            except Exception as e:
                print(f"Discovery for {table_config.table_name}: No files found (expected in demo)")
    else:
        print("File discovery requires PySpark environment")
    
    print("\n=== Strategy Information ===")
    for table_config in config.tables:
        print(f"\n{table_config.table_name}:")
        print(f"  Strategy: {table_config.loading_strategy}")
        print(f"  File format: {table_config.file_format}")
        print(f"  Schema evolution: {table_config.schema_evolution}")
        
        if table_config.loading_strategy == "scd2":
            print(f"  Primary keys: {table_config.primary_keys}")
            print(f"  Tracking columns: {table_config.tracking_columns}")
        
        if table_config.partition_columns:
            print(f"  Partition columns: {table_config.partition_columns}")
    
    # Save configuration for reference
    config_file = "/tmp/example_config.json"
    with open(config_file, 'w') as f:
        json.dump(config_dict, f, indent=2)
    print(f"\n✓ Example configuration saved to: {config_file}")
    
    print("\n=== Next Steps ===")
    print("To run the data loader with this configuration:")
    print(f"1. python -m data_loader.main run --config {config_file}")
    print("2. python -m data_loader.main status --config {config_file}")
    print("3. python -m data_loader.main run --config {config_file} --dry-run")
    
    print("\nFor Databricks deployment:")
    print("1. Upload this package to Databricks workspace or DBFS")
    print("2. Create a job with main.py as the entry point")
    print("3. Set up file triggers for automatic processing")
    print("4. Configure cluster with Delta Lake and PySpark")


if __name__ == "__main__":
    demonstrate_data_loading()