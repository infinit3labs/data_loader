"""
Main entry point for the Databricks Data Loader.

This script can be run as a Databricks job and handles command-line arguments,
configuration loading, and orchestration of the data loading process.
"""

import sys
import json
import argparse
from typing import Optional, Dict, Any
from pathlib import Path
import typer
from loguru import logger

from data_loader.config.table_config import DataLoaderConfig, EXAMPLE_CONFIG
from data_loader.utils.logger import setup_logging, DataLoaderLogger


app = typer.Typer(help="Databricks Data Loader - Parallel file processing with multiple loading strategies")


@app.command()
def run(
    config_file: str = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to configuration JSON file"
    ),
    config_json: str = typer.Option(
        None,
        "--config-json",
        help="Configuration as JSON string"
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    ),
    log_file: str = typer.Option(
        None,
        "--log-file",
        help="Path to log file (optional)"
    ),
    tables: str = typer.Option(
        None,
        "--tables",
        help="Comma-separated list of table names to process (all if not specified)"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Perform dry run without actually loading data"
    ),
    optimize: bool = typer.Option(
        False,
        "--optimize",
        help="Run table optimization after loading"
    ),
    vacuum: bool = typer.Option(
        False,
        "--vacuum",
        help="Run table vacuum after loading"
    ),
    cleanup_days: int = typer.Option(
        30,
        "--cleanup-days",
        help="Days to keep in file tracking table cleanup"
    )
):
    """Run the data loader with specified configuration."""
    
    # Setup logging
    setup_logging(log_level=log_level.upper(), log_file=log_file)
    data_logger = DataLoaderLogger("main")
    
    try:
        # Load configuration
        config = load_configuration(config_file, config_json)
        
        # Filter tables if specified
        if tables:
            table_list = [t.strip() for t in tables.split(",")]
            config.tables = [t for t in config.tables if t.table_name in table_list]
            logger.info(f"Processing only specified tables: {table_list}")
        
        # Initialize processor
        from data_loader.core.processor import DataProcessor
        processor = DataProcessor(config)
        
        if dry_run:
            logger.info("=== DRY RUN MODE - No data will be loaded ===")
            
            # Just discover files and report what would be processed
            for table_config in config.tables:
                discovered_files = processor.discover_files(table_config)
                logger.info(f"Table {table_config.table_name}: {len(discovered_files)} files would be processed")
                
                for file_path in discovered_files[:5]:  # Show first 5 files
                    logger.info(f"  - {file_path}")
                
                if len(discovered_files) > 5:
                    logger.info(f"  ... and {len(discovered_files) - 5} more files")
        
        else:
            # Run actual processing
            logger.info("Starting data loading process")
            results = processor.process_all_tables()
            
            # Log results
            data_logger.log_parallel_execution_complete(
                total_tasks=results["total_files_processed"],
                successful=results["successful_files"],
                failed=results["failed_files"],
                execution_time=results["total_processing_time"]
            )
            
            # Print summary
            print_processing_summary(results)
            
            # Run optimization if requested
            if optimize:
                logger.info("Running table optimization")
                processor.optimize_all_tables()
            
            # Run vacuum if requested
            if vacuum:
                logger.info("Running table vacuum")
                processor.vacuum_all_tables()
            
            # Cleanup old records
            if cleanup_days > 0:
                processor.cleanup_old_records(cleanup_days)
        
        logger.info("Data loader execution completed successfully")
        
    except Exception as e:
        logger.error(f"Data loader execution failed: {e}")
        raise typer.Exit(1)


@app.command()
def status(
    config_file: str = typer.Option(
        None,
        "--config",
        "-c", 
        help="Path to configuration JSON file"
    ),
    config_json: str = typer.Option(
        None,
        "--config-json",
        help="Configuration as JSON string"
    )
):
    """Show current processing status and statistics."""
    
    setup_logging(log_level="INFO")
    
    try:
        config = load_configuration(config_file, config_json)
        from data_loader.core.processor import DataProcessor
        processor = DataProcessor(config)
        
        status_info = processor.get_processing_status()
        
        print("\n=== DATA LOADER STATUS ===")
        print(f"Configuration: {len(config.tables)} tables configured")
        print(f"Max parallel jobs: {config.max_parallel_jobs}")
        print(f"Retry attempts: {config.retry_attempts}")
        
        print("\n=== OVERALL STATISTICS ===")
        overall_stats = status_info["overall_statistics"]
        for status, count in overall_stats.items():
            print(f"{status.title()}: {count}")
        
        print("\n=== TABLE STATISTICS ===")
        table_stats = status_info["table_statistics"]
        for table_name, stats in table_stats.items():
            print(f"\n{table_name}:")
            for status, count in stats.items():
                print(f"  {status.title()}: {count}")
        
    except Exception as e:
        logger.error(f"Failed to get status: {e}")
        raise typer.Exit(1)


@app.command()
def create_example_config(
    output_file: str = typer.Option(
        "data_loader_config.json",
        "--output",
        "-o",
        help="Output file path for example configuration"
    )
):
    """Create an example configuration file."""
    
    try:
        with open(output_file, 'w') as f:
            json.dump(EXAMPLE_CONFIG, f, indent=2, default=str)
        
        print(f"Example configuration created: {output_file}")
        print("Edit this file to match your environment and table requirements.")
        
    except Exception as e:
        logger.error(f"Failed to create example config: {e}")
        raise typer.Exit(1)


def load_configuration(config_file: Optional[str] = None, 
                      config_json: Optional[str] = None) -> DataLoaderConfig:
    """
    Load configuration from file or JSON string.
    
    Args:
        config_file: Path to configuration file
        config_json: Configuration as JSON string
        
    Returns:
        Loaded configuration
    """
    if config_json:
        config_dict = json.loads(config_json)
    elif config_file:
        if not Path(config_file).exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            config_dict = json.load(f)
    else:
        logger.warning("No configuration provided, using example configuration")
        config_dict = EXAMPLE_CONFIG
    
    return DataLoaderConfig(**config_dict)


def print_processing_summary(results: Dict[str, Any]):
    """Print a formatted summary of processing results."""
    
    print("\n" + "="*60)
    print("DATA LOADER EXECUTION SUMMARY")
    print("="*60)
    
    print(f"Total tables processed: {results['tables_processed']}")
    print(f"Total files discovered: {results['total_files_discovered']}")
    print(f"Total files processed: {results['total_files_processed']}")
    print(f"Successful files: {results['successful_files']}")
    print(f"Failed files: {results['failed_files']}")
    
    if results['total_files_processed'] > 0:
        success_rate = (results['successful_files'] / results['total_files_processed']) * 100
        print(f"Success rate: {success_rate:.1f}%")
    
    print(f"Total execution time: {results['total_processing_time']:.2f}s")
    
    if results.get('errors'):
        print(f"\nErrors encountered: {len(results['errors'])}")
        for error in results['errors'][:5]:  # Show first 5 errors
            print(f"  - {error}")
        if len(results['errors']) > 5:
            print(f"  ... and {len(results['errors']) - 5} more errors")
    
    print("\nTable-specific results:")
    for table_name, table_result in results['table_results'].items():
        if isinstance(table_result, dict) and 'files_processed' in table_result:
            print(f"  {table_name}: {table_result['successful_files']}/{table_result['files_processed']} successful")
        else:
            print(f"  {table_name}: ERROR")
    
    print("="*60)


def main():
    """Main entry point for command line usage."""
    try:
        app()
    except KeyboardInterrupt:
        logger.info("Data loader interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()