"""
Main entry point for the Databricks Data Loader.

This script can be run as a Databricks job and handles command-line arguments,
configuration loading, and orchestration of the data loading process.
"""

import sys
import json
import yaml
import argparse
from typing import Optional, Dict, Any
from pathlib import Path
import typer
from loguru import logger

from data_loader.config.table_config import (
    DataLoaderConfig,
    EXAMPLE_CONFIG,
    load_runtime_config,
)
from data_loader.utils.logger import setup_logging, DataLoaderLogger
from data_loader.cluster import ClusterConfig, ClusterDataProcessor, DatabricksEnvironment


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
def run_cluster(
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
    ),
    enable_cluster_optimizations: bool = typer.Option(
        True,
        "--cluster-optimizations/--no-cluster-optimizations",
        help="Enable cluster-specific optimizations"
    ),
    use_unity_catalog: bool = typer.Option(
        False,
        "--unity-catalog/--no-unity-catalog",
        help="Use Unity Catalog for table operations"
    ),
    enable_monitoring: bool = typer.Option(
        True,
        "--monitoring/--no-monitoring",
        help="Enable cluster resource monitoring"
    )
):
    """Run the data loader with cluster-specific optimizations in Databricks."""
    
    # Setup logging
    setup_logging(log_level=log_level.upper(), log_file=log_file)
    data_logger = DataLoaderLogger("cluster_main")
    
    try:
        # Load base configuration
        base_config = load_configuration(config_file, config_json)
        
        # Detect Databricks environment
        environment = DatabricksEnvironment.detect_environment()
        
        if not environment.is_databricks_environment():
            logger.warning("Not running in Databricks environment - cluster optimizations may not be effective")
        
        # Create cluster configuration
        cluster_config = ClusterConfig.from_base_config(
            base_config=base_config,
            environment=environment,
            enable_cluster_optimizations=enable_cluster_optimizations,
            use_unity_catalog=use_unity_catalog,
            enable_performance_monitoring=enable_monitoring
        )
        
        # Filter tables if specified
        if tables:
            table_list = [t.strip() for t in tables.split(",")]
            cluster_config.base_config.tables = [t for t in cluster_config.base_config.tables if t.table_name in table_list]
            logger.info(f"Processing only specified tables: {table_list}")
        
        # Initialize cluster processor
        processor = ClusterDataProcessor(cluster_config)
        
        # Validate cluster configuration
        cluster_validation = processor.validate_cluster_configuration()
        if not cluster_validation['valid']:
            logger.error(f"Cluster validation failed: {cluster_validation['errors']}")
            raise typer.Exit(1)
        
        if cluster_validation['warnings']:
            for warning in cluster_validation['warnings']:
                logger.warning(warning)
        
        if dry_run:
            logger.info("=== DRY RUN MODE - No data will be loaded ===")
            
            # Show cluster status
            cluster_status = processor.get_cluster_status()
            logger.info(f"Cluster ID: {cluster_status['environment']['cluster_id']}")
            logger.info(f"Cluster Mode: {cluster_status['environment']['cluster_mode']}")
            logger.info(f"Worker Count: {cluster_status['environment']['worker_count']}")
            logger.info(f"Unity Catalog: {cluster_status['environment']['unity_catalog_enabled']}")
            
            # Just discover files and report what would be processed
            for table_config in cluster_config.base_config.tables:
                discovered_files = processor.discover_files(table_config)
                logger.info(f"Table {table_config.table_name}: {len(discovered_files)} files would be processed")
                
                for file_path in discovered_files[:5]:  # Show first 5 files
                    logger.info(f"  - {file_path}")
                
                if len(discovered_files) > 5:
                    logger.info(f"  ... and {len(discovered_files) - 5} more files")
        
        else:
            # Run actual processing with cluster optimizations
            logger.info("Starting cluster-optimized data loading process")
            results = processor.process_all_tables()
            
            # Log results with cluster metrics
            data_logger.log_parallel_execution_complete(
                total_tasks=results["total_files_processed"],
                successful=results["successful_files"],
                failed=results["failed_files"],
                execution_time=results["total_processing_time"]
            )
            
            # Print enhanced summary with cluster information
            print_cluster_processing_summary(results, cluster_config, environment)
            
            # Run optimization if requested
            if optimize:
                logger.info("Running cluster-optimized table optimization")
                processor.optimize_all_tables(background=False)
            
            # Run vacuum if requested
            if vacuum:
                logger.info("Running table vacuum")
                processor.vacuum_all_tables()
            
            # Cleanup old records
            if cleanup_days > 0:
                processor.cleanup_old_records(cleanup_days)
        
        logger.info("Cluster data loader execution completed successfully")
        
    except Exception as e:
        logger.error(f"Cluster data loader execution failed: {e}")
        raise typer.Exit(1)


@app.command()
def cluster_status(
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
    """Show cluster status and configuration for Databricks environments."""
    
    setup_logging(log_level="INFO")
    
    try:
        # Load base configuration
        base_config = load_configuration(config_file, config_json)
        
        # Detect environment and create cluster config
        environment = DatabricksEnvironment.detect_environment()
        cluster_config = ClusterConfig.from_base_config(base_config, environment)
        
        # Initialize processor to get status
        processor = ClusterDataProcessor(cluster_config)
        cluster_status = processor.get_cluster_status()
        
        print("\n=== DATABRICKS CLUSTER STATUS ===")
        
        # Environment information
        env = cluster_status['environment']
        print(f"Cluster ID: {env['cluster_id']}")
        print(f"Cluster Name: {env['cluster_name']}")
        print(f"Runtime Version: {env['runtime_version']}")
        print(f"Cluster Mode: {env['cluster_mode']}")
        print(f"Worker Count: {env['worker_count']}")
        print(f"Unity Catalog: {env['unity_catalog_enabled']}")
        
        # Configuration
        config = cluster_status['configuration']
        print(f"\n=== CLUSTER CONFIGURATION ===")
        print(f"Optimizations Enabled: {config['optimizations_enabled']}")
        print(f"Unity Catalog Enabled: {config['unity_catalog_enabled']}")
        print(f"Job Dependencies Enabled: {config['job_dependencies_enabled']}")
        print(f"Max Parallel Jobs: {config['max_parallel_jobs']}")
        
        # Resource information
        if 'resources' in cluster_status:
            resources = cluster_status['resources']
            print(f"\n=== CLUSTER RESOURCES ===")
            print(f"Total Executors: {resources.get('total_executors', 'N/A')}")
            print(f"Active Executors: {resources.get('active_executors', 'N/A')}")
            print(f"Total Cores: {resources.get('total_cores', 'N/A')}")
            print(f"CPU Usage: {resources.get('system_cpu_percent', 'N/A')}%")
            print(f"Memory Usage: {resources.get('system_memory_percent', 'N/A')}%")
        
        # Health status
        if 'resource_health' in cluster_status:
            health = cluster_status['resource_health']
            print(f"\n=== CLUSTER HEALTH ===")
            print(f"Overall Health: {health['overall_health'].upper()}")
            
            if health['warnings']:
                print("Warnings:")
                for warning in health['warnings']:
                    print(f"  - {warning}")
            
            if health['critical_issues']:
                print("Critical Issues:")
                for issue in health['critical_issues']:
                    print(f"  - {issue}")
        
        # Processing status
        processing = cluster_status['processing_status']
        print(f"\n=== DATA PROCESSING STATUS ===")
        overall_stats = processing["overall_statistics"]
        for status, count in overall_stats.items():
            print(f"{status.title()}: {count}")
        
    except Exception as e:
        logger.error(f"Failed to get cluster status: {e}")
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
        "data_loader_config.yaml",
        "--output",
        "-o",
        help="Output file path for example configuration"
    )
):
    """Create an example configuration file."""
    
    try:
        with open(output_file, 'w') as f:
            yaml.safe_dump(EXAMPLE_CONFIG, f, sort_keys=False)
        
        print(f"Example configuration created: {output_file}")
        print("Edit this file to match your environment and table requirements.")
        
    except Exception as e:
        logger.error(f"Failed to create example config: {e}")
        raise typer.Exit(1)


def load_configuration(config_file: Optional[str] = None,
                      config_json: Optional[str] = None) -> DataLoaderConfig:
    """Load configuration from YAML file, environment, and overrides."""

    if config_json:
        data = yaml.safe_load(config_json)
        return DataLoaderConfig(**data)

    if not config_file:
        logger.warning("No configuration file provided, using example configuration")
        return DataLoaderConfig(**EXAMPLE_CONFIG)

    return load_runtime_config(config_file)


def print_cluster_processing_summary(results: Dict[str, Any], cluster_config, environment):
    """Print a formatted summary of cluster processing results."""
    
    print("\n" + "="*70)
    print("CLUSTER DATA LOADER EXECUTION SUMMARY")
    print("="*70)
    
    # Cluster information
    print(f"Cluster ID: {environment.cluster_id}")
    print(f"Cluster Mode: {cluster_config.cluster_mode.value}")
    print(f"Optimizations Enabled: {cluster_config.enable_cluster_optimizations}")
    print(f"Unity Catalog: {cluster_config.use_unity_catalog}")
    
    print(f"\nTotal tables processed: {results['tables_processed']}")
    print(f"Total files discovered: {results['total_files_discovered']}")
    print(f"Total files processed: {results['total_files_processed']}")
    print(f"Successful files: {results['successful_files']}")
    print(f"Failed files: {results['failed_files']}")
    
    if results['total_files_processed'] > 0:
        success_rate = (results['successful_files'] / results['total_files_processed']) * 100
        print(f"Success rate: {success_rate:.1f}%")
    
    print(f"Total execution time: {results['total_processing_time']:.2f}s")
    
    # Cluster-specific metrics
    if 'cluster_metrics' in results:
        cluster_metrics = results['cluster_metrics']
        print(f"\n=== CLUSTER METRICS ===")
        for key, value in cluster_metrics.items():
            print(f"{key}: {value}")
    
    # Resource usage
    if 'resource_usage' in results:
        resource_usage = results['resource_usage']
        print(f"\n=== RESOURCE USAGE ===")
        efficiency = resource_usage.get('resource_efficiency', {})
        if 'efficiency_score' in efficiency:
            print(f"Resource Efficiency Score: {efficiency['efficiency_score']:.1f}/100")
    
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
    
    print("="*70)


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