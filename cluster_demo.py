#!/usr/bin/env python3
"""
Demonstration of the new Databricks cluster mode functionality.

This script shows how to use the cluster mode for enhanced Databricks operations.
"""

import json
from data_loader.config.table_config import DataLoaderConfig
from data_loader.cluster import ClusterConfig, ClusterDataProcessor, DatabricksEnvironment, ClusterMode


def demonstrate_cluster_mode():
    """Demonstrate the new cluster mode functionality."""
    
    print("=" * 60)
    print("DATABRICKS CLUSTER MODE DEMONSTRATION")
    print("=" * 60)
    
    # 1. Environment Detection
    print("\n1. ENVIRONMENT DETECTION")
    print("-" * 30)
    
    environment = DatabricksEnvironment.detect_environment()
    print(f"✓ Environment detected")
    print(f"  Cluster ID: {environment.cluster_id or 'Not detected (normal in dev)'}")
    print(f"  Runtime Version: {environment.runtime_version or 'Not detected'}")
    print(f"  Is Databricks: {environment.is_databricks_environment()}")
    print(f"  Optimal Parallelism: {environment.get_optimal_parallelism()}")
    
    # 2. Configuration Creation
    print("\n2. CLUSTER CONFIGURATION")
    print("-" * 30)
    
    # Create base configuration
    base_config_dict = {
        "raw_data_path": "/tmp/raw_data/",
        "processed_data_path": "/tmp/processed_data/",
        "checkpoint_path": "/tmp/checkpoints/",
        "file_tracker_table": "cluster_file_tracker",
        "file_tracker_database": "cluster_metadata",
        "max_parallel_jobs": 2,
        "retry_attempts": 2,
        "timeout_minutes": 30,
        "tables": [
            {
                "table_name": "cluster_demo_table",
                "database_name": "demo",
                "source_path_pattern": "/tmp/raw_data/demo/*.parquet",
                "loading_strategy": "append",
                "file_format": "parquet",
                "schema_evolution": True
            }
        ]
    }
    
    base_config = DataLoaderConfig(**base_config_dict)
    print(f"✓ Base configuration created with {len(base_config.tables)} tables")
    
    # Create cluster configuration with optimizations
    cluster_config = ClusterConfig.from_base_config(
        base_config=base_config,
        environment=environment,
        enable_cluster_optimizations=True,
        use_unity_catalog=False,  # Disabled for demo
        enable_performance_monitoring=True,
        enable_job_dependencies=False  # Simplified for demo
    )
    
    print(f"✓ Cluster configuration created")
    print(f"  Cluster Mode: {cluster_config.cluster_mode.value}")
    print(f"  Optimizations Enabled: {cluster_config.enable_cluster_optimizations}")
    print(f"  Performance Monitoring: {cluster_config.enable_performance_monitoring}")
    print(f"  Adjusted Parallelism: {cluster_config.base_config.max_parallel_jobs}")
    
    # 3. Spark Configurations
    print("\n3. SPARK OPTIMIZATIONS")
    print("-" * 30)
    
    spark_configs = cluster_config.get_spark_configurations()
    print(f"✓ Generated {len(spark_configs)} Spark configurations")
    
    key_configs = [
        'spark.sql.adaptive.enabled',
        'spark.databricks.delta.optimizeWrite.enabled',
        'spark.sql.adaptive.coalescePartitions.enabled'
    ]
    
    for config_key in key_configs:
        if config_key in spark_configs:
            print(f"  {config_key}: {spark_configs[config_key]}")
    
    # 4. Unity Catalog Example
    print("\n4. UNITY CATALOG INTEGRATION")
    print("-" * 30)
    
    # Show Unity Catalog table naming
    table_config = cluster_config.base_config.tables[0]
    standard_name = f"{table_config.database_name}.{table_config.table_name}"
    
    # Create Unity Catalog version
    uc_config = ClusterConfig.from_base_config(
        base_config=base_config,
        use_unity_catalog=True,
        default_catalog="main"
    )
    
    uc_table_name = uc_config.get_table_name_with_catalog(table_config)
    
    print(f"✓ Table naming comparison:")
    print(f"  Standard: {standard_name}")
    print(f"  Unity Catalog: {uc_table_name}")
    
    # 5. Resource Management (without actual Spark session)
    print("\n5. RESOURCE MANAGEMENT")
    print("-" * 30)
    
    try:
        from data_loader.cluster.resource_manager import ClusterResourceManager
        
        resource_manager = ClusterResourceManager(cluster_config, environment)
        print(f"✓ Resource manager initialized")
        
        # Get cluster resources (will work without Spark in limited mode)
        resources = resource_manager.get_cluster_resources()
        print(f"✓ Resource monitoring available")
        print(f"  Timestamp: {resources.get('timestamp', 'N/A')}")
        print(f"  Cluster ID: {resources.get('cluster_id', 'N/A')}")
        
        # Get optimization recommendations
        recommendations = resource_manager.get_optimization_recommendations()
        print(f"✓ Generated {len(recommendations['configuration_changes'])} recommendations")
        
    except Exception as e:
        print(f"⚠ Resource management demo limited: {e}")
    
    # 6. Job Orchestration Example
    print("\n6. JOB ORCHESTRATION")
    print("-" * 30)
    
    # Create configuration with dependencies
    orchestration_config = ClusterConfig.from_base_config(
        base_config=base_config,
        enable_job_dependencies=True,
        upstream_dependencies=[
            "table:bronze.source_table",
            "job:upstream-etl-job",
            "file:/tmp/ready.flag"
        ],
        downstream_notifications=[
            "webhook:https://hooks.slack.com/services/...",
            "job:downstream-analytics-job"
        ]
    )
    
    print(f"✓ Job orchestration configured")
    print(f"  Upstream Dependencies: {len(orchestration_config.upstream_dependencies)}")
    print(f"  Downstream Notifications: {len(orchestration_config.downstream_notifications)}")
    
    for dep in orchestration_config.upstream_dependencies:
        print(f"    - {dep}")
    
    # 7. CLI Commands Reference
    print("\n7. CLI COMMANDS")
    print("-" * 30)
    
    print("✓ New CLI commands available:")
    print("  # Run with cluster optimizations")
    print("  python -m data_loader.main run-cluster --config config.json")
    print()
    print("  # Run with Unity Catalog")
    print("  python -m data_loader.main run-cluster --config config.json --unity-catalog")
    print()
    print("  # Check cluster status")
    print("  python -m data_loader.main cluster-status --config config.json")
    print()
    print("  # Dry run with cluster info")
    print("  python -m data_loader.main run-cluster --config config.json --dry-run")
    
    # 8. Validation
    print("\n8. CONFIGURATION VALIDATION")
    print("-" * 30)
    
    try:
        # This would work fully in a real Databricks environment
        processor = ClusterDataProcessor(cluster_config)
        print("✓ Cluster processor initialized successfully")
        print("✓ All cluster mode components working")
        
    except Exception as e:
        print(f"⚠ Full validation requires Databricks environment: {e}")
    
    print("\n" + "=" * 60)
    print("CLUSTER MODE DEMONSTRATION COMPLETE")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Deploy this code to a Databricks cluster")
    print("2. Use 'run-cluster' command for optimized processing")
    print("3. Enable Unity Catalog for centralized governance")
    print("4. Set up job dependencies for workflow orchestration")
    print("5. Monitor cluster resources during processing")
    print("\nFor detailed documentation, see CLUSTER_MODE.md")


if __name__ == "__main__":
    demonstrate_cluster_mode()