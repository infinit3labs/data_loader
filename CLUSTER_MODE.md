# Databricks Cluster Mode

This document describes the new cluster mode functionality for running the data loader in Databricks cluster environments with optimized configuration and resource management.

## Overview

The cluster mode provides enhanced functionality for running in Databricks environments including:

- **Automatic Environment Detection**: Detects Databricks runtime and cluster configuration
- **Cluster-Specific Optimizations**: Applies Spark optimizations based on cluster type and size
- **Resource Management**: Monitors and optimizes resource usage during processing
- **Unity Catalog Support**: Seamless integration with Unity Catalog
- **Job Orchestration**: Dependency management and notification systems
- **Enhanced Monitoring**: Cluster-aware metrics and health monitoring

## Architecture

The cluster mode extends the base data loader with additional components:

```
data_loader/cluster/
├── cluster_config.py        # Environment detection and configuration
├── cluster_processor.py     # Cluster-optimized data processor
├── resource_manager.py      # Resource monitoring and optimization
└── job_orchestrator.py      # Dependency and workflow management
```

## Usage

### Command Line Interface

#### Basic Cluster Mode

```bash
# Run with cluster optimizations (auto-detected configuration)
python -m data_loader.main run-cluster --config config.json

# Run with Unity Catalog enabled
python -m data_loader.main run-cluster --config config.json --unity-catalog

# Dry run to see cluster configuration
python -m data_loader.main run-cluster --config config.json --dry-run
```

#### Advanced Options

```bash
# Disable cluster optimizations
python -m data_loader.main run-cluster --config config.json --no-cluster-optimizations

# Enable monitoring with optimization
python -m data_loader.main run-cluster --config config.json --optimize --monitoring

# Run specific tables only
python -m data_loader.main run-cluster --config config.json --tables "customers,orders"
```

#### Cluster Status

```bash
# Check cluster status and configuration
python -m data_loader.main cluster-status --config config.json

# Get status with default configuration
python -m data_loader.main cluster-status
```

### Programmatic Usage

```python
from data_loader.config.table_config import DataLoaderConfig
from data_loader.cluster import ClusterConfig, ClusterDataProcessor, DatabricksEnvironment

# Load base configuration
base_config = DataLoaderConfig(**config_dict)

# Detect environment and create cluster configuration
environment = DatabricksEnvironment.detect_environment()
cluster_config = ClusterConfig.from_base_config(
    base_config=base_config,
    environment=environment,
    enable_cluster_optimizations=True,
    use_unity_catalog=True
)

# Initialize cluster processor
processor = ClusterDataProcessor(cluster_config)

# Validate configuration
validation = processor.validate_cluster_configuration()
if not validation['valid']:
    raise ValueError(f"Configuration invalid: {validation['errors']}")

# Process data with cluster optimizations
results = processor.process_all_tables()

# Get cluster status
status = processor.get_cluster_status()
```

## Configuration

### Cluster Configuration Options

The cluster configuration extends the base configuration with cluster-specific settings:

```python
cluster_config = ClusterConfig(
    base_config=base_config,
    
    # Cluster mode settings
    cluster_mode=ClusterMode.SINGLE_USER,  # or SHARED, NO_ISOLATION_SHARED
    enable_cluster_optimizations=True,
    use_optimized_writes=True,
    enable_auto_compaction=True,
    adaptive_query_execution=True,
    
    # Resource management
    max_memory_fraction=0.8,
    shuffle_partitions=200,  # Auto-calculated if None
    broadcast_threshold="10MB",
    
    # Unity Catalog integration
    use_unity_catalog=False,
    default_catalog="main",
    default_schema=None,  # Uses database_name from table config
    
    # Job orchestration (optional)
    enable_job_dependencies=False,
    upstream_dependencies=[
        "job:upstream-job-id",
        "table:upstream_db.upstream_table",
        "file:/mnt/trigger/ready.flag"
    ],
    downstream_notifications=[
        "webhook:https://hooks.slack.com/...",
        "job:downstream-job-id"
    ],
    
    # Monitoring
    enable_cluster_metrics=True,
    enable_performance_monitoring=True
)
```

### Environment Detection

The system automatically detects the Databricks environment:

- **Cluster ID**: From Spark configuration or environment variables
- **Cluster Mode**: Interactive vs Job clusters
- **Runtime Version**: Databricks Runtime version
- **Unity Catalog**: Availability and configuration
- **Resource Allocation**: Worker count, cores, memory

### Automatic Optimizations

When cluster optimizations are enabled, the system automatically:

1. **Adjusts Parallelism**: Based on cluster size and worker count
2. **Configures Spark**: Optimizes Spark settings for the cluster
3. **Enables Delta Optimizations**: Auto-compaction, optimized writes
4. **Tunes Memory**: Adjusts memory settings based on available resources
5. **Sets Adaptive Query Execution**: Enables Spark 3.x adaptive features

## Features

### 1. Environment Detection

```python
environment = DatabricksEnvironment.detect_environment()

print(f"Cluster ID: {environment.cluster_id}")
print(f"Runtime: {environment.runtime_version}")
print(f"Workers: {environment.num_workers}")
print(f"Unity Catalog: {environment.unity_catalog_enabled}")
print(f"Optimal Parallelism: {environment.get_optimal_parallelism()}")
```

### 2. Resource Monitoring

```python
# Get real-time resource usage
resources = processor.resource_manager.get_cluster_resources()

# Monitor health
health = processor.resource_manager.get_health_status()
if health['overall_health'] != 'healthy':
    print(f"Warnings: {health['warnings']}")
    print(f"Recommendations: {health['recommendations']}")

# Get optimization recommendations
recommendations = processor.resource_manager.get_optimization_recommendations()
```

### 3. Job Dependencies

```python
# Configure job dependencies
cluster_config = ClusterConfig(
    base_config=base_config,
    enable_job_dependencies=True,
    upstream_dependencies=[
        "job:12345",  # Wait for job 12345 to complete
        "table:bronze.raw_events",  # Wait for table to have data
        "file:/mnt/landing/ready.flag"  # Wait for file to exist
    ]
)

# Check dependencies before processing
orchestrator = JobOrchestrator(cluster_config)
deps = orchestrator.check_dependencies()
if not deps['all_dependencies_met']:
    print(f"Missing: {deps['missing_dependencies']}")
```

### 4. Unity Catalog Integration

```python
# Enable Unity Catalog
cluster_config = ClusterConfig(
    base_config=base_config,
    use_unity_catalog=True,
    default_catalog="production",
    default_schema="analytics"
)

# Tables will be created as: production.analytics.table_name
# instead of: database_name.table_name
```

### 5. Cluster-Aware Optimization

```python
# Run optimizations with cluster awareness
processor.optimize_all_tables(background=True)  # For shared clusters

# Vacuum with cluster-appropriate settings
processor.vacuum_all_tables(retention_hours=168)
```

## Best Practices

### 1. Cluster Mode Selection

- **Single User**: Use for dedicated job clusters
- **Shared**: Use for interactive development with conservative resource settings
- **No Isolation Shared**: Use with caution, enable monitoring

### 2. Resource Management

```python
# Monitor resources during processing
status = processor.get_cluster_status()
if status['resource_health']['overall_health'] != 'healthy':
    # Adjust parallelism or scale cluster
    pass
```

### 3. Unity Catalog Usage

```python
# Use Unity Catalog for governance and centralized metadata
if environment.supports_unity_catalog():
    cluster_config.use_unity_catalog = True
    cluster_config.default_catalog = "main"
```

### 4. Job Orchestration

```python
# Set up comprehensive dependencies
cluster_config.upstream_dependencies = [
    "job:bronze-ingestion",           # Previous job
    "table:bronze.events",            # Source table
    "file:/mnt/config/ready.flag"     # Configuration flag
]

cluster_config.downstream_notifications = [
    "webhook:https://hooks.slack.com/team-notifications",
    "job:gold-aggregation"            # Trigger next job
]
```

## Monitoring and Troubleshooting

### 1. Cluster Status

```bash
# Get comprehensive cluster status
python -m data_loader.main cluster-status --config config.json
```

### 2. Resource Monitoring

```python
# Monitor during processing
processor.resource_manager.monitor_processing(duration_minutes=10)

# Get efficiency metrics
efficiency = processor.resource_manager.calculate_efficiency(
    initial_resources, final_resources
)
print(f"Efficiency Score: {efficiency['efficiency_score']}/100")
```

### 3. Configuration Validation

```python
# Validate before running
validation = processor.validate_cluster_configuration()
if validation['warnings']:
    for warning in validation['warnings']:
        print(f"Warning: {warning}")

if validation['recommendations']:
    for rec in validation['recommendations']:
        print(f"Recommendation: {rec}")
```

### 4. Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| High memory usage | Large batch sizes | Reduce `max_parallel_jobs` or increase cluster memory |
| Slow processing | Under-parallelization | Increase workers or adjust `shuffle_partitions` |
| Unity Catalog errors | Permissions or configuration | Check catalog permissions and configuration |
| Dependency failures | Missing upstream data | Verify dependency configuration and upstream jobs |

## Examples

### Example 1: Simple Cluster Mode

```python
from data_loader.cluster import ClusterConfig, ClusterDataProcessor

# Basic cluster configuration
config = ClusterConfig.from_base_config(
    base_config=base_config,
    enable_cluster_optimizations=True
)

processor = ClusterDataProcessor(config)
results = processor.process_all_tables()
```

### Example 2: Production Setup with Dependencies

```python
# Production configuration with full orchestration
cluster_config = ClusterConfig.from_base_config(
    base_config=base_config,
    enable_cluster_optimizations=True,
    use_unity_catalog=True,
    default_catalog="production",
    enable_job_dependencies=True,
    upstream_dependencies=[
        "job:bronze-pipeline",
        "table:bronze.customer_events"
    ],
    downstream_notifications=[
        "webhook:https://monitoring.company.com/data-pipeline",
        "job:silver-transformation"
    ]
)

processor = ClusterDataProcessor(cluster_config)

# Validate before running
validation = processor.validate_cluster_configuration()
if not validation['valid']:
    raise RuntimeError(f"Configuration invalid: {validation['errors']}")

# Process with monitoring
results = processor.process_all_tables()

# Report results
print(f"Processed {results['total_files_processed']} files")
print(f"Cluster efficiency: {results['resource_usage']['resource_efficiency']['efficiency_score']}/100")
```

### Example 3: Development with Monitoring

```python
# Development setup with resource monitoring
cluster_config = ClusterConfig.from_base_config(
    base_config=base_config,
    cluster_mode=ClusterMode.SHARED,
    enable_performance_monitoring=True,
    max_memory_fraction=0.6  # Conservative for shared cluster
)

processor = ClusterDataProcessor(cluster_config)

# Monitor resources
initial_resources = processor.resource_manager.get_cluster_resources()
results = processor.process_all_tables()
final_resources = processor.resource_manager.get_cluster_resources()

# Analyze efficiency
efficiency = processor.resource_manager.calculate_efficiency(
    initial_resources, final_resources
)
print(f"Resource efficiency: {efficiency}")
```

## API Reference

See the inline documentation in the cluster module files for complete API reference:

- `ClusterConfig`: Configuration for cluster operations
- `ClusterDataProcessor`: Main processor with cluster optimizations
- `DatabricksEnvironment`: Environment detection and configuration
- `ClusterResourceManager`: Resource monitoring and optimization
- `JobOrchestrator`: Dependency and notification management