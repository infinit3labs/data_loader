"""
Cluster configuration and environment detection for Databricks cluster context.

This module handles detection of the Databricks environment and provides
cluster-specific configuration optimizations.
"""

import os
import json
from enum import Enum
from typing import Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from loguru import logger

from ..config.table_config import DataLoaderConfig


class ClusterMode(Enum):
    """Databricks cluster access modes."""
    SINGLE_USER = "single_user"
    SHARED = "shared"
    NO_ISOLATION_SHARED = "no_isolation_shared"
    CUSTOM = "custom"


class DatabricksEnvironment(BaseModel):
    """Databricks runtime environment information."""
    
    runtime_version: Optional[str] = None
    cluster_id: Optional[str] = None
    cluster_name: Optional[str] = None
    workspace_url: Optional[str] = None
    cluster_mode: Optional[ClusterMode] = None
    driver_node_type: Optional[str] = None
    worker_node_type: Optional[str] = None
    num_workers: Optional[int] = None
    autoscale_min: Optional[int] = None
    autoscale_max: Optional[int] = None
    unity_catalog_enabled: Optional[bool] = None
    is_interactive: Optional[bool] = None
    
    @classmethod
    def detect_environment(cls) -> 'DatabricksEnvironment':
        """
        Detect the current Databricks environment from available context.
        
        Returns:
            DatabricksEnvironment with detected information
        """
        env_data = {}
        
        # Try to get cluster information from Spark context
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            
            if spark:
                spark_conf = spark.sparkContext.getConf()
                
                # Extract cluster information from Spark configuration
                env_data.update({
                    'cluster_id': spark_conf.get('spark.databricks.clusterUsageTags.clusterId'),
                    'cluster_name': spark_conf.get('spark.databricks.clusterUsageTags.clusterName'),
                    'workspace_url': spark_conf.get('spark.databricks.workspaceUrl'),
                    'runtime_version': spark_conf.get('spark.databricks.clusterUsageTags.sparkVersion'),
                    'driver_node_type': spark_conf.get('spark.databricks.clusterUsageTags.driverNodeTypeId'),
                    'worker_node_type': spark_conf.get('spark.databricks.clusterUsageTags.nodeTypeId'),
                })
                
                # Detect cluster mode
                cluster_source = spark_conf.get('spark.databricks.clusterUsageTags.clusterSource', '')
                if 'INTERACTIVE' in cluster_source.upper():
                    env_data['is_interactive'] = True
                    env_data['cluster_mode'] = ClusterMode.SHARED
                elif 'JOB' in cluster_source.upper():
                    env_data['is_interactive'] = False
                    env_data['cluster_mode'] = ClusterMode.SINGLE_USER
                
                # Check for Unity Catalog
                unity_enabled = spark_conf.get('spark.databricks.unityCatalog.enabled', 'false')
                env_data['unity_catalog_enabled'] = unity_enabled.lower() == 'true'
                
                # Get cluster size information
                try:
                    num_executors = int(spark_conf.get('spark.executor.instances', '0'))
                    if num_executors > 0:
                        env_data['num_workers'] = num_executors
                except (ValueError, TypeError):
                    pass
                    
        except Exception as e:
            logger.debug(f"Could not extract Spark configuration: {e}")
        
        # Try to get information from environment variables
        env_vars = {
            'DATABRICKS_RUNTIME_VERSION': 'runtime_version',
            'DB_CLUSTER_ID': 'cluster_id',
            'DB_CLUSTER_NAME': 'cluster_name', 
            'DATABRICKS_WORKSPACE_URL': 'workspace_url'
        }
        
        for env_var, field_name in env_vars.items():
            value = os.getenv(env_var)
            if value and field_name not in env_data:
                env_data[field_name] = value
        
        # Try to get cluster configuration from Databricks CLI config
        try:
            databricks_cfg_path = os.path.expanduser('~/.databrickscfg')
            if os.path.exists(databricks_cfg_path):
                # This would parse .databrickscfg but we'll skip for now
                pass
        except Exception:
            pass
            
        logger.info(f"Detected Databricks environment: {env_data}")
        return cls(**env_data)
    
    def is_databricks_environment(self) -> bool:
        """Check if we're running in a Databricks environment."""
        return (
            self.cluster_id is not None or 
            self.runtime_version is not None or 
            os.getenv('DATABRICKS_RUNTIME_VERSION') is not None
        )
    
    def supports_unity_catalog(self) -> bool:
        """Check if Unity Catalog is supported/enabled."""
        return self.unity_catalog_enabled is True
    
    def get_optimal_parallelism(self) -> int:
        """
        Calculate optimal parallelism based on cluster configuration.
        
        Returns:
            Recommended number of parallel tasks
        """
        # Default parallelism
        default_parallelism = 4
        
        if self.num_workers is not None:
            # For job clusters, use worker count * cores per worker (estimate 4 cores)
            cores_per_worker = 4
            total_cores = self.num_workers * cores_per_worker
            return min(max(total_cores // 2, 2), 16)  # Conservative estimate
        
        elif self.autoscale_max is not None:
            # For autoscaling clusters, use max workers
            return min(self.autoscale_max * 2, 12)
        
        elif self.cluster_mode == ClusterMode.SHARED:
            # For shared clusters, be more conservative
            return 2
        
        return default_parallelism


class ClusterConfig(BaseModel):
    """
    Configuration specific to Databricks cluster operation.
    
    This extends the base DataLoaderConfig with cluster-specific optimizations.
    """
    
    # Base configuration
    base_config: DataLoaderConfig
    
    # Cluster-specific settings
    cluster_mode: ClusterMode = ClusterMode.SINGLE_USER
    enable_cluster_optimizations: bool = True
    use_optimized_writes: bool = True
    enable_auto_compaction: bool = True
    adaptive_query_execution: bool = True
    
    # Resource management
    max_memory_fraction: float = Field(default=0.8, ge=0.1, le=1.0)
    shuffle_partitions: Optional[int] = None
    broadcast_threshold: Optional[str] = None
    
    # Job orchestration
    enable_job_dependencies: bool = False
    job_timeout_minutes: int = Field(default=120, gt=0)
    enable_job_retries: bool = True
    max_job_retries: int = Field(default=3, ge=0)
    
    # Unity Catalog integration
    use_unity_catalog: bool = False
    default_catalog: Optional[str] = None
    default_schema: Optional[str] = None
    
    # Monitoring and metrics
    enable_cluster_metrics: bool = True
    enable_performance_monitoring: bool = True
    metrics_storage_path: Optional[str] = None
    
    # Workflow integration
    enable_workflow_integration: bool = False
    workflow_run_id: Optional[str] = None
    upstream_dependencies: list[str] = Field(default_factory=list)
    downstream_notifications: list[str] = Field(default_factory=list)
    
    @classmethod
    def from_base_config(
        cls, 
        base_config: DataLoaderConfig,
        environment: Optional[DatabricksEnvironment] = None,
        **cluster_overrides
    ) -> 'ClusterConfig':
        """
        Create a cluster configuration from a base configuration.
        
        Args:
            base_config: Base data loader configuration
            environment: Detected Databricks environment
            **cluster_overrides: Additional cluster-specific overrides
            
        Returns:
            ClusterConfig optimized for the detected environment
        """
        if environment is None:
            environment = DatabricksEnvironment.detect_environment()
        
        # Determine cluster-specific defaults
        cluster_defaults = {}
        
        if environment.is_databricks_environment():
            cluster_defaults.update({
                'enable_cluster_optimizations': True,
                'use_optimized_writes': True,
                'enable_auto_compaction': True,
                'adaptive_query_execution': True,
                'enable_cluster_metrics': True
            })
            
            # Adjust parallelism based on cluster
            optimal_parallelism = environment.get_optimal_parallelism()
            if base_config.max_parallel_jobs != optimal_parallelism:
                logger.info(f"Adjusting parallelism from {base_config.max_parallel_jobs} to {optimal_parallelism}")
                base_config.max_parallel_jobs = optimal_parallelism
            
            # Set cluster mode
            if environment.cluster_mode:
                cluster_defaults['cluster_mode'] = environment.cluster_mode
            
            # Unity Catalog integration
            if environment.supports_unity_catalog():
                cluster_defaults.update({
                    'use_unity_catalog': True,
                    'default_catalog': 'main'  # Default Unity Catalog
                })
        
        else:
            # Non-Databricks environment - disable cluster optimizations
            cluster_defaults.update({
                'enable_cluster_optimizations': False,
                'use_optimized_writes': False,
                'enable_auto_compaction': False,
                'enable_cluster_metrics': False
            })
        
        # Apply overrides
        cluster_defaults.update(cluster_overrides)
        
        return cls(
            base_config=base_config,
            **cluster_defaults
        )
    
    def get_spark_configurations(self) -> Dict[str, str]:
        """
        Get Spark configurations optimized for cluster operation.
        
        Returns:
            Dictionary of Spark configuration properties
        """
        spark_configs = {}
        
        if self.enable_cluster_optimizations:
            # Delta Lake optimizations
            spark_configs.update({
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            })
            
            if self.use_optimized_writes:
                spark_configs.update({
                    'spark.databricks.delta.optimizeWrite.enabled': 'true',
                    'spark.databricks.delta.optimizeWrite.numShuffleBlocks': '50000000'
                })
            
            if self.enable_auto_compaction:
                spark_configs.update({
                    'spark.databricks.delta.autoCompact.enabled': 'true',
                    'spark.databricks.delta.autoCompact.minNumFiles': '10'
                })
            
            if self.adaptive_query_execution:
                spark_configs.update({
                    'spark.sql.adaptive.enabled': 'true',
                    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                    'spark.sql.adaptive.skewJoin.enabled': 'true',
                    'spark.sql.adaptive.localShuffleReader.enabled': 'true'
                })
        
        # Resource management
        if self.shuffle_partitions:
            spark_configs['spark.sql.shuffle.partitions'] = str(self.shuffle_partitions)
        
        if self.broadcast_threshold:
            spark_configs['spark.sql.autoBroadcastJoinThreshold'] = self.broadcast_threshold
        
        # Memory management
        memory_fraction = str(self.max_memory_fraction)
        spark_configs.update({
            'spark.sql.execution.arrow.maxRecordsPerBatch': '10000',
            'spark.sql.execution.arrow.pyspark.enabled': 'true',
            'spark.sql.adaptive.advisoryPartitionSizeInBytes': '128MB'
        })
        
        # Unity Catalog
        if self.use_unity_catalog and self.default_catalog:
            spark_configs['spark.sql.catalog.unity'] = 'com.databricks.sql.managedcatalog.UnityCatalogProvider'
        
        return spark_configs
    
    def get_table_name_with_catalog(self, table_config) -> str:
        """
        Get fully qualified table name including catalog/schema if using Unity Catalog.
        
        Args:
            table_config: Table configuration
            
        Returns:
            Fully qualified table name
        """
        if self.use_unity_catalog:
            catalog = self.default_catalog or 'main'
            schema = self.default_schema or table_config.database_name
            return f"{catalog}.{schema}.{table_config.table_name}"
        else:
            return f"{table_config.database_name}.{table_config.table_name}"