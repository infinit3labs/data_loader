"""
Cluster-optimized data processor for Databricks cluster environments.

This processor extends the base DataProcessor with cluster-specific optimizations,
resource management, and monitoring capabilities.
"""

import time
from typing import Dict, Any, List, Optional
from loguru import logger

from ..core.processor import DataProcessor
from ..config.table_config import TableConfig
from .cluster_config import ClusterConfig, DatabricksEnvironment
from .resource_manager import ClusterResourceManager
from .job_orchestrator import JobOrchestrator


class ClusterDataProcessor(DataProcessor):
    """
    Cluster-optimized data processor with enhanced resource management.
    
    This processor provides additional functionality for running in Databricks
    cluster environments, including resource optimization, job orchestration,
    and cluster-specific monitoring.
    """
    
    def __init__(self, cluster_config: ClusterConfig):
        """
        Initialize the cluster data processor.
        
        Args:
            cluster_config: Cluster-specific configuration
        """
        # Initialize base processor with the embedded base config
        super().__init__(cluster_config.base_config)
        
        self.cluster_config = cluster_config
        self.environment = DatabricksEnvironment.detect_environment()
        
        # Initialize cluster-specific components
        if cluster_config.enable_cluster_optimizations:
            self.resource_manager = ClusterResourceManager(cluster_config, self.environment)
            self._apply_cluster_optimizations()
        else:
            self.resource_manager = None
        
        if cluster_config.enable_job_dependencies:
            self.job_orchestrator = JobOrchestrator(cluster_config)
        else:
            self.job_orchestrator = None
        
        # Cluster-specific metrics
        self.cluster_metrics = {
            'cluster_id': self.environment.cluster_id,
            'cluster_mode': self.cluster_config.cluster_mode.value,
            'optimizations_enabled': cluster_config.enable_cluster_optimizations,
            'unity_catalog_enabled': cluster_config.use_unity_catalog
        }
        
        logger.info(f"Initialized ClusterDataProcessor for cluster {self.environment.cluster_id}")
    
    def _apply_cluster_optimizations(self):
        """Apply cluster-specific optimizations to Spark session."""
        if not self.environment.is_databricks_environment():
            logger.warning("Not in Databricks environment, skipping cluster optimizations")
            return
        
        try:
            # Get optimized Spark configurations
            spark_configs = self.cluster_config.get_spark_configurations()
            
            # Apply configurations to current Spark session
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            
            if spark:
                for key, value in spark_configs.items():
                    try:
                        spark.conf.set(key, value)
                        logger.debug(f"Applied Spark config: {key} = {value}")
                    except Exception as e:
                        logger.warning(f"Could not set Spark config {key}: {e}")
                
                logger.info(f"Applied {len(spark_configs)} cluster optimizations")
            else:
                logger.warning("No active Spark session found for optimization")
                
        except Exception as e:
            logger.error(f"Error applying cluster optimizations: {e}")
    
    def process_all_tables(self) -> Dict[str, Any]:
        """
        Process all tables with cluster-specific enhancements.
        
        Returns:
            Enhanced processing results with cluster metrics
        """
        logger.info("Starting cluster-optimized data processing")
        
        # Check job dependencies if enabled
        if self.job_orchestrator:
            dependency_check = self.job_orchestrator.check_dependencies()
            if not dependency_check['all_dependencies_met']:
                raise RuntimeError(f"Job dependencies not met: {dependency_check['missing_dependencies']}")
        
        # Monitor cluster resources before processing
        if self.resource_manager:
            initial_resources = self.resource_manager.get_cluster_resources()
            logger.info(f"Cluster resources at start: {initial_resources}")
        
        start_time = time.time()
        
        try:
            # Run base processing with cluster optimizations
            results = super().process_all_tables()
            
            # Add cluster-specific metrics
            results.update({
                'cluster_metrics': self.cluster_metrics,
                'cluster_id': self.environment.cluster_id,
                'cluster_mode': self.cluster_config.cluster_mode.value,
                'optimizations_applied': self.cluster_config.enable_cluster_optimizations
            })
            
            # Monitor resource usage during processing
            if self.resource_manager:
                final_resources = self.resource_manager.get_cluster_resources()
                results['resource_usage'] = {
                    'initial_resources': initial_resources,
                    'final_resources': final_resources,
                    'resource_efficiency': self.resource_manager.calculate_efficiency(
                        initial_resources, final_resources
                    )
                }
            
            # Notify downstream dependencies if configured
            if self.job_orchestrator:
                self.job_orchestrator.notify_completion(results)
            
            logger.info("Cluster-optimized processing completed successfully")
            return results
            
        except Exception as e:
            # Enhanced error handling for cluster context
            error_context = {
                'cluster_id': self.environment.cluster_id,
                'cluster_mode': self.cluster_config.cluster_mode.value,
                'error': str(e)
            }
            
            if self.job_orchestrator:
                self.job_orchestrator.notify_failure(error_context)
            
            logger.error(f"Cluster processing failed: {error_context}")
            raise
        
        finally:
            processing_time = time.time() - start_time
            logger.info(f"Total cluster processing time: {processing_time:.2f}s")
    
    def process_table(self, table_config: TableConfig) -> Dict[str, Any]:
        """
        Process a single table with cluster optimizations.
        
        Args:
            table_config: Table configuration
            
        Returns:
            Enhanced processing results
        """
        # Apply Unity Catalog naming if enabled
        if self.cluster_config.use_unity_catalog:
            original_table_name = table_config.table_name
            table_config.table_name = self.cluster_config.get_table_name_with_catalog(table_config)
            logger.info(f"Using Unity Catalog table name: {table_config.table_name}")
        
        try:
            # Monitor resources for this table
            if self.resource_manager:
                table_start_resources = self.resource_manager.get_cluster_resources()
            
            # Process table using base implementation
            result = super().process_table(table_config)
            
            # Add cluster-specific metrics for this table
            if self.resource_manager:
                table_end_resources = self.resource_manager.get_cluster_resources()
                result['table_resource_usage'] = {
                    'start_resources': table_start_resources,
                    'end_resources': table_end_resources
                }
            
            # Add cluster context to result
            result.update({
                'cluster_id': self.environment.cluster_id,
                'unity_catalog_enabled': self.cluster_config.use_unity_catalog,
                'optimizations_enabled': self.cluster_config.enable_cluster_optimizations
            })
            
            return result
            
        finally:
            # Restore original table name if modified
            if self.cluster_config.use_unity_catalog:
                table_config.table_name = original_table_name
    
    def optimize_all_tables(self, background: bool = False):
        """
        Run optimization on all tables with cluster-aware settings.
        
        Args:
            background: Whether to run optimization in background
        """
        logger.info("Running cluster-optimized table optimization")
        
        if background and self.cluster_config.cluster_mode == self.cluster_config.cluster_mode.SHARED:
            logger.info("Running optimization in background mode for shared cluster")
            # In a real implementation, this would submit optimization as a separate job
        
        # Use cluster-optimized optimization settings
        for table_config in self.config.tables:
            try:
                strategy = self._get_loading_strategy(table_config)
                
                # Apply cluster-specific optimization parameters
                if hasattr(strategy, 'optimize_table_cluster'):
                    strategy.optimize_table_cluster(
                        cluster_mode=self.cluster_config.cluster_mode,
                        background=background
                    )
                else:
                    strategy.optimize_table()
                    
                logger.info(f"Optimized table {table_config.table_name}")
                
            except Exception as e:
                logger.warning(f"Could not optimize table {table_config.table_name}: {e}")
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """
        Get comprehensive cluster status and health information.
        
        Returns:
            Dictionary with cluster status information
        """
        status = {
            'environment': {
                'cluster_id': self.environment.cluster_id,
                'cluster_name': self.environment.cluster_name,
                'runtime_version': self.environment.runtime_version,
                'cluster_mode': self.environment.cluster_mode.value if self.environment.cluster_mode else None,
                'worker_count': self.environment.num_workers,
                'unity_catalog_enabled': self.environment.unity_catalog_enabled
            },
            'configuration': {
                'optimizations_enabled': self.cluster_config.enable_cluster_optimizations,
                'unity_catalog_enabled': self.cluster_config.use_unity_catalog,
                'job_dependencies_enabled': self.cluster_config.enable_job_dependencies,
                'max_parallel_jobs': self.config.max_parallel_jobs
            },
            'processing_status': self.get_processing_status()
        }
        
        # Add resource information if available
        if self.resource_manager:
            status['resources'] = self.resource_manager.get_cluster_resources()
            status['resource_health'] = self.resource_manager.get_health_status()
        
        # Add job orchestration status if enabled
        if self.job_orchestrator:
            status['job_orchestration'] = self.job_orchestrator.get_status()
        
        return status
    
    def validate_cluster_configuration(self) -> Dict[str, Any]:
        """
        Validate cluster configuration and environment compatibility.
        
        Returns:
            Validation results with recommendations
        """
        validation_results = {
            'valid': True,
            'warnings': [],
            'errors': [],
            'recommendations': []
        }
        
        # Check if we're in a Databricks environment
        if not self.environment.is_databricks_environment():
            validation_results['warnings'].append(
                "Not running in Databricks environment - cluster optimizations disabled"
            )
        
        # Check cluster mode compatibility
        if (self.cluster_config.cluster_mode == self.cluster_config.cluster_mode.SHARED and 
            self.config.max_parallel_jobs > 4):
            validation_results['warnings'].append(
                f"High parallelism ({self.config.max_parallel_jobs}) on shared cluster may affect other users"
            )
            validation_results['recommendations'].append(
                "Consider reducing max_parallel_jobs for shared cluster"
            )
        
        # Check Unity Catalog configuration
        if (self.cluster_config.use_unity_catalog and 
            not self.environment.supports_unity_catalog()):
            validation_results['errors'].append(
                "Unity Catalog enabled in config but not supported in environment"
            )
            validation_results['valid'] = False
        
        # Check resource requirements
        if self.resource_manager:
            resource_validation = self.resource_manager.validate_resources()
            if not resource_validation['sufficient']:
                validation_results['warnings'].extend(resource_validation['warnings'])
                validation_results['recommendations'].extend(resource_validation['recommendations'])
        
        # Check job dependencies
        if self.job_orchestrator:
            dependency_validation = self.job_orchestrator.validate_dependencies()
            if not dependency_validation['valid']:
                validation_results['errors'].extend(dependency_validation['errors'])
                validation_results['valid'] = False
        
        return validation_results