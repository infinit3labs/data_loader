"""
Cluster resource manager for monitoring and optimizing resource usage.

This module provides functionality to monitor cluster resources, optimize
resource allocation, and ensure efficient utilization in Databricks environments.
"""

import time
from typing import Dict, Any, Optional, List
from loguru import logger

from .cluster_config import ClusterConfig, DatabricksEnvironment, ClusterMode


class ClusterResourceManager:
    """
    Manages cluster resources and provides optimization recommendations.
    
    This class monitors cluster resources, tracks usage patterns, and provides
    recommendations for optimal resource utilization.
    """
    
    def __init__(self, cluster_config: ClusterConfig, environment: DatabricksEnvironment):
        """
        Initialize the cluster resource manager.
        
        Args:
            cluster_config: Cluster configuration
            environment: Databricks environment information
        """
        self.cluster_config = cluster_config
        self.environment = environment
        self.monitoring_enabled = cluster_config.enable_performance_monitoring
        
        # Resource monitoring data
        self.resource_history = []
        self.baseline_resources = None
        
        logger.info("Initialized ClusterResourceManager")
    
    def get_cluster_resources(self) -> Dict[str, Any]:
        """
        Get current cluster resource utilization.
        
        Returns:
            Dictionary with current resource information
        """
        resources = {
            'timestamp': time.time(),
            'cluster_id': self.environment.cluster_id,
            'cluster_mode': self.environment.cluster_mode.value if self.environment.cluster_mode else None
        }
        
        try:
            # Get Spark session metrics
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            
            if spark:
                spark_context = spark.sparkContext
                status_tracker = spark_context.statusTracker()
                
                # Basic executor information
                executor_infos = status_tracker.getExecutorInfos()
                
                total_cores = sum(info.totalCores for info in executor_infos)
                active_executors = len([info for info in executor_infos if info.isActive])
                
                resources.update({
                    'total_executors': len(executor_infos),
                    'active_executors': active_executors,
                    'total_cores': total_cores,
                    'memory_per_executor': getattr(executor_infos[0], 'maxMemory', 0) if executor_infos else 0
                })
                
                # Get application information
                app_id = spark_context.applicationId
                app_name = spark_context.appName
                
                resources.update({
                    'application_id': app_id,
                    'application_name': app_name,
                    'spark_version': spark_context.version
                })
                
                # Job and stage information
                active_jobs = status_tracker.getActiveJobIds()
                active_stages = status_tracker.getActiveStageIds()
                
                resources.update({
                    'active_jobs': len(active_jobs),
                    'active_stages': len(active_stages)
                })
                
                # Storage information if available
                try:
                    storage_status = status_tracker.getStorageStatus()
                    if storage_status:
                        total_memory = sum(status.maxMemory for status in storage_status)
                        used_memory = sum(status.memoryUsed for status in storage_status)
                        
                        resources.update({
                            'storage_memory_total': total_memory,
                            'storage_memory_used': used_memory,
                            'storage_memory_utilization': used_memory / total_memory if total_memory > 0 else 0
                        })
                except Exception as e:
                    logger.debug(f"Could not get storage status: {e}")
                
        except Exception as e:
            logger.warning(f"Could not get Spark resource information: {e}")
            resources.update({
                'error': f"Could not access Spark metrics: {e}",
                'spark_available': False
            })
        
        # Try to get system-level information
        try:
            import psutil
            
            # CPU and memory usage
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            resources.update({
                'system_cpu_percent': cpu_percent,
                'system_memory_total': memory.total,
                'system_memory_used': memory.used,
                'system_memory_percent': memory.percent,
                'system_disk_total': disk.total,
                'system_disk_used': disk.used,
                'system_disk_percent': (disk.used / disk.total) * 100
            })
            
        except ImportError:
            logger.debug("psutil not available for system metrics")
        except Exception as e:
            logger.debug(f"Could not get system metrics: {e}")
        
        # Add to monitoring history if enabled
        if self.monitoring_enabled:
            self.resource_history.append(resources)
            
            # Keep only last 100 entries to prevent memory growth
            if len(self.resource_history) > 100:
                self.resource_history = self.resource_history[-100:]
        
        return resources
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get cluster health status based on resource utilization.
        
        Returns:
            Dictionary with health status and recommendations
        """
        current_resources = self.get_cluster_resources()
        
        health_status = {
            'overall_health': 'healthy',
            'warnings': [],
            'critical_issues': [],
            'recommendations': []
        }
        
        # Check CPU utilization
        if 'system_cpu_percent' in current_resources:
            cpu_percent = current_resources['system_cpu_percent']
            if cpu_percent > 90:
                health_status['critical_issues'].append(f"High CPU utilization: {cpu_percent:.1f}%")
                health_status['overall_health'] = 'critical'
                health_status['recommendations'].append("Consider scaling up the cluster or reducing parallelism")
            elif cpu_percent > 75:
                health_status['warnings'].append(f"Elevated CPU utilization: {cpu_percent:.1f}%")
                if health_status['overall_health'] == 'healthy':
                    health_status['overall_health'] = 'warning'
        
        # Check memory utilization
        if 'system_memory_percent' in current_resources:
            memory_percent = current_resources['system_memory_percent']
            if memory_percent > 90:
                health_status['critical_issues'].append(f"High memory utilization: {memory_percent:.1f}%")
                health_status['overall_health'] = 'critical'
                health_status['recommendations'].append("Consider increasing cluster memory or optimizing data processing")
            elif memory_percent > 80:
                health_status['warnings'].append(f"Elevated memory utilization: {memory_percent:.1f}%")
                if health_status['overall_health'] == 'healthy':
                    health_status['overall_health'] = 'warning'
        
        # Check storage memory utilization
        if 'storage_memory_utilization' in current_resources:
            storage_util = current_resources['storage_memory_utilization']
            if storage_util > 0.9:
                health_status['warnings'].append(f"High Spark storage memory usage: {storage_util:.1%}")
                health_status['recommendations'].append("Consider unpersisting unused DataFrames")
        
        # Check active jobs and stages
        if current_resources.get('active_jobs', 0) > 10:
            health_status['warnings'].append(f"Many active jobs: {current_resources['active_jobs']}")
            health_status['recommendations'].append("Monitor for potential job queuing")
        
        # Check executor health
        total_executors = current_resources.get('total_executors', 0)
        active_executors = current_resources.get('active_executors', 0)
        
        if total_executors > 0 and active_executors / total_executors < 0.8:
            health_status['warnings'].append(f"Some executors inactive: {active_executors}/{total_executors}")
            health_status['recommendations'].append("Check for executor failures or cluster scaling issues")
        
        return health_status
    
    def calculate_efficiency(self, initial_resources: Dict[str, Any], 
                           final_resources: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate resource efficiency between two resource snapshots.
        
        Args:
            initial_resources: Initial resource state
            final_resources: Final resource state
            
        Returns:
            Dictionary with efficiency metrics
        """
        efficiency = {
            'execution_time': final_resources['timestamp'] - initial_resources['timestamp'],
            'resource_changes': {}
        }
        
        # Calculate changes in key metrics
        metrics_to_compare = [
            'system_cpu_percent', 'system_memory_percent', 'active_jobs', 
            'active_stages', 'storage_memory_utilization'
        ]
        
        for metric in metrics_to_compare:
            if metric in initial_resources and metric in final_resources:
                initial_val = initial_resources.get(metric, 0)
                final_val = final_resources.get(metric, 0)
                efficiency['resource_changes'][metric] = {
                    'initial': initial_val,
                    'final': final_val,
                    'change': final_val - initial_val
                }
        
        # Calculate overall efficiency score (0-100)
        efficiency_score = 100
        
        # Penalize high resource usage
        if 'system_cpu_percent' in efficiency['resource_changes']:
            avg_cpu = (efficiency['resource_changes']['system_cpu_percent']['initial'] + 
                      efficiency['resource_changes']['system_cpu_percent']['final']) / 2
            if avg_cpu > 80:
                efficiency_score -= (avg_cpu - 80) * 2
        
        if 'system_memory_percent' in efficiency['resource_changes']:
            avg_memory = (efficiency['resource_changes']['system_memory_percent']['initial'] + 
                         efficiency['resource_changes']['system_memory_percent']['final']) / 2
            if avg_memory > 80:
                efficiency_score -= (avg_memory - 80) * 2
        
        efficiency['efficiency_score'] = max(0, min(100, efficiency_score))
        
        return efficiency
    
    def get_optimization_recommendations(self) -> Dict[str, Any]:
        """
        Get optimization recommendations based on current resource usage.
        
        Returns:
            Dictionary with optimization recommendations
        """
        current_resources = self.get_cluster_resources()
        recommendations = {
            'immediate_actions': [],
            'configuration_changes': [],
            'scaling_recommendations': []
        }
        
        # Analyze resource patterns if we have history
        if len(self.resource_history) >= 3:
            recent_resources = self.resource_history[-3:]
            
            # Check for consistent high CPU usage
            cpu_values = [r.get('system_cpu_percent', 0) for r in recent_resources]
            avg_cpu = sum(cpu_values) / len(cpu_values)
            
            if avg_cpu > 80:
                recommendations['scaling_recommendations'].append(
                    "Consider scaling up cluster - consistent high CPU usage detected"
                )
            elif avg_cpu < 20:
                recommendations['scaling_recommendations'].append(
                    "Consider scaling down cluster - low CPU utilization detected"
                )
            
            # Check memory patterns
            memory_values = [r.get('system_memory_percent', 0) for r in recent_resources]
            avg_memory = sum(memory_values) / len(memory_values)
            
            if avg_memory > 85:
                recommendations['configuration_changes'].append(
                    "Increase executor memory or reduce batch sizes"
                )
        
        # Cluster-specific recommendations
        if self.environment.cluster_mode == ClusterMode.SHARED:
            recommendations['configuration_changes'].append(
                "Running on shared cluster - consider conservative resource usage"
            )
        
        # Parallelism recommendations
        total_cores = current_resources.get('total_cores', 0)
        configured_parallelism = self.cluster_config.base_config.max_parallel_jobs
        
        if total_cores > 0:
            optimal_parallelism = min(total_cores // 2, 16)
            if configured_parallelism > optimal_parallelism:
                recommendations['configuration_changes'].append(
                    f"Consider reducing max_parallel_jobs from {configured_parallelism} to {optimal_parallelism}"
                )
            elif configured_parallelism < optimal_parallelism // 2:
                recommendations['configuration_changes'].append(
                    f"Consider increasing max_parallel_jobs from {configured_parallelism} to {optimal_parallelism}"
                )
        
        return recommendations
    
    def validate_resources(self) -> Dict[str, Any]:
        """
        Validate that cluster has sufficient resources for the configured workload.
        
        Returns:
            Validation results with warnings and recommendations
        """
        current_resources = self.get_cluster_resources()
        validation = {
            'sufficient': True,
            'warnings': [],
            'recommendations': []
        }
        
        # Check minimum executor requirements
        min_executors = 2  # Minimum for meaningful parallel processing
        active_executors = current_resources.get('active_executors', 0)
        
        if active_executors < min_executors:
            validation['sufficient'] = False
            validation['warnings'].append(
                f"Insufficient executors: {active_executors} (minimum: {min_executors})"
            )
            validation['recommendations'].append("Scale up cluster or check executor availability")
        
        # Check memory availability
        if 'system_memory_percent' in current_resources:
            if current_resources['system_memory_percent'] > 90:
                validation['sufficient'] = False
                validation['warnings'].append("Very high memory usage before processing start")
                validation['recommendations'].append("Increase cluster memory or reduce other workloads")
        
        # Check for Unity Catalog requirements
        if self.cluster_config.use_unity_catalog and not self.environment.supports_unity_catalog():
            validation['sufficient'] = False
            validation['warnings'].append("Unity Catalog required but not available")
            validation['recommendations'].append("Enable Unity Catalog or disable in configuration")
        
        return validation
    
    def monitor_processing(self, duration_minutes: int = 5) -> Dict[str, Any]:
        """
        Monitor resource usage during processing for the specified duration.
        
        Args:
            duration_minutes: How long to monitor (in minutes)
            
        Returns:
            Monitoring results with resource usage patterns
        """
        if not self.monitoring_enabled:
            return {'monitoring_disabled': True}
        
        logger.info(f"Starting resource monitoring for {duration_minutes} minutes")
        
        monitoring_data = []
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        while time.time() < end_time:
            resources = self.get_cluster_resources()
            monitoring_data.append(resources)
            time.sleep(30)  # Sample every 30 seconds
        
        # Analyze the monitoring data
        analysis = self._analyze_monitoring_data(monitoring_data)
        
        logger.info(f"Completed resource monitoring: {analysis['summary']}")
        return {
            'monitoring_duration': duration_minutes,
            'samples_collected': len(monitoring_data),
            'analysis': analysis,
            'raw_data': monitoring_data[-10:]  # Keep last 10 samples
        }
    
    def _analyze_monitoring_data(self, monitoring_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze collected monitoring data.
        
        Args:
            monitoring_data: List of resource snapshots
            
        Returns:
            Analysis results
        """
        if not monitoring_data:
            return {'summary': 'No data collected'}
        
        # Calculate averages and peaks
        metrics = ['system_cpu_percent', 'system_memory_percent', 'active_jobs']
        analysis = {'summary': {}, 'peaks': {}, 'trends': {}}
        
        for metric in metrics:
            values = [data.get(metric, 0) for data in monitoring_data if metric in data]
            if values:
                analysis['summary'][metric] = {
                    'average': sum(values) / len(values),
                    'min': min(values),
                    'max': max(values)
                }
                analysis['peaks'][metric] = max(values)
        
        # Identify trends
        if len(monitoring_data) > 2:
            first_half = monitoring_data[:len(monitoring_data)//2]
            second_half = monitoring_data[len(monitoring_data)//2:]
            
            for metric in metrics:
                first_avg = sum(data.get(metric, 0) for data in first_half) / len(first_half)
                second_avg = sum(data.get(metric, 0) for data in second_half) / len(second_half)
                
                if second_avg > first_avg * 1.2:
                    analysis['trends'][metric] = 'increasing'
                elif second_avg < first_avg * 0.8:
                    analysis['trends'][metric] = 'decreasing'
                else:
                    analysis['trends'][metric] = 'stable'
        
        return analysis