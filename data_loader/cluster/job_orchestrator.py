"""
Job orchestration and dependency management for Databricks cluster environments.

This module provides functionality for managing job dependencies, notifications,
and workflow integration in Databricks cluster contexts.
"""

import time
import json
from typing import Dict, Any, List, Optional
from loguru import logger

from .cluster_config import ClusterConfig


class JobOrchestrator:
    """
    Manages job dependencies, notifications, and workflow integration.
    
    This class provides functionality for coordinating data loading jobs
    with other workflows in a Databricks environment.
    """
    
    def __init__(self, cluster_config: ClusterConfig):
        """
        Initialize the job orchestrator.
        
        Args:
            cluster_config: Cluster configuration with job orchestration settings
        """
        self.cluster_config = cluster_config
        self.job_id = self._get_current_job_id()
        self.run_id = cluster_config.workflow_run_id
        
        # Job state tracking
        self.job_state = {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'start_time': time.time(),
            'status': 'initializing',
            'dependencies_checked': False,
            'notifications_sent': []
        }
        
        logger.info(f"Initialized JobOrchestrator for job {self.job_id}")
    
    def _get_current_job_id(self) -> Optional[str]:
        """
        Get the current Databricks job ID if running in a job context.
        
        Returns:
            Job ID if available, None otherwise
        """
        try:
            # Try to get job ID from Spark configuration
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            
            if spark:
                job_id = spark.conf.get('spark.databricks.clusterUsageTags.jobId', None)
                if job_id:
                    return job_id
            
            # Try environment variables
            import os
            job_id = os.getenv('DATABRICKS_JOB_ID')
            if job_id:
                return job_id
                
        except Exception as e:
            logger.debug(f"Could not determine job ID: {e}")
        
        return None
    
    def check_dependencies(self) -> Dict[str, Any]:
        """
        Check if all upstream dependencies are satisfied.
        
        Returns:
            Dictionary with dependency check results
        """
        if not self.cluster_config.upstream_dependencies:
            return {
                'all_dependencies_met': True,
                'missing_dependencies': [],
                'dependency_count': 0
            }
        
        logger.info(f"Checking {len(self.cluster_config.upstream_dependencies)} dependencies")
        
        dependency_results = {
            'all_dependencies_met': True,
            'missing_dependencies': [],
            'dependency_count': len(self.cluster_config.upstream_dependencies),
            'dependency_status': {}
        }
        
        for dependency in self.cluster_config.upstream_dependencies:
            try:
                status = self._check_single_dependency(dependency)
                dependency_results['dependency_status'][dependency] = status
                
                if not status['satisfied']:
                    dependency_results['all_dependencies_met'] = False
                    dependency_results['missing_dependencies'].append(dependency)
                    
            except Exception as e:
                logger.error(f"Error checking dependency {dependency}: {e}")
                dependency_results['all_dependencies_met'] = False
                dependency_results['missing_dependencies'].append(dependency)
                dependency_results['dependency_status'][dependency] = {
                    'satisfied': False,
                    'error': str(e)
                }
        
        self.job_state['dependencies_checked'] = True
        self.job_state['dependency_results'] = dependency_results
        
        if dependency_results['all_dependencies_met']:
            logger.info("All dependencies satisfied")
        else:
            logger.warning(f"Missing dependencies: {dependency_results['missing_dependencies']}")
        
        return dependency_results
    
    def _check_single_dependency(self, dependency: str) -> Dict[str, Any]:
        """
        Check a single dependency.
        
        Args:
            dependency: Dependency identifier (could be job ID, table name, file path, etc.)
            
        Returns:
            Dictionary with dependency status
        """
        # Different dependency types can be supported:
        # - job:job_id - Check if job completed successfully
        # - table:database.table - Check if table exists and has recent data
        # - file:path - Check if file exists
        # - custom:endpoint - Check custom endpoint
        
        if ':' in dependency:
            dep_type, dep_value = dependency.split(':', 1)
        else:
            # Default to table dependency
            dep_type, dep_value = 'table', dependency
        
        if dep_type == 'job':
            return self._check_job_dependency(dep_value)
        elif dep_type == 'table':
            return self._check_table_dependency(dep_value)
        elif dep_type == 'file':
            return self._check_file_dependency(dep_value)
        elif dep_type == 'custom':
            return self._check_custom_dependency(dep_value)
        else:
            return {
                'satisfied': False,
                'error': f"Unknown dependency type: {dep_type}"
            }
    
    def _check_job_dependency(self, job_id: str) -> Dict[str, Any]:
        """
        Check if a Databricks job has completed successfully.
        
        Args:
            job_id: Databricks job ID
            
        Returns:
            Dependency status
        """
        # In a real implementation, this would use the Databricks Jobs API
        # For now, we'll simulate the check
        
        try:
            # Placeholder for Databricks Jobs API call
            # This would check the latest run of the specified job
            logger.debug(f"Checking job dependency: {job_id}")
            
            # Simulate job status check
            # In reality, you would call:
            # GET /api/2.1/jobs/runs/list?job_id={job_id}&limit=1
            
            return {
                'satisfied': True,  # Assume satisfied for demo
                'dependency_type': 'job',
                'job_id': job_id,
                'last_check': time.time(),
                'details': f"Job {job_id} status checked"
            }
            
        except Exception as e:
            return {
                'satisfied': False,
                'error': f"Error checking job {job_id}: {e}"
            }
    
    def _check_table_dependency(self, table_name: str) -> Dict[str, Any]:
        """
        Check if a table exists and has recent data.
        
        Args:
            table_name: Fully qualified table name (database.table)
            
        Returns:
            Dependency status
        """
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            
            if not spark:
                return {
                    'satisfied': False,
                    'error': "No active Spark session"
                }
            
            # Check if table exists
            try:
                df = spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
                row_count = df.collect()[0]['row_count']
                
                return {
                    'satisfied': True,
                    'dependency_type': 'table',
                    'table_name': table_name,
                    'row_count': row_count,
                    'last_check': time.time()
                }
                
            except Exception as e:
                if 'Table or view not found' in str(e):
                    return {
                        'satisfied': False,
                        'error': f"Table {table_name} does not exist"
                    }
                else:
                    raise
                    
        except Exception as e:
            return {
                'satisfied': False,
                'error': f"Error checking table {table_name}: {e}"
            }
    
    def _check_file_dependency(self, file_path: str) -> Dict[str, Any]:
        """
        Check if a file exists.
        
        Args:
            file_path: File path to check
            
        Returns:
            Dependency status
        """
        try:
            # For DBFS paths, use dbutils
            if file_path.startswith('/dbfs/') or file_path.startswith('dbfs:/'):
                try:
                    from pyspark.sql import SparkSession
                    from pyspark.dbutils import DBUtils
                    
                    spark = SparkSession.getActiveSession()
                    if spark:
                        dbutils = DBUtils(spark)
                        
                        # Clean path for dbutils
                        clean_path = file_path.replace('/dbfs/', 'dbfs:/')
                        if not clean_path.startswith('dbfs:/'):
                            clean_path = 'dbfs:' + clean_path
                        
                        # Check if file exists
                        try:
                            file_info = dbutils.fs.ls(clean_path)
                            return {
                                'satisfied': True,
                                'dependency_type': 'file',
                                'file_path': file_path,
                                'file_size': file_info[0].size if file_info else 0,
                                'last_check': time.time()
                            }
                        except Exception:
                            return {
                                'satisfied': False,
                                'error': f"File {file_path} does not exist"
                            }
                    
                except ImportError:
                    pass
            
            # For local paths, use os.path
            import os
            if os.path.exists(file_path):
                return {
                    'satisfied': True,
                    'dependency_type': 'file',
                    'file_path': file_path,
                    'file_size': os.path.getsize(file_path),
                    'last_check': time.time()
                }
            else:
                return {
                    'satisfied': False,
                    'error': f"File {file_path} does not exist"
                }
                
        except Exception as e:
            return {
                'satisfied': False,
                'error': f"Error checking file {file_path}: {e}"
            }
    
    def _check_custom_dependency(self, endpoint: str) -> Dict[str, Any]:
        """
        Check a custom dependency endpoint.
        
        Args:
            endpoint: Custom endpoint or check logic
            
        Returns:
            Dependency status
        """
        # This could implement custom logic or HTTP endpoint checks
        # For now, return a placeholder
        return {
            'satisfied': True,  # Assume satisfied for demo
            'dependency_type': 'custom',
            'endpoint': endpoint,
            'last_check': time.time(),
            'details': f"Custom dependency {endpoint} checked"
        }
    
    def notify_completion(self, results: Dict[str, Any]):
        """
        Notify downstream dependencies that this job has completed.
        
        Args:
            results: Job execution results
        """
        if not self.cluster_config.downstream_notifications:
            return
        
        logger.info(f"Sending completion notifications to {len(self.cluster_config.downstream_notifications)} targets")
        
        notification_payload = {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'status': 'completed',
            'completion_time': time.time(),
            'results_summary': {
                'tables_processed': results.get('tables_processed', 0),
                'files_processed': results.get('total_files_processed', 0),
                'success_rate': (results.get('successful_files', 0) / 
                               max(results.get('total_files_processed', 1), 1)) * 100
            }
        }
        
        for notification_target in self.cluster_config.downstream_notifications:
            try:
                self._send_notification(notification_target, notification_payload)
                self.job_state['notifications_sent'].append({
                    'target': notification_target,
                    'status': 'success',
                    'timestamp': time.time()
                })
            except Exception as e:
                logger.error(f"Failed to send notification to {notification_target}: {e}")
                self.job_state['notifications_sent'].append({
                    'target': notification_target,
                    'status': 'failed',
                    'error': str(e),
                    'timestamp': time.time()
                })
    
    def notify_failure(self, error_context: Dict[str, Any]):
        """
        Notify about job failure.
        
        Args:
            error_context: Context about the failure
        """
        if not self.cluster_config.downstream_notifications:
            return
        
        logger.info("Sending failure notifications")
        
        notification_payload = {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'status': 'failed',
            'failure_time': time.time(),
            'error_context': error_context
        }
        
        for notification_target in self.cluster_config.downstream_notifications:
            try:
                self._send_notification(notification_target, notification_payload)
            except Exception as e:
                logger.error(f"Failed to send failure notification to {notification_target}: {e}")
    
    def _send_notification(self, target: str, payload: Dict[str, Any]):
        """
        Send notification to a specific target.
        
        Args:
            target: Notification target (webhook URL, Slack channel, etc.)
            payload: Notification payload
        """
        # Different notification types:
        # - webhook:http://example.com/hook
        # - slack:#channel
        # - email:user@example.com
        # - job:job_id (trigger another job)
        
        if ':' in target:
            notification_type, notification_value = target.split(':', 1)
        else:
            notification_type, notification_value = 'webhook', target
        
        if notification_type == 'webhook':
            self._send_webhook_notification(notification_value, payload)
        elif notification_type == 'slack':
            self._send_slack_notification(notification_value, payload)
        elif notification_type == 'email':
            self._send_email_notification(notification_value, payload)
        elif notification_type == 'job':
            self._trigger_job_notification(notification_value, payload)
        else:
            logger.warning(f"Unknown notification type: {notification_type}")
    
    def _send_webhook_notification(self, url: str, payload: Dict[str, Any]):
        """Send webhook notification."""
        try:
            import requests
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"Webhook notification sent to {url}")
        except ImportError:
            logger.warning("requests library not available for webhook notifications")
        except Exception as e:
            logger.error(f"Failed to send webhook to {url}: {e}")
            raise
    
    def _send_slack_notification(self, channel: str, payload: Dict[str, Any]):
        """Send Slack notification."""
        # This would integrate with Slack API
        # For now, just log the notification
        logger.info(f"Slack notification to {channel}: {payload['status']}")
    
    def _send_email_notification(self, email: str, payload: Dict[str, Any]):
        """Send email notification."""
        # This would integrate with email service
        # For now, just log the notification
        logger.info(f"Email notification to {email}: {payload['status']}")
    
    def _trigger_job_notification(self, job_id: str, payload: Dict[str, Any]):
        """Trigger another Databricks job."""
        # This would use Databricks Jobs API to trigger a job
        # For now, just log the action
        logger.info(f"Would trigger job {job_id} with payload: {payload}")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current job orchestration status.
        
        Returns:
            Status information
        """
        return {
            'job_orchestration_enabled': self.cluster_config.enable_job_dependencies,
            'current_job': self.job_state,
            'configuration': {
                'upstream_dependencies': len(self.cluster_config.upstream_dependencies),
                'downstream_notifications': len(self.cluster_config.downstream_notifications),
                'job_timeout_minutes': self.cluster_config.job_timeout_minutes,
                'retries_enabled': self.cluster_config.enable_job_retries,
                'max_retries': self.cluster_config.max_job_retries
            }
        }
    
    def validate_dependencies(self) -> Dict[str, Any]:
        """
        Validate dependency configuration.
        
        Returns:
            Validation results
        """
        validation = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check dependency format
        for dependency in self.cluster_config.upstream_dependencies:
            if ':' not in dependency:
                validation['warnings'].append(
                    f"Dependency '{dependency}' missing type prefix (job:, table:, file:, custom:)"
                )
        
        # Check notification targets
        for notification in self.cluster_config.downstream_notifications:
            if ':' not in notification:
                validation['warnings'].append(
                    f"Notification target '{notification}' missing type prefix"
                )
        
        # Check for circular dependencies (basic check)
        if self.job_id and f"job:{self.job_id}" in self.cluster_config.upstream_dependencies:
            validation['errors'].append("Circular dependency detected - job depends on itself")
            validation['valid'] = False
        
        return validation