"""
Tests for the cluster functionality.
"""

import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from data_loader.config.table_config import DataLoaderConfig, EXAMPLE_CONFIG
from data_loader.cluster import (
    ClusterConfig, ClusterMode, DatabricksEnvironment, 
    ClusterDataProcessor, ClusterResourceManager, JobOrchestrator
)


class TestDatabricksEnvironment:
    """Test Databricks environment detection."""
    
    def test_environment_creation(self):
        """Test basic environment creation."""
        env = DatabricksEnvironment(
            cluster_id="test-cluster-123",
            cluster_name="test-cluster",
            runtime_version="11.3.x-scala2.12"
        )
        
        assert env.cluster_id == "test-cluster-123"
        assert env.cluster_name == "test-cluster"
        assert env.runtime_version == "11.3.x-scala2.12"
    
    def test_is_databricks_environment(self):
        """Test Databricks environment detection."""
        # With cluster ID
        env1 = DatabricksEnvironment(cluster_id="test-cluster")
        assert env1.is_databricks_environment()
        
        # With runtime version
        env2 = DatabricksEnvironment(runtime_version="11.3.x")
        assert env2.is_databricks_environment()
        
        # Empty environment
        env3 = DatabricksEnvironment()
        assert not env3.is_databricks_environment()
    
    @patch.dict(os.environ, {'DATABRICKS_RUNTIME_VERSION': '11.3.x'})
    def test_environment_from_env_vars(self):
        """Test environment detection from environment variables."""
        env = DatabricksEnvironment()
        # Simulate environment variable detection
        assert os.getenv('DATABRICKS_RUNTIME_VERSION') == '11.3.x'
    
    def test_get_optimal_parallelism(self):
        """Test optimal parallelism calculation."""
        # Environment with worker count
        env1 = DatabricksEnvironment(num_workers=4)
        assert env1.get_optimal_parallelism() == 8  # 4 workers * 4 cores / 2
        
        # Environment with autoscale
        env2 = DatabricksEnvironment(autoscale_max=6)
        assert env2.get_optimal_parallelism() == 12  # max workers * 2
        
        # Shared cluster
        env3 = DatabricksEnvironment(cluster_mode=ClusterMode.SHARED)
        assert env3.get_optimal_parallelism() == 2  # Conservative for shared
        
        # Default case
        env4 = DatabricksEnvironment()
        assert env4.get_optimal_parallelism() == 4  # Default


class TestClusterConfig:
    """Test cluster configuration."""
    
    def test_cluster_config_creation(self):
        """Test cluster configuration creation."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        
        cluster_config = ClusterConfig(
            base_config=base_config,
            cluster_mode=ClusterMode.SINGLE_USER,
            enable_cluster_optimizations=True
        )
        
        assert cluster_config.base_config == base_config
        assert cluster_config.cluster_mode == ClusterMode.SINGLE_USER
        assert cluster_config.enable_cluster_optimizations
    
    def test_from_base_config(self):
        """Test creating cluster config from base config."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        environment = DatabricksEnvironment(
            cluster_id="test-cluster",
            cluster_mode=ClusterMode.SHARED,
            unity_catalog_enabled=True
        )
        
        cluster_config = ClusterConfig.from_base_config(
            base_config=base_config,
            environment=environment
        )
        
        assert cluster_config.base_config == base_config
        assert cluster_config.cluster_mode == ClusterMode.SHARED
        assert cluster_config.use_unity_catalog  # Should be enabled due to environment
    
    def test_get_spark_configurations(self):
        """Test Spark configuration generation."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(
            base_config=base_config,
            enable_cluster_optimizations=True,
            use_optimized_writes=True
        )
        
        spark_configs = cluster_config.get_spark_configurations()
        
        assert 'spark.sql.extensions' in spark_configs
        assert 'spark.databricks.delta.optimizeWrite.enabled' in spark_configs
        assert spark_configs['spark.databricks.delta.optimizeWrite.enabled'] == 'true'
    
    def test_get_table_name_with_catalog(self):
        """Test Unity Catalog table naming."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        table_config = base_config.tables[0]  # First table from example
        
        # Without Unity Catalog
        cluster_config1 = ClusterConfig(
            base_config=base_config,
            use_unity_catalog=False
        )
        
        table_name1 = cluster_config1.get_table_name_with_catalog(table_config)
        assert table_name1 == f"{table_config.database_name}.{table_config.table_name}"
        
        # With Unity Catalog
        cluster_config2 = ClusterConfig(
            base_config=base_config,
            use_unity_catalog=True,
            default_catalog="main"
        )
        
        table_name2 = cluster_config2.get_table_name_with_catalog(table_config)
        assert table_name2 == f"main.{table_config.database_name}.{table_config.table_name}"


class TestClusterResourceManager:
    """Test cluster resource manager."""
    
    def test_resource_manager_creation(self):
        """Test resource manager initialization."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(base_config=base_config)
        environment = DatabricksEnvironment(cluster_id="test-cluster")
        
        resource_manager = ClusterResourceManager(cluster_config, environment)
        
        assert resource_manager.cluster_config == cluster_config
        assert resource_manager.environment == environment
    
    def test_get_cluster_resources_no_spark(self):
        """Test resource collection without Spark session."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(base_config=base_config)
        environment = DatabricksEnvironment(cluster_id="test-cluster")
        
        resource_manager = ClusterResourceManager(cluster_config, environment)
        resources = resource_manager.get_cluster_resources()
        
        assert 'timestamp' in resources
        assert 'cluster_id' in resources
        assert resources['cluster_id'] == "test-cluster"
    
    def test_get_health_status(self):
        """Test health status assessment."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(base_config=base_config)
        environment = DatabricksEnvironment()
        
        resource_manager = ClusterResourceManager(cluster_config, environment)
        
        # Mock get_cluster_resources to return test data
        with patch.object(resource_manager, 'get_cluster_resources') as mock_resources:
            mock_resources.return_value = {
                'system_cpu_percent': 85,
                'system_memory_percent': 70,
                'active_jobs': 5
            }
            
            health = resource_manager.get_health_status()
            
            assert health['overall_health'] == 'warning'  # Due to high CPU
            assert len(health['warnings']) > 0
    
    def test_calculate_efficiency(self):
        """Test resource efficiency calculation."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(base_config=base_config)
        environment = DatabricksEnvironment()
        
        resource_manager = ClusterResourceManager(cluster_config, environment)
        
        initial_resources = {
            'timestamp': 1000,
            'system_cpu_percent': 20,
            'system_memory_percent': 30
        }
        
        final_resources = {
            'timestamp': 1100,
            'system_cpu_percent': 80,
            'system_memory_percent': 70
        }
        
        efficiency = resource_manager.calculate_efficiency(initial_resources, final_resources)
        
        assert efficiency['execution_time'] == 100
        assert 'resource_changes' in efficiency
        assert 'efficiency_score' in efficiency


class TestJobOrchestrator:
    """Test job orchestration functionality."""
    
    def test_job_orchestrator_creation(self):
        """Test job orchestrator initialization."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(
            base_config=base_config,
            enable_job_dependencies=True,
            upstream_dependencies=["job:upstream-job-1"],
            downstream_notifications=["webhook:http://example.com/hook"]
        )
        
        orchestrator = JobOrchestrator(cluster_config)
        
        assert orchestrator.cluster_config == cluster_config
        assert orchestrator.job_state['status'] == 'initializing'
    
    def test_check_dependencies_empty(self):
        """Test dependency checking with no dependencies."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(base_config=base_config)
        
        orchestrator = JobOrchestrator(cluster_config)
        result = orchestrator.check_dependencies()
        
        assert result['all_dependencies_met'] is True
        assert result['dependency_count'] == 0
        assert len(result['missing_dependencies']) == 0
    
    def test_check_dependencies_with_deps(self):
        """Test dependency checking with dependencies."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(
            base_config=base_config,
            upstream_dependencies=["table:test.customers", "file:/tmp/test.txt"]
        )
        
        orchestrator = JobOrchestrator(cluster_config)
        
        # Mock the dependency check methods
        with patch.object(orchestrator, '_check_single_dependency') as mock_check:
            mock_check.return_value = {'satisfied': True}
            
            result = orchestrator.check_dependencies()
            
            assert result['all_dependencies_met'] is True
            assert result['dependency_count'] == 2
            assert mock_check.call_count == 2
    
    def test_notify_completion(self):
        """Test completion notifications."""
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(
            base_config=base_config,
            downstream_notifications=["webhook:http://example.com/hook"]
        )
        
        orchestrator = JobOrchestrator(cluster_config)
        
        # Mock the notification sending
        with patch.object(orchestrator, '_send_notification') as mock_send:
            results = {
                'tables_processed': 2,
                'total_files_processed': 10,
                'successful_files': 8
            }
            
            orchestrator.notify_completion(results)
            
            assert mock_send.call_count == 1
            # Check that notification was recorded
            assert len(orchestrator.job_state['notifications_sent']) == 1


class TestClusterDataProcessor:
    """Test cluster-optimized data processor."""
    
    @patch('data_loader.cluster.cluster_processor.DatabricksEnvironment')
    @patch('data_loader.core.processor.FileTracker')
    @patch('data_loader.core.processor.ParallelExecutor')
    def test_cluster_processor_creation(self, mock_parallel_exec, mock_file_tracker, mock_env_class):
        """Test cluster processor initialization."""
        # Mock environment
        mock_env = Mock()
        mock_env.is_databricks_environment.return_value = True
        mock_env.cluster_id = "test-cluster"
        mock_env_class.detect_environment.return_value = mock_env
        
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(
            base_config=base_config,
            enable_cluster_optimizations=True
        )
        
        with patch('data_loader.cluster.cluster_processor.ClusterResourceManager'):
            processor = ClusterDataProcessor(cluster_config)
            
            assert processor.cluster_config == cluster_config
            assert processor.environment == mock_env
    
    @patch('data_loader.cluster.cluster_processor.DatabricksEnvironment')
    @patch('data_loader.core.processor.FileTracker')
    @patch('data_loader.core.processor.ParallelExecutor')
    def test_validate_cluster_configuration(self, mock_parallel_exec, mock_file_tracker, mock_env_class):
        """Test cluster configuration validation."""
        # Mock environment
        mock_env = Mock()
        mock_env.is_databricks_environment.return_value = True
        mock_env.supports_unity_catalog.return_value = False
        mock_env.cluster_id = "test-cluster"
        mock_env_class.detect_environment.return_value = mock_env
        
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(
            base_config=base_config,
            use_unity_catalog=True  # This should cause a validation error
        )
        
        with patch('data_loader.cluster.cluster_processor.ClusterResourceManager'):
            processor = ClusterDataProcessor(cluster_config)
            validation = processor.validate_cluster_configuration()
            
            assert not validation['valid']  # Should fail due to Unity Catalog mismatch
            assert len(validation['errors']) > 0
    
    @patch('data_loader.cluster.cluster_processor.DatabricksEnvironment')
    @patch('data_loader.core.processor.FileTracker')
    @patch('data_loader.core.processor.ParallelExecutor')
    def test_get_cluster_status(self, mock_parallel_exec, mock_file_tracker, mock_env_class):
        """Test cluster status retrieval."""
        # Mock environment
        mock_env = Mock()
        mock_env.is_databricks_environment.return_value = True
        mock_env.cluster_id = "test-cluster"
        mock_env.cluster_name = "test-cluster-name"
        mock_env.runtime_version = "11.3.x"
        mock_env.cluster_mode = ClusterMode.SINGLE_USER
        mock_env.num_workers = 4
        mock_env.unity_catalog_enabled = False
        mock_env_class.detect_environment.return_value = mock_env
        
        base_config = DataLoaderConfig(**EXAMPLE_CONFIG)
        cluster_config = ClusterConfig(base_config=base_config)
        
        with patch('data_loader.cluster.cluster_processor.ClusterResourceManager') as mock_rm_class:
            # Mock resource manager
            mock_rm = Mock()
            mock_rm.get_cluster_resources.return_value = {'cpu': 50}
            mock_rm.get_health_status.return_value = {'overall_health': 'healthy'}
            mock_rm_class.return_value = mock_rm
            
            processor = ClusterDataProcessor(cluster_config)
            
            # Mock the get_processing_status method since it depends on file_tracker
            with patch.object(processor, 'get_processing_status') as mock_status:
                mock_status.return_value = {'overall_statistics': {'completed': 0}}
                
                status = processor.get_cluster_status()
                
                assert 'environment' in status
                assert 'configuration' in status
                assert 'processing_status' in status
                assert status['environment']['cluster_id'] == "test-cluster"
                assert status['environment']['cluster_name'] == "test-cluster-name"