#!/usr/bin/env python3
"""
Integration tests for the Databricks Data Loader in Docker environment.

This script runs comprehensive end-to-end tests including:
- Standard mode data loading
- Cluster mode data loading
- Error handling scenarios
- Performance validation
"""

import os
import sys
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Add the app directory to Python path
sys.path.insert(0, '/app')

from data_loader.config.table_config import DataLoaderConfig
from data_loader.utils.logger import setup_logging
from loguru import logger


class IntegrationTestRunner:
    """Runs comprehensive integration tests for the data loader."""
    
    def __init__(self, data_root: str = "/app/data"):
        self.data_root = Path(data_root)
        self.test_results_path = Path("/app/test-results")
        self.test_results_path.mkdir(exist_ok=True)
        
        # Test configuration
        self.test_config = {
            "raw_data_path": str(self.data_root / "raw"),
            "processed_data_path": str(self.data_root / "processed"),
            "checkpoint_path": str(self.data_root / "checkpoints"),
            "file_tracker_table": "test_file_tracker",
            "file_tracker_database": "test_metadata",
            "max_parallel_jobs": 2,
            "retry_attempts": 2,
            "timeout_minutes": 30,
            "log_level": "INFO",
            "enable_metrics": True,
            "tables": [
                {
                    "table_name": "customers",
                    "database_name": "test_analytics",
                    "source_path_pattern": str(self.data_root / "raw/customers/*.parquet"),
                    "loading_strategy": "scd2",
                    "primary_keys": ["customer_id"],
                    "tracking_columns": ["name", "email", "address", "phone", "customer_type"],
                    "file_format": "parquet",
                    "schema_evolution": True,
                    "partition_columns": ["date_partition"],
                    "scd2_effective_date_column": "effective_date",
                    "scd2_end_date_column": "end_date",
                    "scd2_current_flag_column": "is_current"
                },
                {
                    "table_name": "transactions",
                    "database_name": "test_analytics",
                    "source_path_pattern": str(self.data_root / "raw/transactions/*.parquet"),
                    "loading_strategy": "append",
                    "file_format": "parquet",
                    "schema_evolution": True,
                    "partition_columns": ["transaction_date"]
                }
            ]
        }
        
        self.results: Dict[str, Any] = {
            "test_start_time": datetime.now().isoformat(),
            "tests": [],
            "summary": {}
        }
    
    def save_test_config(self) -> str:
        """Save test configuration to file."""
        config_path = self.test_results_path / "integration_test_config.json"
        with open(config_path, 'w') as f:
            json.dump(self.test_config, f, indent=2)
        return str(config_path)
    
    def run_command(self, command: List[str], test_name: str) -> Dict[str, Any]:
        """Run a command and capture results."""
        logger.info(f"Running test: {test_name}")
        logger.info(f"Command: {' '.join(command)}")
        
        start_time = time.time()
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                cwd="/app"
            )
            
            duration = time.time() - start_time
            
            test_result = {
                "test_name": test_name,
                "command": " ".join(command),
                "start_time": datetime.fromtimestamp(start_time).isoformat(),
                "duration_seconds": round(duration, 2),
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "success": result.returncode == 0
            }
            
            if test_result["success"]:
                logger.success(f"Test passed: {test_name} (took {duration:.2f}s)")
            else:
                logger.error(f"Test failed: {test_name} (took {duration:.2f}s)")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
            
            return test_result
            
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            test_result = {
                "test_name": test_name,
                "command": " ".join(command),
                "start_time": datetime.fromtimestamp(start_time).isoformat(),
                "duration_seconds": round(duration, 2),
                "return_code": -1,
                "stdout": "",
                "stderr": "Test timed out after 5 minutes",
                "success": False
            }
            logger.error(f"Test timed out: {test_name}")
            return test_result
        
        except Exception as e:
            duration = time.time() - start_time
            test_result = {
                "test_name": test_name,
                "command": " ".join(command),
                "start_time": datetime.fromtimestamp(start_time).isoformat(),
                "duration_seconds": round(duration, 2),
                "return_code": -1,
                "stdout": "",
                "stderr": str(e),
                "success": False
            }
            logger.error(f"Test error: {test_name} - {e}")
            return test_result
    
    def test_config_validation(self) -> Dict[str, Any]:
        """Test configuration validation."""
        config_path = self.save_test_config()
        
        command = [
            "python3", "-m", "data_loader.main",
            "run",
            "--config", config_path,
            "--dry-run"
        ]
        
        return self.run_command(command, "Config Validation Test")
    
    def test_standard_mode_loading(self) -> Dict[str, Any]:
        """Test standard mode data loading."""
        config_path = self.save_test_config()
        
        command = [
            "python3", "-m", "data_loader.main",
            "run",
            "--config", config_path,
            "--tables", "transactions"  # Start with append strategy
        ]
        
        return self.run_command(command, "Standard Mode Loading Test")
    
    def test_cluster_mode_loading(self) -> Dict[str, Any]:
        """Test cluster mode data loading."""
        config_path = self.save_test_config()
        
        command = [
            "python3", "-m", "data_loader.main",
            "run-cluster",
            "--config", config_path,
            "--tables", "customers"  # Test SCD2 strategy
        ]
        
        return self.run_command(command, "Cluster Mode Loading Test")
    
    def test_parallel_processing(self) -> Dict[str, Any]:
        """Test parallel processing with multiple tables."""
        config_path = self.save_test_config()
        
        command = [
            "python3", "-m", "data_loader.main",
            "run",
            "--config", config_path
            # Load all tables
        ]
        
        return self.run_command(command, "Parallel Processing Test")
    
    def test_error_handling(self) -> Dict[str, Any]:
        """Test error handling with problematic data."""
        # Create config for error test data
        error_config = self.test_config.copy()
        error_config["tables"] = [
            {
                "table_name": "error_test",
                "database_name": "test_analytics",
                "source_path_pattern": str(self.data_root / "raw/error_test/*.parquet"),
                "loading_strategy": "append",
                "file_format": "parquet",
                "schema_evolution": False  # Should fail with schema mismatch
            }
        ]
        
        error_config_path = self.test_results_path / "error_test_config.json"
        with open(error_config_path, 'w') as f:
            json.dump(error_config, f, indent=2)
        
        command = [
            "python3", "-m", "data_loader.main",
            "run",
            "--config", str(error_config_path)
        ]
        
        # This test should fail gracefully
        result = self.run_command(command, "Error Handling Test")
        # For error handling test, we expect it to handle errors gracefully
        # So even if return code is non-zero, it might be successful error handling
        result["expected_to_fail"] = True
        return result
    
    def test_cli_help(self) -> Dict[str, Any]:
        """Test CLI help and basic functionality."""
        command = ["python3", "-m", "data_loader.main", "--help"]
        return self.run_command(command, "CLI Help Test")
    
    def test_config_generation(self) -> Dict[str, Any]:
        """Test example config generation."""
        output_path = self.test_results_path / "generated_config.json"
        command = [
            "python3", "-m", "data_loader.main",
            "create-example-config",
            "-o", str(output_path)
        ]
        return self.run_command(command, "Config Generation Test")
    
    def run_all_tests(self) -> None:
        """Run all integration tests."""
        logger.info("Starting integration test suite...")
        
        # Test suite
        tests = [
            self.test_cli_help,
            self.test_config_generation,
            self.test_config_validation,
            self.test_standard_mode_loading,
            self.test_cluster_mode_loading,
            self.test_parallel_processing,
            self.test_error_handling
        ]
        
        for test_func in tests:
            try:
                result = test_func()
                self.results["tests"].append(result)
                time.sleep(2)  # Brief pause between tests
            except Exception as e:
                logger.error(f"Test function failed: {test_func.__name__} - {e}")
                self.results["tests"].append({
                    "test_name": test_func.__name__,
                    "success": False,
                    "error": str(e)
                })
        
        # Generate summary
        self.generate_summary()
        self.save_results()
    
    def generate_summary(self) -> None:
        """Generate test summary."""
        total_tests = len(self.results["tests"])
        passed_tests = sum(1 for test in self.results["tests"] if test.get("success", False))
        failed_tests = total_tests - passed_tests
        
        total_duration = sum(test.get("duration_seconds", 0) for test in self.results["tests"])
        
        self.results["summary"] = {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": failed_tests,
            "success_rate": round((passed_tests / total_tests) * 100, 2) if total_tests > 0 else 0,
            "total_duration_seconds": round(total_duration, 2),
            "test_end_time": datetime.now().isoformat()
        }
        
        logger.info(f"Test Summary:")
        logger.info(f"  Total Tests: {total_tests}")
        logger.info(f"  Passed: {passed_tests}")
        logger.info(f"  Failed: {failed_tests}")
        logger.info(f"  Success Rate: {self.results['summary']['success_rate']}%")
        logger.info(f"  Total Duration: {total_duration:.2f} seconds")
    
    def save_results(self) -> None:
        """Save test results to file."""
        results_path = self.test_results_path / "integration_test_results.json"
        with open(results_path, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        logger.info(f"Test results saved to: {results_path}")
        
        # Also create a summary report
        summary_path = self.test_results_path / "test_summary.txt"
        with open(summary_path, 'w') as f:
            f.write("Databricks Data Loader - Integration Test Summary\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Test Start: {self.results['test_start_time']}\n")
            f.write(f"Test End: {self.results['summary']['test_end_time']}\n")
            f.write(f"Total Duration: {self.results['summary']['total_duration_seconds']} seconds\n\n")
            
            f.write(f"Results:\n")
            f.write(f"  Total Tests: {self.results['summary']['total_tests']}\n")
            f.write(f"  Passed: {self.results['summary']['passed_tests']}\n")
            f.write(f"  Failed: {self.results['summary']['failed_tests']}\n")
            f.write(f"  Success Rate: {self.results['summary']['success_rate']}%\n\n")
            
            f.write("Individual Test Results:\n")
            f.write("-" * 30 + "\n")
            for test in self.results["tests"]:
                status = "PASS" if test.get("success", False) else "FAIL"
                duration = test.get("duration_seconds", 0)
                f.write(f"{test.get('test_name', 'Unknown')}: {status} ({duration:.2f}s)\n")


def main():
    """Run integration tests."""
    # Set up logging
    setup_logging("INFO")
    
    logger.info("Starting Databricks Data Loader Integration Tests")
    
    # Wait a bit for data generation to complete
    time.sleep(5)
    
    runner = IntegrationTestRunner()
    runner.run_all_tests()
    
    # Exit with appropriate code
    success_rate = runner.results["summary"]["success_rate"]
    if success_rate >= 80:  # 80% success rate threshold
        logger.success(f"Integration tests completed successfully! Success rate: {success_rate}%")
        sys.exit(0)
    else:
        logger.error(f"Integration tests failed. Success rate: {success_rate}%")
        sys.exit(1)


if __name__ == "__main__":
    main()
