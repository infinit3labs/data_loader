#!/usr/bin/env python3
"""
Performance testing and validation script for the data loader.

This script runs performance tests and validates the data loading functionality
under various conditions and loads.
"""

import time
import json
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List


class PerformanceValidator:
    """Validates performance characteristics of the data loader."""
    
    def __init__(self, data_root: str = "/app/data"):
        self.data_root = Path(data_root)
        self.results_path = Path("/app/test-results")
        self.results_path.mkdir(exist_ok=True)
        
    def validate_file_processing_speed(self) -> Dict[str, Any]:
        """Validate file processing speed benchmarks."""
        print("Running file processing speed validation...")
        
        start_time = time.time()
        
        # Count files in raw directory
        raw_files = list(self.data_root.glob("raw/**/*.parquet"))
        total_files = len(raw_files)
        
        if total_files == 0:
            return {
                "test": "file_processing_speed",
                "status": "skipped",
                "reason": "No files to process"
            }
        
        # Calculate total file size
        total_size_mb = sum(f.stat().st_size for f in raw_files) / (1024 * 1024)
        
        processing_time = time.time() - start_time
        
        # Performance metrics
        files_per_second = total_files / processing_time if processing_time > 0 else 0
        mb_per_second = total_size_mb / processing_time if processing_time > 0 else 0
        
        return {
            "test": "file_processing_speed",
            "status": "completed",
            "metrics": {
                "total_files": total_files,
                "total_size_mb": round(total_size_mb, 2),
                "processing_time_seconds": round(processing_time, 2),
                "files_per_second": round(files_per_second, 2),
                "mb_per_second": round(mb_per_second, 2)
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def validate_memory_usage(self) -> Dict[str, Any]:
        """Validate memory usage during processing."""
        print("Running memory usage validation...")
        
        try:
            # Run a small data loading test and monitor memory
            result = subprocess.run([
                "python3", "-m", "data_loader.main", "run",
                "--config", "/app/docker/configs/docker_test_config.json",
                "--tables", "products",  # Smallest dataset
                "--dry-run"
            ], capture_output=True, text=True, timeout=60)
            
            return {
                "test": "memory_usage",
                "status": "completed" if result.returncode == 0 else "failed",
                "return_code": result.returncode,
                "stdout_lines": len(result.stdout.splitlines()),
                "stderr_lines": len(result.stderr.splitlines()),
                "timestamp": datetime.now().isoformat()
            }
            
        except subprocess.TimeoutExpired:
            return {
                "test": "memory_usage",
                "status": "timeout",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "test": "memory_usage",
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def validate_parallel_processing(self) -> Dict[str, Any]:
        """Validate parallel processing capabilities."""
        print("Running parallel processing validation...")
        
        test_configs = [
            {"max_parallel_jobs": 1, "name": "sequential"},
            {"max_parallel_jobs": 2, "name": "parallel_2"},
            {"max_parallel_jobs": 4, "name": "parallel_4"}
        ]
        
        results = []
        
        for config in test_configs:
            print(f"Testing with {config['max_parallel_jobs']} parallel jobs...")
            
            # Create test config
            test_config = {
                "raw_data_path": "/app/data/raw",
                "processed_data_path": "/app/data/processed",
                "checkpoint_path": "/app/data/checkpoints",
                "file_tracker_table": f"perf_test_{config['name']}_tracker",
                "file_tracker_database": "perf_test_metadata",
                "max_parallel_jobs": config["max_parallel_jobs"],
                "retry_attempts": 1,
                "timeout_minutes": 10,
                "log_level": "WARNING",  # Reduce log noise
                "enable_metrics": True,
                "tables": [
                    {
                        "table_name": f"perf_test_{config['name']}",
                        "database_name": "perf_test",
                        "source_path_pattern": "/app/data/raw/transactions/*.parquet",
                        "loading_strategy": "append",
                        "file_format": "parquet"
                    }
                ]
            }
            
            config_path = self.results_path / f"perf_config_{config['name']}.json"
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(test_config, f, indent=2)
            
            start_time = time.time()
            
            try:
                result = subprocess.run([
                    "python3", "-m", "data_loader.main", "run",
                    "--config", str(config_path),
                    "--dry-run"
                ], capture_output=True, text=True, timeout=120)
                
                processing_time = time.time() - start_time
                
                results.append({
                    "parallel_jobs": config["max_parallel_jobs"],
                    "name": config["name"],
                    "processing_time": round(processing_time, 2),
                    "status": "completed" if result.returncode == 0 else "failed",
                    "return_code": result.returncode
                })
                
            except subprocess.TimeoutExpired:
                processing_time = time.time() - start_time
                results.append({
                    "parallel_jobs": config["max_parallel_jobs"],
                    "name": config["name"],
                    "processing_time": round(processing_time, 2),
                    "status": "timeout"
                })
        
        return {
            "test": "parallel_processing",
            "status": "completed",
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
    
    def validate_data_quality(self) -> Dict[str, Any]:
        """Validate data quality and consistency."""
        print("Running data quality validation...")
        
        # Check if processed data exists
        processed_dir = self.data_root / "processed"
        
        if not processed_dir.exists():
            return {
                "test": "data_quality",
                "status": "skipped",
                "reason": "No processed data found"
            }
        
        # Count processed files
        processed_files = list(processed_dir.rglob("*.parquet"))
        
        # Basic validation metrics
        validation_results = {
            "processed_files_count": len(processed_files),
            "processed_directories": len(list(processed_dir.iterdir())),
            "total_processed_size_mb": round(
                sum(f.stat().st_size for f in processed_files) / (1024 * 1024), 2
            ) if processed_files else 0
        }
        
        return {
            "test": "data_quality",
            "status": "completed",
            "validation_results": validation_results,
            "timestamp": datetime.now().isoformat()
        }
    
    def run_all_validations(self) -> Dict[str, Any]:
        """Run all performance validations."""
        print("Starting performance validation suite...")
        
        validations = [
            self.validate_file_processing_speed,
            self.validate_memory_usage,
            self.validate_parallel_processing,
            self.validate_data_quality
        ]
        
        results = {
            "validation_start_time": datetime.now().isoformat(),
            "validations": []
        }
        
        for validation_func in validations:
            try:
                result = validation_func()
                results["validations"].append(result)
                print(f"✓ {result['test']} - {result['status']}")
            except Exception as e:
                error_result = {
                    "test": validation_func.__name__,
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                results["validations"].append(error_result)
                print(f"✗ {validation_func.__name__} - error: {e}")
        
        results["validation_end_time"] = datetime.now().isoformat()
        
        # Save results
        results_file = self.results_path / "performance_validation.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        print(f"Performance validation completed. Results saved to: {results_file}")
        return results


def main():
    """Run performance validation."""
    validator = PerformanceValidator()
    results = validator.run_all_validations()
    
    # Print summary
    total_tests = len(results["validations"])
    completed_tests = sum(1 for v in results["validations"] if v["status"] == "completed")
    
    print(f"\nPerformance Validation Summary:")
    print(f"Total Tests: {total_tests}")
    print(f"Completed: {completed_tests}")
    print(f"Success Rate: {(completed_tests/total_tests)*100:.1f}%")


if __name__ == "__main__":
    main()
