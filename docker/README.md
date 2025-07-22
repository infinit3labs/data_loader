# Docker Testing Environment for Databricks Data Loader

This directory contains a comprehensive Docker-based testing environment for the Databricks Data Loader platform. The setup provides mock Spark/Delta Lake environment with automated testing, data generation, and validation capabilities.

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB of available memory for containers
- Port 4040, 8888 available for Spark UI and Jupyter

### Run Complete Test Suite
```bash
# Run all tests (recommended for first time)
./docker/scripts/run_tests.sh --full

# Or run specific test types
./docker/scripts/run_tests.sh --unit          # Unit tests only
./docker/scripts/run_tests.sh --integration   # Integration tests only
./docker/scripts/run_tests.sh --interactive   # Start interactive environment
```

## 📋 Test Environment Components

### Docker Services

| Service | Purpose | Ports | Volume Mounts |
|---------|---------|-------|---------------|
| `data-loader` | Main application container | 4040-4041 | Code, data, logs |
| `test-runner` | Unit test execution | - | Code, test results |
| `data-generator` | Mock data generation | - | Data volume |
| `integration-tests` | End-to-end testing | - | Data, test results |
| `jupyter` | Interactive analysis | 8888 | Code, data |

### Persistent Volumes

- **data-volume**: Test data storage (raw, processed, checkpoints)
- **logs-volume**: Application logs
- **test-results-volume**: Test outputs and coverage reports

## 🧪 Test Types

### 1. Unit Tests
- **Location**: `data_loader/tests/`
- **Runner**: `pytest` with coverage reporting
- **Output**: `./test-results/`
- **Coverage**: HTML reports in `./test-results/coverage/`

```bash
# Run unit tests only
./docker/scripts/run_tests.sh --unit

# Or manually
docker-compose up test-runner
```

### 2. Integration Tests
- **Location**: `docker/scripts/run_integration_tests.py`
- **Tests**: End-to-end workflows, CLI validation, error handling
- **Output**: `./integration-test-results/`

```bash
# Run integration tests
./docker/scripts/run_tests.sh --integration
```

### 3. Performance Validation
- **Script**: `docker/scripts/validate_performance.py`
- **Tests**: Processing speed, memory usage, parallel execution
- **Output**: Performance metrics and benchmarks

```bash
# Run performance validation
docker-compose run --rm data-loader python3 docker/scripts/validate_performance.py
```

## 📊 Mock Data Generation

The testing environment automatically generates realistic test data:

### Customer Data (SCD2 Testing)
- **Records**: 1,000 initial customers
- **Batches**: 3 batches with updates and new customers
- **Format**: Parquet files with date partitioning
- **Strategy**: Tests Slowly Changing Dimensions Type 2

### Transaction Data (Append Testing)
- **Records**: 5,000 transactions across 5 files
- **Format**: Parquet with temporal distribution
- **Strategy**: Tests append-only loading patterns

### Product Data (Reference Data)
- **Records**: 500 products
- **Format**: Single Parquet file
- **Purpose**: Static reference data testing

### Error Data
- **Empty files**: Tests empty file handling
- **Schema mismatches**: Tests schema evolution
- **Null values**: Tests data quality validation

## 🔧 Configuration Files

### Test Configurations
- `docker/configs/docker_test_config.json` - Standard mode testing
- `docker/configs/cluster_test_config.json` - Cluster mode testing

### Key Differences from Production
- **Paths**: Container-mapped volumes (`/app/data/`)
- **Parallel Jobs**: Reduced for container environment
- **Timeouts**: Shorter for faster testing
- **Log Levels**: More verbose for debugging

## 🔍 Monitoring and Debugging

### Spark UI
- **URL**: http://localhost:4040
- **Purpose**: Monitor Spark job execution
- **Available**: When Spark applications are running

### Jupyter Notebook
- **URL**: http://localhost:8888
- **Purpose**: Interactive data exploration and debugging
- **Notebooks**: Access test data and application code

### Container Logs
```bash
# View application logs
docker-compose logs -f data-loader

# View all service logs
docker-compose logs -f

# Enter container for debugging
docker-compose exec data-loader bash
```

## 🏗️ Development Workflow

### Making Changes
1. **Code Changes**: Edit files in the workspace (auto-mounted)
2. **Test Changes**: Run specific test suites
3. **Debug Issues**: Use Jupyter or container shell access
4. **Validate**: Run full test suite before commits

### Adding New Tests
1. **Unit Tests**: Add to `data_loader/tests/`
2. **Integration Tests**: Extend `run_integration_tests.py`
3. **Mock Data**: Modify `generate_mock_data.py`

### Custom Test Scenarios
```bash
# Run with custom configuration
docker-compose run --rm data-loader python3 -m data_loader.main run \
  --config /path/to/custom/config.json \
  --tables "table1,table2" \
  --dry-run

# Test specific loading strategy
docker-compose run --rm data-loader python3 -m data_loader.main run-cluster \
  --config /app/docker/configs/cluster_test_config.json \
  --tables "customers"
```

## 📈 Performance Benchmarks

The testing environment establishes baseline performance metrics:

### Expected Performance (Container Environment)
- **File Processing**: 10-50 files/second (depending on size)
- **Memory Usage**: < 2GB for test datasets
- **Parallel Efficiency**: 2x speedup with 2 cores
- **Startup Time**: < 30 seconds for Spark initialization

### Performance Validation
```bash
# Run performance benchmarks
docker-compose run --rm data-loader python3 docker/scripts/validate_performance.py
```

## 🐛 Troubleshooting

### Common Issues

#### Port Conflicts
```bash
# If ports 4040 or 8888 are in use
docker-compose down
# Edit docker-compose.yml to change ports
```

#### Memory Issues
```bash
# Increase Docker memory allocation to 4GB+
# Or reduce parallel jobs in test configs
```

#### Permission Issues
```bash
# Fix file permissions
docker-compose run --rm --user root data-loader chown -R spark:spark /app
```

#### Spark Initialization Failures
```bash
# Check Java/Spark setup
docker-compose run --rm data-loader java -version
docker-compose run --rm data-loader spark-submit --version
```

### Debug Mode
```bash
# Run with debug logging
docker-compose run --rm -e LOG_LEVEL=DEBUG data-loader python3 -m data_loader.main --help
```

## 📁 File Structure

```
docker/
├── configs/                    # Test configurations
│   ├── docker_test_config.json
│   └── cluster_test_config.json
├── scripts/                    # Test and utility scripts
│   ├── generate_mock_data.py   # Mock data generation
│   ├── run_integration_tests.py # Integration test suite
│   ├── validate_performance.py  # Performance benchmarks
│   └── run_tests.sh            # Main test runner
├── Dockerfile.test             # Testing-optimized Dockerfile
└── README.md                   # This file

# Generated during testing
test-results/                   # Unit test outputs
├── junit.xml                   # JUnit test results
├── coverage/                   # HTML coverage reports
└── *.xml                      # Various test outputs

integration-test-results/       # Integration test outputs
├── integration_test_results.json
├── test_summary.txt
└── *.json                     # Test configurations and results
```

## 🔄 Continuous Integration

### GitHub Actions Integration
The Docker testing environment is designed to work with CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run Docker Tests
  run: |
    ./docker/scripts/run_tests.sh --full
    
- name: Upload Test Results
  uses: actions/upload-artifact@v3
  with:
    name: test-results
    path: |
      test-results/
      integration-test-results/
```

### Test Reports
- **JUnit XML**: Compatible with most CI/CD systems
- **Coverage HTML**: Visual coverage reports
- **JSON Results**: Machine-readable test outcomes

## 🎯 Best Practices

### Running Tests
1. **Always start clean**: Use `--cleanup` before major test runs
2. **Monitor resources**: Check Docker memory usage during tests
3. **Review logs**: Check container logs for detailed error information
4. **Validate environment**: Ensure all services start correctly

### Debugging
1. **Use interactive mode**: Start with `--interactive` for exploration
2. **Check Spark UI**: Monitor job execution and performance
3. **Examine data**: Use Jupyter for data inspection
4. **Test incrementally**: Run unit tests before integration tests

### Performance
1. **Baseline first**: Establish performance baselines
2. **Monitor trends**: Track performance changes over time
3. **Resource allocation**: Adjust Docker memory as needed
4. **Parallel optimization**: Test different parallel job configurations

## 📞 Support

For issues with the Docker testing environment:

1. **Check logs**: `docker-compose logs -f`
2. **Verify setup**: `./docker/scripts/run_tests.sh --build`
3. **Clean environment**: `./docker/scripts/run_tests.sh --cleanup`
4. **Review documentation**: Check main project README.md

The Docker testing environment provides a comprehensive, isolated testing platform that closely mirrors production Databricks environments while remaining lightweight and fast for development workflows.
