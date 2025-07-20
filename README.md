# Databricks Data Loader

A comprehensive data loading module for Databricks that provides parallel file processing with multiple loading strategies, file tracking, and robust error handling.

## Features

- **File Monitoring**: Automatically discovers and processes new files from configured locations
- **File Tracking**: Tracks processing status to prevent duplicate processing using Delta tables
- **Parallel Processing**: Configurable parallel execution for efficient file processing
- **Multiple Loading Strategies**:
  - **SCD2 (Slowly Changing Dimensions Type 2)**: Maintains historical records with change tracking
  - **Append**: Simple append operation for tables without primary keys
  - **Overwrite**: Replace table contents (coming soon)
  - **Merge**: Custom merge logic (coming soon)
- **Schema Evolution**: Automatic schema evolution support
- **Error Handling**: Robust error handling with configurable retry logic
- **Monitoring**: Comprehensive logging and metrics collection
- **Optimization**: Automatic table optimization and vacuum operations

## Architecture

```
data_loader/
├── config/                  # Configuration management
│   ├── table_config.py     # Table and loading strategy configuration
│   └── databricks_config.py # Databricks-specific settings
├── core/                   # Core processing components
│   ├── file_tracker.py     # File processing status tracking
│   ├── processor.py        # Main orchestrator
│   └── parallel_executor.py # Parallel processing framework
├── strategies/             # Loading strategy implementations
│   ├── base_strategy.py    # Base strategy interface
│   ├── scd2_strategy.py    # SCD2 implementation
│   └── append_strategy.py  # Append strategy implementation
├── utils/                  # Utility functions
│   ├── logger.py          # Logging utilities
│   └── helpers.py         # Helper functions
└── main.py                # Entry point for Databricks jobs
```

## Installation

1. Install the package:
```bash
pip install -e .
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The data loader uses JSON configuration files to define tables, loading strategies, and processing options.

### Example Configuration

```json
{
  "raw_data_path": "/mnt/raw/",
  "processed_data_path": "/mnt/processed/",
  "checkpoint_path": "/mnt/checkpoints/",
  "file_tracker_table": "file_processing_tracker",
  "file_tracker_database": "metadata",
  "max_parallel_jobs": 4,
  "retry_attempts": 3,
  "timeout_minutes": 60,
  "log_level": "INFO",
  "enable_metrics": true,
  "tables": [
    {
      "table_name": "customers",
      "database_name": "analytics",
      "source_path_pattern": "/mnt/raw/customers/*.parquet",
      "loading_strategy": "scd2",
      "primary_keys": ["customer_id"],
      "tracking_columns": ["name", "email", "address"],
      "file_format": "parquet",
      "schema_evolution": true,
      "partition_columns": ["date_partition"]
    },
    {
      "table_name": "transactions",
      "database_name": "analytics",
      "source_path_pattern": "/mnt/raw/transactions/*.parquet",
      "loading_strategy": "append",
      "file_format": "parquet",
      "schema_evolution": true,
      "partition_columns": ["transaction_date"]
    }
  ]
}
```

### Configuration Options

#### Global Settings
- `raw_data_path`: Path to raw data location
- `processed_data_path`: Path to processed data location
- `checkpoint_path`: Path for checkpoints and metadata
- `max_parallel_jobs`: Maximum number of concurrent processing jobs
- `retry_attempts`: Number of retry attempts for failed files
- `timeout_minutes`: Timeout for processing a single file

#### Table Configuration
- `table_name`: Name of the target table
- `database_name`: Target database/schema name
- `source_path_pattern`: File path pattern to match source files (supports wildcards)
- `loading_strategy`: Loading strategy (`scd2`, `append`, `overwrite`, `merge`)
- `file_format`: Source file format (`parquet`, `csv`, `json`, `delta`)
- `schema_evolution`: Enable automatic schema evolution
- `partition_columns`: Columns to partition the target table by

#### SCD2 Specific Options
- `primary_keys`: Primary key columns for SCD2
- `tracking_columns`: Columns to track for changes
- `scd2_effective_date_column`: Effective date column name
- `scd2_end_date_column`: End date column name
- `scd2_current_flag_column`: Current flag column name

## Usage

### Command Line Interface

#### Run Data Loading
```bash
# Run with configuration file
python -m data_loader.main run --config config.json

# Run with inline JSON configuration
python -m data_loader.main run --config-json '{"raw_data_path": "/mnt/raw/", ...}'

# Run specific tables only
python -m data_loader.main run --config config.json --tables "customers,transactions"

# Dry run to see what would be processed
python -m data_loader.main run --config config.json --dry-run

# Run with optimization and vacuum
python -m data_loader.main run --config config.json --optimize --vacuum
```

#### Check Processing Status
```bash
python -m data_loader.main status --config config.json
```

#### Create Example Configuration
```bash
python -m data_loader.main create-example-config --output my_config.json
```

### Databricks Job Setup

1. **Upload the package** to Databricks workspace or DBFS
2. **Create a new job** with the following configuration:
   - **Cluster**: Use a cluster with Databricks Runtime 11.0+ and Delta Lake support
   - **Task Type**: Python script
   - **Script path**: Path to `main.py` in your uploaded package
   - **Parameters**: `["run", "--config", "/path/to/config.json"]`

3. **Set up file trigger** (if using file-based triggers):
   - Configure the job to trigger on file arrival in your raw data location
   - Use Databricks Auto Loader for streaming ingestion scenarios

### Programmatic Usage

```python
from data_loader.config.table_config import DataLoaderConfig
from data_loader.core.processor import DataProcessor

# Load configuration
config = DataLoaderConfig(**config_dict)

# Initialize processor
processor = DataProcessor(config)

# Process all tables
results = processor.process_all_tables()

# Process specific table
table_config = config.get_table_config("customers")
table_result = processor.process_table(table_config)

# Check status
status = processor.get_processing_status()
```

## Loading Strategies

### SCD2 (Slowly Changing Dimensions Type 2)

The SCD2 strategy maintains historical records by:
1. Comparing incoming records with current records
2. Identifying new and changed records
3. Marking changed records as inactive (setting end_date and is_current=false)
4. Inserting new/changed records as active

**Requirements**:
- `primary_keys`: Columns that uniquely identify records
- `tracking_columns`: Columns to monitor for changes
- SCD2 metadata columns (effective_date, end_date, is_current)

### Append Strategy

The append strategy simply adds new data to the target table without any deduplication or change detection. Suitable for:
- Event/transaction tables
- Log tables
- Tables without primary keys
- Any scenario where all incoming data should be preserved

**Features**:
- Automatic audit column addition (`_load_timestamp`, `_source_file`, `_batch_id`)
- Optional deduplication
- Late-arriving data handling

## File Tracking

The data loader maintains a Delta table to track file processing status:

```sql
CREATE TABLE metadata.file_processing_tracker (
  file_path STRING,
  file_size INT,
  file_modified_time TIMESTAMP,
  table_name STRING,
  status STRING,  -- pending, processing, completed, failed, skipped
  processing_start_time TIMESTAMP,
  processing_end_time TIMESTAMP,
  error_message STRING,
  retry_count INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

This ensures that:
- Files are never processed more than once
- Failed files can be retried
- Processing history is maintained
- Status can be monitored and reported

## Monitoring and Logging

### Logging
- Structured logging with configurable levels
- JSON format support for log aggregation
- File and console output options
- Performance metrics and execution timing

### Metrics
- File processing statistics
- Table-level metrics
- Success/failure rates
- Execution times
- Resource usage monitoring

## Error Handling

The data loader provides robust error handling:
- **File-level errors**: Individual file failures don't stop the entire process
- **Retry logic**: Configurable retry attempts with exponential backoff
- **Error tracking**: All errors are logged and tracked in the file tracker
- **Graceful degradation**: Processing continues even if some files fail

## Performance Optimization

### Parallel Processing
- Configurable number of concurrent workers
- Thread-safe file status tracking
- Resource usage monitoring

### Databricks Optimizations
- Delta Lake optimizations enabled by default
- Adaptive query execution
- Auto-compaction and optimize write
- Partitioning support

### Best Practices
1. **Partitioning**: Use appropriate partition columns for large tables
2. **File sizes**: Aim for file sizes between 100MB-1GB for optimal performance
3. **Batch processing**: Process files in batches rather than one-by-one
4. **Resource allocation**: Size your cluster appropriately for the workload

## Testing

Run the test suite:
```bash
# Run all tests
pytest data_loader/tests/

# Run with coverage
pytest --cov=data_loader data_loader/tests/

# Run specific test file
pytest data_loader/tests/test_basic.py
```

## Development

### Setting up Development Environment
1. Clone the repository
2. Install in development mode: `pip install -e .`
3. Install development dependencies: `pip install -r requirements.txt`
4. Run tests to verify setup: `pytest`

### Adding New Loading Strategies
1. Create a new strategy class inheriting from `BaseLoadingStrategy`
2. Implement required methods: `load_data()`, `validate_config()`
3. Add strategy to the factory in `processor.py`
4. Add configuration options to `table_config.py`
5. Add tests for the new strategy

## Troubleshooting

### Common Issues

1. **Permission errors**: Ensure the Databricks cluster has access to all specified paths
2. **Schema conflicts**: Enable schema evolution or ensure consistent schemas
3. **Memory issues**: Reduce batch sizes or increase cluster memory
4. **Timeout errors**: Increase timeout settings or optimize file processing

### Debug Mode
Enable debug logging for detailed execution information:
```bash
python -m data_loader.main run --config config.json --log-level DEBUG
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run the test suite
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
