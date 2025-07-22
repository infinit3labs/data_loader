# Copilot Instructions for Databricks Data Loader

## Project Architecture

This is a Databricks-optimized data loading framework with dual execution modes:
- **Standard Mode**: General-purpose data processing with configurable parallel execution
- **Cluster Mode**: Databricks-optimized processing with environment detection and resource management

### Core Components

```
data_loader/
├── config/           # Pydantic-based configuration management
├── core/            # File tracking, parallel execution, main processor
├── cluster/         # Databricks environment detection & cluster optimizations  
├── strategies/      # Pluggable loading strategies (SCD2, Append, etc.)
├── utils/           # Logging with loguru, helper functions
└── main.py         # Typer CLI with dual command structure
```

**Key Pattern**: All components use dependency injection via config objects. The `DataLoaderConfig` flows through the entire system, with `ClusterConfig` extending it for Databricks environments.

## Configuration System

Configuration is **the backbone** of this system. Everything is driven by JSON/Pydantic config:

- **`DataLoaderConfig`**: Main config with paths, parallel settings, table definitions
- **`TableConfig`**: Per-table strategy selection, SCD2 parameters, file patterns
- **`ClusterConfig`**: Extends base config with Databricks environment detection

```python
# Standard pattern for config loading
config = DataLoaderConfig(**config_dict)
table_config = config.get_table_config("table_name")
```

**Critical**: All strategies validate their config requirements in `validate_config()`. SCD2 requires `primary_keys` and `tracking_columns`. Append strategy is more lenient.

## Entry Points & CLI Patterns

Use **Typer** for CLI with two main commands:
- `run`: Standard processing mode
- `run-cluster`: Databricks-optimized mode with resource detection

```bash
# Standard mode
python -m data_loader.main run --config config.json

# Cluster mode (recommended for Databricks)  
python -m data_loader.main run-cluster --config config.json --unity-catalog
```

**Important**: CLI commands filter tables using `--tables "table1,table2"` comma-separated syntax.

## Loading Strategy Pattern

**Strategy Selection**: Based on `loading_strategy` enum in table config:
- `scd2`: Slowly Changing Dimensions Type 2 with historical tracking
- `append`: Simple append with audit columns
- `overwrite`/`merge`: Planned but not implemented

```python
# Strategy factory pattern in processor.py
def _get_loading_strategy(self, table_config: TableConfig) -> BaseLoadingStrategy:
    if table_config.loading_strategy == LoadingStrategy.SCD2:
        return SCD2Strategy(table_config)
    elif table_config.loading_strategy == LoadingStrategy.APPEND:
        return AppendStrategy(table_config)
```

**Key Insight**: Each strategy inherits from `BaseLoadingStrategy` and implements `load_data()` and `validate_config()`.

## File Tracking System

**Critical Component**: Delta table-based file tracking prevents duplicate processing:
- Uses `file_tracker_table` in `file_tracker_database` 
- Tracks processing status: `pending`, `processing`, `completed`, `failed`
- Thread-safe for parallel execution

```python
# File discovery pattern - glob-based with status filtering
files = glob.glob(table_config.source_path_pattern)
unprocessed_files = file_tracker.get_unprocessed_files(files)
```

## Databricks Integration Patterns

**Environment Detection**: `DatabricksEnvironment.detect_environment()` auto-detects cluster properties:
- Cluster mode, worker count, Unity Catalog availability
- Optimizes parallel job count based on cluster size
- Falls back gracefully in non-Databricks environments

**Unity Catalog**: When enabled, tables use `catalog.schema.table` format automatically.

## Development Workflows

### Testing
```bash
# Poetry-based development workflow
poetry install
poetry run pytest                    # All tests
poetry run pytest data_loader/tests/test_basic.py  # Specific test
```

**Test Pattern**: Uses `@pytest.fixture(autouse=True)` to mock Spark session in `test_basic.py` - essential for non-Databricks testing.

### Running Locally
```bash
# Demo workflow
poetry run python demo/run_demo.py
python -m data_loader.main create-example-config -o config.json
```

### Databricks Deployment
Deploy as Databricks job with parameters: `["run-cluster", "--config", "/path/to/config.json"]`

## Code Conventions

- **Logging**: Use `loguru` throughout. Setup via `setup_logging()` with structured JSON option
- **Error Handling**: File-level errors don't stop entire pipeline. Retry logic with exponential backoff
- **Pydantic Models**: Heavy use of Pydantic for config validation with `Field()` descriptions
- **Type Hints**: Comprehensive typing throughout, especially for config classes

## Key Integration Points

- **Spark**: All strategies assume Spark/Delta Lake environment via `databricks_config.spark`
- **Delta Tables**: File tracker and all target tables are Delta format
- **Parallel Execution**: Thread-based using `ParallelExecutor` with configurable `max_parallel_jobs`

## Common Patterns to Follow

1. **Config First**: Start any new component by defining its config requirements
2. **Strategy Pattern**: New loading strategies should inherit from `BaseLoadingStrategy`
3. **Cluster Awareness**: Use `ClusterConfig.from_base_config()` for Databricks optimizations
4. **Status Tracking**: Always update file tracker status for new processing paths
