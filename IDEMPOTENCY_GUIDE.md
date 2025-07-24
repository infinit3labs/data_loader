# Idempotency and Recovery Guide

## Overview

The Databricks Data Loader has been enhanced with comprehensive idempotency and recovery mechanisms to ensure that all operations can be safely rerun multiple times and that the pipeline can recover from any failure point. This guide explains how these features work and how to use them.

## Key Concepts

### Idempotency
An operation is **idempotent** if it can be performed multiple times with the same result. In data loading, this means:
- Running the same pipeline multiple times produces the same final state
- No duplicate data is created
- Failed operations can be safely retried
- Partial failures don't corrupt the data

### Recovery
The ability to restore the pipeline to a consistent state after failures, including:
- Detecting inconsistent states
- Automatically fixing common issues
- Resuming from the point of failure
- Preventing data corruption

## Idempotency Features

### 1. Pipeline Lock System

Prevents concurrent pipeline executions that could interfere with each other:

```python
# Automatic lock management
processor = DataProcessor(config)
result = processor.process_all_tables()  # Uses lock by default

# Manual lock management
with processor.pipeline_lock:
    # Your processing logic here
    pass
```

**Features:**
- File-based locking with process verification
- Automatic stale lock detection and cleanup
- Configurable timeout periods
- Force unlock capability for emergency situations

### 2. Enhanced File Tracking

Tracks the processing status of every file to prevent reprocessing:

```python
# Files are automatically tracked through these states:
# PENDING -> PROCESSING -> COMPLETED/FAILED
```

**Guarantees:**
- Files marked as COMPLETED are never reprocessed
- Failed files can be retried up to the configured limit
- Orphaned processing records are automatically detected
- Atomic status updates prevent race conditions

### 3. Strategy-Level Idempotency

Each loading strategy implements idempotency checks:

#### Append Strategy
- Detects duplicate loads using audit columns (`_source_file`, `_load_timestamp`)
- Automatically cleans up partial loads on failure
- Prevents duplicate records from the same source file

#### SCD2 Strategy  
- Tracks source files to prevent duplicate processing
- Maintains historical consistency
- Sophisticated cleanup for partial SCD2 operations

### 4. Consistency Validation

Comprehensive validation to detect and fix inconsistent states:

```bash
# Check pipeline consistency
python -m data_loader.main validate-consistency --config config.json

# Automatically fix issues
python -m data_loader.main validate-consistency --config config.json --fix

# Or run fixes separately
python -m data_loader.main fix-inconsistencies --config config.json
```

## Recovery Features

### 1. Automatic State Detection

The system automatically detects various inconsistent states:
- Orphaned processing records (files stuck in "processing" state)
- Tables marked complete with failed files
- Stale pipeline locks
- Missing or corrupted state files

### 2. Self-Healing Mechanisms

Many issues are automatically resolved:
- Orphaned records are marked as failed for retry
- Stale locks are automatically released
- Inconsistent table states are reset appropriately

### 3. Recovery Commands

#### Validate Consistency
```bash
python -m data_loader.main validate-consistency --config config.json

# Example output:
# === PIPELINE CONSISTENCY VALIDATION ===
# Overall Status: INCONSISTENT
# 
# File Tracker Status: inconsistent
# Total Files Checked: 150
# Inconsistencies Found: 3
#   - Orphaned processing record: /data/file1.parquet
#   - Orphaned processing record: /data/file2.parquet
# 
# Table Consistency:
#   ✓ customers
#   ✗ transactions
#     - Table marked as completed but has 2 failed files
```

#### Fix Inconsistencies
```bash
python -m data_loader.main fix-inconsistencies --config config.json

# Example output:
# === FIXING PIPELINE INCONSISTENCIES ===
# Fixes Applied: 5
# Fixes Failed: 0
# 
# Actions taken:
#   - Fixed 2 orphaned processing records
#   - Reset transactions status from completed to failed
#   - Cleaned up 3 stale processing records
```

#### Force Unlock
```bash
python -m data_loader.main force-unlock --config config.json

# Use only when pipeline is stuck with stale lock
# Example output:
# Current lock held by PID 12345 since 2024-01-15T10:00:00
# Are you sure you want to force release the lock? [y/N]: y
# ✓ Pipeline lock force released
```

## Best Practices

### 1. Configuration for Idempotency

Ensure your configuration supports idempotency:

```yaml
# Enable audit columns for tracking
tables:
  - table_name: "customers"
    loading_strategy: "scd2"
    # Required for SCD2 idempotency
    primary_keys: ["customer_id"]
    tracking_columns: ["name", "email"]
    scd2_effective_date_column: "effective_date"
    scd2_end_date_column: "end_date"
    scd2_current_flag_column: "is_current"

  - table_name: "transactions"
    loading_strategy: "append"
    # Audit columns added automatically for tracking

# Configure appropriate retry settings
retry_attempts: 3
timeout_minutes: 60

# Ensure checkpoint directory is persistent
checkpoint_path: "/dbfs/checkpoints/data_loader"
```

### 2. Monitoring and Alerting

Regularly check pipeline health:

```bash
# Daily health check
python -m data_loader.main validate-consistency --config config.json

# In your monitoring system, alert on:
# - validation exit code != 0
# - presence of inconsistencies
# - repeated failures
```

### 3. Recovery Procedures

Standard recovery workflow:

1. **Detect Issues**
   ```bash
   python -m data_loader.main validate-consistency --config config.json
   ```

2. **Analyze Problems**
   - Review inconsistencies and warnings
   - Check logs for error patterns
   - Verify source data integrity

3. **Apply Fixes**
   ```bash
   python -m data_loader.main fix-inconsistencies --config config.json
   ```

4. **Verify Resolution**
   ```bash
   python -m data_loader.main validate-consistency --config config.json
   ```

5. **Resume Processing**
   ```bash
   python -m data_loader.main run --config config.json
   ```

### 4. Handling Emergency Situations

#### Stuck Pipeline
```bash
# Check lock status
python -m data_loader.main status --config config.json

# Force unlock if necessary (use caution)
python -m data_loader.main force-unlock --config config.json
```

#### Data Corruption
```bash
# Reset pipeline state to restart from beginning
python -m data_loader.main reset-state --config config.json

# Validate and fix any remaining issues
python -m data_loader.main validate-consistency --config config.json --fix
```

## Technical Implementation

### File Tracker Schema

The file tracker uses the following schema for complete audit trail:

```sql
CREATE TABLE file_processing_tracker (
  file_path STRING,                 -- Primary key
  file_size BIGINT,
  file_modified_time TIMESTAMP,
  table_name STRING,
  status STRING,                    -- pending, processing, completed, failed
  processing_start_time TIMESTAMP,
  processing_end_time TIMESTAMP,
  error_message STRING,
  retry_count INTEGER,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
```

### Lock File Format

Pipeline locks use JSON format for cross-platform compatibility:

```json
{
  "pid": 12345,
  "hostname": "worker-node-01",
  "acquired_at": "2024-01-15T10:00:00.000Z",
  "timeout_at": "2024-01-15T12:00:00.000Z",
  "lock_name": "data_loader_pipeline"
}
```

### Audit Columns

All strategies automatically add audit columns:

- `_source_file`: Original file path for traceability
- `_load_timestamp`: When the record was loaded
- `_batch_id`: Batch identifier for grouping

## Testing Idempotency

Verify your pipeline is truly idempotent:

```python
# Run the same pipeline multiple times
results = []
for i in range(3):
    result = processor.process_all_tables()
    results.append(result)

# Verify idempotent behavior:
# 1. Same files discovered each time
assert all(r["total_files_discovered"] == results[0]["total_files_discovered"] 
           for r in results)

# 2. Decreasing files processed (already completed files skipped)
processed_counts = [r["total_files_processed"] for r in results]
assert processed_counts[1] <= processed_counts[0]
assert processed_counts[2] <= processed_counts[1]

# 3. Final data state is identical
```

## Troubleshooting

### Common Issues

1. **Lock File Exists**
   - Check if pipeline is actually running: `ps aux | grep data_loader`
   - Use force unlock if process is gone: `--force-unlock`

2. **Orphaned Processing Records**
   - Automatically detected by consistency validation
   - Fixed by `fix-inconsistencies` command

3. **Duplicate Data**
   - Usually indicates idempotency failure
   - Check audit columns configuration
   - Verify strategy implementation

4. **Inconsistent State**
   - Run full consistency validation
   - Apply automatic fixes
   - Check logs for underlying issues

### Performance Considerations

- File tracker queries scale with number of files
- Lock operations are lightweight but require shared storage
- Consistency validation time increases with data volume
- Recovery operations may require significant I/O

### Monitoring Metrics

Track these metrics for operational health:

- Files processed per run (should decrease over time for idempotent runs)
- Consistency validation results
- Lock acquisition failures
- Recovery operation frequency
- Processing time trends

## Conclusion

The enhanced idempotency and recovery features make the Databricks Data Loader robust and production-ready. By following the best practices and using the recovery tools, you can ensure reliable, repeatable data processing even in the face of various failure scenarios.

For more detailed information, see the API documentation and test files:
- `test_idempotency.py` - Core idempotency tests
- `test_recovery.py` - Recovery scenario tests  
- `test_integration.py` - End-to-end integration tests