# Configuration Templates

Below are example configuration snippets that can be reused in your own
projects.

## Basic Table Configuration
```yaml
table_name: my_table
database_name: analytics
source_path_pattern: /path/to/files/*.parquet
loading_strategy: append
file_format: parquet
schema_evolution: true
partition_columns:
  - date
```

## Full Data Loader Configuration
```yaml
raw_data_path: /data/raw/
processed_data_path: /data/processed/
checkpoint_path: /data/checkpoints/
tables:
  - table_name: customers
    database_name: analytics
    source_path_pattern: /data/raw/customers/*.parquet
    loading_strategy: scd2
    primary_keys:
      - customer_id
    tracking_columns:
      - name
      - email
    partition_columns:
      - country
```
