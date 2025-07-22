# Runtime Configuration

The loader now supports configuration merging from multiple sources:

1. **YAML file** – base configuration stored in a file.
2. **Environment variables** – override any top level option using the prefix `DATALOADER_`.
3. **Databricks widgets** – when executed on Databricks, widget values are also merged.

The `load_runtime_config` helper combines these sources so environment variables and widget values override settings from the YAML file.

Example:
```bash
export DATALOADER_RAW_DATA_PATH=/mnt/new_raw
python demo/config_merge_demo.py
```
