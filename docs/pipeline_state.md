# Pipeline State Management

The loader now persists pipeline progress in a JSON file so that interrupted runs can resume.

## How it works

- Each table has a status of `not_started`, `in_progress`, `completed` or `failed`.
- The status is stored under the configured `checkpoint_path` using the filename defined by `state_file` in the configuration (default: `pipeline_state.json`).
- The `PipelineStateManager` automatically loads this file on startup and updates it after each table is processed.
- When `process_all_tables` runs, tables already marked as `completed` are skipped making the operation idempotent.

## Resetting State

Call `DataProcessor.reset_pipeline_state()` to clear the saved state and process all tables from the beginning.

## Example

```bash
poetry run python demo/state_management_demo.py
```
