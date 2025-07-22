# Resetting Pipeline State

The loader persists progress under the configured `checkpoint_path` so interrupted runs can be resumed.
Use the `reset-state` command to clear this saved state.

```bash
python -m data_loader.main reset-state --config path/to/config.yaml
```

The command loads the configuration (including any environment overrides) and deletes the saved pipeline state file. Use this when you want to restart processing from scratch.
