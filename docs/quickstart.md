# Quickstart

This guide helps you get the data loader running using **Poetry**.

## Installation

```bash
# Install dependencies
poetry install

# Run tests to verify the installation
poetry run pytest
```

## Running the demos

Execute the example scripts using `poetry run` so they use the
project's virtual environment.

```bash
# Basic configuration demo
poetry run python demo/run_demo.py

# Configuration merging
poetry run python demo/config_merge_demo.py

# Pipeline state management
poetry run python demo/state_management_demo.py

# Full usage example
poetry run python example_usage.py

# Cluster mode demonstration
poetry run python cluster_demo.py
```

The `demo/demo_config.yaml` file is used by the demos and can be modified to
experiment with different settings.
