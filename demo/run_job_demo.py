"""Demo script showing how to invoke the Databricks job runner."""

import json
import os
from pathlib import Path

from data_loader.config.table_config import EXAMPLE_CONFIG


def main() -> None:
    demo_dir = Path("demo")
    demo_dir.mkdir(exist_ok=True)
    config_file = demo_dir / "demo_config.json"
    with open(config_file, "w") as fh:
        json.dump(EXAMPLE_CONFIG, fh, indent=2)

    os.environ["CONFIG_PATH"] = str(config_file)
    print(f"Example configuration written to {config_file}")
    print("Set the CONFIG_PATH environment variable and run:")
    print("    python -m data_loader.databricks_job")


if __name__ == "__main__":
    main()
