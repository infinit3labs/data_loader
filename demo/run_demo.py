"""Demo script showcasing configuration driven usage of the data loader."""
from pathlib import Path
from data_loader.config import load_config_from_file
from data_loader.utils.logger import setup_logging


def main() -> None:
    config_path = Path(__file__).parent / "demo_config.yaml"
    config = load_config_from_file(str(config_path))

    setup_logging(config.log_level)

    print("Configuration loaded with", len(config.tables), "table(s)")
    print("This demo does not execute Spark jobs but illustrates the workflow.")
    for table in config.tables:
        print(f"- {table.table_name} from pattern {table.source_path_pattern}")


if __name__ == "__main__":
    main()
