from pathlib import Path
from data_loader.config import load_runtime_config


def main() -> None:
    cfg_path = Path(__file__).parent / "demo_config.yaml"
    print(f"Loading base configuration from {cfg_path}")

    config = load_runtime_config(str(cfg_path))
    print("Raw data path:", config.raw_data_path)
    print("Processed data path:", config.processed_data_path)
    print("(Override with environment variables e.g. DATALOADER_RAW_DATA_PATH)")


if __name__ == "__main__":
    main()
