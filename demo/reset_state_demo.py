"""Demo for resetting the pipeline state using configuration."""
from pathlib import Path
from data_loader.config import load_config_from_file
from data_loader.core.processor import DataProcessor


def main() -> None:
    cfg_path = Path(__file__).parent / "demo_config.yaml"
    config = load_config_from_file(str(cfg_path))
    processor = DataProcessor(config)
    processor.reset_pipeline_state()
    print("Pipeline state reset for demo")


if __name__ == "__main__":
    main()
