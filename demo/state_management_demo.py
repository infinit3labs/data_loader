from pathlib import Path
from data_loader.core.state_manager import PipelineStateManager, PipelineStageStatus


def main() -> None:
    checkpoint = Path(__file__).parent / "state_checkpoint"
    manager = PipelineStateManager(checkpoint)
    manager.reset()

    # Simulate processing two tables
    manager.update_table_status("customers", PipelineStageStatus.IN_PROGRESS)
    manager.update_table_status(
        "customers", PipelineStageStatus.COMPLETED, {"rows": 10}
    )
    manager.update_table_status(
        "transactions", PipelineStageStatus.FAILED, {"error": "network"}
    )

    print("Saved state:\n", manager.state)

    # Reload to demonstrate persistence
    manager2 = PipelineStateManager(checkpoint)
    print("Loaded state after restart:\n", manager2.state)


if __name__ == "__main__":
    main()
