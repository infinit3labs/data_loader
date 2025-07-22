from __future__ import annotations

"""Pipeline state management for tracking progress and supporting restarts."""

import json
from enum import Enum
from datetime import datetime
from pathlib import Path
from typing import Dict, Any


class PipelineStageStatus(str, Enum):
    """Status of a pipeline stage."""

    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class PipelineStateManager:
    """Manage pipeline state persistence in a JSON file."""

    def __init__(self, checkpoint_path: str, state_file: str = "pipeline_state.json"):
        self.checkpoint_path = Path(checkpoint_path)
        self.state_file = self.checkpoint_path / state_file
        self.state: Dict[str, Any] = {"tables": {}}
        self._load_state()

    def _load_state(self) -> None:
        if self.state_file.exists():
            with open(self.state_file, "r", encoding="utf-8") as fh:
                self.state = json.load(fh)
        else:
            self.state = {"tables": {}}

    def save_state(self) -> None:
        self.checkpoint_path.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w", encoding="utf-8") as fh:
            json.dump(self.state, fh, indent=2)

    def get_table_status(self, table_name: str) -> PipelineStageStatus:
        entry = self.state.get("tables", {}).get(table_name)
        if entry:
            return PipelineStageStatus(
                entry.get("status", PipelineStageStatus.NOT_STARTED.value)
            )
        return PipelineStageStatus.NOT_STARTED

    def update_table_status(
        self,
        table_name: str,
        status: PipelineStageStatus,
        details: Dict[str, Any] | None = None,
    ) -> None:
        entry = self.state.setdefault("tables", {}).get(table_name, {})
        entry["status"] = status.value
        entry["last_updated"] = datetime.utcnow().isoformat()
        if details is not None:
            entry["details"] = details
        self.state["tables"][table_name] = entry
        self.save_state()

    def reset(self) -> None:
        self.state = {"tables": {}}
        self.save_state()
