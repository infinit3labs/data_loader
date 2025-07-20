import os

import pytest


@pytest.fixture(autouse=True)
def disable_spark(monkeypatch):
    monkeypatch.setenv("DATALOADER_DISABLE_SPARK", "1")
    yield
