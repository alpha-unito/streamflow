from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.recovery import CheckpointManager, FailureManager
from streamflow.recovery.checkpoint_manager import (
    DefaultCheckpointManager,
    DummyCheckpointManager,
)
from streamflow.recovery.failure_manager import (
    DefaultFailureManager,
    DummyFailureManager,
)

checkpoint_manager_classes: MutableMapping[str, type[CheckpointManager]] = {
    "default": DefaultCheckpointManager,
    "dummy": DummyCheckpointManager,
}

failure_manager_classes: MutableMapping[str, type[FailureManager]] = {
    "default": DefaultFailureManager,
    "dummy": DummyFailureManager,
}
