from streamflow.recovery.checkpoint_manager import DefaultCheckpointManager, DummyCheckpointManager
from streamflow.recovery.failure_manager import DefaultFailureManager, DummyFailureManager

checkpoint_manager_classes = {
    'default': DefaultCheckpointManager,
    'dummy': DummyCheckpointManager
}

failure_manager_classes = {
    'default': DefaultFailureManager,
    'dummy': DummyFailureManager
}
