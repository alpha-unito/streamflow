from collections.abc import MutableMapping

from streamflow.core.recovery import RecoveryPolicy
from streamflow.recovery.policy.recovery import RollbackRecoveryPolicy

policy_classes: MutableMapping[str, type[RecoveryPolicy]] = {
    "rollback_recovery": RollbackRecoveryPolicy
}
