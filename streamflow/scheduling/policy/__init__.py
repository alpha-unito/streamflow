from collections.abc import MutableMapping

from streamflow.core.scheduling import SchedulingPolicy
from streamflow.scheduling.policy.data_locality import DataLocalityPolicy

policy_classes: MutableMapping[str, type[SchedulingPolicy]] = {
    "data_locality": DataLocalityPolicy
}
