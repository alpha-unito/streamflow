from collections.abc import MutableMapping

from streamflow.core.scheduling import Policy
from streamflow.scheduling.policy.data_locality import DataLocalityPolicy

policy_classes: MutableMapping[str, type[Policy]] = {
    "data_locality": DataLocalityPolicy
}
