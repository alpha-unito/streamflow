from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.deployment import BindingFilter
from streamflow.deployment.filter.matching import MatchingBindingFilter
from streamflow.deployment.filter.shuffle import ShuffleBindingFilter

binding_filter_classes: MutableMapping[str, type[BindingFilter]] = {
    "matching": MatchingBindingFilter,
    "shuffle": ShuffleBindingFilter,
}
