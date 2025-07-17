from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.data import DataManager
from streamflow.data.manager import DefaultDataManager

data_manager_classes: MutableMapping[str, type[DataManager]] = {
    "default": DefaultDataManager
}
