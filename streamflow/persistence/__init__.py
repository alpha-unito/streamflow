from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.persistence import Database
from streamflow.persistence.sqlite import SqliteDatabase

database_classes: MutableMapping[str, type[Database]] = {
    "default": SqliteDatabase,
    "sqlite": SqliteDatabase,
}
