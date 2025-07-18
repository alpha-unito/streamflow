from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.scheduling import Scheduler
from streamflow.scheduling.scheduler import DefaultScheduler

scheduler_classes: MutableMapping[str, type[Scheduler]] = {"default": DefaultScheduler}
