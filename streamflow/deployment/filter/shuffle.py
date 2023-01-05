import os
import random
from typing import MutableSequence

import pkg_resources

from streamflow.core.deployment import BindingFilter, Target
from streamflow.core.workflow import Job


class ShuffleBindingFilter(BindingFilter):
    async def get_targets(
        self, job: Job, targets: MutableSequence[Target]
    ) -> MutableSequence[Target]:
        random.shuffle(targets)
        return targets

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "shuffle.json")
        )
