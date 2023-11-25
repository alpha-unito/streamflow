import random
from typing import MutableSequence

from importlib_resources import files

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
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("shuffle.json")
            .read_text("utf-8")
        )
