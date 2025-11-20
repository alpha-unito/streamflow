import re
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files

from streamflow.core.deployment import BindingFilter, Target
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.workflow import Job
from streamflow.log_handler import logger
from streamflow.workflow.token import FileToken, ListToken, ObjectToken


class MatchingBindingFilter(BindingFilter):
    # TODO: Add documentation
    #  example:
    #  bindingFilters:
    #   f_local:
    #     type: matching
    #     config:
    #       match:
    #         - deployment: locally
    #           job:
    #             - input: extractfile
    #               regex: ".*.java"
    #         - deployment: leonardo
    #           job:
    #             - input: extractfile
    #               regex: ".*.txt"
    # TODO: Add service
    # TODO: Add a check to ensure that the deployments and services defined inside the filter exist in the deployments section.
    # TODO: Refactor constructor parameters
    def __init__(self, **kwargs):
        self.deployments: MutableMapping[str, MutableMapping[str, str]] = {}
        for deployments in kwargs["match"]:
            deployment = deployments["deployment"]
            self.deployments.setdefault(deployment, {})
            for job in deployments["job"]:
                if job["input"] in self.deployments[deployment]:
                    raise ValueError("Duplicate input")
                self.deployments[deployment][job["input"]] = job["regex"]

    async def get_targets(
        self, job: Job, targets: MutableSequence[Target]
    ) -> MutableSequence[Target]:
        filtered_targets = []
        for target in targets:
            # TODO: If the target.deployment.name is not defined in the filter,
            #  should it be removed from the targets list? Evaluate if this is the correct behavior.
            for input_name, regex in self.deployments.get(
                target.deployment.name, {}
            ).items():
                if input_name not in job.inputs:
                    raise ValueError(f"Job {job.name} has no input '{input_name}'.")
                if isinstance(
                    job.inputs[input_name], (FileToken, ListToken, ObjectToken)
                ):
                    raise WorkflowDefinitionException(
                        f"Job {job.name} input '{input_name}' cannot be of type 'file', 'list', or 'object'. "
                        f"These types are not supported for matching."
                    )
                if not isinstance(job.inputs[input_name].value, str):
                    logger.warning(
                        f"Job {job.name} input '{input_name}' is not a string. "
                        f"It will be implicitly cast to a string for matching."
                    )
                if re.match(regex, str(job.inputs[input_name].value)):
                    filtered_targets.append(target)
        if len(filtered_targets) == 0:
            raise WorkflowExecutionException(
                f"No matching targets found for job '{job.name}' based on the provided inputs. "
            )
        return filtered_targets

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("matching.json")
            .read_text("utf-8")
        )
