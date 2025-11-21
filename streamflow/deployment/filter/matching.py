import logging
import re
from collections.abc import MutableMapping, MutableSequence, MutableSet
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
    def __init__(
        self,
        name: str,
        filters: MutableSequence[
            MutableMapping[
                str,
                MutableMapping[str, str] | MutableSequence[MutableMapping[str, str]],
            ]
        ],
    ):
        super().__init__(name)
        self.deployments_regex: MutableMapping[str, MutableMapping[str, str]] = {}
        self.deployments_services: MutableMapping[str, MutableSet[str]] = {}
        for deployments in filters:
            # Retrieve deployment
            target = deployments["target"]
            deployment = target if isinstance(target, str) else target["deployment"]
            self.deployments_regex.setdefault(deployment, {})
            self.deployments_services.setdefault(deployment, set())
            if isinstance(target, MutableMapping) and "service" in target:
                self.deployments_services[deployment].add(target["service"])
            # Retrieve job
            for job in deployments["job"]:
                if job["port"] in self.deployments_regex[deployment]:
                    raise ValueError("Duplicate input")
                self.deployments_regex[deployment][job["port"]] = job["regex"]

    async def get_targets(
        self, job: Job, targets: MutableSequence[Target]
    ) -> MutableSequence[Target]:
        filtered_targets = []
        target_deployments = {t.deployment.name for t in targets}
        filter_deployments = self.deployments_regex.keys()
        if target_deployments - filter_deployments:
            logger.warning(
                f"Filter {self.name} on job {job.name} discards the following deployments because "
                f"no matching rules are defined: {target_deployments - filter_deployments}"
            )
        if filter_deployments - target_deployments:
            logger.warning(
                f"Filter {self.name} on job {job.name} contains filter deployments that do not match "
                f"any target deployment. Please check for potential typos in the filter: "
                f"{filter_deployments - target_deployments}"
            )

        for target in targets:
            if (
                services := self.deployments_services.get(target.deployment.name, [])
            ) and target.service not in services:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Filter {self.name} on job {job.name} is filtering on "
                        f"the {target.deployment.name} deployment, but no match was found "
                        f"for the {target.service} service."
                    )
                continue
            for input_name, regex in self.deployments_regex.get(
                target.deployment.name, {}
            ).items():
                if input_name not in job.inputs:
                    raise ValueError(f"Job {job.name} has no input '{input_name}'.")
                if isinstance(
                    job.inputs[input_name], (FileToken, ListToken, ObjectToken)
                ):
                    raise WorkflowDefinitionException(
                        f"Filter {self.name} on port {input_name} cannot be of type 'file', 'list', or 'object'. "
                        f"These types are not supported for matching."
                    )
                if not isinstance(job.inputs[input_name].value, str):
                    logger.warning(
                        f"Filter {self.name} on job {job.name} is processing port {input_name}, "
                        f"but the value is not a string. "
                        f"It will be implicitly cast to a string for matching."
                    )
                if re.match(regex, str(job.inputs[input_name].value)):
                    filtered_targets.append(target)
                elif logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Filter {self.name} on the port {input_name} of the job {job.name} "
                        f"did not match the regex {regex}. "
                        f"The deployment {target.deployment.name} is discarded."
                    )

        if len(filtered_targets) == 0:
            raise WorkflowExecutionException(
                f"Filter {self.name} did not find any matching targets for job {job.name} with the provided inputs."
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
