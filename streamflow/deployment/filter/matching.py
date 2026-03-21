import logging
from collections.abc import MutableMapping, MutableSequence, MutableSet
from importlib.resources import files

from streamflow.core.deployment import BindingFilter, Target
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.utils import get_job_step_name
from streamflow.core.workflow import Job
from streamflow.log_handler import logger
from streamflow.workflow.token import FileToken, ListToken, ObjectToken


class MatchingRule:
    __slots__ = ("deployment", "filter", "predicates", "service")

    def __init__(
        self,
        deployment: str,
        filter_: str,
        predicates: MutableMapping[str, str],
        service: str | None = None,
    ) -> None:
        self.deployment: str = deployment
        self.filter: str = filter_
        self.predicates: MutableMapping[str, str] = predicates
        self.service: str | None = service

    def eval(self, job: Job, deployment: str, service: str | None = None) -> bool:
        if deployment != self.deployment:
            return False
        if self.service is not None and self.service != service:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Filter {self.filter} on job {job.name} is filtering on "
                    f"the {deployment} deployment, but no match was found "
                    f"for the {service} service."
                )
            return False
        for input_name, match in self.predicates.items():
            if input_name not in job.inputs.keys():
                raise ValueError(f"Job {job.name} has no input '{input_name}'.")
            if isinstance(job.inputs[input_name], (FileToken, ListToken, ObjectToken)):
                raise WorkflowDefinitionException(
                    f"Filter {self.filter} on port {input_name} cannot be of type 'file', 'list', or 'object'. "
                    f"These types are not supported for matching."
                )
            if not isinstance(job.inputs[input_name].value, str):
                logger.warning(
                    f"Filter {self.filter} on job {job.name} is processing port {input_name}, "
                    f"but the value is not a string. "
                    f"It will be implicitly cast to a string for matching."
                )
            if match != str(job.inputs[input_name].value):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Filter {self.filter} on the port {input_name} of the job {job.name} "
                        f"did not match {match}. "
                        f"The deployment {deployment} is discarded."
                    )
                return False
        return True


class MatchingBindingFilter(BindingFilter):
    __slots__ = ("matching_rules", "name", "_evaluated_steps")

    def __init__(
        self,
        name: str,
        filters: MutableSequence[
            MutableMapping[
                str,
                MutableMapping[str, str] | MutableSequence[MutableMapping[str, str]],
            ]
        ],
    ) -> None:
        super().__init__(name)
        self.matching_rules: MutableSequence[MatchingRule] = []
        for deployments in filters:
            # Retrieve deployment
            target = deployments["target"]
            deployment = target if isinstance(target, str) else target["deployment"]
            service = (
                target["service"]
                if isinstance(target, MutableMapping) and "service" in target
                else None
            )
            self.matching_rules.append(
                MatchingRule(
                    deployment=deployment,
                    filter_=self.name,
                    predicates={
                        job["port"]: job["match"] for job in deployments["job"]
                    },
                    service=service,
                )
            )
        self._evaluated_steps: MutableSet[str] = set()

    async def get_targets(
        self, job: Job, targets: MutableSequence[Target]
    ) -> MutableSequence[Target]:
        if (
            logger.isEnabledFor(logging.WARNING)
            and not (step_name := get_job_step_name(job.name)) in self._evaluated_steps
        ):
            self._evaluated_steps.add(step_name)
            if (target_deployments := {t.deployment.name for t in targets}) - (
                filter_deployments := {c.deployment for c in self.matching_rules}
            ):
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
        filtered_targets = set()
        for target in targets:
            if any(
                matching_rule.eval(
                    job=job,
                    deployment=target.deployment.name,
                    service=target.service,
                )
                for matching_rule in self.matching_rules
            ):
                filtered_targets.add(target)
        if len(filtered_targets) == 0:
            raise WorkflowExecutionException(
                f"Filter {self.name} did not find any matching targets for job {job.name} with the provided inputs."
            )
        return list(filtered_targets)

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("matching.json")
            .read_text("utf-8")
        )
