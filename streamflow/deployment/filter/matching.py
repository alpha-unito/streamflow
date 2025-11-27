import logging
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


class ConjunctionExpression:
    def __init__(
        self,
        deployment_name: str,
        service: str | None,
        additional_predicates: MutableMapping[str, str],
    ) -> None:
        self.deployment: str = deployment_name
        self.service: str | None = service
        self.additional_predicates: MutableMapping[str, str] = additional_predicates

    def eval(
        self, filter_name: str, deployment_name: str, service: str | None, job: Job
    ) -> bool:
        if deployment_name != self.deployment:
            return False
        if self.service is not None and self.service != service:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Filter {filter_name} on job {job.name} is filtering on "
                    f"the {deployment_name} deployment, but no match was found "
                    f"for the {service} service."
                )
            return False
        for input_name, match in self.additional_predicates.items():
            if input_name not in job.inputs.keys():
                raise ValueError(f"Job {job.name} has no input '{input_name}'.")
            if isinstance(job.inputs[input_name], (FileToken, ListToken, ObjectToken)):
                raise WorkflowDefinitionException(
                    f"Filter {filter_name} on port {input_name} cannot be of type 'file', 'list', or 'object'. "
                    f"These types are not supported for matching."
                )
            if not isinstance(job.inputs[input_name].value, str):
                logger.warning(
                    f"Filter {filter_name} on job {job.name} is processing port {input_name}, "
                    f"but the value is not a string. "
                    f"It will be implicitly cast to a string for matching."
                )
            if match != str(job.inputs[input_name].value):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Filter {filter_name} on the port {input_name} of the job {job.name} "
                        f"did not match {match}. "
                        f"The deployment {deployment_name} is discarded."
                    )
                return False
        return True


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
        self.conditions: MutableSequence[ConjunctionExpression] = []
        for deployments in filters:
            # Retrieve deployment
            target = deployments["target"]
            deployment = target if isinstance(target, str) else target["deployment"]
            service = (
                target["service"]
                if isinstance(target, MutableMapping) and "service" in target
                else None
            )
            self.conditions.append(
                ConjunctionExpression(
                    deployment_name=deployment,
                    service=service,
                    additional_predicates={
                        job["port"]: job["match"] for job in deployments["job"]
                    },
                )
            )

    async def get_targets(
        self, job: Job, targets: MutableSequence[Target]
    ) -> MutableSequence[Target]:
        target_deployments = {t.deployment.name for t in targets}
        filter_deployments = {c.deployment for c in self.conditions}
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
        filtered_targets = set()
        for target in targets:
            for condition in self.conditions:
                # Disjunction expression. The target is added if at least one condition is satisfied
                if condition.eval(
                    filter_name=self.name,
                    deployment_name=target.deployment.name,
                    service=target.service,
                    job=job,
                ):
                    filtered_targets.add(target)
                    break
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
