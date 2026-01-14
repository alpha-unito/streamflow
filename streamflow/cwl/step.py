from __future__ import annotations

import asyncio
import logging
import urllib.parse
from abc import ABC
from collections.abc import MutableMapping, MutableSequence
from typing import Any, cast

from typing_extensions import Self

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation, DataType
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.processor import CommandOutputProcessor
from streamflow.core.utils import get_entity_ids, get_tag, random_name
from streamflow.core.workflow import (
    Command,
    CommandOutput,
    Job,
    Port,
    Token,
)
from streamflow.cwl import utils
from streamflow.cwl.utils import LoadListing, build_token
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.data import remotepath
from streamflow.data.remotepath import StreamFlowPath
from streamflow.deployment.utils import get_path_processor
from streamflow.log_handler import logger
from streamflow.workflow.port import JobPort
from streamflow.workflow.step import (
    ConditionalStep,
    ExecuteStep,
    InputInjectorStep,
    LoopOutputStep,
    ScheduleStep,
    TransferStep,
)
from streamflow.workflow.token import IterationTerminationToken, ListToken
from streamflow.workflow.utils import get_job_token


async def _download_file(job: Job, url: str, context: StreamFlowContext) -> str:
    locations = context.scheduler.get_locations(job.name)
    try:
        filepaths = set(
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        remotepath.download(context, location, url, job.input_directory)
                    )
                    for location in locations
                )
            )
        )
        if len(filepaths) > 1:
            raise WorkflowExecutionException(
                "StreamFlow does not currently support multiple download "
                "paths on different locations for the same file"
            )
        else:
            return next(iter(filepaths))
    except Exception:
        raise WorkflowExecutionException("Error downloading file from " + url)


class CWLBaseConditionalStep(ConditionalStep, ABC):
    def __init__(self, name: str, workflow: CWLWorkflow):
        super().__init__(name, workflow)
        self.skip_ports: MutableMapping[str, str] = {}

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "skip_ports": {k: p.persistent_id for k, p in self.get_skip_ports().items()}
        }

    def add_skip_port(self, name: str, port: Port) -> None:
        if port.name not in self.workflow.ports:
            self.workflow.ports[port.name] = port
        self.skip_ports[name] = port.name

    def get_skip_ports(self) -> MutableMapping[str, Port]:
        return {k: self.workflow.ports[v] for k, v in self.skip_ports.items()}


class CWLConditionalStep(CWLBaseConditionalStep):
    def __init__(
        self,
        name: str,
        workflow: CWLWorkflow,
        expression: str,
        expression_lib: MutableSequence[str] | None = None,
        full_js: bool = False,
    ):
        super().__init__(name, workflow)
        self.expression: str = expression
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.full_js: bool = full_js

    async def _eval(self, inputs: MutableMapping[str, Token]) -> bool:
        context = utils.build_context(inputs)
        condition = utils.eval_expression(
            expression=self.expression,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib,
        )
        if condition is True or condition is False:
            return condition
        else:
            raise WorkflowDefinitionException(
                "Conditional 'when' must evaluate to 'true' or 'false'"
            )

    async def _on_true(self, inputs: MutableMapping[str, Token]) -> None:
        # Propagate output tokens
        for port_name, port in self.get_output_ports().items():
            port.put(
                await self._persist_token(
                    token=inputs[port_name].update(inputs[port_name].value),
                    port=port,
                    input_token_ids=get_entity_ids(inputs.values()),
                )
            )

    async def _on_false(self, inputs: MutableMapping[str, Token]) -> None:
        # Propagate skip tokens
        for port in self.get_skip_ports().values():
            port.put(
                await self._persist_token(
                    token=Token(value=None, tag=get_tag(inputs.values())),
                    port=port,
                    input_token_ids=get_entity_ids(inputs.values()),
                )
            )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "expression": self.expression,
            "expression_lib": self.expression_lib,
            "full_js": self.full_js,
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        params = row["params"]
        step = cls(
            name=row["name"],
            workflow=cast(
                CWLWorkflow,
                await loading_context.load_workflow(context, row["workflow"]),
            ),
            expression=params["expression"],
            expression_lib=params["expression_lib"],
            full_js=params["full_js"],
        )
        for k, port in zip(
            params["skip_ports"].keys(),
            await asyncio.gather(
                *(
                    asyncio.create_task(loading_context.load_port(context, port_id))
                    for port_id in params["skip_ports"].values()
                )
            ),
            strict=True,
        ):
            step.add_skip_port(k, port)
        return step


class CWLLoopConditionalStep(CWLConditionalStep):
    async def _eval(self, inputs: MutableMapping[str, Token]) -> bool:
        context = utils.build_context(inputs)
        condition = utils.eval_expression(
            expression=self.expression,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib,
        )
        if condition is True or condition is False:
            return condition
        else:
            raise WorkflowDefinitionException(
                "Conditional 'when' must evaluate to 'true' or 'false'"
            )

    async def _on_true(self, inputs: MutableMapping[str, Token]) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Step {self.name} condition evaluated true "
                f"on inputs {[t.tag for t in inputs.values()]}"
            )
        # Next iteration: propagate outputs to the loop
        for port_name, port in self.get_output_ports().items():
            port.put(
                await self._persist_token(
                    token=inputs[port_name].update(inputs[port_name].value),
                    port=port,
                    input_token_ids=get_entity_ids(inputs.values()),
                )
            )

    async def _on_false(self, inputs: MutableMapping[str, Token]) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Step {self.name} condition evaluated false "
                f"on inputs {[t.tag for t in inputs.values()]}"
            )
        # Loop termination: propagate outputs outside the loop
        for port in self.get_skip_ports().values():
            port.put(IterationTerminationToken(tag=get_tag(inputs.values())))


class CWLEmptyScatterConditionalStep(CWLBaseConditionalStep):
    def __init__(self, name: str, workflow: CWLWorkflow, scatter_method: str):
        super().__init__(name, workflow)
        self.scatter_method: str = scatter_method

    async def _eval(self, inputs: MutableMapping[str, Token]) -> bool:
        return all(
            isinstance(i, ListToken) and len(i.value) > 0 for i in inputs.values()
        )

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            workflow=cast(
                CWLWorkflow,
                await loading_context.load_workflow(context, row["workflow"]),
            ),
            scatter_method=row["params"]["scatter_method"],
        )

    async def _on_true(self, inputs: MutableMapping[str, Token]) -> None:
        # Propagate output tokens
        for port_name, port in self.get_output_ports().items():
            port.put(
                await self._persist_token(
                    token=inputs[port_name].update(inputs[port_name].value),
                    port=port,
                    input_token_ids=get_entity_ids(inputs.values()),
                )
            )

    async def _on_false(self, inputs: MutableMapping[str, Token]) -> None:
        # Get empty scatter return value
        if self.scatter_method == "nested_crossproduct":
            token_value = [
                ListToken(value=[], tag=get_tag(inputs.values())) for _ in inputs
            ]
        else:
            token_value = []
        # Propagate skip tokens
        for port in self.get_skip_ports().values():
            port.put(
                await self._persist_token(
                    token=ListToken(value=token_value, tag=get_tag(inputs.values())),
                    port=port,
                    input_token_ids=get_entity_ids(inputs.values()),
                )
            )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "scatter_method": self.scatter_method
        }


class CWLExecuteStep(ExecuteStep):
    def __init__(
        self,
        name: str,
        workflow: CWLWorkflow,
        job_port: JobPort,
        recoverable: bool | str,
        expression_lib: MutableSequence[str] | None = None,
        full_js: bool = False,
    ):
        super().__init__(name, workflow, job_port)
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.full_js: bool = full_js
        self.recoverable: bool | str = recoverable
        self._recoverable_map: MutableMapping[str, bool] = {}

    def _is_recoverable(self, job: Job) -> bool:
        if (recoverable_ := self._recoverable_map.get(job.name)) is None:
            if isinstance(self.recoverable, bool):
                recoverable_ = self.recoverable
            else:
                context = utils.build_context(job.inputs)
                recoverable_ = utils.eval_expression(
                    expression=self.recoverable,
                    context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                )
            self._recoverable_map[job.name] = recoverable_
        return recoverable_

    async def _retrieve_output(
        self,
        job: Job,
        output_name: str,
        output_port: Port,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
    ) -> None:
        if (
            token := await self.output_processors[output_name].process(
                job, command_output, connector, self._is_recoverable(job)
            )
        ) is not None:
            job_token = get_job_token(
                job.name, self.get_input_port("__job__").token_list
            )
            output_port.put(
                await self._persist_token(
                    token=token,
                    port=output_port,
                    input_token_ids=get_entity_ids((*job.inputs.values(), job_token)),
                )
            )
            await self.workflow.context.failure_manager.notify(
                output_port.name,
                token,
                job_token,
            )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "recoverable": self.recoverable,
            "expression_lib": self.expression_lib,
            "full_js": self.full_js,
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        params = row["params"]
        step = cls(
            name=row["name"],
            workflow=cast(
                CWLWorkflow,
                await loading_context.load_workflow(context, row["workflow"]),
            ),
            recoverable=params["recoverable"],
            expression_lib=params["expression_lib"],
            full_js=params["full_js"],
            job_port=cast(
                JobPort, await loading_context.load_port(context, params["job_port"])
            ),
        )
        step.output_connectors = params["output_connectors"]
        step.output_processors = {
            k: v
            for k, v in zip(
                params["output_processors"].keys(),
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            CommandOutputProcessor.load(context, p, loading_context)
                        )
                        for p in params["output_processors"].values()
                    )
                ),
                strict=True,
            )
        }
        if params["command"]:
            step.command = await Command.load(
                context, params["command"], loading_context, step
            )
        return step


class CWLInputInjectorStep(InputInjectorStep):
    def __init__(self, name: str, workflow: CWLWorkflow, job_port: JobPort):
        super().__init__(name, workflow, job_port)

    async def process_input(self, job: Job, token_value: Any) -> Token:
        return await build_token(
            job=job,
            inputs=job.inputs,
            token_value=token_value,
            cwl_version=cast(CWLWorkflow, self.workflow).cwl_version,
            streamflow_context=self.workflow.context,
            recoverable=True,
        )


class CWLLoopOutputAllStep(LoopOutputStep):
    async def _process_output(self, tag: str) -> Token:
        return ListToken(
            tag=tag,
            value=sorted(
                self.token_map.get(tag, []), key=lambda t: int(t.tag.split(".")[-1])
            ),
        )


class CWLLoopOutputLastStep(LoopOutputStep):
    async def _process_output(self, tag: str) -> Token:
        return sorted(
            self.token_map.get(tag, [Token(value=None)]),
            key=lambda t: int(t.tag.split(".")[-1]),
        )[-1].retag(tag=tag)


class CWLScheduleStep(ScheduleStep):
    async def _set_job_directories(
        self,
        connector: Connector,
        locations: MutableSequence[ExecutionLocation],
        job: Job,
    ) -> None:
        await super()._set_job_directories(connector, locations, job)
        hardware = self.workflow.context.scheduler.get_hardware(job.name)
        hardware.storage["__outdir__"].paths = {job.output_directory}
        hardware.storage["__tmpdir__"].paths = {job.tmp_directory}


class CWLTransferStep(TransferStep):
    def __init__(
        self,
        name: str,
        workflow: CWLWorkflow,
        job_port: JobPort,
        prefix_path: bool = True,
        writable: bool = False,
    ):
        super().__init__(name, workflow, job_port)
        self.prefix_path: bool = prefix_path
        self.writable: bool = writable

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        params = row["params"]
        step = cls(
            name=row["name"],
            workflow=cast(
                CWLWorkflow,
                await loading_context.load_workflow(context, row["workflow"]),
            ),
            job_port=cast(
                JobPort, await loading_context.load_port(context, params["job_port"])
            ),
            prefix_path=params["prefix_path"],
            writable=params["writable"],
        )
        return step

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "prefix_path": self.prefix_path,
            "writable": self.writable,
        }

    async def _transfer_value(self, job: Job, token_value: Any) -> Any:
        match token_value:
            case Token():
                token = token_value.update(
                    await self._transfer_value(job, token_value.value)
                )
                token.recoverable = False
                return token
            case MutableSequence():
                return await asyncio.gather(
                    *(
                        asyncio.create_task(self._transfer_value(job, element))
                        for element in token_value
                    )
                )
            case MutableMapping():
                if utils.get_token_class(token_value) in ["File", "Directory"]:
                    return await self._update_file_token(job, token_value)
                else:
                    return dict(
                        zip(
                            token_value.keys(),
                            await asyncio.gather(
                                *(
                                    asyncio.create_task(
                                        self._transfer_value(job, element)
                                    )
                                    for element in token_value.values()
                                )
                            ),
                            strict=True,
                        )
                    )
            case _:
                return token_value

    async def _update_listing(
        self,
        job: Job,
        token_value: MutableMapping[str, Any],
        dst_path: StreamFlowPath | None = None,
        src_location: DataLocation | None = None,
    ) -> MutableSequence[MutableMapping[str, Any]]:
        existing, tasks = [], []
        for element in token_value["listing"]:
            if src_location and self.workflow.context.data_manager.get_data_locations(
                path=element["path"],
                deployment=src_location.deployment,
                location_name=src_location.name,
            ):
                # adjust the path
                existing.append(
                    utils.remap_token_value(
                        path_processor=get_path_processor(
                            self.workflow.context.scheduler.get_connector(job.name)
                        ),
                        old_dir=token_value["path"],
                        new_dir=str(dst_path),
                        value=element,
                    )
                )
            else:
                tasks.append(
                    asyncio.create_task(
                        self._update_file_token(
                            job=job, token_value=element, dst_path=dst_path
                        )
                    )
                )
        return sorted(
            existing + await asyncio.gather(*tasks), key=lambda t: t["basename"]
        )

    async def _update_file_token(
        self,
        job: Job,
        token_value: MutableMapping[str, Any],
        dst_path: StreamFlowPath | None = None,
    ) -> MutableMapping[str, Any]:
        token_class = utils.get_token_class(token_value)
        # Get destination coordinates
        dst_connector = self.workflow.context.scheduler.get_connector(job.name)
        dst_locations = self.workflow.context.scheduler.get_locations(job.name)
        dst_dir = StreamFlowPath(
            job.input_directory,
            context=self.workflow.context,
            location=next(iter(dst_locations)),
        )
        if dst_path is not None and dst_path.is_relative_to(dst_dir):
            if len((rel_path := dst_path.relative_to(dst_dir)).parts) > 0:
                dst_dir /= rel_path.parts[0]
        elif self.prefix_path:
            dst_dir /= random_name()
        # Extract location
        location = token_value.get("location", token_value.get("path"))
        if location and "://" in location:
            # Manage remote files
            match urllib.parse.urlsplit(location).scheme:
                case "http" | "https":
                    location = await _download_file(
                        job, location, self.workflow.context
                    )
                case "file":
                    location = urllib.parse.unquote(location[7:])
                case scheme:
                    raise WorkflowExecutionException(
                        f"Unsupported scheme `{scheme}` in location `{location}`"
                    )
        # If basename is explicitly stated in the token, use it as destination path
        if "basename" in token_value:
            dst_path = (dst_path or dst_dir) / token_value["basename"]
        # If source data exist, get source locations
        if location and (
            selected_location := await self.workflow.context.data_manager.get_source_location(
                path=location, dst_deployment=dst_connector.deployment_name
            )
        ):
            # Build unique destination path and transfer file
            filepath = dst_path or (dst_dir / selected_location.relpath)
            try:
                async with await cast(CWLWorkflow, self.workflow).get_output_path(
                    unique=not self.prefix_path,
                    path=filepath,
                    src_location=selected_location,
                ) as filepath:
                    await self.workflow.context.data_manager.transfer_data(
                        src_location=selected_location.location,
                        src_path=selected_location.path,
                        dst_locations=dst_locations,
                        dst_path=str(filepath),
                        writable=self.writable,
                    )
            except FileExistsError:
                pass
            # Transform token value
            new_token_value = {
                "class": token_class,
                "path": str(filepath),
                "location": "file://" + str(filepath),
                "basename": filepath.name,
                "dirname": str(filepath.parent),
            }
            # If token contains a file
            if token_class == "File":  # nosec
                # Retrieve symbolic link data locations
                data_locations = self.workflow.context.data_manager.get_data_locations(
                    path=str(filepath),
                    deployment=dst_connector.deployment_name,
                    data_type=DataType.SYMBOLIC_LINK,
                )
                # If the remote location is not a symbolic link, perform remote checksum
                original_checksum = token_value["checksum"]
                for location in dst_locations:
                    for data_location in data_locations:
                        if (
                            data_location.name == location.name
                            and data_location.path == str(filepath)
                        ):
                            break
                    else:
                        loc_path = StreamFlowPath(
                            str(filepath),
                            context=self.workflow.context,
                            location=location,
                        )
                        checksum = f"sha1${await loc_path.checksum()}"
                        if checksum != original_checksum:
                            raise WorkflowExecutionException(
                                "Error transferring file {} in location {} to {} in location {}".format(
                                    selected_location.path,
                                    selected_location.name,
                                    filepath,
                                    location,
                                )
                            )
                # Add size, checksum and format fields
                new_token_value |= {
                    "nameroot": token_value["nameroot"],
                    "nameext": token_value["nameext"],
                    "size": token_value["size"],
                    "checksum": original_checksum,
                }
                if "format" in token_value:
                    new_token_value["format"] = token_value["format"]
                if "contents" in token_value:
                    new_token_value["contents"] = token_value["contents"]
                # Check secondary files
                if "secondaryFiles" in token_value:
                    new_token_value["secondaryFiles"] = await asyncio.gather(
                        *(
                            asyncio.create_task(
                                self._update_file_token(
                                    job=job,
                                    token_value=element,
                                    dst_path=filepath.parent,
                                )
                            )
                            for element in token_value["secondaryFiles"]
                        )
                    )
            # If token contains a directory, propagate listing if present
            elif token_class == "Directory" and "listing" in token_value:  # nosec
                new_token_value["listing"] = await self._update_listing(
                    job, token_value, filepath, selected_location
                )
            return new_token_value
        # Otherwise, create elements remotely
        else:
            # Build unique destination path
            async with await cast(CWLWorkflow, self.workflow).get_output_path(
                unique=not self.prefix_path, path=dst_path or dst_dir
            ) as filepath:
                # If the token contains a directory, simply create it
                if token_class == "Directory":  # nosec
                    await utils.create_remote_directory(
                        context=self.workflow.context,
                        locations=dst_locations,
                        path=str(filepath),
                        relpath=(
                            str(filepath.relative_to(job.output_directory))
                            if filepath.is_relative_to(job.output_directory)
                            else (
                                str(filepath.relative_to(dst_dir))
                                if filepath != dst_dir
                                else str(dst_dir)
                            )
                        ),
                    )
                # Otherwise, create the parent directories structure and write file contents
                else:
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                StreamFlowPath(
                                    str(filepath.parent),
                                    context=self.workflow.context,
                                    location=location,
                                ).mkdir(mode=0o777, exist_ok=True)
                            )
                            for location in dst_locations
                        )
                    )
                    await utils.write_remote_file(
                        context=self.workflow.context,
                        locations=dst_locations,
                        content=token_value.get("contents", ""),
                        path=str(filepath),
                        relpath=(
                            str(filepath.relative_to(job.output_directory))
                            if filepath.is_relative_to(job.output_directory)
                            else (
                                str(filepath.relative_to(dst_dir))
                                if filepath != dst_dir
                                else str(dst_dir)
                            )
                        ),
                    )
            # Build file token
            new_token_value = await utils.get_file_token(
                context=self.workflow.context,
                connector=dst_connector,
                cwl_version=cast(CWLWorkflow, self.workflow).cwl_version,
                locations=dst_locations,
                token_class=token_class,
                filepath=str(filepath),
                load_contents="contents" in token_value,
                load_listing=LoadListing.no_listing,
            )
            if (
                "checksum" in token_value
                and new_token_value["checksum"] != token_value["checksum"]
            ):
                raise WorkflowExecutionException(
                    "Error creating file {} with path {} in locations {}.".format(
                        token_value["path"],
                        new_token_value["path"],
                        [str(loc) for loc in dst_locations],
                    )
                )
            # Check secondary files
            if "secondaryFiles" in token_value:
                new_token_value["secondaryFiles"] = await asyncio.gather(
                    *(
                        asyncio.create_task(
                            self._update_file_token(
                                job=job,
                                token_value=element,
                                dst_path=filepath.parent,
                            )
                        )
                        for element in token_value["secondaryFiles"]
                    )
                )

            # If listing is specified, recursively process its contents
            if "listing" in token_value:
                new_token_value["listing"] = await self._update_listing(
                    job, token_value, filepath
                )
            # Return the new token value
            return new_token_value

    async def transfer(self, job: Job, token: Token) -> Token:
        token = token.update(await self._transfer_value(job, token.value))
        token.recoverable = False
        return token
