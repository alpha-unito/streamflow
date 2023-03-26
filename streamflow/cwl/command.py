from __future__ import annotations

import asyncio
import base64
import json
import logging
import posixpath
import re
import shlex
import time
from abc import ABC
from asyncio.subprocess import STDOUT
from decimal import Decimal
from types import ModuleType
from typing import Any, IO, Iterable, MutableMapping, MutableSequence, Type, cast

from ruamel.yaml import RoundTripRepresenter
from ruamel.yaml.scalarfloat import ScalarFloat

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation
from streamflow.core.deployment import Connector
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.utils import (
    flatten_list,
    get_class_from_name,
    get_class_fullname,
    get_tag,
)
from streamflow.core.workflow import (
    Command,
    CommandOutput,
    Job,
    Status,
    Step,
    Token,
    Workflow,
)
from streamflow.cwl import utils
from streamflow.cwl.processor import (
    CWLCommandOutput,
    CWLCommandOutputProcessor,
    CWLMapCommandOutputProcessor,
    CWLObjectCommandOutputProcessor,
    CWLUnionCommandOutputProcessor,
)
from streamflow.data import remotepath
from streamflow.deployment.connector import LocalConnector
from streamflow.deployment.utils import get_path_processor
from streamflow.log_handler import logger
from streamflow.workflow.step import ExecuteStep


def _adjust_cwl_output(base_path: str, path_processor: ModuleType, value: Any) -> Any:
    if isinstance(value, MutableSequence):
        return [_adjust_cwl_output(base_path, path_processor, v) for v in value]
    elif isinstance(value, MutableMapping):
        if utils.get_token_class(value) in ["File", "Directory"]:
            path = utils.get_path_from_token(value)
            if not path_processor.isabs(path):
                path = path_processor.join(base_path, path)
                value["path"] = path
                value["location"] = f"file://{path}"
            return value
        else:
            return {
                k: _adjust_cwl_output(base_path, path_processor, v)
                for k, v in value.items()
            }
    else:
        return value


def _adjust_inputs(
    inputs: MutableSequence[MutableMapping[str, Any]],
    path_processor: ModuleType,
    src_path: str,
    dest_path: str,
) -> MutableSequence[MutableMapping[str, Any]]:
    for inp in inputs:
        if (token_class := utils.get_token_class(inp)) in ("File", "Directory"):
            path = utils.get_path_from_token(inp)
            if path == src_path:
                inp["path"] = dest_path
                dirname, basename = path_processor.split(dest_path)
                inp["dirname"] = dirname
                inp["basename"] = basename
                if token_class == "File":  # nosec
                    nameroot, nameext = path_processor.splitext(basename)
                    inp["nameroot"] = nameroot
                    inp["nameext"] = nameext
            elif (
                token_class == "Directory"  # nosec
                and src_path.startswith(path)
                and "listing" in inp
            ):
                inp["listing"] = _adjust_inputs(
                    inp["listing"], path_processor, src_path, dest_path
                )
    return inputs


def _build_command_output_processor(name: str, step: Step, value: Any):
    if isinstance(value, MutableSequence):
        return CWLMapCommandOutputProcessor(
            name=name,
            workflow=step.workflow,
            processor=CWLUnionCommandOutputProcessor(
                name=name,
                workflow=step.workflow,
                processors=[
                    _build_command_output_processor(name=name, step=step, value=v)
                    for v in value
                ],
            ),
        )
    elif isinstance(value, MutableMapping):
        if (token_type := utils.get_token_class(value)) in ["File", "Directory"]:
            return CWLCommandOutputProcessor(
                name=name, workflow=step.workflow, token_type=token_type  # nosec
            )
        else:
            return CWLObjectCommandOutputProcessor(
                name=name,
                workflow=step.workflow,
                processors={
                    k: _build_command_output_processor(name=name, step=step, value=v)
                    for k, v in value.items()
                },
            )
    else:
        return CWLCommandOutputProcessor(  # nosec
            name=name, workflow=step.workflow, token_type="Any"
        )


def _check_command_token(command_token: CWLCommandToken, input_value: Any) -> bool:
    # CWLMapCommandToken is suitable for input lists
    if isinstance(command_token, CWLMapCommandToken):
        if isinstance(input_value, MutableSequence):
            if len(input_value) > 0:
                return _check_command_token(command_token.value, input_value[0])
            else:
                return True
        else:
            return False
    # At least one command token in a CWLUnionCommandToken must be suitable for the input value
    elif isinstance(command_token, CWLUnionCommandToken):
        for ct in command_token.value:
            if _check_command_token(ct, input_value):
                return True
        return False
    # A CWLObjectCommandToken must match the input value structure to be suitable
    elif isinstance(command_token, CWLObjectCommandToken):
        if isinstance(input_value, MutableMapping):
            for k, v in input_value.items():
                if not (
                    k in command_token.value
                    and _check_command_token(command_token.value[k], v)
                ):
                    return False
            return True
        else:
            return False
    # A default command token must match its type with the token value type
    else:
        if isinstance(input_value, Token):
            input_value = input_value.value
        return command_token.token_type == utils.infer_type_from_token(input_value)


async def _check_cwl_output(job: Job, step: Step, result: Any) -> Any:
    connector = step.workflow.context.scheduler.get_connector(job.name)
    locations = step.workflow.context.scheduler.get_locations(job.name)
    path_processor = get_path_processor(connector)
    cwl_output_path = path_processor.join(job.output_directory, "cwl.output.json")
    for location in locations:
        if await remotepath.exists(connector, location, cwl_output_path):
            # If file exists, use its contents as token value
            result = json.loads(
                await remotepath.read(connector, location, cwl_output_path)
            )
            # Update step output ports at runtime if needed
            if isinstance(result, MutableMapping):
                for out_name, out in result.items():
                    out = _adjust_cwl_output(
                        base_path=job.output_directory,
                        path_processor=path_processor,
                        value=out,
                    )
                    if out_name not in step.output_ports:
                        cast(ExecuteStep, step).add_output_port(
                            name=out_name,
                            port=step.workflow.create_port(),
                            output_processor=_build_command_output_processor(
                                name=out_name, step=step, value=out
                            ),
                        )
                        # Update workflow outputs if needed
                        if out_name not in step.workflow.output_ports:
                            step.workflow.output_ports[out_name] = step.output_ports[
                                out_name
                            ]
                for out_name in [p for p in step.output_ports if p not in result]:
                    result[out_name] = None
                    if out_name in step.workflow.output_ports:
                        del step.workflow.output_ports[out_name]
            break
    return result


async def _get_source_location(
    src_path: str, input_directory: str, connector: Connector, workflow: Workflow
) -> DataLocation | None:
    path_processor = get_path_processor(connector)
    src_path = path_processor.normpath(src_path)
    input_directory = path_processor.normpath(input_directory)
    relpath = path_processor.basename(src_path)
    base_path = None
    # If source path refers to job input directory
    if src_path.startswith(input_directory):
        relpath = path_processor.join(
            *path_processor.relpath(src_path, input_directory).split(
                path_processor.sep
            )[1:]
        )
        base_path = path_processor.normpath(src_path[: -len(relpath)])
    # Register parent locations
    await utils.search_in_parent_locations(
        context=workflow.context,
        connector=connector,
        path=src_path,
        relpath=relpath,
        base_path=base_path,
    )
    # Return the best source location ofr the current transfer
    return workflow.context.data_manager.get_source_location(
        src_path, connector.deployment_name
    )


def _get_value_for_command(token: Any, item_separator: str | None) -> Any:
    if isinstance(token, MutableSequence):
        value = []
        for element in token:
            value.append(_get_value_for_command(element, item_separator))
        if item_separator is not None:
            value = item_separator.join([_get_value_repr(v) for v in value])
        return value or None
    elif isinstance(token, MutableMapping):
        if (path := utils.get_path_from_token(token)) is not None:
            return path
        else:
            raise WorkflowExecutionException(
                "Unsupported value " + str(token) + " from expression"
            )
    else:
        return token


def _get_value_repr(value: Any) -> str | None:
    if isinstance(value, ScalarFloat):
        rep = RoundTripRepresenter()
        dec_value = Decimal(rep.represent_scalar_float(value).value)
        if "E" in str(dec_value):
            return str(dec_value.quantize(1))
        return str(dec_value)
    else:
        return str(value)


def _escape_value(value: Any) -> Any:
    if isinstance(value, MutableSequence):
        return [_escape_value(v) for v in value]
    else:
        return shlex.quote(_get_value_repr(value))


def _merge_tokens(
    bindings_map: MutableMapping[str, MutableSequence[Any]]
) -> MutableSequence[Any]:
    command = []
    for binding_position in alphanumeric_sorting(bindings_map.keys()):
        for binding in bindings_map[binding_position]:
            command.extend(flatten_list(binding))
    return [_get_value_repr(token) for token in command]


def alphanumeric_sorting(list_to_sort: Iterable) -> Iterable:
    regex = "(-{0,1}[0-9]+)"
    return sorted(
        list_to_sort,
        key=lambda key: [
            int(c) if re.match(regex, c) else c for c in re.split(regex, key)
        ],
    )


class CWLBaseCommand(Command, ABC):
    def __init__(
        self,
        step: Step,
        absolute_initial_workdir_allowed: bool = False,
        expression_lib: MutableSequence[str] | None = None,
        initial_work_dir: str | MutableSequence[Any] | None = None,
        inplace_update: bool = False,
        full_js: bool = False,
        time_limit: int | str | None = None,
    ):
        super().__init__(step)
        self.absolute_initial_workdir_allowed: bool = absolute_initial_workdir_allowed
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.full_js: bool = full_js
        self.initial_work_dir: None | (str | MutableSequence[Any]) = initial_work_dir
        self.inplace_update: bool = inplace_update
        self.time_limit: int | str | None = time_limit

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {
            **await super()._save_additional_params(context),
            **{
                "absolute_initial_workdir_allowed": self.absolute_initial_workdir_allowed,
                "expression_lib": self.expression_lib,
                "initial_work_dir": self.initial_work_dir,
                "inplace_update": self.inplace_update,
                "full_js": self.full_js,
                "time_limit": self.time_limit,
            },
        }

    def _get_timeout(self, job: Job, step: Step) -> int | None:
        timeout = 0
        if isinstance(self.time_limit, int):
            timeout = self.time_limit
        elif isinstance(self.time_limit, str):
            context = utils.build_context(
                inputs=job.inputs,
                output_directory=job.output_directory,
                tmp_directory=job.tmp_directory,
                hardware=self.step.workflow.context.scheduler.get_hardware(job.name),
            )
            timeout = int(
                utils.eval_expression(
                    expression=self.time_limit,
                    context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                )
            )
        if timeout and timeout < 0:
            raise WorkflowDefinitionException(
                f"Invalid time limit for step {step.name}: {timeout}. Time limit should be >= 0."
            )
        elif timeout == 0:
            return None
        else:
            return timeout

    async def _prepare_work_dir(
        self,
        job: Job,
        context: MutableMapping[str, Any],
        element: Any,
        base_path: str | None = None,
        dest_path: str | None = None,
        writable: bool = False,
    ) -> None:
        connector = self.step.workflow.context.scheduler.get_connector(job.name)
        locations = self.step.workflow.context.scheduler.get_locations(job.name)
        path_processor = get_path_processor(connector)
        # Initialize base path to job output directory if present
        base_path = base_path or job.output_directory
        # If current element is a string, it must be an expression
        if isinstance(element, str):
            listing = utils.eval_expression(
                expression=element,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
        else:
            listing = element
        # If listing is a list, each of its elements must be processed independently
        if isinstance(listing, MutableSequence):
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        self._prepare_work_dir(
                            job=job,
                            context=context,
                            element=el,
                            base_path=base_path,
                            writable=writable,
                        )
                    )
                    for el in listing
                )
            )
        # If listing is a dictionary, it could be a File, a Directory, a Dirent or some other object
        elif isinstance(listing, MutableMapping):
            # If it is a File or Directory element, put the corresponding file in the output directory
            if (listing_class := utils.get_token_class(listing)) in [
                "File",
                "Directory",
            ]:
                src_path = utils.get_path_from_token(listing)
                # If a compatible source location exists, simply transfer data
                if src_path is not None and (
                    selected_location := await _get_source_location(
                        src_path, job.input_directory, connector, self.step.workflow
                    )
                ):
                    dest_path = dest_path or path_processor.join(
                        base_path, path_processor.basename(src_path)
                    )
                    await self.step.workflow.context.data_manager.transfer_data(
                        src_location=selected_location,
                        src_path=src_path,
                        dst_locations=locations,
                        dst_path=dest_path,
                        writable=writable,
                    )
                    _adjust_inputs(
                        inputs=context["inputs"].values(),
                        path_processor=path_processor,
                        src_path=src_path,
                        dest_path=dest_path,
                    )
                # Otherwise create a File or a Directory in the remote path
                else:
                    if dest_path is None:
                        dest_path = base_path
                    if src_path is not None:
                        dest_path = path_processor.join(
                            dest_path, path_processor.basename(src_path)
                        )
                    if listing_class == "Directory":
                        await remotepath.mkdir(connector, locations, dest_path)
                    else:
                        await utils.write_remote_file(
                            context=self.step.workflow.context,
                            job=job,
                            content=listing["contents"]
                            if "contents" in listing
                            else "",
                            path=dest_path,
                        )
                # If `listing` is present, recursively process folder contents
                if "listing" in listing:
                    if "basename" in listing:
                        folder_path = path_processor.join(
                            base_path, listing["basename"]
                        )
                        await remotepath.mkdir(connector, locations, folder_path)
                    else:
                        folder_path = dest_path or base_path
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                self._prepare_work_dir(
                                    job=job,
                                    context=context,
                                    element=element,
                                    base_path=folder_path,
                                    writable=writable,
                                )
                            )
                            for element in listing["listing"]
                        )
                    )
                # If `secondaryFiles` is present, recursively process secondary files
                if "secondaryFiles" in listing:
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                self._prepare_work_dir(
                                    job=job,
                                    context=context,
                                    element=element,
                                    base_path=base_path,
                                    writable=writable,
                                )
                            )
                            for element in listing["secondaryFiles"]
                        )
                    )
            # If it is a Dirent element, put or create the corresponding file according to the entryname field
            elif "entry" in listing:
                entry = utils.eval_expression(
                    expression=listing["entry"],
                    context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                    strip_whitespace=False,
                )
                if "entryname" in listing:
                    dest_path = utils.eval_expression(
                        expression=listing["entryname"],
                        context=context,
                        full_js=self.full_js,
                        expression_lib=self.expression_lib,
                    )
                    if not path_processor.isabs(dest_path):
                        dest_path = posixpath.abspath(
                            posixpath.join(job.output_directory, dest_path)
                        )
                    if not (
                        dest_path.startswith(job.output_directory)
                        or self.absolute_initial_workdir_allowed
                    ):
                        raise WorkflowDefinitionException(
                            "Entryname cannot be outside the job's output directory"
                        )
                    # The entryname field overrides the value of basename of the File or Directory object
                    if (
                        isinstance(entry, MutableMapping)
                        and "path" not in entry
                        and "location" not in entry
                        and "basename" in entry
                    ):
                        entry["basename"] = dest_path
                    if not path_processor.isabs(dest_path):
                        dest_path = path_processor.join(job.output_directory, dest_path)
                writable = (
                    listing["writable"]
                    if "writable" in listing and not self.inplace_update
                    else False
                )
                # If entry is a string, a new text file must be created with the string as the file contents
                if isinstance(entry, str):
                    await utils.write_remote_file(
                        context=self.step.workflow.context,
                        job=job,
                        content=entry,
                        path=dest_path or base_path,
                    )
                # If entry is a list
                elif isinstance(entry, MutableSequence):
                    # If all elements are Files or Directories, each of them must be processed independently
                    if all(
                        utils.get_token_class(t) in ["File", "Directory"] for t in entry
                    ):
                        await self._prepare_work_dir(
                            job=job,
                            context=context,
                            element=entry,
                            base_path=base_path,
                            dest_path=dest_path,
                            writable=writable,
                        )
                    # Otherwise, the content should be serialised to JSON
                    else:
                        await utils.write_remote_file(
                            context=self.step.workflow.context,
                            job=job,
                            content=json.dumps(entry),
                            path=dest_path or base_path,
                        )
                # If entry is a dict
                elif isinstance(entry, MutableMapping):
                    # If it is a File or Directory, it must be put in the destination path
                    if utils.get_token_class(entry) in ["File", "Directory"]:
                        await self._prepare_work_dir(
                            job=job,
                            context=context,
                            element=entry,
                            base_path=base_path,
                            dest_path=dest_path,
                            writable=writable,
                        )
                    # Otherwise, the content should be serialised to JSON
                    else:
                        await utils.write_remote_file(
                            context=self.step.workflow.context,
                            job=job,
                            content=json.dumps(entry),
                            path=dest_path or base_path,
                        )
                # Every object different from a string should be serialised to JSON
                elif entry is not None:
                    await utils.write_remote_file(
                        context=self.step.workflow.context,
                        job=job,
                        content=json.dumps(entry),
                        path=dest_path or base_path,
                    )


class CWLCommand(CWLBaseCommand):
    def __init__(
        self,
        step: Step,
        absolute_initial_workdir_allowed: bool = False,
        base_command: MutableSequence[str] | None = None,
        command_tokens: MutableSequence[CWLCommandToken] | None = None,
        expression_lib: MutableSequence[str] | None = None,
        failure_codes: MutableSequence[int] | None = None,
        full_js: bool = False,
        initial_work_dir: str | MutableSequence[Any] | None = None,
        inplace_update: bool = False,
        is_shell_command: bool = False,
        success_codes: MutableSequence[int] | None = None,
        step_stderr: str | None = None,
        step_stdin: str | None = None,
        step_stdout: str | None = None,
        time_limit: int | str | None = None,
    ):
        super().__init__(
            step=step,
            absolute_initial_workdir_allowed=absolute_initial_workdir_allowed,
            expression_lib=expression_lib,
            initial_work_dir=initial_work_dir,
            inplace_update=inplace_update,
            full_js=full_js,
            time_limit=time_limit,
        )
        self.base_command: MutableSequence[str] = base_command or []
        self.command_tokens: MutableSequence[CWLCommandToken] = command_tokens or []
        self.environment: MutableMapping[str, str] = {}
        self.failure_codes: MutableSequence[int] | None = failure_codes
        self.is_shell_command: bool = is_shell_command
        self.success_codes: MutableSequence[int] | None = success_codes
        self.stderr: str | IO = step_stderr
        self.stdin: str | IO = step_stdin
        self.stdout: str | IO = step_stdout

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {
            **await super()._save_additional_params(context),
            **{
                "base_command": self.base_command,
                "command_tokens": await asyncio.gather(
                    *(
                        asyncio.create_task(command_token.save(context))
                        for command_token in self.command_tokens
                    )
                ),
                "environment": self.environment,
                "failure_codes": self.failure_codes,
                "is_shell_command": self.is_shell_command,
                "success_codes": self.success_codes,
                "stderr": self.stderr,  # TODO: manage when is IO type
                "stdin": self.stdin,  # TODO: manage when is IO type
                "stdout": self.stdout,  # TODO: manage when is IO type
            },
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ) -> CWLCommand:
        return cls(
            step=step,
            absolute_initial_workdir_allowed=row["absolute_initial_workdir_allowed"],
            base_command=row["base_command"],
            command_tokens=cast(
                MutableSequence[CWLCommandToken],
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            CWLCommandToken.load(
                                context, command_token, loading_context
                            )
                        )
                        for command_token in row["command_tokens"]
                    )
                ),
            ),
            expression_lib=row["expression_lib"],
            failure_codes=row["failure_codes"],
            full_js=row["full_js"],
            initial_work_dir=row["initial_work_dir"],
            inplace_update=row["inplace_update"],
            is_shell_command=row["is_shell_command"],
            success_codes=row["success_codes"],
            step_stderr=row["stderr"],
            step_stdin=row["stdin"],
            step_stdout=row["stdout"],
            time_limit=row["time_limit"],
        )

    def _get_executable_command(
        self, context: MutableMapping[str, Any]
    ) -> MutableSequence[str]:
        command = []
        bindings_map = {}
        # Process baseCommand
        if self.base_command:
            command.append(shlex.join(self.base_command))
        # Process tokens
        for command_token in self.command_tokens:
            if command_token.name is not None:
                context["self"] = context["inputs"].get(command_token.name)
                # If input is None, skip the command token
                if context["self"] is None:
                    continue
            bindings_map = command_token.get_binding(
                context=context,
                bindings_map=bindings_map,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
        # Merge tokens
        command.extend(_merge_tokens(bindings_map))
        return command

    async def execute(self, job: Job) -> CWLCommandOutput:
        # Build context
        context = utils.build_context(
            inputs=job.inputs,
            output_directory=job.output_directory,
            tmp_directory=job.tmp_directory,
            hardware=self.step.workflow.context.scheduler.get_hardware(job.name),
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Job {job.name} inputs: "
                f"{json.dumps(context['inputs'], indent=4, sort_keys=True)}"
            )
        # Build working directory
        if self.initial_work_dir is not None:
            await self._prepare_work_dir(job, context, self.initial_work_dir)
        # Build command string
        cmd = self._get_executable_command(context)
        # Build environment variables
        parsed_env = {
            k: str(
                utils.eval_expression(
                    expression=v,
                    context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                )
            )
            for (k, v) in self.environment.items()
        }
        if "HOME" not in parsed_env:
            parsed_env["HOME"] = job.output_directory
        if "TMPDIR" not in parsed_env:
            parsed_env["TMPDIR"] = job.tmp_directory
        # Get execution target
        connector = self.step.workflow.context.scheduler.get_connector(job.name)
        locations = self.step.workflow.context.scheduler.get_locations(job.name)
        cmd_string = " \\\n\t".join(
            ["/bin/sh", "-c", '"{cmd}"'.format(cmd=" ".join(cmd))]
            if self.is_shell_command
            else cmd
        )
        if logger.isEnabledFor(logging.INFO):
            is_local = isinstance(
                self.step.workflow.context.deployment_manager.get_connector(
                    locations[0].deployment
                ),
                LocalConnector,
            )
            logger.info(
                "EXECUTING step {step} (job {job}) {location} into directory {outdir}:\n{command}".format(
                    step=self.step.name,
                    job=job.name,
                    location="locally" if is_local else f"on location {locations[0]}",
                    outdir=job.output_directory,
                    command=cmd_string,
                )
            )
        # Persist command
        command_id = await self.step.workflow.context.database.add_command(
            step_id=self.step.persistent_id,
            tag=get_tag(job.inputs.values()),
            cmd=cmd_string,
        )
        # Escape shell command when needed
        if self.is_shell_command:
            cmd = [
                "/bin/sh",
                "-c",
                '"$(echo {command} | base64 -d)"'.format(
                    command=base64.b64encode(" ".join(cmd).encode("utf-8")).decode(
                        "utf-8"
                    )
                ),
            ]
        # If step is assigned to multiple locations, add the STREAMFLOW_HOSTS environment variable
        if len(locations) > 1:
            parsed_env["STREAMFLOW_HOSTS"] = ",".join(
                [loc.hostname for loc in locations]
            )
        # Process streams
        stdin = utils.eval_expression(
            expression=self.stdin,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib,
        )
        stdout = (
            utils.eval_expression(
                expression=self.stdout,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
            if self.stdout is not None
            else STDOUT
        )
        stderr = (
            utils.eval_expression(
                expression=self.stderr,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
            if self.stderr is not None
            else stdout
        )
        # Get timeout
        timeout = self._get_timeout(job=job, step=self.step)
        # Execute remote command
        start_time = time.time_ns()
        result, exit_code = await connector.run(
            locations[0],
            cmd,
            environment=parsed_env,
            workdir=job.output_directory,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            capture_output=True,
            timeout=timeout,
            job_name=job.name,
        )
        end_time = time.time_ns()
        # Handle exit codes
        if self.failure_codes and exit_code in self.failure_codes:
            status = Status.FAILED
        elif (self.success_codes and exit_code in self.success_codes) or exit_code == 0:
            status = Status.COMPLETED
            if logger.isEnabledFor(logging.INFO):
                if result:
                    logger.info(result)
        else:
            status = Status.FAILED
        # Update command persistence
        await self.step.workflow.context.database.update_command(
            command_id,
            {
                "status": status.value,
                "output": str(result),
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        # Check if file `cwl.output.json` exists either locally on at least one location
        result = await _check_cwl_output(job, self.step, result)
        return CWLCommandOutput(value=result, status=status, exit_code=exit_code)


class CWLCommandToken:
    def __init__(
        self,
        name: str,
        value: Any,
        token_type: str | None = None,
        is_shell_command: bool = False,
        item_separator: str | None = None,
        position: str | int = 0,
        prefix: str | None = None,
        separate: bool = True,
        shell_quote: bool = True,
    ):
        self.name: str = name
        self.value: Any = value
        self.token_type: str | None = token_type
        self.is_shell_command: bool = is_shell_command
        self.item_separator: str | None = item_separator
        self.position: str | int = position
        self.prefix: str | None = prefix
        self.separate: bool = separate
        self.shell_quote: bool = shell_quote

    def _compute_binding(
        self,
        processed_token: Any,
        context: MutableMapping[str, Any],
        bindings_map: MutableMapping[str, MutableSequence[Any]],
        full_js: bool = False,
        expression_lib: MutableSequence[str] | None = None,
    ) -> MutableMapping[str, MutableSequence[Any]]:
        # Obtain token value
        value = _get_value_for_command(processed_token, self.item_separator)
        # If token value is null or an empty array, skip the command token
        if value is None:
            return bindings_map
        # Otherwise
        else:
            # Obtain prefix if present
            if self.prefix is not None:
                if isinstance(value, bool):
                    value = [self.prefix] if value else value
                elif self.separate:
                    if isinstance(value, MutableSequence):
                        value = [self.prefix] + list(value)
                    else:
                        value = [self.prefix, value]
                elif isinstance(value, MutableSequence):
                    value = [self.prefix].extend(value)
                else:
                    value = [self.prefix + _get_value_repr(value)]
            # If value is a boolean with no prefix, skip it
            if isinstance(value, bool):
                return bindings_map
            # Ensure value is a list
            if not isinstance(value, MutableSequence):
                value = [value]
            # Process shell escape only on the single command token
            if not self.is_shell_command or self.shell_quote:
                value = [_escape_value(v) for v in value]
            # Obtain token position
            if isinstance(self.position, str) and not self.position.isnumeric():
                context["self"] = processed_token
                position = utils.eval_expression(
                    expression=self.position,
                    context=context,
                    full_js=full_js,
                    expression_lib=expression_lib,
                )
                try:
                    position = int(position) if position is not None else 0
                except ValueError:
                    # If position is not a number, do nothing
                    pass
            else:
                position = int(self.position)
            sort_key = (
                "".join([str(position), self.name]) if self.name else str(position)
            )
            if sort_key not in bindings_map:
                bindings_map[sort_key] = []
            # Place value in proper position
            bindings_map[sort_key].append(value)
            return bindings_map

    def _process_token(
        self,
        token_value: Any,
        context: MutableMapping[str, Any],
        full_js: bool = False,
        expression_lib: MutableSequence[str] | None = None,
    ) -> Any:
        if isinstance(token_value, CWLCommandToken):
            local_bindings = token_value.get_binding(
                context=context,
                bindings_map={},
                full_js=full_js,
                expression_lib=expression_lib,
            )
            processed_token = _merge_tokens(local_bindings)
        elif isinstance(token_value, str):
            processed_token = utils.eval_expression(
                expression=self.value,
                context=context,
                full_js=full_js,
                expression_lib=expression_lib,
            )
        elif token_value is None:
            processed_token = context["inputs"][self.name]
            processed_token = (
                processed_token.value
                if isinstance(processed_token, Token)
                else processed_token
            )
        else:
            processed_token = token_value
        return processed_token

    def get_binding(
        self,
        context: MutableMapping[str, Any],
        bindings_map: MutableMapping[str, MutableSequence[Any]],
        full_js: bool = False,
        expression_lib: MutableSequence[str] | None = None,
    ) -> Any:
        processed_token = self._process_token(
            token_value=self.value,
            context=context,
            full_js=full_js,
            expression_lib=expression_lib,
        )
        return self._compute_binding(
            processed_token=processed_token,
            context=context,
            bindings_map=bindings_map,
            full_js=full_js,
            expression_lib=expression_lib,
        )

    @classmethod
    async def _load_value(cls, value, context, loading_context):
        if isinstance(value, MutableMapping):
            return await CWLCommandToken.load(context, value, loading_context)
        else:
            return value

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        return cls(
            name=row["name"],
            value=await cls._load_value(row["value"], context, loading_context),
            token_type=row["token_type"],
            is_shell_command=row["is_shell_command"],
            item_separator=row["item_separator"],
            position=row["position"],
            prefix=row["prefix"],
            separate=row["separate"],
            shell_quote=row["shell_quote"],
        )

    async def _save_value(self, context):
        if isinstance(self.value, CWLCommandToken):
            return await self.value.save(context)
        else:
            return self.value

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {
            "name": self.name,
            "value": await self._save_value(context),
            "token_type": self.token_type,
            "is_shell_command": self.is_shell_command,
            "item_separator": self.item_separator,
            "position": self.position,
            "prefix": self.prefix,
            "separate": self.separate,
            "shell_quote": self.shell_quote,
        }

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> CWLCommandToken:
        type_t = cast(Type[CWLCommandToken], get_class_from_name(row["type"]))
        return await type_t._load(context, row["params"], loading_context)

    async def save(self, context: StreamFlowContext):
        return {
            "type": get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


class CWLObjectCommandToken(CWLCommandToken):
    def _check_dict(self, value: Any):
        if not isinstance(value, MutableMapping):
            raise WorkflowDefinitionException(
                f"A {self.__class__.__name__} object can only be used to process dict values"
            )

    def get_binding(
        self,
        context: MutableMapping[str, Any],
        bindings_map: MutableMapping[str, MutableSequence[Any]],
        full_js: bool = False,
        expression_lib: MutableSequence[str] | None = None,
    ) -> Any:
        self._check_dict(self.value)
        context = {**context, **{"inputs": context["inputs"][self.name]}}
        for key, token in self.value.items():
            if key in context["inputs"] and token is not None:
                bindings_map = token.get_binding(
                    context=context,
                    bindings_map=bindings_map,
                    full_js=full_js,
                    expression_lib=expression_lib,
                )
        return bindings_map

    async def _save_value(self, context):
        return {
            k: v
            for k, v in zip(
                self.value.keys(),
                await asyncio.gather(
                    *(
                        asyncio.create_task(elem.save(context))
                        for elem in self.value.values()
                    )
                ),
            )
        }

    @classmethod
    async def _load_value(cls, value, context, loading_context):
        return {
            k: v
            for k, v in zip(
                value.keys(),
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            CWLCommandToken.load(context, elem, loading_context)
                        )
                        for elem in value.values()
                    )
                ),
            )
        }


class CWLUnionCommandToken(CWLCommandToken):
    def get_binding(
        self,
        context: MutableMapping[str, Any],
        bindings_map: MutableMapping[str, MutableSequence[Any]],
        full_js: bool = False,
        expression_lib: MutableSequence[str] | None = None,
    ) -> Any:
        inputs = context["inputs"][self.name]
        command_token = self.get_command_token(inputs)
        return command_token.get_binding(
            context=context,
            bindings_map=bindings_map,
            full_js=full_js,
            expression_lib=expression_lib,
        )

    def get_command_token(self, inputs: Any) -> CWLCommandToken:
        if self.value is not None:
            for command_token in self.value:
                if _check_command_token(command_token, inputs):
                    return command_token
            raise WorkflowDefinitionException(
                "No suitable command token for input value " + str(inputs)
            )
        else:
            return super()

    async def _save_value(self, context):
        return await asyncio.gather(
            *(asyncio.create_task(elem.save(context)) for elem in self.value)
        )

    @classmethod
    async def _load_value(cls, value, context, loading_context):
        return await asyncio.gather(
            *(
                asyncio.create_task(
                    CWLCommandToken.load(context, elem, loading_context)
                )
                for elem in value
            )
        )


class CWLMapCommandToken(CWLCommandToken):
    def _check_list(self, value: Any):
        if not isinstance(value, MutableSequence):
            raise WorkflowDefinitionException(
                f"A {self.__class__.__name__} object can only be used to process list values"
            )

    def get_binding(
        self,
        context: MutableMapping[str, Any],
        bindings_map: MutableMapping[str, MutableSequence[Any]],
        full_js: bool = False,
        expression_lib: MutableSequence[str] | None = None,
    ) -> Any:
        inputs = context["inputs"][self.name]
        if isinstance(inputs, Token):
            inputs = inputs.value
        self._check_list(inputs)
        for value in inputs:
            bindings_map = super().get_binding(
                context={**context, **{"inputs": {self.name: value}, "self": value}},
                bindings_map=bindings_map,
                full_js=full_js,
                expression_lib=expression_lib,
            )
        return bindings_map

    async def _save_value(self, context):
        return await self.value.save(context)

    @classmethod
    async def _load_value(cls, value, context, loading_context):
        return await CWLCommandToken.load(context, value, loading_context)


class CWLExpressionCommand(CWLBaseCommand):
    def __init__(
        self,
        step: Step,
        expression: str,
        absolute_initial_workdir_allowed: bool = False,
        expression_lib: list[str] | None = None,
        full_js: bool = False,
        initial_work_dir: str | list[Any] | None = None,
        inplace_update: bool = False,
    ):
        super().__init__(
            step=step,
            absolute_initial_workdir_allowed=absolute_initial_workdir_allowed,
            expression_lib=expression_lib,
            initial_work_dir=initial_work_dir,
            inplace_update=inplace_update,
            full_js=full_js,
        )
        self.expression: str = expression

    async def execute(self, job: Job) -> CWLCommandOutput:
        context = utils.build_context(
            inputs=job.inputs,
            output_directory=job.output_directory,
            tmp_directory=job.tmp_directory,
            hardware=self.step.workflow.context.scheduler.get_hardware(job.name),
        )
        if self.initial_work_dir is not None:
            await self._prepare_work_dir(job, context, self.initial_work_dir)
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Evaluating expression for step {self.step.name} (job {job.name})"
            )
        # Persist command
        command_id = await self.step.workflow.context.database.add_command(
            step_id=self.step.persistent_id,
            tag=get_tag(job.inputs.values()),
            cmd=self.expression,
        )
        # Execute command
        start_time = time.time_ns()
        result = utils.eval_expression(
            expression=self.expression,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib,
        )
        end_time = time.time_ns()
        # Update command persistence
        await self.step.workflow.context.database.update_command(
            command_id,
            {
                "status": Status.COMPLETED.value,
                "output": str(result),
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        # Return result
        return CWLCommandOutput(value=result, status=Status.COMPLETED, exit_code=0)

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {
            **await super()._save_additional_params(context),
            **{"expression": self.expression},
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ) -> CWLExpressionCommand:
        return cls(
            step=step,
            absolute_initial_workdir_allowed=row["absolute_initial_workdir_allowed"],
            expression_lib=row["expression_lib"],
            full_js=row["full_js"],
            initial_work_dir=row["initial_work_dir"],
            inplace_update=row["inplace_update"],
            expression=row["expression"],
        )


class CWLStepCommand(CWLBaseCommand):
    def __init__(
        self,
        step: Step,
        absolute_initial_workdir_allowed: bool = False,
        expression_lib: MutableSequence[str] | None = None,
        full_js: bool = False,
        initial_work_dir: str | MutableSequence[Any] | None = None,
        inplace_update: bool = False,
        time_limit: int | str | None = None,
    ):
        super().__init__(
            step=step,
            absolute_initial_workdir_allowed=absolute_initial_workdir_allowed,
            expression_lib=expression_lib,
            initial_work_dir=initial_work_dir,
            inplace_update=inplace_update,
            full_js=full_js,
            time_limit=time_limit,
        )
        self.input_expressions: MutableMapping[str, str] = {}

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ) -> CWLStepCommand:
        cwl_step_command = cls(
            step=step,
            absolute_initial_workdir_allowed=row["absolute_initial_workdir_allowed"],
            expression_lib=row["expression_lib"],
            full_js=row["full_js"],
            initial_work_dir=row["initial_work_dir"],
            inplace_update=row["inplace_update"],
            time_limit=row["time_limit"],
        )
        cwl_step_command.input_expressions = row["input_expressions"]
        return cwl_step_command

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {
            **await super()._save_additional_params(context),
            **{"input_expressions": self.input_expressions},
        }

    async def execute(self, job: Job) -> CommandOutput:
        context = utils.build_context(
            inputs=job.inputs,
            output_directory=job.output_directory,
            tmp_directory=job.tmp_directory,
            hardware=self.step.workflow.context.scheduler.get_hardware(job.name),
        )
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"EXECUTING job {job.name}")
        # Process expressions
        processed_inputs = {}
        for k, v in self.input_expressions.items():
            context = {**context, **{"self": context["inputs"].get(k)}}
            processed_inputs[k] = utils.eval_expression(
                expression=v,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
        context["inputs"] = {**context["inputs"], **processed_inputs}
        return CWLCommandOutput(
            value=context["inputs"], status=Status.COMPLETED, exit_code=0
        )
