from __future__ import annotations

import asyncio
import base64
import json
import logging
import posixpath
import shlex
import time
from asyncio.subprocess import STDOUT
from collections.abc import MutableMapping, MutableSequence
from decimal import Decimal
from types import ModuleType
from typing import Any, cast

from ruamel.yaml import RoundTripRepresenter
from ruamel.yaml.scalarfloat import ScalarFloat
from typing_extensions import Self

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation
from streamflow.core.deployment import Connector
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.processor import (
    CommandOutputProcessor,
    MapCommandOutputProcessor,
    UnionCommandOutputProcessor,
)
from streamflow.core.utils import flatten_list
from streamflow.core.workflow import (
    Command,
    CommandOptions,
    CommandToken,
    CommandTokenProcessor,
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
    CWLObjectCommandOutputProcessor,
    CWLOutputJSONCommandOutput,
)
from streamflow.cwl.step import build_token
from streamflow.cwl.utils import is_literal_file
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.data.remotepath import StreamFlowPath
from streamflow.deployment.utils import get_path_processor
from streamflow.log_handler import logger
from streamflow.workflow.command import (
    ListCommandToken,
    MapCommandTokenProcessor,
    ObjectCommandToken,
    ObjectCommandTokenProcessor,
    TokenizedCommand,
)
from streamflow.workflow.step import ExecuteStep
from streamflow.workflow.utils import get_job_token, get_token_value


def _adjust_cwl_output(
    base_path: StreamFlowPath, job_name: str, path_processor: ModuleType, value: Any
) -> Any:
    match value:
        case MutableSequence():
            return [
                _adjust_cwl_output(base_path, job_name, path_processor, v)
                for v in value
            ]
        case MutableMapping():
            if utils.get_token_class(value) in ["File", "Directory"]:
                if (path := utils.get_path_from_token(value)) is None:
                    raise WorkflowExecutionException(
                        f"Job {job_name} cannot retrieve output. "
                        f"The value should be a file, but the path field is not defined."
                    )
                elif not path_processor.isabs(path):
                    path = base_path / path
                    value |= {"path": path, "location": f"file://{path}"}
                return value
            else:
                return {
                    k: _adjust_cwl_output(base_path, job_name, path_processor, v)
                    for k, v in value.items()
                }
        case _:
            return value


def _adjust_input(
    job_name: str,
    input_: Any,
    path_processor: ModuleType,
    src_path: str,
    dst_path: str,
) -> bool:
    """
    Adjust the input if it contains a file with the specified `src_path`.
    If the input is a `Directory`, the function will recursively adjust all
    the files listed within the directory. This function does not handle
    the `secondaryFiles` associated with a `File` object.

    :param input_: The input to process. It can be of any type, including
                           lists, objects (e.g., CWL records), files, and directories.
    :param path_processor: A module used to process and manipulate paths (e.g., posixpath).
    :param src_path: The source path to search for within the input values.
    :param dst_path: The destination path that will replace `src_path` in the input values.
    :return: True if the adjustment is successful; False if `src_path` is not found.
    """
    match input_:
        case MutableMapping():
            # Process the input if it is a file or an object
            if (token_class := utils.get_token_class(input_)) in ("File", "Directory"):
                if (path := utils.get_path_from_token(input_)) is None:
                    raise WorkflowExecutionException(
                        f"Job {job_name} cannot adjust the input. "
                        "The value should be a file, but the path field is not defined."
                    )
                elif path == src_path:
                    input_["path"] = dst_path
                    dirname, basename = path_processor.split(dst_path)
                    input_["dirname"] = dirname
                    input_["basename"] = basename
                    input_["location"] = f"file://{dst_path}"
                    if token_class == "File":  # nosec
                        nameroot, nameext = path_processor.splitext(basename)
                        input_["nameroot"] = nameroot
                        input_["nameext"] = nameext
                    for ins in input_.get("listing", ()):
                        _adjust_input(
                            job_name,
                            ins,
                            path_processor,
                            ins["path"],
                            path_processor.join(
                                dst_path, path_processor.basename(ins["path"])
                            ),
                        )
                    return True
                elif src_path.startswith(path) and "listing" in input_:
                    for inp in input_["listing"]:
                        if _adjust_input(
                            job_name, inp, path_processor, src_path, dst_path
                        ):
                            return True
            else:
                for inp in input_.values():
                    if _adjust_input(job_name, inp, path_processor, src_path, dst_path):
                        return True
        case MutableSequence():
            for inp in input_:
                if _adjust_input(job_name, inp, path_processor, src_path, dst_path):
                    return True
    return False


def _adjust_inputs(
    job_name: str,
    inputs: MutableSequence[MutableMapping[str, Any]],
    path_processor: ModuleType,
    src_path: str,
    dst_path: str,
) -> None:
    """
    Search for `src_path` within the values of the `inputs` parameter and replaces it with `dst_path`.
    The update is performed in place on the `inputs` parameter.

    :param inputs: A dictionary mapping input names to values of any type,
                   including lists, objects (e.g., CWL records), files, and directories.
    :param path_processor: A module used to process and manipulate paths (e.g., posixpath).
    :param src_path: The path to search for within the input values.
    :param dst_path: The path that will replace `src_path` in the input values.
    :return: None
    """
    for inp in inputs:
        if _adjust_input(job_name, inp, path_processor, src_path, dst_path):
            break


def _build_command_output_processor(
    name: str, step: Step, value: Any
) -> CommandOutputProcessor:
    match value:
        case MutableSequence():
            return MapCommandOutputProcessor(
                name=name,
                workflow=cast(CWLWorkflow, step.workflow),
                processor=UnionCommandOutputProcessor(
                    name=name,
                    workflow=cast(CWLWorkflow, step.workflow),
                    processors=[
                        _build_command_output_processor(name=name, step=step, value=v)
                        for v in value
                    ],
                ),
            )
        case MutableMapping():
            if (token_type := utils.get_token_class(value)) in ["File", "Directory"]:
                return CWLCommandOutputProcessor(
                    name=name,
                    workflow=cast(CWLWorkflow, step.workflow),
                    token_type=token_type,  # nosec
                )
            else:
                return CWLObjectCommandOutputProcessor(
                    name=name,
                    workflow=cast(CWLWorkflow, step.workflow),
                    processors={
                        k: _build_command_output_processor(
                            name=name, step=step, value=v
                        )
                        for k, v in value.items()
                    },
                )
        case _:
            return CWLCommandOutputProcessor(  # nosec
                name=name, workflow=cast(CWLWorkflow, step.workflow), token_type="Any"
            )


def _escape_value(value: Any) -> Any:
    if isinstance(value, MutableSequence):
        return [_escape_value(v) for v in value]
    else:
        return shlex.quote(_get_value_repr(value))


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
    # Return the best source location of the current transfer
    return await workflow.context.data_manager.get_source_location(
        src_path, connector.deployment_name
    )


def _get_value_for_command(token: Any, item_separator: str | None) -> Any:
    match token:
        case MutableSequence():
            value = [
                _get_value_for_command(element, item_separator) for element in token
            ]
            if value and item_separator is not None:
                return item_separator.join([_get_value_repr(v) for v in value])
            return value or None
        case MutableMapping():
            if (path := utils.get_path_from_token(token)) is not None:
                return path
            else:
                raise WorkflowExecutionException(
                    "Unsupported value " + str(token) + " from expression"
                )
        case _:
            return token


def _get_value_repr(value: Any) -> str:
    if isinstance(value, ScalarFloat):
        rep = RoundTripRepresenter()
        dec_value = Decimal(rep.represent_scalar_float(value).value)
        if "E" in str(dec_value):
            return str(dec_value.quantize(1))
        return str(dec_value)
    else:
        return str(value)


def _merge_tokens(token: CommandToken) -> Any:
    match token:
        case ListCommandToken():
            return [_merge_tokens(t) for t in token.value if t is not None]
        case ObjectCommandToken():
            tokens = sorted(
                filter(
                    lambda t: t.position is not None,
                    flatten_list(
                        [
                            _merge_tokens(t)
                            for t in token.value.values()
                            if t is not None
                        ]
                    ),
                ),
                key=lambda t: (
                    [t.position, t.name] if t.name is not None else [t.position]
                ),
            )
            return tokens or [
                CommandToken(name=token.name, position=token.position, value="")
            ]
        case None:
            return None
        case _:
            if isinstance(token.value, CommandToken):
                return [_merge_tokens(token.value)]
            else:
                return [token]


async def _prepare_work_dir(
    options: InitialWorkDirOptions,
    element: Any,
    base_path: str | None = None,
    dst_path: str | None = None,
    writable: bool = False,
) -> None:
    context = options.step.workflow.context
    connector = context.scheduler.get_connector(options.job.name)
    locations = context.scheduler.get_locations(options.job.name)
    path_processor = get_path_processor(connector)
    # Initialize base path to job output directory if present
    base_path = base_path or options.job.output_directory
    # If current element is a string, it must be an expression
    if isinstance(element, str):
        listing = utils.eval_expression(
            expression=element,
            context=options.context,
            full_js=options.full_js,
            expression_lib=options.expression_lib,
        )
    else:
        listing = element
    match listing:
        # If listing is a list, each of its elements must be processed independently
        case MutableSequence():
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        _prepare_work_dir(
                            element=el,
                            options=options,
                            base_path=base_path,
                            writable=writable,
                        )
                    )
                    for el in listing
                )
            )
        # If listing is a dictionary, it could be a File, a Directory, a Dirent or some other object
        case MutableMapping():
            # If it is a File or Directory element, put the corresponding file in the output directory
            if (listing_class := utils.get_token_class(listing)) in [
                "File",
                "Directory",
            ]:
                # If a compatible source location exists, simply transfer data
                if (src_path := utils.get_path_from_token(listing)) is not None and (
                    selected_location := await _get_source_location(
                        src_path,
                        options.job.input_directory,
                        connector,
                        options.step.workflow,
                    )
                ):
                    dst_path = dst_path or path_processor.join(
                        base_path, path_processor.basename(src_path)
                    )
                    await context.data_manager.transfer_data(
                        src_location=selected_location.location,
                        src_path=selected_location.path,
                        dst_locations=locations,
                        dst_path=dst_path,
                        writable=writable,
                    )
                    _adjust_inputs(
                        job_name=options.job.name,
                        inputs=options.context["inputs"].values(),
                        path_processor=path_processor,
                        src_path=src_path,
                        dst_path=dst_path,
                    )
                # Otherwise create a File or a Directory in the remote path
                elif is_literal_file(listing_class, listing, options.job.name):
                    if dst_path is None:
                        dst_path = (
                            path_processor.join(base_path, listing["basename"])
                            if "basename" in listing
                            else base_path
                        )
                    if src_path is not None:
                        dst_path = path_processor.join(
                            dst_path, path_processor.basename(src_path)
                        )
                    if listing_class == "Directory":
                        await utils.create_remote_directory(
                            context=context,
                            locations=locations,
                            path=dst_path,
                            relpath=(
                                path_processor.relpath(dst_path, base_path)
                                if dst_path
                                else base_path
                            ),
                        )
                        await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    _prepare_work_dir(
                                        element=element,
                                        options=options,
                                        base_path=dst_path,
                                        writable=writable,
                                    )
                                )
                                for element in listing["listing"]
                            )
                        )
                    else:
                        await utils.write_remote_file(
                            context=context,
                            locations=locations,
                            content=(
                                listing["contents"] if "contents" in listing else ""
                            ),
                            path=dst_path,
                            relpath=path_processor.relpath(dst_path, base_path),
                        )
                # The file is not literal; it has a path, but no specific data locations are available.
                else:
                    raise WorkflowExecutionException(
                        f"Impossible to copy the {src_path} file in the working directory: No data locations found"
                    )
                # If `secondaryFiles` is present, recursively process secondary files
                if "secondaryFiles" in listing:
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                _prepare_work_dir(
                                    element=element,
                                    options=options,
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
                    context=options.context,
                    full_js=options.full_js,
                    expression_lib=options.expression_lib,
                    strip_whitespace=False,
                )
                if "entryname" in listing:
                    dst_path = utils.eval_expression(
                        expression=listing["entryname"],
                        context=options.context,
                        full_js=options.full_js,
                        expression_lib=options.expression_lib,
                    )
                    if not path_processor.isabs(dst_path):
                        dst_path = posixpath.abspath(
                            posixpath.join(options.job.output_directory, dst_path)
                        )
                    if not (
                        dst_path.startswith(options.job.output_directory)
                        or options.absolute_initial_workdir_allowed
                    ):
                        raise WorkflowDefinitionException(
                            "Entryname cannot be outside the job's output directory"
                        )
                    # The entryname field overrides the value of basename of the File or Directory object
                    if (
                        isinstance(entry, MutableMapping)
                        and utils.get_token_class(entry) in ["File", "Directory"]
                        and "path" not in entry
                        and "location" not in entry
                    ):
                        entry["basename"] = dst_path
                    if not path_processor.isabs(dst_path):
                        dst_path = path_processor.join(
                            options.job.output_directory, dst_path
                        )
                elif isinstance(entry, str):
                    raise WorkflowExecutionException(
                        "`entry` is a string and requires an `entryname`"
                    )
                writable = (
                    listing["writable"]
                    if "writable" in listing and not options.inplace_update
                    else False
                )
                match entry:
                    # If entry is a string, a new text file must be created with the string as the file contents
                    case str():
                        await utils.write_remote_file(
                            context=context,
                            locations=locations,
                            content=entry,
                            path=dst_path or base_path,
                            relpath=(
                                path_processor.relpath(dst_path, base_path)
                                if dst_path
                                else base_path
                            ),
                        )
                    # If entry is a list
                    case MutableSequence():
                        # If all elements are Files or Directories, each of them must be processed independently
                        if all(
                            utils.get_token_class(t) in ["File", "Directory"]
                            for t in entry
                        ):
                            await _prepare_work_dir(
                                element=entry,
                                options=options,
                                base_path=base_path,
                                dst_path=dst_path,
                                writable=writable,
                            )
                        # Otherwise, the content should be serialised to JSON
                        else:
                            await utils.write_remote_file(
                                context=context,
                                locations=locations,
                                content=json.dumps(entry),
                                path=dst_path or base_path,
                                relpath=(
                                    path_processor.relpath(dst_path, base_path)
                                    if dst_path
                                    else base_path
                                ),
                            )
                    # If entry is a dict
                    case MutableMapping():
                        # If it is a File or Directory, it must be put in the destination path
                        if utils.get_token_class(entry) in ["File", "Directory"]:
                            await _prepare_work_dir(
                                element=entry,
                                options=options,
                                base_path=base_path,
                                dst_path=dst_path,
                                writable=writable,
                            )
                        # Otherwise, the content should be serialised to JSON
                        else:
                            await utils.write_remote_file(
                                context=context,
                                locations=locations,
                                content=json.dumps(entry),
                                path=dst_path or base_path,
                                relpath=(
                                    path_processor.relpath(dst_path, base_path)
                                    if dst_path
                                    else base_path
                                ),
                            )
                    # Every object different from a string should be serialised to JSON
                    case entry if entry is not None:
                        await utils.write_remote_file(
                            context=context,
                            locations=locations,
                            content=json.dumps(entry),
                            path=dst_path or base_path,
                            relpath=(
                                path_processor.relpath(dst_path, base_path)
                                if dst_path
                                else base_path
                            ),
                        )


class CWLCommand(TokenizedCommand):
    def __init__(
        self,
        step: Step,
        processors: MutableSequence[CommandTokenProcessor] | None = None,
        absolute_initial_workdir_allowed: bool = False,
        base_command: MutableSequence[str] | None = None,
        environment: MutableMapping[str, str] | None = None,
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
        super().__init__(step=step, processors=processors)
        self.absolute_initial_workdir_allowed: bool = absolute_initial_workdir_allowed
        self.base_command: MutableSequence[str] = base_command or []
        self.environment: MutableMapping[str, str] = environment or {}
        self.expression_lib: MutableSequence[str] = expression_lib or []
        self.failure_codes: MutableSequence[int] | None = failure_codes
        self.full_js: bool = full_js
        self.initial_work_dir: str | MutableSequence[Any] | None = initial_work_dir
        self.inplace_update: bool = inplace_update
        self.is_shell_command: bool = is_shell_command
        self.success_codes: MutableSequence[int] | None = success_codes
        self.stderr: str | None = step_stderr
        self.stdin: str | None = step_stdin
        self.stdout: str | None = step_stdout
        self.time_limit: int | str | None = time_limit

    def _get_timeout(self, job: Job) -> int | None:
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
                f"Invalid time limit for step {self.step.name}: {timeout}. Time limit should be >= 0."
            )
        elif timeout == 0:
            return None
        else:
            return timeout

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "absolute_initial_workdir_allowed": self.absolute_initial_workdir_allowed,
            "base_command": self.base_command,
            "environment": self.environment,
            "expression_lib": self.expression_lib,
            "failure_codes": self.failure_codes,
            "full_js": self.full_js,
            "initial_work_dir": self.initial_work_dir,
            "inplace_update": self.inplace_update,
            "is_shell_command": self.is_shell_command,
            "success_codes": self.success_codes,
            "stderr": self.stderr,
            "stdin": self.stdin,
            "stdout": self.stdout,
            "time_limit": self.time_limit,
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ) -> Self:
        return cls(
            step=step,
            absolute_initial_workdir_allowed=row["absolute_initial_workdir_allowed"],
            base_command=row["base_command"],
            processors=await cls._load_command_token_processors(
                context=context, row=row, loading_context=loading_context
            ),
            environment=row["environment"],
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
        self, context: MutableMapping[str, Any], inputs: MutableMapping[str, Token]
    ) -> MutableSequence[str]:
        command = []
        options = CWLCommandOptions(
            context=context,
            expression_lib=self.expression_lib,
            full_js=self.full_js,
        )
        # Process baseCommand
        if self.base_command:
            command.append(shlex.join(self.base_command))
        # Process tokens
        bindings = ListCommandToken(name=None, position=None, value=[])
        for processor in self.processors:
            if processor.name is not None:
                context["self"] = context["inputs"].get(processor.name)
                # If input is None, skip the command token
                if context["self"] is None:
                    continue
            bindings.value.append(
                processor.bind(
                    token=(
                        inputs[processor.name] if processor.name is not None else None
                    ),
                    position=None,
                    options=options,
                )
            )
        # Merge tokens
        command.extend(
            flatten_list(
                [_get_value_repr(val) for val in t.value]
                for t in sorted(
                    (
                        t
                        for t in flatten_list(_merge_tokens(bindings))
                        if t.position is not None
                    ),
                    key=lambda t: (
                        [t.position, t.name] if t.name is not None else [t.position]
                    ),
                )
            )
        )
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
            await _prepare_work_dir(
                element=self.initial_work_dir,
                options=InitialWorkDirOptions(
                    absolute_initial_workdir_allowed=self.absolute_initial_workdir_allowed,
                    context=context,
                    expression_lib=self.expression_lib,
                    full_js=self.full_js,
                    inplace_update=self.inplace_update,
                    job=job,
                    step=self.step,
                ),
            )
            inputs: MutableMapping[str, Token] = dict(
                zip(
                    job.inputs.keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                build_token(
                                    cwl_version=cast(
                                        CWLWorkflow, self.step.workflow
                                    ).cwl_version,
                                    inputs=job.inputs,
                                    streamflow_context=self.step.workflow.context,
                                    token_value=context["inputs"][key],
                                    job=job,
                                )
                            )
                            for key in job.inputs.keys()
                        )
                    ),
                    strict=True,
                )
            )
        else:
            inputs = job.inputs
        # Build command string
        cmd = self._get_executable_command(context, inputs)
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
            logger.info(
                "EXECUTING step {step} (job {job}) {location} into directory {outdir}:\n{command}".format(
                    step=self.step.name,
                    job=job.name,
                    location=(
                        "locally"
                        if locations[0].local
                        else f"on location {locations[0]}"
                    ),
                    outdir=job.output_directory,
                    command=cmd_string,
                )
            )
        # Persist command
        job_token = get_job_token(
            job.name, cast(ExecuteStep, self.step).get_job_port().token_list
        )
        execution_id = await self.step.workflow.context.database.add_execution(
            step_id=self.step.persistent_id,
            job_token_id=job_token.persistent_id,
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
        if len(locations) > 1 and (
            hostnames := [loc.hostname for loc in locations if loc.hostname is not None]
        ):
            parsed_env["STREAMFLOW_HOSTS"] = ",".join(hostnames)
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
        timeout = self._get_timeout(job=job)
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
        await self.step.workflow.context.database.update_execution(
            execution_id,
            {
                "status": status.value,
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        # Check if file `cwl.output.json` exists on any location
        path_processor = get_path_processor(connector)
        for location in locations:
            cwl_output_path = StreamFlowPath(
                job.output_directory,
                "cwl.output.json",
                context=self.step.workflow.context,
                location=location,
            )
            if await cwl_output_path.exists():
                # If file exists, use its contents as token value
                result = json.loads(await cwl_output_path.read_text())
                # Update step output ports at runtime if needed
                if isinstance(result, MutableMapping):
                    result = {
                        k: _adjust_cwl_output(
                            base_path=StreamFlowPath(
                                job.output_directory,
                                context=self.step.workflow.context,
                                location=location,
                            ),
                            job_name=job.name,
                            path_processor=path_processor,
                            value=v,
                        )
                        for k, v in result.items()
                    }
                else:
                    raise WorkflowExecutionException(
                        f"Invalid `cwl.output.json` file produced by step `{self.step.name}`: "
                        "it should contain a JSON object"
                    )
                # Put a `None` token if an output value is not present
                for out_name in self.step.output_ports:
                    if out_name not in result:
                        result[out_name] = None
                # Propagate a CWLOutputJSONCommandOutput
                return CWLOutputJSONCommandOutput(
                    value=result, status=status, exit_code=exit_code
                )
        # Otherwise, propagate a CWLCommandOutput object
        return CWLCommandOutput(value=result, status=status, exit_code=exit_code)


class CWLCommandOptions(CommandOptions):
    __slots__ = ("context", "expression_lib", "full_js")

    def __init__(
        self,
        context: MutableMapping[str, Any],
        expression_lib: MutableSequence[str] | None = None,
        full_js: bool = False,
    ):
        self.context: MutableMapping[str, Any] = context
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.full_js: bool = full_js


class CWLCommandTokenProcessor(CommandTokenProcessor):
    def __init__(
        self,
        name: str,
        expression: Any,
        processor: CommandTokenProcessor | None = None,
        token_type: str | None = None,
        is_shell_command: bool = False,
        item_separator: str | None = None,
        position: str | int = 0,
        prefix: str | None = None,
        separate: bool = True,
        shell_quote: bool = True,
    ):
        super().__init__(name)
        self.expression: Any = expression
        self.processor: CommandTokenProcessor | None = processor
        self.token_type: str | None = token_type
        self.is_shell_command: bool = is_shell_command
        self.item_separator: str | None = item_separator
        self.position: str | int = position
        self.prefix: str | None = prefix
        self.separate: bool = separate
        self.shell_quote: bool = shell_quote

    def bind(
        self,
        token: Token,
        position: int | None,
        options: CommandOptions,
    ) -> CommandToken:
        if not isinstance(options, CWLCommandOptions):
            raise TypeError(
                "The `options` argument must be a `CWLCommandOptions` instance"
            )
        # Obtain token value
        if isinstance(self.expression, str):
            value = utils.eval_expression(
                expression=self.expression,
                context=options.context,
                full_js=options.full_js,
                expression_lib=options.expression_lib,
            )
        elif self.expression is None:
            if self.processor is not None:
                value = flatten_list(
                    [
                        t.value
                        for t in flatten_list(
                            _merge_tokens(
                                self.processor.bind(
                                    token=token, position=position, options=options
                                )
                            )
                        )
                    ]
                )
            else:
                value = get_token_value(token)
        else:
            value = self.expression
        # If token value is an empty array, skip the command token
        if (value := _get_value_for_command(value, self.item_separator)) is not None:
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
                    value = [self.prefix, *value]
                else:
                    value = [self.prefix + _get_value_repr(value)]
            # If value is None or a boolean with no prefix, skip it
            if value is not None and not isinstance(value, bool):
                # Ensure value is a list
                if not isinstance(value, MutableSequence):
                    value = [value]
                # Process shell escape only on the single command token
                if not self.is_shell_command or self.shell_quote:
                    value = [_escape_value(v) for v in value]
                # Obtain token position
                if isinstance(self.position, str) and not self.position.isnumeric():
                    position = utils.eval_expression(
                        expression=self.position,
                        context=cast(dict[str, Any], options.context)
                        | {"self": get_token_value(token) if token else None},
                        full_js=options.full_js,
                        expression_lib=options.expression_lib,
                    )
                    try:
                        position = int(position) if position is not None else 0
                    except ValueError:
                        # If position is not a number, do nothing
                        pass
                else:
                    position = int(self.position)
                # Place value in proper position
                return CommandToken(
                    name=self.name,
                    position=position,
                    value=value,
                )
        return None

    def check_type(self, token: Token) -> bool:
        if self.processor is not None:
            return self.processor.check_type(token)
        else:
            if self.token_type != (
                inferred_type := utils.infer_type_from_token(get_token_value(token))
            ):
                # In CWL, long is considered as a subtype of double
                return inferred_type != "long" or self.token_type != "double"  # nosec
            return True

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            expression=row["expression"],
            processor=(
                await CommandTokenProcessor.load(
                    context=context,
                    row=row["processor"],
                    loading_context=loading_context,
                )
                if row["processor"] is not None
                else None
            ),
            token_type=row["token_type"],
            is_shell_command=row["is_shell_command"],
            item_separator=row["item_separator"],
            position=row["position"],
            prefix=row["prefix"],
            separate=row["separate"],
            shell_quote=row["shell_quote"],
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "expression": self.expression,
            "processor": (
                await self.processor.save(context)
                if self.processor is not None
                else None
            ),
            "token_type": self.token_type,
            "is_shell_command": self.is_shell_command,
            "item_separator": self.item_separator,
            "position": self.position,
            "prefix": self.prefix,
            "separate": self.separate,
            "shell_quote": self.shell_quote,
        }


class CWLForwardCommandTokenProcessor(CommandTokenProcessor):
    def __init__(
        self,
        name: str,
        token_type: str | None = None,
    ):
        super().__init__(name)
        self.token_type: str | None = token_type

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            token_type=row["token_type"],
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "token_type": self.token_type
        }

    def bind(
        self, token: Token, position: int | None, options: CommandOptions
    ) -> CommandToken:
        value = get_token_value(token)
        return CommandToken(
            name=self.name,
            position=position,
            value=[value] if not isinstance(value, MutableSequence) else value,
        )

    def check_type(self, token: Token) -> bool:
        if self.token_type != (
            inferred_type := utils.infer_type_from_token(get_token_value(token))
        ):
            # In CWL, long is considered as a subtype of double
            return inferred_type != "long" or self.token_type != "double"  # nosec
        return True


class CWLObjectCommandTokenProcessor(ObjectCommandTokenProcessor):
    def _update_options(
        self, options: CommandOptions, token: Token
    ) -> CWLCommandOptions:
        if not isinstance(options, CWLCommandOptions):
            raise TypeError(
                "The `options` argument must be a `CWLCommandOptions` instance"
            )
        return CWLCommandOptions(
            context=cast(dict[str, Any], options.context)
            | {"inputs": {self.name: get_token_value(token)}},
            expression_lib=options.expression_lib,
            full_js=options.full_js,
        )


class CWLMapCommandTokenProcessor(MapCommandTokenProcessor):
    def _update_options(
        self, options: CommandOptions, token: Token
    ) -> CWLCommandOptions:
        if not isinstance(options, CWLCommandOptions):
            raise TypeError(
                "The `options` argument must be a `CWLCommandOptions` instance"
            )
        value = get_token_value(token)
        return CWLCommandOptions(
            context=cast(dict[str, Any], options.context)
            | {"inputs": {self.name: value}, "self": value},
            expression_lib=options.expression_lib,
            full_js=options.full_js,
        )


class CWLExpressionCommand(Command):
    def __init__(
        self,
        step: Step,
        expression: str,
        absolute_initial_workdir_allowed: bool = False,
        expression_lib: MutableSequence[str] | None = None,
        full_js: bool = False,
        initial_work_dir: str | MutableSequence[Any] | None = None,
        inplace_update: bool = False,
        time_limit: int | str | None = None,
    ):
        super().__init__(step)
        self.absolute_initial_workdir_allowed: bool = absolute_initial_workdir_allowed
        self.expression: str = expression
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.full_js: bool = full_js
        self.initial_work_dir: str | MutableSequence[Any] | None = initial_work_dir
        self.inplace_update: bool = inplace_update
        self.time_limit: int | str | None = time_limit

    async def execute(self, job: Job) -> CWLCommandOutput:
        context = utils.build_context(
            inputs=job.inputs,
            output_directory=job.output_directory,
            tmp_directory=job.tmp_directory,
            hardware=self.step.workflow.context.scheduler.get_hardware(job.name),
        )
        if self.initial_work_dir is not None:
            await _prepare_work_dir(
                element=self.initial_work_dir,
                options=InitialWorkDirOptions(
                    absolute_initial_workdir_allowed=self.absolute_initial_workdir_allowed,
                    context=context,
                    expression_lib=self.expression_lib,
                    full_js=self.full_js,
                    inplace_update=self.inplace_update,
                    job=job,
                    step=self.step,
                ),
            )
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Evaluating expression for step {self.step.name} (job {job.name})"
            )
        # Persist command
        job_token = get_job_token(
            job.name, cast(ExecuteStep, self.step).get_job_port().token_list
        )
        execution_id = await self.step.workflow.context.database.add_execution(
            step_id=self.step.persistent_id,
            job_token_id=job_token.persistent_id,
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
        await self.step.workflow.context.database.update_execution(
            execution_id,
            {
                "status": Status.COMPLETED.value,
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        # Return result
        return CWLCommandOutput(value=result, status=Status.COMPLETED, exit_code=0)

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "absolute_initial_workdir_allowed": self.absolute_initial_workdir_allowed,
            "expression": self.expression,
            "expression_lib": self.expression_lib,
            "initial_work_dir": self.initial_work_dir,
            "inplace_update": self.inplace_update,
            "full_js": self.full_js,
            "time_limit": self.time_limit,
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ) -> Self:
        return cls(
            step=step,
            absolute_initial_workdir_allowed=row["absolute_initial_workdir_allowed"],
            expression_lib=row["expression_lib"],
            full_js=row["full_js"],
            initial_work_dir=row["initial_work_dir"],
            inplace_update=row["inplace_update"],
            expression=row["expression"],
            time_limit=row["time_limit"],
        )


class InitialWorkDirOptions:
    __slots__ = (
        "absolute_initial_workdir_allowed",
        "context",
        "expression_lib",
        "full_js",
        "inplace_update",
        "job",
        "step",
    )

    def __init__(
        self,
        absolute_initial_workdir_allowed: bool,
        context: MutableMapping[str, Any],
        expression_lib: MutableSequence[str] | None,
        inplace_update: bool,
        full_js: bool,
        job: Job,
        step: Step,
    ):
        self.absolute_initial_workdir_allowed: bool = absolute_initial_workdir_allowed
        self.context: MutableMapping[str, Any] = context
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.inplace_update: bool = inplace_update
        self.full_js: bool = full_js
        self.job: Job = job
        self.step: Step = step
