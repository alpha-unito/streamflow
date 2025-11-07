from __future__ import annotations

import asyncio
import logging
from collections.abc import MutableMapping, MutableSequence
from typing import Any, cast

import cwl_utils.file_formats
from schema_salad.exceptions import ValidationException
from typing_extensions import Self

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector, LocalTarget, Target
from streamflow.core.exception import (
    ProcessorTypeError,
    WorkflowExecutionException,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.processor import (
    CommandOutputProcessor,
    ObjectCommandOutputProcessor,
    TokenProcessor,
)
from streamflow.core.utils import eval_processors, flatten_list, get_tag, make_future
from streamflow.core.workflow import (
    CommandOutput,
    Job,
    Status,
    Token,
)
from streamflow.cwl import utils
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.utils import (
    LoadListing,
    SecondaryFile,
    build_token,
    is_expression,
    resolve_dependencies,
)
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.deployment.utils import get_path_processor
from streamflow.log_handler import logger
from streamflow.workflow.token import ListToken, ObjectToken


def _check_token_type(
    name: str,
    token_value: Any,
    token_type: str | MutableSequence[str],
    enum_symbols: MutableSequence[str],
    optional: bool,
    check_file: bool,
) -> None:
    if isinstance(token_type, MutableSequence):
        messages = []
        for t in token_type:
            try:
                _check_token_type(
                    name, token_value, t, enum_symbols, optional, check_file
                )
                return
            except ProcessorTypeError as e:
                messages.append(str(e))
        raise ProcessorTypeError("\n".join(messages))
    if isinstance(token_value, Token):
        _check_token_type(
            name, token_value.value, token_type, enum_symbols, optional, check_file
        )
    elif token_type == "Any":  # nosec
        if not optional and token_value is None:
            raise ProcessorTypeError(
                f"Token {name} is of type `Any`: it cannot be null."
            )
    elif token_type == "null":  # nosec
        if token_value is not None:
            raise ProcessorTypeError(f"Token {name} should be null.")
    elif token_value is None:
        if not optional:
            raise ProcessorTypeError(f"Token {name} is not optional.")
    elif token_type == "enum":  # nosec
        if token_value not in enum_symbols:
            raise ProcessorTypeError(
                f"Value {token_value} is not valid for token {name}."
            )
    elif (inferred_type := utils.infer_type_from_token(token_value)) != token_type:
        if check_file or token_type not in ["File", "Directory"]:
            # In CWL, long is considered as a subtype of double
            if inferred_type != "long" or token_type != "double":  # nosec
                raise ProcessorTypeError(
                    f"Expected {name} token of type {token_type}, got {inferred_type}."
                )
    return


async def _fill_context(
    context: MutableMapping[str, Any],
    command_output: asyncio.Future[CommandOutput],
    output_eval: str,
    full_js: bool,
    expression_lib: MutableSequence[str] | None,
):
    # Fill context with exit code if required
    if "exitCode" in resolve_dependencies(
        expression=output_eval,
        full_js=full_js,
        expression_lib=expression_lib,
        context_key="runtime",
    ):
        context["runtime"]["exitCode"] = cast(
            CWLCommandOutput, await command_output
        ).exit_code


class CWLCommandOutput(CommandOutput):
    __slots__ = "exit_code"

    def __init__(
        self,
        value: Any,
        status: Status,
        exit_code: int | None = 0,
    ):
        super().__init__(value, status)
        self.exit_code: int = exit_code

    def update(self, value: Any) -> Self:
        return self.__class__(value=value, status=self.status, exit_code=self.exit_code)


class CWLOutputJSONCommandOutput(CWLCommandOutput):
    pass


class CWLTokenProcessor(TokenProcessor):
    def __init__(
        self,
        name: str,
        workflow: CWLWorkflow,
        token_type: str | MutableSequence[str] | None = None,
        enum_symbols: MutableSequence[str] | None = None,
        expression_lib: MutableSequence[str] | None = None,
        file_format: str | None = None,
        full_js: bool = False,
        load_contents: bool | None = None,
        load_listing: LoadListing | None = None,
        only_propagate_secondary_files: bool = True,
        secondary_files: MutableSequence[SecondaryFile] | None = None,
        streamable: bool = False,
    ):
        super().__init__(name, workflow)
        self.token_type: str | MutableSequence[str] | None = token_type
        self.enum_symbols: MutableSequence[str] = enum_symbols or []
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.file_format: str | None = file_format
        self.full_js: bool = full_js
        self.load_contents: bool | None = load_contents
        self.load_listing: LoadListing | None = load_listing
        self.only_propagate_secondary_files: bool = only_propagate_secondary_files
        self.secondary_files: MutableSequence[SecondaryFile] = secondary_files or []
        self.streamable: bool = streamable

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
            token_type=row["token_type"],
            enum_symbols=row["enum_symbols"],
            expression_lib=row["expression_lib"],
            file_format=row["file_format"],
            full_js=row["full_js"],
            load_contents=row["load_contents"],
            load_listing=(
                LoadListing(row["load_listing"])
                if row["load_listing"] is not None
                else None
            ),
            only_propagate_secondary_files=row["only_propagate_secondary_files"],
            secondary_files=[
                SecondaryFile(sf["pattern"], sf["required"])
                for sf in row["secondary_files"]
            ],
            streamable=row["streamable"],
        )

    async def _process_file_token(
        self, inputs: MutableMapping[str, Token], token_value: Any
    ) -> Any:
        # Build context
        context = utils.build_context(inputs)
        # Check file format if present
        if self.file_format:
            context |= {"self": token_value}
            input_formats = utils.eval_expression(
                expression=self.file_format,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
            try:
                cwl_utils.file_formats.check_format(
                    token_value,
                    input_formats,
                    cast(CWLWorkflow, self.workflow).format_graph,
                )
            except ValidationException as e:
                raise WorkflowExecutionException(e.message) from e
        # If file exists, get coordinates
        if filepath := utils.get_path_from_token(token_value):
            try:
                # Privilege locations on the destination connector to ensure existence of secondaryFiles
                data_locations = self.workflow.context.data_manager.get_data_locations(
                    path=filepath,
                )
                data_location = next(
                    loc for loc in data_locations if loc.path == filepath
                )
            except StopIteration:
                # If such location does not exist, apply the standard heuristic to select the best one
                data_location = (
                    await self.workflow.context.data_manager.get_source_location(
                        path=filepath, dst_deployment=LocalTarget.deployment_name
                    )
                )
            if data_location:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Processing {filepath} on location {data_location.location}."
                    )
                connector = self.workflow.context.deployment_manager.get_connector(
                    data_location.deployment
                )
                path_processor = get_path_processor(connector)
                base_path = path_processor.normpath(
                    data_location.path[: -len(data_location.relpath)]
                )
                cwl_workflow = cast(CWLWorkflow, self.workflow)
                # Process file contents
                token_value = await utils.update_file_token(
                    context=self.workflow.context,
                    connector=connector,
                    cwl_version=cwl_workflow.cwl_version,
                    location=data_location.location,
                    token_value=token_value,
                    load_contents=self.load_contents,
                    load_listing=self.load_listing,
                )
                # Process secondary files
                if token_value.get("secondaryFiles"):
                    initial_paths = [
                        utils.get_path_from_token(sf)
                        for sf in token_value["secondaryFiles"]
                    ]
                    sf_map = dict(
                        zip(
                            initial_paths,
                            await asyncio.gather(
                                *(
                                    asyncio.create_task(
                                        utils.update_file_token(
                                            context=self.workflow.context,
                                            connector=connector,
                                            cwl_version=cwl_workflow.cwl_version,
                                            location=data_location.location,
                                            token_value=sf,
                                            load_contents=self.load_contents,
                                            load_listing=self.load_listing,
                                        )
                                    )
                                    for sf in token_value["secondaryFiles"]
                                )
                            ),
                            strict=True,
                        )
                    )
                else:
                    sf_map = {}
                if self.secondary_files:
                    sf_context = cast(dict[str, Any], context) | {"self": token_value}
                    await utils.process_secondary_files(
                        context=self.workflow.context,
                        cwl_version=cwl_workflow.cwl_version,
                        secondary_files=self.secondary_files,
                        sf_map=sf_map,
                        js_context=sf_context,
                        full_js=self.full_js,
                        expression_lib=self.expression_lib,
                        connector=connector,
                        locations=[data_location.location],
                        token_value=token_value,
                        load_contents=self.load_contents,
                        load_listing=self.load_listing,
                        only_retrieve_from_token=self.only_propagate_secondary_files,
                    )
                # Add all secondary files to the token
                if sf_map:
                    token_value["secondaryFiles"] = list(sf_map.values())
                # Register path
                await utils.register_data(
                    context=self.workflow.context,
                    connector=connector,
                    locations=[data_location.location],
                    token_value=token_value,
                    base_path=base_path,
                )
        return token_value

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "token_type": self.token_type,
            "enum_symbols": self.enum_symbols,
            "expression_lib": self.expression_lib,
            "file_format": self.file_format,
            "full_js": self.full_js,
            "load_contents": self.load_contents,
            "load_listing": self.load_listing.value if self.load_listing else None,
            "only_propagate_secondary_files": self.only_propagate_secondary_files,
            "secondary_files": await asyncio.gather(
                *(asyncio.create_task(s.save(context)) for s in self.secondary_files)
            ),
            "streamable": self.streamable,
        }

    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        # Process file token
        if utils.get_token_class(token.value) in ["File", "Directory"]:
            token = token.update(await self._process_file_token(inputs, token.value))
        # Check type
        _check_token_type(
            name=self.name,
            token_value=token.value,
            token_type=self.token_type,
            enum_symbols=self.enum_symbols,
            optional=False,
            check_file=True,
        )
        # Return the token
        return token.update(token.value)


class CWLCommandOutputProcessor(CommandOutputProcessor):
    def __init__(
        self,
        name: str,
        workflow: CWLWorkflow,
        target: Target | None = None,
        token_type: str | MutableSequence[str] | None = None,
        enum_symbols: MutableSequence[str] | None = None,
        expression_lib: MutableSequence[str] | None = None,
        file_format: str | None = None,
        full_js: bool = False,
        glob: str | None = None,
        load_contents: bool = False,
        load_listing: LoadListing = LoadListing.no_listing,
        optional: bool = False,
        output_eval: str | None = None,
        secondary_files: MutableSequence[SecondaryFile] | None = None,
        single: bool = False,
        streamable: bool = False,
    ):
        super().__init__(name, workflow, target)
        self.token_type: str | MutableSequence[str] | None = token_type
        self.enum_symbols: MutableSequence[str] = enum_symbols or []
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.file_format: str | None = file_format
        self.full_js: bool = full_js
        self.glob: str | MutableSequence[str] | None = glob
        self.load_contents: bool = load_contents
        self.load_listing: LoadListing = load_listing
        self.optional: bool = optional
        self.output_eval: str | None = output_eval
        self.secondary_files: MutableSequence[SecondaryFile] = secondary_files or []
        self.single: bool = single
        self.streamable: bool = streamable

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
            target=(
                (await loading_context.load_target(context, row["target"]))
                if row["target"]
                else None
            ),
            token_type=row["token_type"],
            enum_symbols=row["enum_symbols"],
            expression_lib=row["expression_lib"],
            file_format=row["file_format"],
            full_js=row["full_js"],
            glob=row["glob"],
            load_contents=row["load_contents"],
            load_listing=(
                LoadListing(row["load_listing"])
                if row["load_listing"] is not None
                else None
            ),
            optional=row["optional"],
            output_eval=row["output_eval"],
            secondary_files=[
                SecondaryFile(sf["pattern"], sf["required"])
                for sf in row["secondary_files"]
            ],
            single=row["single"],
            streamable=row["streamable"],
        )

    async def _build_token(
        self,
        job: Job,
        connector: Connector | None,
        context: MutableMapping[str, Any],
        token_value: Any,
        recoverable: bool,
    ) -> Token:
        match token_value:
            case MutableMapping():
                match utils.get_token_class(token_value):
                    case "File" | "Directory":
                        connector = self._get_connector(connector, job)
                        locations = await self._get_locations(connector, job)
                        # Register path
                        await utils.register_data(
                            context=self.workflow.context,
                            connector=connector,
                            locations=locations,
                            token_value=token_value,
                            base_path=(
                                self.target.workdir
                                if self.target
                                else job.output_directory
                            ),
                        )
                        # Process file format
                        if self.file_format:
                            context |= {"self": token_value}
                            token_value["format"] = utils.eval_expression(
                                expression=self.file_format,
                                context=context,
                                full_js=self.full_js,
                                expression_lib=self.expression_lib,
                            )
                        return CWLFileToken(
                            value=token_value,
                            tag=get_tag(job.inputs.values()),
                            recoverable=recoverable,
                        )
                    case _:
                        token_tasks = {
                            k: asyncio.create_task(
                                self._build_token(
                                    job, connector, context, v, recoverable
                                )
                            )
                            for k, v in token_value.items()
                        }
                        return ObjectToken(
                            value=dict(
                                zip(
                                    token_tasks.keys(),
                                    await asyncio.gather(*token_tasks.values()),
                                    strict=True,
                                )
                            ),
                            tag=get_tag(job.inputs.values()),
                        )
            case MutableSequence():
                return ListToken(
                    value=await asyncio.gather(
                        *(
                            asyncio.create_task(
                                self._build_token(
                                    job, connector, context, t, recoverable
                                )
                            )
                            for t in token_value
                        )
                    ),
                    tag=get_tag(job.inputs.values()),
                )
            case _:
                return Token(
                    value=token_value,
                    tag=get_tag(job.inputs.values()),
                    recoverable=recoverable,
                )

    async def _process_command_output(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None,
        context: MutableMapping[str, Any],
    ) -> MutableMapping[str, Any]:
        connector = self._get_connector(connector, job)
        locations = await self._get_locations(connector, job)
        cwl_workflow = cast(CWLWorkflow, self.workflow)
        # Generate the output object as described in `outputs` field
        if self.glob is not None:
            # Adjust glob path
            globpaths = []
            for glob in (
                self.glob if isinstance(self.glob, MutableSequence) else [self.glob]
            ):
                globpath = (
                    utils.eval_expression(
                        expression=glob,
                        context=context,
                        full_js=self.full_js,
                        expression_lib=self.expression_lib,
                    )
                    if is_expression(glob)
                    else glob
                )
                globpaths.extend(
                    globpath if isinstance(globpath, MutableSequence) else [globpath]
                )
            # Resolve glob
            for location in locations:
                globpaths = dict(
                    flatten_list(
                        await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    utils.expand_glob(
                                        connector=connector,
                                        workflow=self.workflow,
                                        location=location,
                                        input_directory=(
                                            self.target.workdir
                                            if self.target
                                            else job.input_directory
                                        ),
                                        output_directory=(
                                            self.target.workdir
                                            if self.target
                                            else job.output_directory
                                        ),
                                        tmp_directory=(
                                            self.target.workdir
                                            if self.target
                                            else job.tmp_directory
                                        ),
                                        path=cast(str, path),
                                    )
                                )
                                for path in globpaths
                            )
                        )
                    )
                )
                # Get token class from paths
                globpaths = [
                    {"path": p, "class": c}
                    for p, c in zip(
                        globpaths.keys(),
                        await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    utils.get_class_from_path(
                                        p, job, self.workflow.context
                                    )
                                )
                                for p in globpaths.values()
                            )
                        ),
                        strict=True,
                    )
                ]
            # If evaluation is not needed, simply return paths as token value
            if self.output_eval is None:
                token_list = await utils.build_token_value(
                    context=self.workflow.context,
                    cwl_version=cwl_workflow.cwl_version,
                    js_context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                    secondary_files=self.secondary_files,
                    connector=connector,
                    locations=locations,
                    token_value=globpaths,
                    load_contents=self.load_contents,
                    load_listing=self.load_listing,
                )
                return (
                    token_list
                    if len(token_list) > 1
                    else token_list[0] if len(token_list) == 1 else None
                )
            # Otherwise, fill context['self'] with glob data and proceed
            else:
                context["self"] = await utils.build_token_value(
                    context=self.workflow.context,
                    cwl_version=cwl_workflow.cwl_version,
                    js_context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                    secondary_files=self.secondary_files,
                    connector=connector,
                    locations=locations,
                    token_value=globpaths,
                    load_contents=self.load_contents,
                    load_listing=self.load_listing,
                )
        if self.output_eval is not None:
            await _fill_context(
                context=context,
                command_output=command_output,
                output_eval=self.output_eval,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
            # Evaluate output
            token = utils.eval_expression(
                expression=self.output_eval,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
            # Build token
            return await utils.build_token_value(
                context=self.workflow.context,
                cwl_version=cwl_workflow.cwl_version,
                js_context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
                secondary_files=self.secondary_files,
                connector=connector,
                locations=locations,
                token_value=token,
                load_contents=False,
                load_listing=LoadListing.no_listing,
            )
        # As the default value (no return path is met in previous code), simply process the command output
        token_value = cast(CWLCommandOutput, await command_output).value
        if isinstance(token_value, MutableMapping) and self.name in token_value:
            raise ProcessorTypeError(
                f"Token {self.name} should be extracted by a `PopCommandOutputProcessor` before being evaluated"
            )
        return await utils.build_token_value(
            context=self.workflow.context,
            cwl_version=cwl_workflow.cwl_version,
            js_context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib,
            secondary_files=self.secondary_files,
            connector=connector,
            locations=locations,
            token_value=token_value,
            load_contents=self.load_contents,
            load_listing=self.load_listing,
        )

    async def _process_command_output_json(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None,
        context: MutableMapping[str, Any],
    ) -> MutableMapping[str, Any]:
        connector = self._get_connector(connector, job)
        locations = await self._get_locations(connector, job)
        result = cast(CWLCommandOutput, await command_output)
        if not isinstance(result, CWLOutputJSONCommandOutput):
            raise ProcessorTypeError(
                f"Expected CWLOutputJSONCommandOutput, got {type(result)}"
            )
        token_value = result.value
        if isinstance(token_value, MutableMapping) and self.name in token_value:
            raise ProcessorTypeError(
                f"Token {self.name} should be extracted by a `PopCommandOutputProcessor` before being evaluated"
            )
        return await utils.build_token_value(
            context=self.workflow.context,
            cwl_version=cast(CWLWorkflow, self.workflow).cwl_version,
            js_context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib,
            secondary_files=self.secondary_files,
            connector=connector,
            locations=locations,
            token_value=token_value,
            load_contents=self.load_contents,
            load_listing=self.load_listing,
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "token_type": self.token_type,
            "enum_symbols": self.enum_symbols,
            "expression_lib": self.expression_lib,
            "file_format": self.file_format,
            "full_js": self.full_js,
            "glob": self.glob,
            "load_contents": self.load_contents,
            "load_listing": self.load_listing.value if self.load_listing else None,
            "optional": self.optional,
            "output_eval": self.output_eval,
            "secondary_files": await asyncio.gather(
                *(asyncio.create_task(s.save(context)) for s in self.secondary_files)
            ),
            "single": self.single,
            "streamable": self.streamable,
        }

    async def process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None:
        # Remap output and tmp directories when target is specified
        output_directory = self.target.workdir if self.target else job.output_directory
        tmp_directory = self.target.workdir if self.target else job.tmp_directory
        # Retrieve token value
        context = utils.build_context(
            inputs=job.inputs,
            output_directory=output_directory,
            tmp_directory=tmp_directory,
            hardware=self.workflow.context.scheduler.get_hardware(job.name),
        )
        if isinstance(await command_output, CWLOutputJSONCommandOutput):
            token_value = await self._process_command_output_json(
                job,
                command_output,
                connector,
                context,
            )
        else:
            token_value = await self._process_command_output(
                job,
                command_output,
                connector,
                context,
            )
        if self.token_type != "Any":  # nosec
            if isinstance(token_value, MutableSequence):
                if self.single:
                    if len(token_value) == 1:
                        token_value = token_value[0]
                        _check_token_type(
                            name=self.name,
                            token_value=token_value,
                            token_type=self.token_type,
                            enum_symbols=self.enum_symbols,
                            optional=self.optional,
                            check_file=True,
                        )
                    else:
                        raise ProcessorTypeError(
                            f"Expected {self.name} token of type {self.token_type}, got list."
                        )
                else:
                    for value in token_value:
                        _check_token_type(
                            name=self.name,
                            token_value=value,
                            token_type=self.token_type,
                            enum_symbols=self.enum_symbols,
                            optional=self.optional,
                            check_file=True,
                        )
            else:
                _check_token_type(
                    name=self.name,
                    token_value=token_value,
                    token_type=self.token_type,
                    enum_symbols=self.enum_symbols,
                    optional=self.optional,
                    check_file=True,
                )
        token = await self._build_token(
            job, connector, context, token_value, recoverable
        )
        if self.single or isinstance(token, ListToken):
            return token
        else:
            return ListToken(
                value=[token] if token.value is not None else [],
                tag=token.tag,
            )


class CWLObjectCommandOutputProcessor(ObjectCommandOutputProcessor):
    def __init__(
        self,
        name: str,
        workflow: CWLWorkflow,
        processors: MutableMapping[str, CommandOutputProcessor],
        expression_lib: MutableSequence[str] | None = None,
        full_js: bool = False,
        output_eval: str | None = None,
        target: Target | None = None,
        single: bool = False,
    ):
        super().__init__(
            name=name, processors=processors, workflow=workflow, target=target
        )
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.full_js: bool = full_js
        self.output_eval: str | None = output_eval
        self.single: bool = single

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
            target=(
                (await loading_context.load_target(context, row["target"]))
                if row["target"]
                else None
            ),
            processors={
                k: v
                for k, v in zip(
                    row["processors"].keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                CommandOutputProcessor.load(context, v, loading_context)
                            )
                            for v in row["processors"].values()
                        )
                    ),
                    strict=True,
                )
            },
            expression_lib=row["expression_lib"],
            full_js=row["full_js"],
            output_eval=row["output_eval"],
            single=row["single"],
        )

    async def _process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None:
        result = await command_output
        if isinstance(token_value := result.value, MutableSequence):
            if self.single:
                if len(token_value) == 1:
                    token_value = token_value[0]
                    return await super().process(
                        job=job,
                        command_output=make_future(result.update(token_value)),
                        connector=connector,
                        recoverable=recoverable,
                    )
                else:
                    raise ProcessorTypeError(
                        f"Expected {self.name} token of type record, got list."
                    )
            else:
                return ListToken(
                    value=await asyncio.gather(
                        *(
                            asyncio.create_task(
                                super(CWLObjectCommandOutputProcessor, self).process(
                                    job=job,
                                    command_output=make_future(result.update(value)),
                                    connector=connector,
                                    recoverable=recoverable,
                                )
                            )
                            for value in token_value
                        ),
                    ),
                    tag=get_tag(job.inputs.values()),
                )
        else:
            return await super().process(
                job=job,
                command_output=command_output,
                connector=connector,
                recoverable=recoverable,
            )

    async def _propagate(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> ObjectToken:
        return ObjectToken(
            value=dict(
                zip(
                    self.processors.keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                p.process(
                                    job=job,
                                    command_output=command_output,
                                    connector=connector,
                                    recoverable=recoverable,
                                )
                            )
                            for p in self.processors.values()
                        )
                    ),
                    strict=True,
                )
            ),
            tag=get_tag(job.inputs.values()),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "expression_lib": self.expression_lib,
            "full_js": self.full_js,
            "output_eval": self.output_eval,
            "single": self.single,
        }

    async def process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None:
        # Remap output and tmp directories when target is specified
        output_directory = self.target.workdir if self.target else job.output_directory
        tmp_directory = self.target.workdir if self.target else job.tmp_directory
        if self.output_eval:
            # Build context and fill it with exit code
            context = utils.build_context(
                inputs=job.inputs,
                output_directory=output_directory,
                tmp_directory=tmp_directory,
                hardware=self.workflow.context.scheduler.get_hardware(job.name),
            )
            await _fill_context(
                context=context,
                command_output=command_output,
                output_eval=self.output_eval,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
            # Evaluate output
            command_output = make_future(
                (await command_output).update(
                    utils.eval_expression(
                        expression=self.output_eval,
                        context=context,
                        full_js=self.full_js,
                        expression_lib=self.expression_lib,
                    )
                )
            )
        # Process output
        process_tasks = [
            asyncio.create_task(
                self._process(
                    job=job,
                    command_output=command_output,
                    connector=connector,
                    recoverable=recoverable,
                )
            ),
            asyncio.create_task(
                self._propagate(
                    job=job,
                    command_output=command_output,
                    connector=connector,
                    recoverable=recoverable,
                )
            ),
        ]
        return await eval_processors(process_tasks, name=self.name)


class CWLExpressionToolOutputProcessor(CommandOutputProcessor):
    def __init__(
        self,
        name: str,
        workflow: CWLWorkflow,
        target: Target | None = None,
        token_type: str | MutableSequence[str] | None = None,
        enum_symbols: MutableSequence[str] | None = None,
        file_format: str | None = None,
        optional: bool = False,
        streamable: bool = False,
    ):
        super().__init__(name, workflow, target)
        self.token_type: str | MutableSequence[str] | None = token_type
        self.enum_symbols: str | MutableSequence[str] = enum_symbols or []
        self.file_format: str | None = file_format
        self.optional: bool = optional
        self.streamable: bool = streamable

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
            target=(
                (await loading_context.load_target(context, row["target"]))
                if row["target"]
                else None
            ),
            token_type=row["token_type"],
            file_format=row["file_format"],
            enum_symbols=row["enum_symbols"],
            optional=row["optional"],
            streamable=row["streamable"],
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "token_type": self.token_type,
            "enum_symbols": self.enum_symbols,
            "file_format": self.file_format,
            "optional": self.optional,
            "streamable": self.streamable,
        }

    async def process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None:
        result = cast(CWLCommandOutput, await command_output)
        token_value = result.value
        if self.token_type != "Any":  # nosec
            if isinstance(token_value, MutableSequence):
                for value in token_value:
                    _check_token_type(
                        name=self.name,
                        token_value=value,
                        token_type=self.token_type,
                        enum_symbols=self.enum_symbols,
                        optional=self.optional,
                        check_file=True,
                    )
            else:
                _check_token_type(
                    name=self.name,
                    token_value=token_value,
                    token_type=self.token_type,
                    enum_symbols=self.enum_symbols,
                    optional=self.optional,
                    check_file=True,
                )
        return await build_token(
            cwl_version=cast(CWLWorkflow, self.workflow).cwl_version,
            inputs=job.inputs,
            token_value=token_value,
            streamflow_context=self.workflow.context,
            recoverable=True,
        )
