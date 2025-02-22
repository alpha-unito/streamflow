from __future__ import annotations

import asyncio
import logging
import os
import posixpath
import urllib.parse
from collections.abc import MutableMapping, MutableSequence
from enum import Enum
from pathlib import PurePath
from types import ModuleType
from typing import Any, cast

import cwl_utils.expression
import cwl_utils.parser
import cwl_utils.parser.utils
from cwl_utils.parser.cwl_v1_2_utils import CONTENT_LIMIT

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation, DataType
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.scheduling import Hardware
from streamflow.core.utils import random_name
from streamflow.core.workflow import Job, Token, Workflow
from streamflow.cwl.expression import DependencyResolver
from streamflow.data.remotepath import StreamFlowPath
from streamflow.deployment.utils import get_path_processor
from streamflow.log_handler import logger
from streamflow.workflow.utils import get_token_value


async def _check_glob_path(
    connector: Connector,
    workflow: Workflow,
    location: ExecutionLocation | None,
    input_directory: str,
    output_directory: str,
    tmp_directory: str,
    path: StreamFlowPath,
    real_path: StreamFlowPath,
) -> None:
    # Cannot glob outside the job output folder
    if not (
        real_path.is_relative_to(input_directory)
        or real_path.is_relative_to(output_directory)
        or real_path.is_relative_to(tmp_directory)
    ):
        relative_path = path.relative_to(output_directory)
        base_path = StreamFlowPath(
            (
                str(real_path).removesuffix(str(relative_path))
                if str(real_path).endswith(str(relative_path))
                else real_path.parent
            ),
            context=workflow.context,
            location=location,
        )
        if not await search_in_parent_locations(
            context=workflow.context,
            connector=connector,
            path=str(real_path),
            relpath=str(relative_path),
            base_path=str(base_path),
        ):
            indir = StreamFlowPath(
                input_directory, context=workflow.context, location=location
            )
            async for _, dirnames, _ in indir.walk(follow_symlinks=True):
                for dirname in dirnames:
                    if await (indir / dirname / relative_path).exists():
                        return
                break
            raise WorkflowDefinitionException(
                "Globs outside the job's output folder are not allowed"
            )


async def _get_contents(
    path: StreamFlowPath,
    size: int,
    cwl_version: str,
):
    if (cwl_version not in ("v1.0", "v.1.1")) and size > CONTENT_LIMIT:
        raise WorkflowExecutionException(
            f"Cannot read contents from files larger than "
            f"{CONTENT_LIMIT / 1024}kB: file {str(path)} is {size / 1024}kB"
        )
    return await path.read_text(n=CONTENT_LIMIT)


async def _get_listing(
    context: StreamFlowContext,
    connector: Connector,
    cwl_version: str,
    locations: MutableSequence[ExecutionLocation],
    dirpath: StreamFlowPath,
    load_contents: bool,
    recursive: bool,
) -> MutableSequence[MutableMapping[str, Any]]:
    listing_tokens = {}
    for location in locations:
        loc_path = StreamFlowPath(dirpath, context=context, location=location)
        async for dirpath, dirnames, filenames in loc_path.walk(follow_symlinks=True):
            for dirname in dirnames:
                directory = dirpath / dirname
                if str(directory) not in listing_tokens:
                    load_listing = (
                        LoadListing.deep_listing
                        if recursive
                        else LoadListing.no_listing
                    )
                    listing_tokens[str(directory)] = asyncio.create_task(
                        get_file_token(  # nosec
                            context=context,
                            connector=connector,
                            cwl_version=cwl_version,
                            locations=locations,
                            token_class="Directory",
                            filepath=str(directory),
                            load_contents=load_contents,
                            load_listing=load_listing,
                        )
                    )
            for filename in filenames:
                file = dirpath / filename
                if str(file) not in listing_tokens:
                    listing_tokens[str(file)] = asyncio.create_task(
                        get_file_token(  # nosec
                            context=context,
                            connector=connector,
                            cwl_version=cwl_version,
                            locations=locations,
                            token_class="File",
                            filepath=str(file),
                            load_contents=load_contents,
                        )
                    )
            break
    return cast(
        MutableSequence[MutableMapping[str, Any]],
        await asyncio.gather(*listing_tokens.values()),
    )


async def _process_secondary_file(
    context: StreamFlowContext,
    connector: Connector,
    cwl_version: str,
    locations: MutableSequence[ExecutionLocation],
    secondary_file: Any,
    token_value: MutableMapping[str, Any],
    from_expression: bool,
    existing_sf: MutableMapping[str, Any],
    load_contents: bool,
    load_listing: LoadListing | None,
    only_retrieve_from_token: bool,
) -> MutableMapping[str, Any] | None:
    # If value is None, simply return None
    if secondary_file is None:
        return None
    # If value is a dictionary, simply append it to the list
    elif isinstance(secondary_file, MutableMapping):
        filepath = get_path_from_token(secondary_file)
        for location in locations:
            path = StreamFlowPath(filepath, context=context, location=location)
            if await path.exists():
                return await get_file_token(
                    context=context,
                    connector=connector,
                    cwl_version=cwl_version,
                    locations=locations,
                    token_class=get_token_class(secondary_file),
                    filepath=filepath,
                    file_format=secondary_file.get("format"),
                    basename=secondary_file.get("basename"),
                    load_contents=load_contents,
                    load_listing=load_listing,
                )
        return None
    # If value is a string
    else:
        # If value doesn't come from an expression, apply it to the primary path
        path_processor = get_path_processor(connector)
        filepath = (
            secondary_file
            if from_expression
            else _process_sf_path(
                path_processor, secondary_file, get_path_from_token(token_value)
            )
        )
        if not path_processor.isabs(filepath):
            filepath = path_processor.join(
                path_processor.dirname(get_path_from_token(token_value)), filepath
            )
        if filepath not in existing_sf:
            # If must only retrieve elements from token, return None
            if only_retrieve_from_token:
                return None
            else:
                # Search file in job locations and build token value
                for location in locations:
                    path = StreamFlowPath(filepath, context=context, location=location)
                    if await path.exists():
                        token_class = "File" if await path.is_file() else "Directory"
                        return await get_file_token(
                            context=context,
                            connector=connector,
                            cwl_version=cwl_version,
                            locations=locations,
                            token_class=token_class,
                            filepath=filepath,
                            load_contents=load_contents,
                            load_listing=load_listing,
                        )
                return None
        else:
            return existing_sf[filepath]


def _process_sf_path(
    path_processor: ModuleType, pattern: str, primary_path: str
) -> str:
    if pattern.startswith("^"):
        return _process_sf_path(
            path_processor, pattern[1:], path_processor.splitext(primary_path)[0]
        )
    else:
        return primary_path + pattern


async def _register_path(
    context: StreamFlowContext,
    connector: Connector,
    location: ExecutionLocation,
    path: str,
    relpath: str,
    data_type: DataType = DataType.PRIMARY,
) -> DataLocation | None:
    path = StreamFlowPath(path, context=context, location=location)
    if real_path := await path.resolve():
        if real_path != path:
            if data_locations := context.data_manager.get_data_locations(
                path=str(real_path), deployment=connector.deployment_name
            ):
                data_location = next(iter(data_locations))
            else:
                base_path = StreamFlowPath(
                    str(path).removesuffix(str(relpath)),
                    context=context,
                    location=location,
                )
                if real_path.is_relative_to(base_path):
                    data_location = context.data_manager.register_path(
                        location=location,
                        path=str(real_path),
                        relpath=str(real_path.relative_to(base_path)),
                    )
                elif data_locations := await search_in_parent_locations(
                    context=context,
                    connector=connector,
                    path=str(real_path),
                    relpath=real_path.name,
                ):
                    data_location = data_locations[0]
                else:
                    return None
            link_location = context.data_manager.register_path(
                location=location,
                path=str(path),
                relpath=relpath,
                data_type=DataType.SYMBOLIC_LINK,
            )
            context.data_manager.register_relation(data_location, link_location)
            return data_location
        else:
            return context.data_manager.register_path(
                location=location, path=str(path), relpath=relpath, data_type=data_type
            )
    return None


def build_context(
    inputs: MutableMapping[str, Token],
    output_directory: str | None = None,
    tmp_directory: str | None = None,
    hardware: Hardware | None = None,
) -> MutableMapping[str, Any]:
    context = {"inputs": {}, "self": None, "runtime": {}}
    for name, token in inputs.items():
        context["inputs"][name] = get_token_value(token)
    if output_directory:
        context["runtime"]["outdir"] = output_directory
    if tmp_directory:
        context["runtime"]["tmpdir"] = tmp_directory
    if hardware:
        context["runtime"]["cores"] = hardware.cores
        context["runtime"]["ram"] = hardware.memory
        context["runtime"]["tmpdirSize"] = hardware.storage["__tmpdir__"].size
        context["runtime"]["outdirSize"] = hardware.storage["__outdir__"].size
    return context


async def build_token_value(
    context: StreamFlowContext,
    cwl_version: str,
    js_context: MutableMapping[str, Any],
    full_js: bool,
    expression_lib: MutableSequence[str] | None,
    secondary_files: MutableSequence[SecondaryFile] | None,
    connector: Connector,
    locations: MutableSequence[ExecutionLocation],
    token_value: Any,
    load_contents: bool,
    load_listing: LoadListing,
) -> Any:
    if isinstance(token_value, MutableSequence):
        value_tasks = []
        for t in token_value:
            value_tasks.append(
                asyncio.create_task(
                    build_token_value(
                        context=context,
                        cwl_version=cwl_version,
                        js_context=js_context,
                        full_js=full_js,
                        expression_lib=expression_lib,
                        secondary_files=secondary_files,
                        connector=connector,
                        locations=locations,
                        token_value=t,
                        load_contents=load_contents,
                        load_listing=load_listing,
                    )
                )
            )
        return await asyncio.gather(*value_tasks)
    elif isinstance(token_value, MutableMapping) and (
        token_class := get_token_class(token_value)
    ) in ["File", "Directory"]:
        path_processor = get_path_processor(connector)
        # Get filepath
        filepath = get_path_from_token(token_value)
        if filepath is not None:
            # Process secondary files in token value
            sf_map = {}
            if "secondaryFiles" in token_value:
                sf_tasks = []
                for sf in token_value.get("secondaryFiles", []):
                    sf_path = get_path_from_token(sf)
                    if not path_processor.isabs(sf_path):
                        path_processor.join(path_processor.dirname(filepath), sf_path)
                    sf_tasks.append(
                        asyncio.create_task(
                            get_file_token(
                                context=context,
                                connector=connector,
                                cwl_version=cwl_version,
                                locations=locations,
                                token_class=get_token_class(sf),
                                filepath=sf_path,
                                file_format=sf.get("format"),
                                basename=sf.get("basename"),
                                load_contents=load_contents,
                                load_listing=load_listing,
                            )
                        )
                    )
                sf_map = {
                    get_path_from_token(sf): sf
                    for sf in await asyncio.gather(*sf_tasks)
                }
            # Compute the new token value
            token_value = await get_file_token(
                context=context,
                connector=connector,
                cwl_version=cwl_version,
                locations=locations,
                token_class=token_class,
                filepath=filepath,
                file_format=token_value.get("format"),
                basename=token_value.get("basename"),
                load_contents=load_contents,
                load_listing=load_listing,
            )
            # Compute new secondary files from port specification
            sf_context = cast(dict[str, Any], js_context) | {"self": token_value}
            if secondary_files:
                await process_secondary_files(
                    context=context,
                    cwl_version=cwl_version,
                    secondary_files=secondary_files,
                    sf_map=sf_map,
                    js_context=sf_context,
                    full_js=full_js,
                    expression_lib=expression_lib,
                    connector=connector,
                    locations=locations,
                    token_value=token_value,
                    load_contents=load_contents,
                    load_listing=load_listing,
                )
                # Add all secondary files to the token
                if sf_map:
                    token_value["secondaryFiles"] = list(sf_map.values())
        # If there is only a 'contents' field, propagate the parameter
        elif "contents" in token_value:
            filepath = path_processor.join(
                js_context["runtime"]["outdir"],
                token_value.get("basename", random_name()),
            )
            contents = token_value["contents"]
            token_value = await get_file_token(
                context=context,
                connector=connector,
                cwl_version=cwl_version,
                locations=locations,
                token_class=token_class,
                filepath=filepath,
                file_format=token_value.get("format"),
                basename=token_value.get("basename"),
                load_listing=load_listing,
            )
            token_value["contents"] = contents
        # If there is only a 'listing' field, build a folder token and process all the listing entries recursively
        elif "listing" in token_value:
            filepath = js_context["runtime"]["outdir"]
            if "basename" in token_value:
                filepath = path_processor.join(filepath, token_value["basename"])
            # When `shallow_listing` is set, stop propagation
            must_load_listing = (
                LoadListing.no_listing
                if load_listing == LoadListing.shallow_listing
                else load_listing
            )
            # Build listing tokens
            listing_tokens = await asyncio.gather(
                *(
                    asyncio.create_task(
                        build_token_value(
                            context=context,
                            cwl_version=cwl_version,
                            js_context=js_context,
                            full_js=full_js,
                            expression_lib=expression_lib,
                            secondary_files=secondary_files,
                            connector=connector,
                            locations=locations,
                            token_value=lst,
                            load_contents=load_contents,
                            load_listing=must_load_listing,
                        )
                    )
                    for lst in token_value["listing"]
                )
            )
            token_value = await get_file_token(
                context=context,
                connector=connector,
                cwl_version=cwl_version,
                locations=locations,
                token_class=token_class,
                filepath=filepath,
                file_format=token_value.get("format"),
                basename=token_value.get("basename"),
                load_contents=load_contents,
                load_listing=load_listing,
            )
            token_value["listing"] = listing_tokens
    return token_value


async def create_remote_directory(
    context: StreamFlowContext,
    locations: MutableSequence[ExecutionLocation],
    path: str,
    relpath: str,
):
    tasks = []
    for location in locations:
        path = StreamFlowPath(path, context=context, location=location)
        if not await path.exists():
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "Creating {path} {location}".format(
                        path=str(path),
                        location=(
                            "on local file-system"
                            if location.local
                            else f"on location {location}"
                        ),
                    )
                )
            tasks.append(
                asyncio.create_task(path.mkdir(mode=0o777, parents=True, exist_ok=True))
            )
            await _register_path(
                context=context,
                connector=context.deployment_manager.get_connector(location.deployment),
                location=location,
                path=str(path),
                relpath=relpath,
            )
    await asyncio.gather(*tasks)


def eval_expression(
    expression: str,
    context: MutableMapping[str, Any],
    full_js: bool = False,
    expression_lib: MutableSequence[str] | None = None,
    timeout: int | None = None,
    strip_whitespace: bool = True,
) -> Any:
    if isinstance(expression, str) and ("$(" in expression or "${" in expression):
        return cwl_utils.expression.interpolate(
            expression,
            context,
            jslib=(
                cwl_utils.expression.jshead(expression_lib or [], context)
                if full_js
                else ""
            ),
            fullJS=full_js,
            strip_whitespace=strip_whitespace,
            timeout=timeout,
        )
    else:
        return expression


async def expand_glob(
    connector: Connector,
    workflow: Workflow,
    location: ExecutionLocation | None,
    input_directory: str,
    output_directory: str,
    tmp_directory: str,
    path: str,
) -> MutableSequence[tuple[str, str]]:
    outdir = StreamFlowPath(
        output_directory, context=workflow.context, location=location
    )
    paths = sorted([p async for p in outdir.glob(path)])
    effective_paths = await asyncio.gather(
        *(asyncio.create_task(p.resolve()) for p in paths)
    )
    await asyncio.gather(
        *(
            asyncio.create_task(
                _check_glob_path(
                    connector=connector,
                    workflow=workflow,
                    location=location,
                    input_directory=input_directory,
                    output_directory=output_directory,
                    tmp_directory=tmp_directory,
                    path=p,
                    real_path=ep or p,
                )
            )
            for p, ep in zip(paths, effective_paths)
        )
    )
    return [(str(p), str(ep or p)) for p, ep in zip(paths, effective_paths)]


async def get_class_from_path(
    path: str, job: Job, context: StreamFlowContext
) -> str | None:
    locations = context.scheduler.get_locations(job.name)
    for location in locations:
        return (
            "File"
            if await StreamFlowPath(path, context=context, location=location).is_file()
            else "Directory"
        )
    raise WorkflowExecutionException(
        f"Impossible to retrieve CWL token class for path {path}"
    )


async def get_file_token(
    context: StreamFlowContext,
    connector: Connector,
    cwl_version: str,
    locations: MutableSequence[ExecutionLocation],
    token_class: str,
    filepath: str,
    file_format: str | None = None,
    basename: str | None = None,
    load_contents: bool = False,
    load_listing: LoadListing | None = None,
) -> MutableMapping[str, Any]:
    path_processor = get_path_processor(connector)
    basename = basename or path_processor.basename(filepath)
    file_location = "".join(["file://", urllib.parse.quote(filepath)])
    token = {
        "class": token_class,
        "location": file_location,
        "basename": basename,
        "path": filepath,
        "dirname": path_processor.dirname(filepath),
    }
    if token_class == "File":  # nosec
        if file_format:
            token["format"] = file_format
        token["nameroot"], token["nameext"] = path_processor.splitext(basename)
        for location in locations:
            path = StreamFlowPath(filepath, context=context, location=location)
            if real_path := await path.resolve():
                token["size"] = await real_path.size()
                if load_contents:
                    token["contents"] = await _get_contents(
                        real_path,
                        token["size"],
                        cwl_version,
                    )
                if (checksum := await real_path.checksum()) is not None:
                    token["checksum"] = f"sha1${checksum}"
                else:
                    raise WorkflowExecutionException(
                        f"Impossible to retrieve checksum of {real_path} on {location}"
                    )
                break
    elif token_class == "Directory" and load_listing != LoadListing.no_listing:  # nosec
        for location in locations:
            path = StreamFlowPath(filepath, context=context, location=location)
            if await path.exists():
                token["listing"] = await _get_listing(
                    context=context,
                    connector=connector,
                    cwl_version=cwl_version,
                    locations=locations,
                    dirpath=path,
                    load_contents=load_contents,
                    recursive=load_listing == LoadListing.deep_listing,
                )
                break
    return token


def get_name(
    name_prefix: str,
    cwl_name_prefix: str,
    element_id: Any,
    preserve_cwl_prefix: bool = False,
) -> str:
    name = (element_id if isinstance(element_id, str) else element_id.id).split("#")[-1]
    return (
        posixpath.join(posixpath.sep, name)
        if preserve_cwl_prefix
        else posixpath.join(
            name_prefix,
            posixpath.relpath(posixpath.join(posixpath.sep, name), cwl_name_prefix),
        )
    )


def get_path_from_token(token_value: MutableMapping[str, Any]) -> str | None:
    location = token_value.get("location", token_value.get("path"))
    if location and "://" in location:
        scheme = urllib.parse.urlsplit(location).scheme
        return (
            str(PurePath(urllib.parse.unquote(location[7:])))
            if scheme == "file"
            else None
        )
    return location


def get_token_class(token_value: Any) -> str | None:
    if isinstance(token_value, MutableMapping):
        return token_value.get("class", token_value.get("type"))
    else:
        return None


def infer_type_from_token(token_value: Any) -> str:
    if isinstance(token_value, MutableMapping):
        return get_token_class(token_value) or "record"
    elif isinstance(token_value, MutableSequence):
        return "array"
    elif isinstance(token_value, str):
        return "string"
    elif isinstance(token_value, bool):
        return "boolean"
    elif isinstance(token_value, int):
        return "long"
    elif isinstance(token_value, float):
        return "double"
    else:
        # Could not infer token type: mark as Any
        return "Any"


class LoadListing(Enum):
    no_listing = 0
    shallow_listing = 1
    deep_listing = 2


def process_embedded_tool(
    cwl_element: cwl_utils.parser.WorkflowStep,
    cwl_name_prefix: str,
    step_name: str,
    name_prefix: str,
    context: MutableMapping[str, Any],
) -> tuple[cwl_utils.parser.Process, str, MutableMapping[str, Any]]:
    run_command = cwl_element.run
    inner_context = dict(context)
    # If the `run` options contains an inline CWL object
    if cwl_utils.parser.is_process(run_command):
        # Add the CWL version, which is not present by default
        run_command.cwlVersion = context["version"]
        # Process `stdout` and `stderr` directives
        cwl_utils.parser.utils.convert_stdstreams_to_files(run_command)
        # Compute the prefix of the inner CWL element
        if ":" in run_command.id.split("#")[-1]:
            cwl_step_name = get_name(
                name_prefix,
                cwl_name_prefix,
                cwl_element.id,
                preserve_cwl_prefix=True,
            )
            inner_cwl_name_prefix = (
                step_name
                if context["version"] == "v1.0"
                else posixpath.join(cwl_step_name, "run")
            )
        else:
            inner_cwl_name_prefix = get_name(
                name_prefix,
                cwl_name_prefix,
                run_command.id,
                preserve_cwl_prefix=True,
            )
    # Otherwise, the `run` options contains an URI
    else:
        # Fetch and translate the target file
        run_command = cwl_utils.parser.load_document_by_uri(
            path=cwl_element.loadingOptions.fetcher.urljoin(
                base_url=cwl_element.loadingOptions.fileuri, url=run_command
            ),
            loadingOptions=cwl_element.loadingOptions,
        )
        # Process `stdout` and `stderr` directives
        cwl_utils.parser.utils.convert_stdstreams_to_files(run_command)
        # Compute the prefix of the inner CWL element
        inner_cwl_name_prefix = (
            get_name(posixpath.sep, posixpath.sep, run_command.id)
            if "#" in run_command.id
            else posixpath.sep
        )
        # Set the inner CWL version, which may differ from the outer one
        inner_context |= {"version": run_command.cwlVersion}
    return run_command, inner_cwl_name_prefix, inner_context


async def process_secondary_files(
    context: StreamFlowContext,
    cwl_version: str,
    secondary_files: MutableSequence[SecondaryFile],
    sf_map: MutableMapping[str, Any],
    js_context: MutableMapping[str, Any],
    full_js: bool,
    expression_lib: MutableSequence[str] | None,
    connector: Connector,
    locations: MutableSequence[ExecutionLocation],
    token_value: Any,
    load_contents: bool | None = None,
    load_listing: LoadListing | None = None,
    only_retrieve_from_token: bool = False,
) -> None:
    sf_tasks, sf_specs = [], []
    for secondary_file in secondary_files:
        # If pattern is an expression, evaluate it and process result
        if "$(" in secondary_file.pattern or "${" in secondary_file.pattern:
            sf_value = eval_expression(
                expression=secondary_file.pattern,
                context=js_context,
                full_js=full_js,
                expression_lib=expression_lib,
            )
            # If the expression explicitly returns None, do not process this entry
            if sf_value is None:
                continue
            elif isinstance(sf_value, MutableSequence):
                for sf in sf_value:
                    sf_tasks.append(
                        asyncio.create_task(
                            _process_secondary_file(
                                context=context,
                                connector=connector,
                                cwl_version=cwl_version,
                                locations=locations,
                                secondary_file=sf,
                                token_value=token_value,
                                from_expression=True,
                                existing_sf=sf_map,
                                load_contents=load_contents,
                                load_listing=load_listing,
                                only_retrieve_from_token=only_retrieve_from_token,
                            )
                        )
                    )
                    sf_specs.append(secondary_file)
            else:
                sf_tasks.append(
                    asyncio.create_task(
                        _process_secondary_file(
                            context=context,
                            connector=connector,
                            cwl_version=cwl_version,
                            locations=locations,
                            secondary_file=sf_value,
                            token_value=token_value,
                            from_expression=True,
                            existing_sf=sf_map,
                            load_contents=load_contents,
                            load_listing=load_listing,
                            only_retrieve_from_token=only_retrieve_from_token,
                        )
                    )
                )
                sf_specs.append(secondary_file)
        # Otherwise, simply process the pattern string
        else:
            sf_tasks.append(
                asyncio.create_task(
                    _process_secondary_file(
                        context=context,
                        connector=connector,
                        cwl_version=cwl_version,
                        locations=locations,
                        secondary_file=secondary_file.pattern,
                        token_value=token_value,
                        from_expression=False,
                        existing_sf=sf_map,
                        load_contents=load_contents,
                        load_listing=load_listing,
                        only_retrieve_from_token=only_retrieve_from_token,
                    )
                )
            )
            sf_specs.append(secondary_file)
    for sf_value, sf_spec in zip(await asyncio.gather(*sf_tasks), sf_specs):
        if sf_value is not None:
            sf_map[get_path_from_token(cast(MutableMapping[str, Any], sf_value))] = (
                sf_value
            )
        else:
            required = eval_expression(
                expression=sf_spec.required,
                context=js_context,
                full_js=full_js,
                expression_lib=expression_lib,
            )
            if required:
                raise WorkflowExecutionException(
                    f"Required secondary file {sf_spec.pattern} not found"
                )


async def register_data(
    context: StreamFlowContext,
    connector: Connector,
    locations: MutableSequence[ExecutionLocation],
    base_path: str | None,
    token_value: MutableSequence[MutableMapping[str, Any]] | MutableMapping[str, Any],
):
    # If `token_value` is a list, process every item independently
    if isinstance(token_value, MutableSequence):
        await asyncio.gather(
            *(
                asyncio.create_task(
                    register_data(context, connector, locations, base_path, t)
                )
                for t in token_value
            )
        )
    # Otherwise, if token value is a dictionary and it refers to a File or a Directory, register the path
    elif get_token_class(token_value) in ["File", "Directory"]:
        path_processor = get_path_processor(connector)
        # Extract paths from token
        paths = []
        if "path" in token_value and token_value["path"] is not None:
            paths.append(token_value["path"])
        elif "location" in token_value and token_value["location"] is not None:
            paths.append(token_value["location"])
        elif "listing" in token_value:
            paths.extend(
                [
                    t.get("path", t["location"])
                    for t in token_value["listing"]
                    if "path" in t or "location" in t
                ]
            )
        if "secondaryFiles" in token_value:
            for sf in token_value["secondaryFiles"]:
                paths.append(get_path_from_token(sf))
        # Remove `file` protocol if present
        paths = [
            urllib.parse.unquote(p[7:]) if p.startswith("file://") else p for p in paths
        ]
        # Register paths to the `DataManager`
        for path in (path_processor.normpath(p) for p in paths):
            relpath = (
                path_processor.relpath(path, base_path)
                if base_path and path.startswith(base_path)
                else path_processor.basename(path)
            )
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        _register_path(
                            context=context,
                            connector=connector,
                            location=location,
                            path=path,
                            relpath=relpath,
                        )
                    )
                    for location in locations
                )
            )


def remap_path(
    path_processor: ModuleType, path: str, old_dir: str, new_dir: str
) -> str:
    if ":/" in path:
        scheme = urllib.parse.urlsplit(path).scheme
        if scheme == "file":
            return "file://{}".format(
                path_processor.join(
                    new_dir,
                    *os.path.relpath(urllib.parse.unquote(path[7:]), old_dir).split(
                        os.path.sep
                    ),
                )
            )
        else:
            return path
    else:
        return path_processor.join(
            new_dir,
            *os.path.relpath(urllib.parse.unquote(path), old_dir).split(os.path.sep),
        )


def remap_token_value(
    path_processor: ModuleType, old_dir: str, new_dir: str, value: Any
) -> Any:
    if isinstance(value, MutableSequence):
        return [remap_token_value(path_processor, old_dir, new_dir, v) for v in value]
    elif isinstance(value, MutableMapping):
        if get_token_class(value) in ["File", "Directory"]:
            if "location" in value:
                value["location"] = remap_path(
                    path_processor=path_processor,
                    path=value["location"],
                    old_dir=old_dir,
                    new_dir=new_dir,
                )
            if "path" in value:
                value["path"] = remap_path(
                    path_processor=path_processor,
                    path=value["path"],
                    old_dir=old_dir,
                    new_dir=new_dir,
                )
            if "secondaryFiles" in value:
                value["secondaryFiles"] = [
                    remap_token_value(path_processor, old_dir, new_dir, sf)
                    for sf in value["secondaryFiles"]
                ]
            if "listing" in value:
                value["listing"] = [
                    remap_token_value(path_processor, old_dir, new_dir, sf)
                    for sf in value["listing"]
                ]
            return value
        else:
            return {
                k: remap_token_value(path_processor, old_dir, new_dir, v)
                for k, v in value.items()
            }
    else:
        return value


def resolve_dependencies(
    expression: str,
    full_js: bool = False,
    expression_lib: MutableSequence[str] | None = None,
    timeout: int | None = None,
    strip_whitespace: bool = True,
) -> set[str]:
    if isinstance(expression, str) and ("$(" in expression or "${" in expression):
        context = {"inputs": {}, "self": {}, "runtime": {}}
        engine = DependencyResolver()
        cwl_utils.expression.interpolate(
            expression,
            context,
            jslib=(
                cwl_utils.expression.jshead(expression_lib or [], context)
                if full_js
                else ""
            ),
            fullJS=full_js,
            strip_whitespace=strip_whitespace,
            timeout=timeout,
            resolve_dependencies=True,
            js_engine=engine,
        )
        return engine.deps
    else:
        return set()


async def search_in_parent_locations(
    context: StreamFlowContext,
    connector: Connector,
    path: str,
    relpath: str,
    base_path: str | None = None,
) -> MutableSequence[DataLocation]:
    path_processor = get_path_processor(connector)
    current_path = path
    while current_path != (base_path or path_processor.sep):
        # Retrieve all data locations
        if data_locations := context.data_manager.get_data_locations(path=current_path):
            # If there is no data location for the exact source path
            actual_locations = {}
            if current_path != path:
                # Add source path to all the involved locations
                previous_location = None
                for data_location in sorted(
                    data_locations,
                    key=lambda loc: 0 if loc.data_type == DataType.PRIMARY else 1,
                ):
                    data_path = (
                        path
                        if data_location.path.startswith(current_path)
                        else path_processor.join(
                            path_processor.normpath(
                                data_location.path[: -len(data_location.relpath)]
                            ),
                            relpath,
                        )
                    )
                    data_connector = context.deployment_manager.get_connector(
                        data_location.deployment
                    )
                    if current_location := await _register_path(
                        context=context,
                        connector=data_connector,
                        location=data_location.location,
                        path=data_path,
                        relpath=relpath,
                        data_type=data_location.data_type,
                    ):
                        actual_locations[current_location.name] = current_location
                        if previous_location is not None:
                            context.data_manager.register_relation(
                                previous_location, current_location
                            )
                        previous_location = current_location
                if not actual_locations:
                    raise WorkflowExecutionException(f"Error registering path {path}")
                return list(actual_locations.values())
            else:
                return sorted(
                    data_locations,
                    key=lambda loc: 0 if loc.data_type == DataType.PRIMARY else 1,
                )
        path_tokens = [path_processor.sep]
        path_tokens.extend(
            current_path.lstrip(path_processor.sep).split(path_processor.sep)[:-1]
        )
        current_path = path_processor.normpath(path_processor.join(*path_tokens))
    return []


class SecondaryFile:
    __slots__ = ("pattern", "required")

    def __init__(self, pattern: str, required: bool | str):
        self.pattern: str = pattern
        self.required: bool | str = required

    def __eq__(self, other):
        if not isinstance(other, SecondaryFile):
            return False
        else:
            return self.pattern == other.pattern

    def __hash__(self):
        return hash(self.pattern)

    async def save(self, context: StreamFlowContext):
        return {"pattern": self.pattern, "required": self.required}


async def update_file_token(
    context: StreamFlowContext,
    connector: Connector,
    cwl_version: str,
    location: ExecutionLocation,
    token_value: MutableMapping[str, Any],
    load_contents: bool | None,
    load_listing: LoadListing | None = None,
):
    filepath = StreamFlowPath(
        get_path_from_token(token_value), context=context, location=location
    )
    # Process contents
    if get_token_class(token_value) == "File" and load_contents is not None:
        if load_contents and "contents" not in token_value:
            token_value |= {
                "contents": await _get_contents(
                    filepath,
                    token_value["size"],
                    cwl_version,
                )
            }
        elif not load_contents and "contents" in token_value:
            token_value = {k: token_value[k] for k in token_value if k != "contents"}
    # Process listings
    if get_token_class(token_value) == "Directory" and load_listing is not None:
        # If load listing is set to `no_listing`, remove the listing entries in present
        if load_listing == LoadListing.no_listing:
            if "listing" in token_value:
                token_value = {k: token_value[k] for k in token_value if k != "listing"}
        # If listing is not present or if the token needs a deep listing, process directory contents
        elif "listing" not in token_value or load_listing == LoadListing.deep_listing:
            token_value |= {
                "listing": await _get_listing(
                    context=context,
                    connector=connector,
                    cwl_version=cwl_version,
                    locations=[location],
                    dirpath=filepath,
                    load_contents=False,
                    recursive=load_listing == LoadListing.deep_listing,
                )
            }
        # If load listing is set to `shallow_listing`, remove the deep listing entries if present
        elif load_listing == LoadListing.shallow_listing:
            token_value |= {
                "listing": [
                    {k: v[k] for k in v if k != "listing"}
                    for v in token_value["listing"]
                ]
            }
    return token_value


async def write_remote_file(
    context: StreamFlowContext,
    locations: MutableSequence[ExecutionLocation],
    content: str,
    path: str,
    relpath: str,
):
    for location in locations:
        path = StreamFlowPath(path, context=context, location=location)
        if not await path.exists():
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "Creating {path} {location}".format(
                        path=str(path),
                        location=(
                            "on local file-system"
                            if location.local
                            else f"on location {location}"
                        ),
                    )
                )
            await path.write_text(content)
            await _register_path(
                context=context,
                connector=context.deployment_manager.get_connector(location.deployment),
                location=location,
                path=str(path),
                relpath=relpath,
            )
