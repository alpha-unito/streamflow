from __future__ import annotations

import asyncio
import urllib.parse
from enum import Enum
from typing import Any, MutableMapping, MutableSequence, Optional, Set, Tuple, Union, cast

import cwl_utils.expression
from cwltool.utils import CONTENT_LIMIT

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation, DataType, FileType, LOCAL_LOCATION
from streamflow.core.deployment import Connector
from streamflow.core.exception import WorkflowDefinitionException, WorkflowExecutionException
from streamflow.core.scheduling import Hardware
from streamflow.core.utils import get_path_processor, get_token_value, random_name
from streamflow.core.workflow import Job, Token, Workflow
from streamflow.cwl.expression import DependencyResolver
from streamflow.data import remotepath
from streamflow.log_handler import logger


async def _check_glob_path(connector: Connector,
                           workflow: Workflow,
                           location: Optional[str],
                           input_directory: str,
                           output_directory: str,
                           tmp_directory: str,
                           path: str,
                           effective_path: str) -> None:
    # Cannot glob outside the job output folder
    if not (effective_path.startswith(output_directory) or
            effective_path.startswith(input_directory) or
            effective_path.startswith(tmp_directory) or
            workflow.context.data_manager.get_data_locations(path=path)):
        path_processor = get_path_processor(connector)
        input_dirs = await remotepath.listdir(connector, location, input_directory, FileType.DIRECTORY)
        for input_dir in input_dirs:
            input_path = path_processor.join(input_dir, path_processor.relpath(path, output_directory))
            if await remotepath.exists(connector, location, input_path):
                return
        raise WorkflowDefinitionException("Globs outside the job's output folder are not allowed")


async def _process_secondary_file(context: StreamFlowContext,
                                  connector: Connector,
                                  locations: MutableSequence[str],
                                  secondary_file: Any,
                                  token_value: MutableMapping[str, Any],
                                  from_expression: bool,
                                  existing_sf: MutableMapping[str, Any],
                                  load_contents: bool,
                                  load_listing: Optional[LoadListing],
                                  only_retrieve_from_token: bool) -> Optional[MutableMapping[str, Any]]:
    # If value is None, simply return None
    if secondary_file is None:
        return None
    # If value is a dictionary, simply append it to the list
    elif isinstance(secondary_file, MutableMapping):
        filepath = get_path_from_token(secondary_file)
        for location in locations:
            if await remotepath.exists(connector, location, filepath):
                return await get_file_token(
                    context=context,
                    connector=connector,
                    locations=locations,
                    token_class=get_token_class(secondary_file),
                    filepath=filepath,
                    file_format=secondary_file.get('format'),
                    basename=secondary_file.get('basename'),
                    load_contents=load_contents,
                    load_listing=load_listing)
    # If value is a string
    else:
        # If value doesn't come from an expression, apply it to the primary path
        path_processor = get_path_processor(connector)
        filepath = (secondary_file if from_expression else _process_sf_path(
            path_processor, secondary_file, get_path_from_token(token_value)))
        if not path_processor.isabs(filepath):
            filepath = path_processor.join(path_processor.dirname(get_path_from_token(token_value)), filepath)
        if filepath not in existing_sf:
            # If must only retrieve elements from token, return None
            if only_retrieve_from_token:
                return None
            else:
                # Search file in job locations and build token value
                for location in locations:
                    if await remotepath.exists(connector, location, filepath):
                        token_class = 'File' if await remotepath.isfile(connector, location, filepath) else 'Directory'
                        return await get_file_token(
                            context=context,
                            connector=connector,
                            locations=locations,
                            token_class=token_class,
                            filepath=filepath,
                            load_contents=load_contents,
                            load_listing=load_listing)
        else:
            return existing_sf[filepath]


def _process_sf_path(path_processor,
                     pattern: str,
                     primary_path: str) -> str:
    if pattern.startswith('^'):
        return _process_sf_path(path_processor, pattern[1:], path_processor.splitext(primary_path)[0])
    else:
        return primary_path + pattern


async def _register_path(context: StreamFlowContext,
                         connector: Connector,
                         location: str,
                         path: str,
                         relpath: str,
                         data_type: DataType = DataType.PRIMARY) -> Optional[DataLocation]:
    if real_path := await remotepath.follow_symlink(connector, location, path):
        if real_path != path:
            if data_locations := context.data_manager.get_data_locations(
                    path=real_path,
                    deployment=connector.deployment_name):
                data_location = next(iter(data_locations))
            else:
                path_processor = get_path_processor(connector)
                base_path = path_processor.normpath(path[:-len(relpath)])
                if real_path.startswith(base_path):
                    data_location = context.data_manager.register_path(
                        deployment=connector.deployment_name,
                        location=location,
                        path=real_path,
                        relpath=path_processor.relpath(real_path, base_path))
                elif data_locations := await search_in_parent_locations(
                        context=context,
                        connector=connector,
                        path=real_path,
                        relpath=path_processor.basename(real_path)):
                    data_location = next(iter(data_locations))
                else:
                    raise WorkflowExecutionException("Error registering path {}".format(path))
            link_location = context.data_manager.register_path(
                deployment=connector.deployment_name,
                location=location,
                path=path,
                relpath=relpath,
                data_type=DataType.SYMBOLIC_LINK)
            context.data_manager.register_relation(data_location, link_location)
            return data_location
        else:
            return context.data_manager.register_path(
                deployment=connector.deployment_name,
                location=location,
                path=path,
                relpath=relpath,
                data_type=data_type)
    return None


def build_context(inputs: MutableMapping[str, Token],
                  output_directory: Optional[str] = None,
                  tmp_directory: Optional[str] = None,
                  hardware: Optional[Hardware] = None) -> MutableMapping[str, Any]:
    context = {
        'inputs': {},
        'self': None,
        'runtime': {}
    }
    for name, token in inputs.items():
        context['inputs'][name] = get_token_value(token)
    if output_directory:
        context['runtime']['outdir'] = output_directory
    if tmp_directory:
        context['runtime']['tmpdir'] = tmp_directory
    if hardware:
        context['runtime']['cores'] = hardware.cores
        context['runtime']['ram'] = hardware.memory
        # noinspection PyUnresolvedReferences
        context['runtime']['tmpdirSize'] = hardware.tmp_directory
        # noinspection PyUnresolvedReferences
        context['runtime']['outdirSize'] = hardware.output_directory
    return context


async def build_token_value(context: StreamFlowContext,
                            js_context: MutableMapping[str, Any],
                            full_js: bool,
                            expression_lib: Optional[MutableSequence[str]],
                            secondary_files: Optional[MutableSequence[SecondaryFile]],
                            connector: Connector,
                            locations: MutableSequence[str],
                            token_value: Any,
                            load_contents: bool,
                            load_listing: LoadListing) -> Any:
    if isinstance(token_value, MutableSequence):
        value_tasks = []
        for t in token_value:
            value_tasks.append(asyncio.create_task(build_token_value(
                context=context,
                js_context=js_context,
                full_js=full_js,
                expression_lib=expression_lib,
                secondary_files=secondary_files,
                connector=connector,
                locations=locations,
                token_value=t,
                load_contents=load_contents,
                load_listing=load_listing)))
        return await asyncio.gather(*value_tasks)
    elif isinstance(token_value, MutableMapping) and (
            token_class := get_token_class(token_value)) in ['File', 'Directory']:
        path_processor = get_path_processor(connector)
        # Get filepath
        filepath = get_path_from_token(token_value)
        if filepath is not None:
            # Process secondary files in token value
            sf_map = {}
            if 'secondaryFiles' in token_value:
                sf_tasks = []
                for sf in token_value.get('secondaryFiles', []):
                    sf_path = get_path_from_token(sf)
                    if not path_processor.isabs(sf_path):
                        path_processor.join(path_processor.dirname(filepath), sf_path)
                    sf_tasks.append(asyncio.create_task(get_file_token(
                        context=context,
                        connector=connector,
                        locations=locations,
                        token_class=get_token_class(sf),
                        filepath=sf_path,
                        file_format=sf.get('format'),
                        basename=sf.get('basename'),
                        load_contents=load_contents,
                        load_listing=load_listing)))
                sf_map = {get_path_from_token(sf): sf for sf in await asyncio.gather(*sf_tasks)}
            # Compute the new token value
            token_value = await get_file_token(
                context=context,
                connector=connector,
                locations=locations,
                token_class=token_class,
                filepath=filepath,
                file_format=token_value.get('format'),
                basename=token_value.get('basename'),
                load_contents=load_contents,
                load_listing=load_listing)
            # Compute new secondary files from port specification
            sf_context = {**js_context, **{'self': token_value}}
            if secondary_files:
                await process_secondary_files(
                    context=context,
                    secondary_files=secondary_files,
                    sf_map=sf_map,
                    js_context=sf_context,
                    full_js=full_js,
                    expression_lib=expression_lib,
                    connector=connector,
                    locations=locations,
                    token_value=token_value,
                    load_contents=load_contents,
                    load_listing=load_listing)
                # Add all secondary files to the token
                if sf_map:
                    token_value['secondaryFiles'] = list(sf_map.values())
        # If there is only a 'contents' field, propagate the parameter
        elif 'contents' in token_value:
            filepath = path_processor.join(js_context['runtime']['outdir'], token_value.get('basename', random_name()))
            contents = token_value['contents']
            token_value = await get_file_token(
                context=context,
                connector=connector,
                locations=locations,
                token_class=token_class,
                filepath=filepath,
                file_format=token_value.get('format'),
                basename=token_value.get('basename'),
                load_listing=load_listing)
            token_value['contents'] = contents
        # If there is only a 'listing' field, build a folder token and process all the listing entries recursively
        elif 'listing' in token_value:
            filepath = js_context['runtime']['outdir']
            if 'basename' in token_value:
                filepath = path_processor.join(filepath, token_value['basename'])
            # When `shallow_listing` is set, stop propagation
            must_load_listing = (LoadListing.no_listing if load_listing == LoadListing.shallow_listing
                                 else load_listing)
            # Build listing tokens
            listing_tokens = await asyncio.gather(*(
                build_token_value(
                    context=context,
                    js_context=js_context,
                    full_js=full_js,
                    expression_lib=expression_lib,
                    secondary_files=secondary_files,
                    connector=connector,
                    locations=locations,
                    token_value=lst,
                    load_contents=load_contents,
                    load_listing=must_load_listing
                ) for lst in token_value['listing']))
            token_value = await get_file_token(
                context=context,
                connector=connector,
                locations=locations,
                token_class=token_class,
                filepath=filepath,
                file_format=token_value.get('format'),
                basename=token_value.get('basename'),
                load_contents=load_contents,
                load_listing=load_listing)
            token_value['listing'] = listing_tokens
    return token_value


def check_token_type(name: str,
                     token_value: Any,
                     token_type: str,
                     enum_symbols: Optional[MutableSequence[str]],
                     optional: bool):
    if isinstance(token_value, Token):
        check_token_type(name, token_value.value, token_type, enum_symbols, optional)
    elif token_type == 'Any':
        if token_value is None:
            raise WorkflowExecutionException('Token {} is of type `Any`: it cannot be null.'.format(name))
    elif token_type == 'null':
        if token_value is not None:
            raise WorkflowExecutionException('Token {} should be null.'.format(name))
    elif token_value is None:
        if not optional:
            raise WorkflowExecutionException('Token {} is not optional.'.format(name))
    elif token_type == 'enum':
        if token_value not in enum_symbols:
            raise WorkflowExecutionException('Value {} is not valid for token {}.'.format(token_value, name))
    elif (inferred_type := infer_type_from_token(token_value)) != token_type:
        # In CWL, long is considered as a subtype of double
        if inferred_type != 'long' or token_type != 'double':
            raise WorkflowExecutionException('Expected {} token of type {}, got {}.'.format(
                name, token_type, inferred_type))


def eval_expression(expression: str,
                    context: MutableMapping[str, Any],
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[str]] = None,
                    timeout: Optional[int] = None,
                    strip_whitespace: bool = True) -> Any:
    if isinstance(expression, str) and ('$(' in expression or '${' in expression):
        return cwl_utils.expression.interpolate(
            expression,
            context,
            jslib=cwl_utils.expression.jshead(expression_lib or [], context) if full_js else "",
            fullJS=full_js,
            strip_whitespace=strip_whitespace,
            timeout=timeout)
    else:
        return expression


async def expand_glob(connector: Connector,
                      workflow: Workflow,
                      location: Optional[str],
                      input_directory: str,
                      output_directory: str,
                      tmp_directory: str,
                      path: str) -> MutableSequence[Tuple[str, str]]:
    paths = await remotepath.resolve(connector, location, path) or []
    effective_paths = await asyncio.gather(*(asyncio.create_task(
        remotepath.follow_symlink(connector, location, p)
    ) for p in paths))
    await asyncio.gather(*(asyncio.create_task(
        _check_glob_path(
            connector=connector,
            workflow=workflow,
            location=location,
            input_directory=input_directory,
            output_directory=output_directory,
            tmp_directory=tmp_directory,
            path=p,
            effective_path=ep or p)
    ) for p, ep in zip(paths, effective_paths)))
    return [(p, (ep or p)) for p, ep in zip(paths, effective_paths)]


async def get_class_from_path(path: str, job: Job, context: StreamFlowContext) -> str:
    connector = context.scheduler.get_connector(job.name)
    locations = context.scheduler.get_locations(job.name)
    for location in locations:
        return 'File' if await remotepath.isfile(connector, location, path) else 'Directory'


async def get_file_token(
        context: StreamFlowContext,
        connector: Connector,
        locations: MutableSequence[str],
        token_class: str,
        filepath: str,
        file_format: Optional[str] = None,
        basename: Optional[str] = None,
        load_contents: bool = False,
        load_listing: Optional[LoadListing] = None) -> MutableMapping[str, Any]:
    path_processor = get_path_processor(connector)
    basename = basename or path_processor.basename(filepath)
    location = ''.join(['file://', urllib.parse.quote(filepath)])
    token = {
        'class': token_class,
        'location': location,
        'basename': basename,
        'path': filepath,
        'dirname': path_processor.dirname(filepath)
    }
    if token_class == 'File':
        if file_format:
            token['format'] = file_format
        token['nameroot'], token['nameext'] = path_processor.splitext(basename)
        for location in locations:
            if real_path := await remotepath.follow_symlink(connector, location, filepath):
                token['size'] = await remotepath.size(connector, location, real_path)
                if load_contents:
                    if token['size'] > CONTENT_LIMIT:
                        raise WorkflowExecutionException(
                            "Cannot read contents from files larger than {limit}kB".format(limit=CONTENT_LIMIT / 1024))
                    token['contents'] = await remotepath.head(
                        connector, location, real_path, CONTENT_LIMIT)
                token['checksum'] = 'sha1${checksum}'.format(
                    checksum=await remotepath.checksum(context, connector, location, real_path))
                break
    elif token_class == 'Directory' and load_listing != LoadListing.no_listing:
        for location in locations:
            if await remotepath.exists(connector, location, filepath):
                token['listing'] = await get_listing(
                    context=context,
                    connector=connector,
                    locations=locations,
                    dirpath=filepath,
                    load_contents=load_contents,
                    recursive=load_listing == LoadListing.deep_listing)
                break
    return token


async def get_listing(
        context: StreamFlowContext,
        connector: Connector,
        locations: MutableSequence[str],
        dirpath: str,
        load_contents: bool,
        recursive: bool) -> MutableSequence[MutableMapping[str, Any]]:
    listing_tokens = {}
    for location in locations:
        directories = await remotepath.listdir(connector, location, dirpath, FileType.DIRECTORY)
        for directory in directories:
            if directory not in listing_tokens:
                load_listing = LoadListing.deep_listing if recursive else LoadListing.no_listing
                listing_tokens[directory] = asyncio.create_task(get_file_token(
                    context=context,
                    connector=connector,
                    locations=locations,
                    token_class='Directory',
                    filepath=directory,
                    load_contents=load_contents,
                    load_listing=load_listing))
        files = await remotepath.listdir(connector, location, dirpath, FileType.FILE)
        for file in files:
            if file not in listing_tokens:
                listing_tokens[file] = asyncio.create_task(get_file_token(
                    context=context,
                    connector=connector,
                    locations=locations,
                    token_class='File',
                    filepath=file,
                    load_contents=load_contents))
    return cast(MutableSequence[MutableMapping[str, Any]], await asyncio.gather(*listing_tokens.values()))


def get_path_from_token(token_value: MutableMapping[str, Any]) -> Optional[str]:
    location = token_value.get('location', token_value.get('path'))
    if location and '://' in location:
        scheme = urllib.parse.urlsplit(location).scheme
        return urllib.parse.unquote(location[7:]) if scheme == 'file' else None
    return location


def get_token_class(token_value: Any) -> Optional[str]:
    if isinstance(token_value, MutableMapping):
        return token_value.get('class', token_value.get('type'))
    else:
        return None


def infer_type_from_token(token_value: Any) -> str:
    if isinstance(token_value, MutableMapping):
        return get_token_class(token_value) or 'record'
    elif isinstance(token_value, MutableSequence):
        return 'array'
    elif isinstance(token_value, str):
        return 'string'
    elif isinstance(token_value, bool):
        return 'boolean'
    elif isinstance(token_value, int):
        return 'long'
    elif isinstance(token_value, float):
        return 'double'
    else:
        # Could not infer token type: mark as Any
        return 'Any'


class LoadListing(Enum):
    no_listing = 0
    shallow_listing = 1
    deep_listing = 2


async def process_secondary_files(context: StreamFlowContext,
                                  secondary_files: MutableSequence[SecondaryFile],
                                  sf_map: MutableMapping[str, Any],
                                  js_context: MutableMapping[str, Any],
                                  full_js: bool,
                                  expression_lib: Optional[MutableSequence[str]],
                                  connector: Connector,
                                  locations: MutableSequence[str],
                                  token_value: Any,
                                  load_contents: Optional[bool] = None,
                                  load_listing: Optional[LoadListing] = None,
                                  only_retrieve_from_token: bool = False) -> None:
    sf_tasks, sf_specs = [], []
    for secondary_file in secondary_files:
        # If pattern is an expression, evaluate it and process result
        if '$(' in secondary_file.pattern or '${' in secondary_file.pattern:
            sf_value = eval_expression(
                expression=secondary_file.pattern,
                context=js_context,
                full_js=full_js,
                expression_lib=expression_lib)
            # If the expression explicitly returns None, do not process this entry
            if sf_value is None:
                continue
            elif isinstance(sf_value, MutableSequence):
                for sf in sf_value:
                    sf_tasks.append(asyncio.create_task(_process_secondary_file(
                        context=context,
                        connector=connector,
                        locations=locations,
                        secondary_file=sf,
                        token_value=token_value,
                        from_expression=True,
                        existing_sf=sf_map,
                        load_contents=load_contents,
                        load_listing=load_listing,
                        only_retrieve_from_token=only_retrieve_from_token)))
                    sf_specs.append(secondary_file)
            else:
                sf_tasks.append(asyncio.create_task(_process_secondary_file(
                    context=context,
                    connector=connector,
                    locations=locations,
                    secondary_file=sf_value,
                    token_value=token_value,
                    from_expression=True,
                    existing_sf=sf_map,
                    load_contents=load_contents,
                    load_listing=load_listing,
                    only_retrieve_from_token=only_retrieve_from_token)))
                sf_specs.append(secondary_file)
        # Otherwise, simply process the pattern string
        else:
            sf_tasks.append(asyncio.create_task(_process_secondary_file(
                context=context,
                connector=connector,
                locations=locations,
                secondary_file=secondary_file.pattern,
                token_value=token_value,
                from_expression=False,
                existing_sf=sf_map,
                load_contents=load_contents,
                load_listing=load_listing,
                only_retrieve_from_token=only_retrieve_from_token)))
            sf_specs.append(secondary_file)
    for sf_value, sf_spec in zip(await asyncio.gather(*sf_tasks), sf_specs):
        if sf_value is not None:
            sf_map[get_path_from_token(cast(MutableMapping[str, Any], sf_value))] = sf_value
        else:
            required = eval_expression(
                expression=sf_spec.required,
                context=js_context,
                full_js=full_js,
                expression_lib=expression_lib)
            if required:
                raise WorkflowExecutionException(
                    "Required secondary file {sf} not found".format(sf=sf_spec.pattern))


async def register_data(context: StreamFlowContext,
                        connector: Connector,
                        locations: MutableSequence[str],
                        base_path: Optional[str],
                        token_value: Union[MutableSequence[MutableMapping[str, Any]], MutableMapping[str, Any]]):
    # If `token_value` is a list, process every item independently
    if isinstance(token_value, MutableSequence):
        await asyncio.gather(*(asyncio.create_task(
            register_data(context, connector, locations, base_path, t)) for t in token_value))
    # Otherwise, if token value is a dictionary and it refers to a File or a Directory, register the path
    elif get_token_class(token_value) in ['File', 'Directory']:
        path_processor = get_path_processor(connector)
        # Extract paths from token
        paths = []
        if 'path' in token_value and token_value['path'] is not None:
            paths.append(token_value['path'])
        elif 'location' in token_value and token_value['location'] is not None:
            paths.append(token_value['location'])
        elif 'listing' in token_value:
            paths.extend([t['path'] if 'path' in t else t['location'] for t in token_value['listing']])
        if 'secondaryFiles' in token_value:
            for sf in token_value['secondaryFiles']:
                paths.append(get_path_from_token(sf))
        # Remove `file` protocol if present
        paths = [urllib.parse.unquote(p[7:]) if p.startswith('file://') else p for p in paths]
        # Register paths to the `DataManager`
        for path in (path_processor.normpath(p) for p in paths):
            relpath = (path_processor.relpath(path, base_path) if base_path and path.startswith(base_path)
                       else path_processor.basename(path))
            await asyncio.gather(*(asyncio.create_task(
                _register_path(
                    context=context,
                    connector=connector,
                    location=location,
                    path=path,
                    relpath=relpath)
            ) for location in locations))


def resolve_dependencies(expression: str,
                         full_js: bool = False,
                         expression_lib: Optional[MutableSequence[str]] = None,
                         timeout: Optional[int] = None,
                         strip_whitespace: bool = True) -> Set[str]:
    if isinstance(expression, str) and ('$(' in expression or '${' in expression):
        context = {'inputs': {}, 'self': {}, 'runtime': {}}
        engine = DependencyResolver()
        cwl_utils.expression.interpolate(
            expression,
            context,
            jslib=cwl_utils.expression.jshead(expression_lib or [], context) if full_js else "",
            fullJS=full_js,
            strip_whitespace=strip_whitespace,
            timeout=timeout,
            resolve_dependencies=True,
            js_engine=engine)
        return engine.deps
    else:
        return set()


async def search_in_parent_locations(context: StreamFlowContext,
                                     connector: Connector,
                                     path: str,
                                     relpath: str,
                                     base_path: Optional[str] = None) -> Set[DataLocation]:
    path_processor = get_path_processor(connector)
    current_path = path
    while current_path != (base_path or path_processor.sep):
        # Retrieve all data locations
        if data_locations := context.data_manager.get_data_locations(path=current_path):
            # If there is no data location for the exact source path
            actual_locations = set()
            if current_path != path:
                # Add source path to all the involved locations
                previous_location = None
                for data_location in sorted(data_locations, key=lambda l: 0 if l.data_type == DataType.PRIMARY else 1):
                    data_path = path if data_location.path.startswith(current_path) else path_processor.join(
                        path_processor.normpath(data_location.path[:-len(data_location.relpath)]),
                        relpath)
                    data_connector = context.deployment_manager.get_connector(
                        data_location.deployment)
                    if current_location := await _register_path(
                            context=context,
                            connector=data_connector,
                            location=data_location.location,
                            path=data_path,
                            relpath=relpath,
                            data_type=data_location.data_type):
                        actual_locations.add(current_location)
                        if previous_location is not None:
                            context.data_manager.register_relation(previous_location, current_location)
                        previous_location = current_location
                if not actual_locations:
                    raise WorkflowExecutionException("Error registering path {}".format(path))
            return actual_locations
        path_tokens = [path_processor.sep]
        path_tokens.extend(current_path.lstrip(path_processor.sep).split(path_processor.sep)[:-1])
        current_path = path_processor.normpath(path_processor.join(*path_tokens))


class SecondaryFile(object):
    __slots__ = ('pattern', 'required')

    def __init__(self,
                 pattern: str,
                 required: Union[bool, str]):
        self.pattern: str = pattern
        self.required: Union[bool, str] = required

    def __eq__(self, other):
        if not isinstance(other, SecondaryFile):
            return False
        else:
            return self.pattern == other.pattern

    def __hash__(self):
        return hash(self.pattern)

    async def save(self, context: StreamFlowContext):
        return {'pattern': self.pattern, 'required': self.required}


async def update_file_token(context: StreamFlowContext,
                            connector: Connector,
                            location: str,
                            token_value: MutableMapping[str, Any],
                            load_contents: Optional[bool],
                            load_listing: Optional[LoadListing] = None):
    filepath = get_path_from_token(token_value)
    if load_contents is not None:
        if load_contents and 'contents' not in token_value:
            if token_value['size'] > CONTENT_LIMIT:
                raise WorkflowExecutionException(
                    "Cannot read contents from files larger than {}kB".format(round(CONTENT_LIMIT / 1024)))
            token_value = {**token_value, **{
                'contents': await remotepath.head(
                    connector, location, filepath, CONTENT_LIMIT)}}
        elif not load_contents and 'contents' in token_value:
            token_value = {k: token_value[k] for k in token_value if k != 'contents'}
    # Process listings
    if get_token_class(token_value) == 'Directory' and load_listing is not None:
        # If load listing is set to `no_listing`, remove the listing entries in present
        if load_listing == LoadListing.no_listing:
            if 'listing' in token_value:
                token_value = {k: token_value[k] for k in token_value if k != 'listing'}
        # If listing is not present or if the token needs a deep listing, process directory contents
        elif 'listing' not in token_value or load_listing == LoadListing.deep_listing:
            token_value = {**token_value, **{
                'listing': await get_listing(
                    context=context,
                    connector=connector,
                    locations=[location],
                    dirpath=filepath,
                    load_contents=False,
                    recursive=load_listing == LoadListing.deep_listing)}}
        # If load listing is set to `shallow_listing`, remove the deep listing entries if present
        elif load_listing == LoadListing.shallow_listing:
            token_value = {**token_value, **{'listing': [{k: v[k] for k in v if k != 'listing'}
                                                         for v in token_value['listing']]}}
    return token_value


async def write_remote_file(context: StreamFlowContext,
                            job: Job,
                            content: str,
                            path: str):
    connector = context.scheduler.get_connector(job.name)
    locations = context.scheduler.get_locations(job.name)
    path_processor = get_path_processor(connector)
    for location in locations:
        if not await remotepath.exists(connector, location, path):
            logger.info("Creating {path} {location}".format(
                path=path,
                location=("on local file-system" if location == LOCAL_LOCATION else
                          "on location {res}".format(res=location))))
            await remotepath.write(connector, location, path, content)
            context.data_manager.register_path(
                deployment=connector.deployment_name,
                location=location,
                path=path,
                relpath=path_processor.relpath(path_processor.normpath(path), job.output_directory))
