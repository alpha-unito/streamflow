from __future__ import annotations

import asyncio
import json
import os
import urllib.parse
from enum import Enum, auto
from typing import Optional, Any, List, Union, MutableMapping, Set, cast, MutableSequence

from cwltool.utils import CONTENT_LIMIT
from typing_extensions import Text

from streamflow.core.data import FileType, LOCAL_RESOURCE, DataLocationType
from streamflow.core.deployment import Connector
from streamflow.core.exception import WorkflowExecutionException, WorkflowDefinitionException, \
    UnrecoverableTokenException
from streamflow.core.utils import get_path_processor, random_name, flatten_list, get_tag
from streamflow.core.workflow import Step, Port, InputPort, Job, Token, Status, TokenProcessor, CommandOutput
from streamflow.cwl import utils
from streamflow.cwl.command import CWLCommandOutput
from streamflow.cwl.utils import get_path_from_token
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.workflow.port import DefaultTokenProcessor, UnionTokenProcessor, MapTokenProcessor
from streamflow.workflow.step import BaseStep, BaseJob


async def _download_file(job: Job, url: Text) -> Text:
    connector = job.step.get_connector()
    resources = job.get_resources()
    try:
        return await remotepath.download(connector, resources, url, job.input_directory)
    except Exception:
        raise WorkflowExecutionException("Error downloading file from " + url)


async def _get_class_from_path(path: Text, job: Job) -> Text:
    connector = job.step.get_connector()
    for resource in (job.get_resources() or [None]) if job is not None else [None]:
        t_path = await remotepath.follow_symlink(connector, resource, path)
        return 'File' if await remotepath.isfile(connector, resource, t_path) else 'Directory'


async def _get_file_token(step: Step,
                          job: Job,
                          token_class: Text,
                          filepath: Text,
                          basename: Optional[Text] = None,
                          load_contents: bool = False,
                          load_listing: Optional[LoadListing] = None) -> MutableMapping[Text, Any]:
    connector = step.get_connector()
    resources = job.get_resources() or [None] if job is not None else [None]
    path_processor = get_path_processor(step)
    basename = basename or path_processor.basename(filepath)
    location = ''.join(['file://', filepath])
    token = {
        'class': token_class,
        'location': location,
        'basename': basename,
        'path': filepath,
        'dirname': path_processor.dirname(filepath)
    }
    if token_class == 'File':
        token['nameroot'], token['nameext'] = path_processor.splitext(basename)
        for resource in resources:
            if await remotepath.exists(connector, resource, filepath):
                token['size'] = await remotepath.size(connector, resource, filepath)
                if load_contents:
                    if token['size'] > CONTENT_LIMIT:
                        raise WorkflowExecutionException(
                            "Cannot read contents from files larger than {limit}kB".format(limit=CONTENT_LIMIT / 1024))
                    token['contents'] = await remotepath.head(
                        connector, resource, filepath, CONTENT_LIMIT)
                filepath = await remotepath.follow_symlink(connector, resource, filepath)
                token['checksum'] = 'sha1${checksum}'.format(
                    checksum=await remotepath.checksum(connector, resource, filepath))
                break
    elif token_class == 'Directory' and load_listing != LoadListing.no_listing:
        for resource in resources:
            if await remotepath.exists(connector, resource, filepath):
                token['listing'] = await _get_listing(
                    step, job, filepath, load_contents, load_listing == LoadListing.deep_listing)
                break
    return token


async def _get_listing(
        step: Step,
        job: Job,
        dirpath: Text,
        load_contents: bool,
        recursive: bool) -> MutableSequence[MutableMapping[Text, Any]]:
    listing_tokens = {}
    connector = step.get_connector()
    resources = job.get_resources() or [None]
    for resource in resources:
        directories = await remotepath.listdir(connector, resource, dirpath, FileType.DIRECTORY)
        for directory in directories:
            if directory not in listing_tokens:
                load_listing = LoadListing.deep_listing if recursive else LoadListing.no_listing
                listing_tokens[directory] = asyncio.create_task(_get_file_token(
                    step=step,
                    job=job,
                    token_class='Directory',
                    filepath=directory,
                    load_contents=load_contents,
                    load_listing=load_listing))
        files = await remotepath.listdir(connector, resource, dirpath, FileType.FILE)
        for file in files:
            if file not in listing_tokens:
                listing_tokens[file] = asyncio.create_task(_get_file_token(
                    step=step,
                    job=job,
                    token_class='File',
                    filepath=file,
                    load_contents=load_contents))
    return cast(MutableSequence[MutableMapping[Text, Any]], await asyncio.gather(*listing_tokens.values()))


def _get_paths(token_value: Any) -> MutableSequence[Text]:
    path = get_path_from_token(token_value)
    if path is not None:
        return [path]
    elif 'listing' in token_value:
        paths = []
        for listing in token_value['listing']:
            paths.extend(_get_paths(listing))
        return paths
    else:
        return []


async def _expand_glob(connector: Optional[Connector], resource: Optional[Text], path: Text) -> MutableSequence[Text]:
    return await remotepath.resolve(connector, resource, path) or []
    # paths = await remotepath.resolve(connector, resource, path) or []
    # follow_tasks = []
    # for path in paths:
    #    follow_tasks.append(remotepath.follow_symlink(connector, resource, path))
    # return await asyncio.gather(*follow_tasks)


class LoadListing(Enum):
    no_listing = auto()
    shallow_listing = auto()
    deep_listing = auto()


class SecondaryFile(object):
    __slots__ = ('pattern', 'required')

    def __init__(self,
                 pattern: Text,
                 required: bool):
        self.pattern: Text = pattern
        self.required: bool = required

    def __eq__(self, other):
        if not isinstance(other, SecondaryFile):
            return False
        else:
            return self.pattern == other.pattern

    def __hash__(self):
        return hash(self.pattern)


class CWLTokenProcessor(DefaultTokenProcessor):

    def __init__(self,
                 port: Port,
                 port_type: Text,
                 default_value: Optional[Any] = None,
                 expression_lib: Optional[MutableSequence[Text]] = None,
                 file_format: Optional[Text] = None,
                 full_js: bool = False,
                 glob: Optional[Text] = None,
                 load_contents: bool = False,
                 load_listing: LoadListing = LoadListing.no_listing,
                 optional: bool = False,
                 output_eval: Optional[Text] = None,
                 secondary_files: Optional[MutableSequence[SecondaryFile]] = None,
                 streamable: bool = False,
                 writable: bool = False):
        super().__init__(port)
        self.expression_lib: Optional[MutableSequence[Text]] = expression_lib
        self.full_js: bool = full_js
        self.glob: Optional[Text] = glob
        self.optional: bool = optional
        self.output_eval: Optional[Text] = output_eval
        self.port_type: Text = port_type
        self.default_value: Optional[Any] = default_value
        self.file_format: Optional[Text] = file_format
        self.load_contents: bool = load_contents
        self.load_listing: LoadListing = load_listing
        self.secondary_files: Optional[MutableSequence[SecondaryFile]] = secondary_files
        self.streamable: bool = streamable
        self.writable: bool = writable

    async def _build_token_value(self,
                                 job: Job,
                                 token_value: Any,
                                 load_contents: Optional[bool] = None,
                                 load_listing: Optional[LoadListing] = None) -> Any:
        if load_contents is None:
            load_contents = self.load_contents
        if token_value is None:
            return self.default_value
        elif isinstance(token_value, MutableSequence):
            value_tasks = []
            for t in token_value:
                value_tasks.append(asyncio.create_task(self._build_token_value(job, t, load_listing)))
            return await asyncio.gather(*value_tasks)
        elif (isinstance(token_value, MutableMapping)
              and token_value.get('class', token_value.get('type')) in ['File', 'Directory']):
            step = job.step if job is not None else self.port.step
            # Get filepath
            filepath = get_path_from_token(token_value)
            if filepath is not None:
                # Process secondary files in token value
                sf_map = {}
                if 'secondaryFiles' in token_value:
                    sf_tasks = []
                    for sf in token_value.get('secondaryFiles', []):
                        sf_path = get_path_from_token(sf)
                        path_processor = get_path_processor(step)
                        if not path_processor.isabs(sf_path):
                            path_processor.join(path_processor.dirname(filepath), sf_path)
                        sf_tasks.append(asyncio.create_task(_get_file_token(
                            step=step,
                            job=job,
                            token_class=sf['class'],
                            filepath=sf_path,
                            basename=sf.get('basename'),
                            load_contents=load_contents,
                            load_listing=load_listing or self.load_listing)))
                    sf_map = {get_path_from_token(sf): sf for sf in await asyncio.gather(*sf_tasks)}
                # Compute the new token value
                token_value = await _get_file_token(
                    step=step,
                    job=job,
                    token_class=token_value.get('class', token_value.get('type')),
                    filepath=filepath,
                    basename=token_value.get('basename'),
                    load_contents=load_contents,
                    load_listing=load_listing or self.load_listing)
                # Compute new secondary files from port specification
                if self.secondary_files:
                    context = utils.build_context(job)
                    context['self'] = token_value
                    sf_tasks, sf_specs = [], []
                    for secondary_file in self.secondary_files:
                        # If pattern is an expression, evaluate it and process result
                        if '$(' in secondary_file.pattern or '${' in secondary_file.pattern:
                            sf_value = utils.eval_expression(
                                expression=secondary_file.pattern,
                                context=context,
                                full_js=self.full_js,
                                expression_lib=self.expression_lib)
                            if isinstance(sf_value, MutableSequence):
                                for sf in sf_value:
                                    sf_tasks.append(asyncio.create_task(self._process_secondary_file(
                                        job=job,
                                        secondary_file=sf,
                                        token_value=token_value,
                                        from_expression=True,
                                        existing_sf=sf_map,
                                        load_contents=load_contents,
                                        load_listing=load_listing or self.load_listing)))
                                    sf_specs.append(secondary_file)
                            else:
                                sf_tasks.append(asyncio.create_task(self._process_secondary_file(
                                    job=job,
                                    secondary_file=sf_value,
                                    token_value=token_value,
                                    from_expression=True,
                                    existing_sf=sf_map,
                                    load_contents=load_contents,
                                    load_listing=load_listing or self.load_listing)))
                                sf_specs.append(secondary_file)
                        # Otherwise, simply process the pattern string
                        else:
                            sf_tasks.append(asyncio.create_task(self._process_secondary_file(
                                job=job,
                                secondary_file=secondary_file.pattern,
                                token_value=token_value,
                                from_expression=False,
                                existing_sf=sf_map,
                                load_contents=load_contents,
                                load_listing=load_listing or self.load_listing)))
                            sf_specs.append(secondary_file)
                    for sf_value, sf_spec in zip(await asyncio.gather(*sf_tasks), sf_specs):
                        if sf_value is not None:
                            sf_map[get_path_from_token(sf_value)] = sf_value
                        elif sf_spec.required:
                            raise WorkflowExecutionException(
                                "Required secondary file {sf} not found".format(sf=sf_spec.pattern))
                # Add all secondary files to the token
                if sf_map:
                    token_value['secondaryFiles'] = list(sf_map.values())
            # If there is only a 'contents' field, create a file on the step's resource and build the token
            elif 'contents' in token_value:
                path_processor = get_path_processor(self.port.step)
                filepath = path_processor.join(job.output_directory, token_value.get('basename', random_name()))
                connector = job.step.get_connector()
                resources = job.get_resources() or [None] if job is not None else [None]
                await asyncio.gather(*[asyncio.create_task(
                    remotepath.write(connector, res, filepath, token_value['contents'])) for res in resources])
                token_value = await _get_file_token(
                    step=step,
                    job=job,
                    token_class=token_value.get('class', token_value.get('type')),
                    filepath=filepath,
                    basename=token_value.get('basename'),
                    load_contents=load_contents,
                    load_listing=load_listing or self.load_listing)
        return token_value

    async def _get_value_from_command(self, job: Job, command_output: CWLCommandOutput):
        context = utils.build_context(job)
        path_processor = get_path_processor(self.port.step)
        connector = job.step.get_connector()
        resources = job.get_resources() or [None]
        token_value = command_output.value if command_output.value is not None else self.default_value
        # Check if file `cwl.output.json` exists either locally on at least one resource
        cwl_output_path = path_processor.join(job.output_directory, 'cwl.output.json')
        for resource in resources:
            if await remotepath.exists(connector, resource, cwl_output_path):
                # If file exists, use its contents as token value
                token_value = json.loads(await remotepath.read(connector, resource, cwl_output_path))
                break
        # If `token_value` is a dictionary, directly extract the token value from it
        if isinstance(token_value, MutableMapping) and self.port.name in token_value:
            token = token_value[self.port.name]
            return await self._build_token_value(job, token)
        # Otherwise, generate the output object as described in `outputs` field
        if self.glob is not None:
            # Adjust glob path
            if '$(' in self.glob or '${' in self.glob:
                globpath = utils.eval_expression(
                    expression=self.glob,
                    context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib)
            else:
                globpath = self.glob
            # Resolve glob
            resolve_tasks = []
            for resource in resources:
                if isinstance(globpath, MutableSequence):
                    for path in globpath:
                        if not path_processor.isabs(path):
                            path = path_processor.join(job.output_directory, path)
                        resolve_tasks.append(_expand_glob(connector, resource, path))
                else:
                    if not path_processor.isabs(globpath):
                        globpath = path_processor.join(job.output_directory, globpath)
                    resolve_tasks.append(_expand_glob(connector, resource, globpath))
            paths = flatten_list(await asyncio.gather(*resolve_tasks))
            # Cannot glob outside the job output folder
            for path in paths:
                if not path.startswith(job.output_directory):
                    raise WorkflowDefinitionException("Globs outside the job's output folder are not allowed")
            # Get token class from paths
            class_tasks = [asyncio.create_task(_get_class_from_path(p, job)) for p in paths]
            paths = [{'path': p, 'class': c} for p, c in zip(paths, await asyncio.gather(*class_tasks))]
            # If evaluation is not needed, simply return paths as token value
            if self.output_eval is None:
                token_list = await self._build_token_value(job, paths)
                return token_list if len(token_list) > 1 else token_list[0] if len(token_list) == 1 else None
            # Otherwise, fill context['self'] with glob data and proceed
            else:
                context['self'] = await self._build_token_value(job, paths)
        if self.output_eval is not None:
            # Fill context with exit code
            context['runtime']['exitCode'] = command_output.exit_code
            # Evaluate output
            token = utils.eval_expression(
                expression=self.output_eval,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib
            )
            # Build token
            if isinstance(token, MutableSequence):
                paths = [{'path': el['path'], 'class': el['class']} for el in token]
                return await self._build_token_value(job, paths)
            else:
                return await self._build_token_value(job, token)
        # As the default value (no return path is met in previous code), simply process the command output
        return await self._build_token_value(job, token_value)

    async def _process_secondary_file(self,
                                      job: Job,
                                      secondary_file: Any,
                                      token_value: MutableMapping[Text, Any],
                                      from_expression: bool,
                                      existing_sf: MutableMapping[Text, Any],
                                      load_contents: bool,
                                      load_listing: Optional[LoadListing]) -> Optional[MutableMapping[Text, Any]]:
        step = job.step if job is not None else self.port.step
        # If value is None, simply return None
        if secondary_file is None:
            return None
        # If value is a dictionary, simply append it to the list
        elif isinstance(secondary_file, MutableMapping):
            connector = job.step.get_connector()
            filepath = utils.get_path_from_token(secondary_file)
            for resource in job.get_resources() or [None]:
                if await remotepath.exists(connector, resource, filepath):
                    return await _get_file_token(
                        step=step,
                        job=job,
                        token_class=secondary_file['class'],
                        filepath=filepath,
                        basename=secondary_file.get('basename'),
                        load_contents=load_contents,
                        load_listing=load_listing)
        # If value is a string
        else:
            # If value doesn't come from an expression, apply it to the primary path
            filepath = (secondary_file if from_expression else
                        self._process_sf_path(secondary_file, utils.get_path_from_token(token_value)))
            path_processor = get_path_processor(step)
            if not path_processor.isabs(filepath):
                filepath = path_processor.join(path_processor.dirname(get_path_from_token(token_value)), filepath)
            if filepath not in existing_sf:
                # Search file in job resources and build token value
                connector = job.step.get_connector()
                for resource in job.get_resources() or [None]:
                    if await remotepath.exists(connector, resource, filepath):
                        token_class = 'File' if await remotepath.isfile(connector, resource, filepath) else 'Directory'
                        return await _get_file_token(
                            step=step,
                            job=job,
                            token_class=token_class,
                            filepath=filepath,
                            load_contents=load_contents,
                            load_listing=load_listing)
            else:
                return existing_sf[filepath]

    def _process_sf_path(self,
                         pattern: Text,
                         primary_path: Text) -> Text:
        if pattern.startswith('^'):
            path_processor = get_path_processor(self.port.step)
            return self._process_sf_path(pattern[1:], path_processor.splitext(primary_path)[0])
        else:
            return primary_path + pattern

    async def _recover_path(self,
                            job: Job,
                            resources: MutableSequence[Text],
                            token: Token,
                            path: Text) -> Optional[Text]:
        context = self.get_context()
        connector = self.port.step.get_connector()
        job_resources = job.get_resources() or [None]
        # Check if path is already present in actual job's resources
        for resource in job_resources:
            if await remotepath.exists(connector, resource, path):
                return path
        # Otherwise, get the list of other file locations from DataManager
        data_locations = set()
        for resource in resources:
            data_locations.update(context.data_manager.get_data_locations(resource, path, DataLocationType.PRIMARY))
        # Check if path is still present in original resources
        for location in data_locations:
            if location.resource in job_resources:
                if await remotepath.exists(connector, location.resource, path):
                    return path
                else:
                    context.data_manager.invalidate_location(location.resource, path)
        # Check if files are saved locally
        for location in data_locations:
            if location.resource == LOCAL_RESOURCE:
                return await self._transfer_file(None, job, location.path)
        # If not, check if files are stored elsewhere
        for location in data_locations:
            if location.resource not in job_resources and location.resource != LOCAL_RESOURCE:
                location_job = context.scheduler.get_job(location.job)
                location_connector = location_job.step.get_connector()
                available_resources = await location_connector.get_available_resources(
                    location_job.step.target.service)
                if (location.resource in available_resources and
                        await remotepath.exists(location_connector, location.resource, location.path)):
                    return await self._transfer_file(location_job, job, location.path)
                else:
                    context.data_manager.invalidate_location(location.resource, location.path)
        # If file has been lost, raise an exception
        message = "Failed to recover path {path} for token {token} from job {job}".format(
            path=path, token=token.name, job=token.job)
        logger.info(message)
        raise UnrecoverableTokenException(message, token)

    async def _recover_token(self,
                             job: Job,
                             resources: MutableSequence[Text],
                             token: Token) -> Token:
        if isinstance(token.value, MutableSequence):
            elements = []
            for t in token.value:
                elements.append(await self._recover_token_value(job, resources, token, t))
            return token.update(elements)
        else:
            return token.update(await self._recover_token_value(job, resources, token, token.value))

    async def _recover_token_value(self,
                                   job: Job,
                                   resources: MutableSequence[Text],
                                   token: Token,
                                   token_value: Any) -> Any:
        new_token_value = {'class': token_value['class']}
        if 'path' in token_value and token_value['path'] is not None:
            path = await self._recover_path(job, resources, token, token_value['path'])
            new_token_value['path'] = path[7:] if path.startswith('file://') else path
        elif 'location' in token_value and token_value['location'] is not None:
            path = await self._recover_path(job, resources, token, token_value['location'])
            new_token_value['location'] = path[7:] if path.startswith('file://') else path
        elif 'listing' in token_value:
            if 'basename' in token_value:
                new_token_value['basename'] = token_value['basename']
            new_token_value['listing'] = []
            for listing in token_value['listing']:
                path = listing['path'] if 'path' in listing else listing['location']
                path = path[7:] if path.startswith('file://') else path
                recovered_path = await self._recover_path(job, resources, token, path)
                new_token_value['listing'].append(await _get_file_token(
                    step=job.step,
                    job=job,
                    token_class=listing['class'],
                    filepath=recovered_path))
        secondary_files = []
        if 'secondaryFiles' in token_value:
            sf_tasks = [asyncio.create_task(self._recover_path(job, resources, token, get_path_from_token(sf))) for sf
                        in token_value['secondaryFiles']]
            secondary_files = [{'class': sf['class'], 'path': p}
                               for p, sf in zip(await asyncio.gather(*sf_tasks), token_value['secondaryFiles'])]
        token = await self._update_file_token(job, job, new_token_value)
        if secondary_files:
            token['secondaryFiles'] = secondary_files
        return token

    def _register_data(self,
                       job: Job,
                       token_value: Union[MutableSequence[MutableMapping[Text, Any]], MutableMapping[Text, Any]]):
        context = self.get_context()
        # If `token_value` is a list, process every item independently
        if isinstance(token_value, MutableSequence):
            for t in token_value:
                self._register_data(job, t)
        # Otherwise, if token value is a dictionary and it refers to a File or a Directory, register the path
        elif (isinstance(token_value, MutableMapping)
              and 'class' in token_value
              and token_value['class'] in ['File', 'Directory']):
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
            paths = [p[7:] if p.startswith('file://') else p for p in paths]
            # Register paths to the `DataManager`
            resources = job.get_resources() or [None]
            for path in paths:
                if resources:
                    for resource in resources or [None]:
                        context.data_manager.register_path(job, resource, path)

    async def _transfer_file(self,
                             src_job: Optional[Job],
                             dest_job: Optional[Job],
                             src_path: Text,
                             dest_path: Optional[Text] = None,
                             writable: Optional[bool] = None) -> Text:
        if dest_path is None:
            if isinstance(self.port, InputPort) and src_job is not None:
                if src_path.startswith(src_job.output_directory):
                    path_processor = get_path_processor(self.port.dependee.step)
                    relpath = path_processor.relpath(path_processor.normpath(src_path), src_job.output_directory)
                    path_processor = get_path_processor(self.port.step)
                    dest_path = path_processor.join(dest_job.input_directory, relpath)
                else:
                    path_processor = get_path_processor(self.port.dependee.step)
                    basename = path_processor.basename(path_processor.normpath(src_path))
                    path_processor = get_path_processor(self.port.step)
                    dest_path = path_processor.join(dest_job.input_directory, basename)
            else:
                path_processor = get_path_processor(self.port.step)
                dest_path = path_processor.join(
                    dest_job.input_directory,
                    os.path.basename(os.path.normpath(src_path)))
        await self.get_context().data_manager.transfer_data(
            src=src_path,
            src_job=src_job,
            dst=dest_path,
            dst_job=dest_job,
            writable=writable if writable is not None else self.writable)
        return dest_path

    async def _update_file_token(self,
                                 job: Job,
                                 src_job: Job,
                                 token_value: Any,
                                 load_listing: Optional[LoadListing] = None,
                                 writable: Optional[bool] = None) -> MutableMapping[Text, Any]:
        path_processor = get_path_processor(src_job.step) if src_job is not None else os.path
        if 'location' not in token_value and 'path' in token_value:
            token_value['location'] = token_value['path']
        if 'location' in token_value and token_value['location'] is not None:
            location = token_value['location']
            # Manage remote files
            scheme = urllib.parse.urlsplit(location).scheme
            if scheme in ['http', 'https']:
                location = await _download_file(job, location)
            elif scheme == 'file':
                location = location[7:]
            # If basename is explicitly stated in the token, use it as destination path
            dest_path = None
            if 'basename' in token_value:
                path_processor = get_path_processor(self.port.step)
                dest_path = path_processor.join(job.input_directory, token_value['basename'])
            # Check if source file exists
            src_connector = src_job.step.get_connector() if src_job is not None else None
            src_resources = src_job.get_resources() or [None] if src_job is not None else [None]
            src_found = False
            for src_resource in src_resources:
                if await remotepath.exists(src_connector, src_resource, location):
                    src_found = True
                    break
            # If source_path exists, ransfer file in task's input folder
            if src_found:
                filepath = await self._transfer_file(
                    src_job=src_job,
                    dest_job=job,
                    src_path=location,
                    dest_path=dest_path,
                    writable=writable)
            # Otherwise, keep the current path
            else:
                filepath = location
            new_token_value = {'class': token_value['class'], 'path': filepath}
            # If token contains secondary files, transfer them, too
            if 'secondaryFiles' in token_value:
                sf_tasks = []
                for sf in token_value['secondaryFiles']:
                    path = get_path_from_token(sf)
                    # If basename is explicitly stated in the token, use it as destination path
                    dest_path = None
                    if 'basename' in sf:
                        path_processor = get_path_processor(self.port.step)
                        dest_path = path_processor.join(job.input_directory, sf['basename'])
                    sf_tasks.append(asyncio.create_task(self._transfer_file(
                        src_job=src_job,
                        dest_job=job,
                        src_path=path,
                        dest_path=dest_path)))
                sf_paths = await asyncio.gather(*sf_tasks)
                new_token_value['secondaryFiles'] = [{'class': sf['class'], 'path': sf_path}
                                                     for sf, sf_path in zip(token_value['secondaryFiles'], sf_paths)]
            # Build token
            token_value = await self._build_token_value(
                job=job,
                token_value=new_token_value,
                load_contents=self.load_contents or 'contents' in token_value,
                load_listing=load_listing)
            return token_value
        # If there is only a 'contents' field, simply build the token value
        elif 'contents' in token_value:
            return await self._build_token_value(job, token_value, load_listing)
        # If there is only a 'listing' field, transfer all the listed files to the remote resource
        elif 'listing' in token_value:
            # Compute destination path
            dest_path = get_path_from_token(token_value)
            if dest_path is None and 'basename' in token_value:
                dest_path = path_processor.join(job.input_directory, token_value['basename'])
            # Copy each element of the listing into the destination folder
            tasks = []
            classes = []
            for element in cast(List, token_value['listing']):
                # Compute destination path
                if dest_path is not None:
                    basename = path_processor.basename(element['path'])
                    current_dest_path = path_processor.join(dest_path, basename)
                else:
                    current_dest_path = None
                # Transfer element to the remote resource
                tasks.append(asyncio.create_task(
                    self._transfer_file(
                        src_job=src_job,
                        dest_job=job,
                        src_path=element['path'],
                        dest_path=current_dest_path,
                        writable=writable)))
                classes.append(element['class'])
            dest_paths = await asyncio.gather(*tasks)
            # Compute listing on remote resource
            listing_tasks = []
            for token_class, path in zip(classes, dest_paths):
                listing_tasks.append(asyncio.create_task(_get_file_token(
                    step=self.port.step,
                    job=job,
                    token_class=token_class,
                    filepath=path)))
            token_value['listing'] = await asyncio.gather(*listing_tasks)
        return token_value

    async def collect_output(self, token: Token, output_dir: Text) -> Token:
        if isinstance(token.job, MutableSequence) or self.port_type not in ['File', 'Directory']:
            return await super().collect_output(token, output_dir)
        if token.value is not None and self.port_type in ['File', 'Directory']:
            context = self.get_context()
            output_collector = BaseJob(
                name=random_name(),
                step=BaseStep(name=random_name(), context=context),
                inputs=[],
                input_directory=output_dir)
            return token.update(await self._update_file_token(
                job=output_collector,
                src_job=context.scheduler.get_job(token.job),
                token_value=token.value,
                load_listing=LoadListing.deep_listing,
                writable=True))
        else:
            return token

    async def compute_token(self, job: Job, command_output: CWLCommandOutput) -> Any:
        if command_output.status == Status.SKIPPED:
            return None
        else:
            token_value = await self._get_value_from_command(job, command_output)
            self._register_data(job, token_value)
            weight = await self.weight_token(job, token_value)
            return Token(
                name=self.port.name,
                value=token_value,
                job=job.name,
                tag=get_tag(job.inputs),
                weight=weight)

    def get_related_resources(self, token: Token) -> Set[Text]:
        if self.port_type in ['File', 'Directory']:
            context = self.get_context()
            # If the token is actually an aggregate of multiple tokens, consider each token separately
            paths = []
            if isinstance(token.value, MutableSequence):
                for value in token.value:
                    if path := get_path_from_token(value) is not None:
                        paths.append(path)
            else:
                if path := get_path_from_token(token.value):
                    paths.append(path)
            resources = context.scheduler.get_job(token.job).get_resources()
            data_locations = set()
            for resource in resources:
                for path in paths:
                    data_locations.update(context.data_manager.get_data_locations(
                        resource, path, DataLocationType.PRIMARY))
            return set(loc.resource for loc in filter(lambda l: l.resource not in resources, data_locations))
        else:
            return set()

    async def recover_token(self, job: Job, resources: MutableSequence[Text], token: Token) -> Token:
        if isinstance(token.job, MutableSequence) or self.port_type not in ['File', 'Directory']:
            return await super().recover_token(job, resources, token)
        else:
            return await self._recover_token(job, resources, token)

    async def update_token(self, job: Job, token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().update_token(job, token)
        if token.value is None and self.default_value is not None:
            token = token.update(await self._build_token_value(job, self.default_value))
        if self.port_type == 'Any' or self.port_type is None:
            if (isinstance(self.port, InputPort) and
                    isinstance(self.port.dependee.token_processor, CWLTokenProcessor) and
                    self.port.dependee.token_processor.port_type != 'Any' and
                    self.port.dependee.token_processor.port_type is not None):
                self.port_type = self.port.dependee.token_processor.port_type
            else:
                self.port_type = utils.infer_type_from_token(token.value)
        if isinstance(token.value, MutableMapping) and token.value.get('class') in ['File', 'Directory']:
            context = self.get_context()
            src_job = context.scheduler.get_job(token.job)
            if isinstance(token.value, MutableSequence):
                elements = []
                for element in token.value:
                    elements.append(await self._update_file_token(job, src_job, element))
                return token.update(elements)
            elif token.value is not None:
                return token.update(await self._update_file_token(job, src_job, token.value))
            else:
                return token
        else:
            return token.update(await self._build_token_value(job, token.value))

    async def weight_token(self, job: Job, token_value: Any) -> int:
        if token_value is None or self.port_type not in ['File', 'Directory']:
            return 0
        elif isinstance(token_value, MutableSequence):
            return sum(await asyncio.gather(*[asyncio.create_task(self.weight_token(job, t)) for t in token_value]))
        elif 'size' in token_value:
            weight = token_value['size']
            if 'secondaryFiles' in token_value:
                sf_tasks = []
                for sf in token_value['secondaryFiles']:
                    sf_tasks.append(asyncio.create_task(self.weight_token(job, sf)))
                weight += sum(await asyncio.gather(*sf_tasks))
            return weight
        else:
            if job is not None and job.get_resources():
                connector = job.step.get_connector()
                for resource in job.get_resources():
                    return await remotepath.size(connector, resource, _get_paths(token_value))
                return 0
            else:
                return await remotepath.size(None, None, _get_paths(token_value)) if token_value is not None else 0


class CWLMapTokenProcessor(MapTokenProcessor):

    def __init__(self,
                 port: Port,
                 token_processor: TokenProcessor,
                 default_value: Optional[Any] = None,
                 optional: bool = False):
        super().__init__(port, token_processor)
        self.default_value: Optional[Any] = default_value
        self.optional: bool = optional

    async def compute_token(self, job: Job, command_output: CommandOutput) -> Token:
        if isinstance(command_output.value, MutableMapping) and self.port.name in command_output.value:
            command_output = command_output.update([{self.port.name: v} for v in command_output.value[self.port.name]])
        return await super().compute_token(job, command_output)

    async def update_token(self, job: Job, token: Token) -> Token:
        if token.value is None:
            token = token.update(self.default_value)
        return await super().update_token(job, token)


class CWLSkipTokenProcessor(DefaultTokenProcessor):

    async def compute_token(self, job: Job, command_output: CWLCommandOutput) -> Any:
        if command_output.status == Status.SKIPPED:
            return Token(
                name=self.port.name,
                value=None,
                job=job.name,
                tag=get_tag(job.inputs))
        else:
            return None


class CWLUnionTokenProcessor(UnionTokenProcessor):

    def __init__(self,
                 port: Port,
                 processors: MutableSequence[TokenProcessor],
                 default_value: Optional[Any] = None,
                 optional: bool = False):
        super().__init__(port, processors)
        self.default_value: Optional[Any] = default_value
        self.optional: bool = optional
        self.check_processor[CWLTokenProcessor] = self._check_cwl_processor
        self.check_processor[CWLMapTokenProcessor] = super()._check_map_processor
        self.check_processor[CWLUnionTokenProcessor] = super()._check_union_processor

    # noinspection PyMethodMayBeStatic
    def _check_cwl_processor(self, processor: CWLTokenProcessor, token_value: Any):
        return (processor.port_type == utils.infer_type_from_token(token_value) if token_value is not None
                else True)

    def get_processor(self, token_value: Any) -> TokenProcessor:
        if token_value is None:
            token_value = self.default_value
        if isinstance(token_value, MutableMapping) and self.port.name in token_value:
            token_value = token_value[self.port.name]
        return super().get_processor(token_value)
