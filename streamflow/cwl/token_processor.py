from __future__ import annotations

import asyncio
import os
import urllib.parse
from abc import abstractmethod
from enum import Enum
from typing import Optional, Any, List, Union, MutableMapping, Iterable

import cwltool.expression
from cwltool.utils import CONTENT_LIMIT
from typing_extensions import Text

from streamflow.core.data import FileType
from streamflow.core.scheduling import JobStatus
from streamflow.core.workflow import Task, Port, InputPort, Job, Token
from streamflow.cwl import utils
from streamflow.data import remotepath
from streamflow.workflow.exception import WorkflowExecutionException
from streamflow.workflow.port import DefaultTokenProcessor


async def _download_file(job: Job, url: Text) -> Text:
    connector = job.task.get_connector()
    resources = job.get_resources()
    try:
        filepath = await remotepath.download(connector, resources, url, job.input_directory)
    except Exception:
        raise WorkflowExecutionException("Error downloading file from " + url)
    return ''.join(['file://', filepath])


async def _get_listing(job: Job, dirpath: Text, recursive: bool) -> List[MutableMapping[Text, Any]]:
    connector = job.task.get_connector()
    resource = job.get_resource()
    listing_tokens = []
    if job.task.target is not None:
        directories = await remotepath.listdir(connector, resource, dirpath, recursive, FileType.DIRECTORY)
        for directory in directories:
            listing_tokens.append(_get_token(job.task, 'Directory', directory))
        files = await remotepath.listdir(connector, resource, dirpath, recursive, FileType.FILE)
        for file in files:
            listing_tokens.append(_get_token(job.task, 'File', file))
    else:
        content = await remotepath.listdir(connector, resource, dirpath, recursive, FileType.DIRECTORY)
        for element in content:
            is_directory = await remotepath.isdir(connector, resource, element)
            token_class = 'Directory' if is_directory else 'File'
            listing_tokens.append(_get_token(job.task, token_class, element))
    return listing_tokens


def _get_paths(token_value: Any) -> List[Text]:
    if isinstance(token_value, List):
        paths = []
        for t in token_value:
            paths.extend(_get_paths(t))
        return paths
    else:
        if 'path' in token_value:
            return [token_value['path']]
        elif 'listing' in token_value:
            paths = []
            for listing in token_value['listing']:
                paths.extend(_get_paths(listing))
            return paths
        else:
            return []


def _get_token(task: Task, token_class: Text, filepath: Text) -> MutableMapping[Text, Any]:
    path_processor = utils.get_path_processor(task)
    location = ''.join(['file://', filepath])
    basename = path_processor.basename(filepath)
    token = {
        'class': token_class,
        'location': location,
        'basename': basename,
        'path': filepath,
        'dirname': path_processor.dirname(filepath)
    }
    if token_class == 'File':
        token['nameroot'], token['nameext'] = path_processor.splitext(basename)
    return token


class LoadListing(Enum):
    no_listing = 1
    shallow_listing = 2
    deep_listing = 3


class SecondaryFile(object):

    def __init__(self,
                 pattern: Text,
                 required: bool):
        self.pattern: Text = pattern
        self.required: bool = required


class CWLTokenProcessor(DefaultTokenProcessor):

    def __init__(self,
                 port: Port,
                 expression_lib: Optional[List[Text]] = None,
                 full_js: bool = False,
                 glob: Optional[Text] = None,
                 is_array: bool = False,
                 output_eval: Optional[Text] = None):
        super().__init__(port)
        self.expression_lib: Optional[List[Text]] = expression_lib
        self.full_js: bool = full_js
        self.glob: Optional[Text] = glob
        self.is_array: bool = is_array
        self.output_eval: Optional[Text] = output_eval

    @abstractmethod
    async def _build_token_value(self, job: Job, token_value: Any) -> Any:
        pass

    async def _get_tokens_from_paths(self, job: Job, paths: List[Text]) -> List[Any]:
        tasks_list = []
        for path in paths:
            tasks_list.append(asyncio.create_task(self._build_token_value(job, path)))
        return await asyncio.gather(*tasks_list)

    async def compute_token(self, job: Job, result: Any, status: JobStatus) -> Any:
        if status == JobStatus.SKIPPED:
            return Token(name=self.port.name, value='null', job=job.name, weight=0)
        context = None
        if self.output_eval is not None:
            context = utils.build_context(job)
            context['runtime']['exitCode'] = result
        if self.glob is not None:
            # Adjust glob path
            path_processor = utils.get_path_processor(self.port.task)
            if '$(' in self.glob or '${' in self.glob:
                context = utils.build_context(job)
                globpath = cwltool.expression.interpolate(
                    self.glob,
                    context,
                    jslib=cwltool.expression.jshead(
                        self.expression_lib or [], context) if self.full_js else "",
                    fullJS=self.full_js)
            else:
                globpath = self.glob
            if not path_processor.isabs(globpath):
                globpath = path_processor.join(job.output_directory, globpath)
            # Resolve glob
            connector = self.port.task.get_connector()
            resource = job.get_resource()
            if isinstance(globpath, List):
                paths = []
                for path in globpath:
                    paths.extend((await remotepath.resolve(connector, resource, path)) or [])
            else:
                paths = (await remotepath.resolve(connector, resource, globpath)) or []
            if self.output_eval is None:
                # Build token
                token_list = await self._get_tokens_from_paths(job, paths)
                if self.is_array:
                    weight = await self.weight_token(job, token_list)
                    return Token(name=self.port.name, value=token_list, job=job.name, weight=weight)
                else:
                    weight = await self.weight_token(job, token_list[0])
                    return Token(name=self.port.name, value=token_list[0], job=job.name, weight=weight)
            else:
                # Fill context['self'] with glob data
                token_list = await self._get_tokens_from_paths(job, paths)
                context['self'] = token_list
        if self.output_eval is not None:
            # Evaluate output
            token = cwltool.expression.interpolate(
                self.output_eval,
                context,
                jslib=cwltool.expression.jshead(
                    self.expression_lib or [], context) if self.full_js else "",
                fullJS=self.full_js)
            # Build token
            if isinstance(token, List):
                paths = []
                for element in token:
                    paths.append(element['path'])
                token_value = await self._get_tokens_from_paths(job, paths)
                weight = await self.weight_token(job, token_value)
                return Token(name=self.port.name, value=token_value, job=job.name, weight=weight)
            else:
                token_value = await self._build_token_value(job, token['path'])
                weight = await self.weight_token(job, token_value)
                return Token(name=self.port.name, value=token_value, job=job.name, weight=weight)
        if isinstance(result, MutableMapping):
            # Extract output directly from command result
            token = result[self.port.name]
            # Build token
            if isinstance(token, List):
                token_value = await self._get_tokens_from_paths(job, token)
                weight = await self.weight_token(job, token_value)
                return Token(name=self.port.name, value=token_value, job=job.name, weight=weight)
            else:
                token_value = await self._build_token_value(job, result[self.port.name])
                weight = await self.weight_token(job, token_value)
                return Token(name=self.port.name, value=token_value, job=job.name, weight=weight)


class CWLFileProcessor(CWLTokenProcessor):

    def __init__(self,
                 port: Port,
                 file_type: FileType,
                 is_array: bool = False,
                 default_path: Optional[Union[Text, List[Text]]] = None,
                 expression_lib: Optional[List[Text]] = None,
                 file_format: Optional[Text] = None,
                 full_js: bool = False,
                 glob: Optional[Text] = None,
                 load_contents: bool = False,
                 load_listing: LoadListing = LoadListing.no_listing,
                 output_eval: Optional[Text] = None,
                 secondary_files: Optional[List[SecondaryFile]] = None,
                 streamable: bool = False,
                 writable: bool = False):
        super().__init__(
            port=port,
            expression_lib=expression_lib,
            full_js=full_js,
            glob=glob,
            is_array=is_array,
            output_eval=output_eval)
        self.file_type: FileType = file_type
        self.default_path: Optional[Union[Text, List[Text]]] = default_path
        self.file_format: Optional[Text] = file_format
        self.load_contents: bool = load_contents
        self.load_listing: LoadListing = load_listing
        self.secondary_files: Optional[List[SecondaryFile]] = secondary_files
        self.streamable: bool = streamable
        self.writable: bool = writable

    async def _build_token_value(self, job: Job, token_value: Any) -> Any:
        # TODO: manage secondary files
        connector = self.port.task.get_connector()
        resource = job.get_resource()
        if isinstance(token_value, Text):
            if token_value == 'null' and self.default_path is not None:
                token_value = self.default_path
            token_class = 'File' if self.file_type == FileType.FILE else 'Directory'
            token_value = _get_token(self.port.task, token_class, token_value)
        if 'path' not in token_value and 'location' in token_value:
            if token_value['location'].startswith('file://'):
                token_value['path'] = token_value['location'][7:]
        if self.load_contents and 'contents' not in token_value:
            token_value['contents'] = await remotepath.head(
                connector, resource, token_value['path'], CONTENT_LIMIT)
        if self.load_listing != LoadListing.no_listing and 'listing' not in token_value:
            token_value['listing'] = await _get_listing(
                job, token_value['path'], self.load_listing == LoadListing.deep_listing)
        return token_value

    async def _update_file_token(self,
                                 job: Job,
                                 src_job: Job,
                                 token_value: MutableMapping[Text, Any]) -> MutableMapping[Text, Any]:
        if isinstance(token_value, Text):
            token_value = {'location': ''.join(['file://', token_value])}
        if 'location' in token_value:
            location = token_value['location']
            # Manage remote files
            scheme = urllib.parse.urlsplit(location).scheme
            if scheme in ['http', 'https']:
                location = await _download_file(job, location)
            # Transfer file in task's input folder
            filepath = await self._transfer_file(src_job, job, location[7:])
            # Build token
            return await self._build_token_value(job, filepath)
        else:
            # If there is only a 'listing' field, transfer all the listed files to the remote resource
            if 'listing' in token_value:
                # Compute destination path
                if 'path' in token_value:
                    dest_path = token_value['path']
                elif 'basename' in token_value:
                    path_processor = utils.get_path_processor(self.port.task)
                    dest_path = path_processor.join(job.input_directory, token_value['basename'])
                else:
                    dest_path = None
                # Copy each element of the listing into the destination folder
                tasks = []
                classes = []
                for element in token_value['listing']:
                    if dest_path is not None:
                        path_processor = utils.get_path_processor(src_job.task) if src_job is not None else os.path
                        basename = path_processor.basename(element['path'])
                        path_processor = utils.get_path_processor(self.port.task)
                        current_dest_path = path_processor.join(dest_path, basename)
                    else:
                        current_dest_path = None
                    tasks.append(asyncio.create_task(
                        self._transfer_file(src_job, job, element['path'], current_dest_path)))
                    classes.append(element['class'])
                dest_paths = await asyncio.gather(*tasks)
                token_value['listing'] = [
                    _get_token(self.port.task, token_class, path) for token_class, path in zip(classes, dest_paths)]
            return token_value

    async def _transfer_file(self,
                             src_job: Job,
                             dest_job: Job,
                             filepath: Text,
                             dest_path: Optional[Text] = None) -> Text:
        if dest_path is None:
            if isinstance(self.port, InputPort) and src_job is not None:
                if filepath.startswith(src_job.output_directory):
                    path_processor = utils.get_path_processor(self.port.dependee.task)
                    relpath = path_processor.relpath(filepath, src_job.output_directory)
                    path_processor = utils.get_path_processor(self.port.task)
                    dest_path = path_processor.join(dest_job.input_directory, relpath)
                else:
                    path_processor = utils.get_path_processor(self.port.dependee.task)
                    basename = path_processor.basename(filepath)
                    path_processor = utils.get_path_processor(self.port.task)
                    dest_path = path_processor.join(dest_job.input_directory, basename)
            else:
                path_processor = utils.get_path_processor(self.port.task)
                dest_path = path_processor.join(dest_job.input_directory, os.path.basename(filepath))
        await self.port.task.context.data_manager.transfer_data(
            src=filepath,
            src_job=src_job,
            dst=dest_path,
            dst_job=dest_job,
            symlink_if_possible=not self.writable)
        return dest_path

    async def collect_output(self, token: Token, output_dir: Text) -> None:
        context = self.port.task.context
        src_job = context.scheduler.get_job(token.job)
        dest_path = os.path.join(output_dir, token.value['basename'])
        await self.port.task.context.data_manager.transfer_data(
            src=token.value['path'],
            src_job=src_job,
            dst=dest_path,
            dst_job=None,
            symlink_if_possible=not self.writable)

    async def update_token(self, job: Job, token: Token) -> Token:
        # If value is a list of tokens, process each token separately
        if isinstance(token.job, List):
            return await super().update_token(job, token)
        # Otherwise simply process token
        else:
            context = self.port.task.context
            src_job = context.scheduler.get_job(token.job)
            if self.is_array and isinstance(token.value, List):
                elements = []
                for element in token.value:
                    elements.append(await self._update_file_token(job, src_job, element))
                return Token(name=token.name, value=elements, job=token.job, weight=token.weight)
            else:
                token_value = await self._update_file_token(job, src_job, token.value)
                return Token(name=token.name, value=token_value, job=token.job, weight=token.weight)

    async def weight_token(self, job: Job, token_value: Any) -> int:
        connector = job.task.get_connector() if job is not None else None
        resource = job.get_resource() if job is not None else None
        return await remotepath.size(connector, resource, _get_paths(token_value))


class CWLValueProcessor(CWLTokenProcessor):

    def __init__(self,
                 port: Port,
                 is_array: bool,
                 port_type: Text,
                 default_value: Optional[Any] = None,
                 expression_lib: Optional[List[Text]] = None,
                 full_js: bool = False,
                 glob: Optional[Text] = None,
                 output_eval: Optional[Text] = None):
        super().__init__(
            port=port,
            expression_lib=expression_lib,
            full_js=full_js,
            glob=glob,
            is_array=is_array,
            output_eval=output_eval)
        self.port_type: Text = port_type
        self.default_value: Optional[Any] = default_value

    async def _build_token_value(self, job: Job, token_value: Any) -> Any:
        if token_value == 'null' and self.default_value is not None:
            return self.default_value
        else:
            return token_value
