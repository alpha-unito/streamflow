import asyncio
import urllib.parse
from abc import ABC
from typing import MutableMapping, MutableSequence, Any, Optional

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowExecutionException, WorkflowDefinitionException
from streamflow.core.utils import random_name, get_tag
from streamflow.core.workflow import Token, Job, Workflow, Port
from streamflow.cwl import utils
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.utils import LoadListing
from streamflow.data import remotepath
from streamflow.workflow.port import JobPort
from streamflow.workflow.step import TransferStep, ConditionalStep, InputInjectorStep
from streamflow.workflow.token import ListToken, ObjectToken


async def _download_file(job: Job, url: str, context: StreamFlowContext) -> str:
    connector = context.scheduler.get_connector(job.name)
    locations = context.scheduler.get_locations(job.name)
    try:
        return await remotepath.download(connector, locations, url, job.input_directory)
    except Exception:
        raise WorkflowExecutionException("Error downloading file from " + url)


class CWLBaseConditionalStep(ConditionalStep, ABC):

    def __init__(self,
                 name: str,
                 workflow: Workflow):
        super().__init__(name, workflow)
        self.skip_ports: MutableMapping[str, str] = {}

    def add_skip_port(self, name: str, port: Port) -> None:
        if port.name not in self.workflow.ports:
            self.workflow.ports[port.name] = port
        self.skip_ports[name] = port.name

    def get_skip_ports(self) -> MutableMapping[str, Port]:
        return {k: self.workflow.ports[v] for k, v in self.skip_ports.items()}


class CWLConditionalStep(CWLBaseConditionalStep):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 expression: str,
                 expression_lib: Optional[MutableSequence[str]] = None,
                 full_js: bool = False):
        super().__init__(name, workflow)
        self.expression: str = expression
        self.expression_lib: Optional[MutableSequence[str]] = expression_lib
        self.full_js: bool = full_js

    async def _eval(self, inputs: MutableMapping[str, Token]):
        context = utils.build_context(inputs)
        condition = utils.eval_expression(
            expression=self.expression,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib)
        if condition is True or condition is False:
            return condition
        else:
            raise WorkflowDefinitionException("Conditional 'when' must evaluate to 'true' or 'false'")

    async def _on_true(self, inputs: MutableMapping[str, Token]):
        # Propagate output tokens
        for port_name, port in self.get_output_ports().items():
            port.put(inputs[port_name])

    async def _on_false(self, inputs: MutableMapping[str, Token]):
        # Propagate skip tokens
        for port in self.get_skip_ports().values():
            port.put(Token(value=None, tag=get_tag(inputs.values())))


class CWLEmptyScatterConditionalStep(CWLBaseConditionalStep):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 scatter_method: str):
        super().__init__(name, workflow)
        self.scatter_method: str = scatter_method

    async def _eval(self, inputs: MutableMapping[str, Token]):
        return all(isinstance(i, ListToken) and len(i.value) > 0 for i in inputs.values())

    async def _on_true(self, inputs: MutableMapping[str, Token]):
        # Propagate output tokens
        for port_name, port in self.get_output_ports().items():
            port.put(inputs[port_name])

    async def _on_false(self, inputs: MutableMapping[str, Token]):
        # Get empty scatter return value
        if self.scatter_method == 'nested_crossproduct':
            token_value = [ListToken(value=[], tag=get_tag(inputs.values())) for _ in inputs]
        else:
            token_value = []
        # Propagate skip tokens
        for port in self.get_skip_ports().values():
            port.put(ListToken(value=token_value, tag=get_tag(inputs.values())))


class CWLInputInjectorStep(InputInjectorStep):

    async def _process_file_token(self,
                                  job: Job,
                                  token_value: Any):
        filepath = utils.get_path_from_token(token_value)
        connector = self.workflow.context.scheduler.get_connector(job.name)
        locations = self.workflow.context.scheduler.get_locations(job.name)
        new_token_value = token_value
        if filepath:
            await utils.register_data(
                context=self.workflow.context,
                connector=connector,
                locations=locations,
                base_path=job.output_directory,
                token_value=token_value)
            new_token_value = await utils.get_file_token(
                context=self.workflow.context,
                connector=connector,
                locations=locations,
                token_class=utils.get_token_class(token_value),
                filepath=filepath,
                file_format=token_value.get('format'),
                basename=token_value.get('basename'))
            if 'secondaryFiles' in token_value:
                new_token_value['secondaryFiles'] = await asyncio.gather(*(asyncio.create_task(
                    utils.get_file_token(
                        context=self.workflow.context,
                        connector=connector,
                        locations=locations,
                        token_class=utils.get_token_class(sf),
                        filepath=utils.get_path_from_token(sf),
                        file_format=sf.get('format'),
                        basename=sf.get('basename'))) for sf in token_value['secondaryFiles']))
        if 'listing' in token_value:
            listing = await asyncio.gather(*(asyncio.create_task(
                self._process_file_token(job, t)) for t in token_value['listing']))
            new_token_value = {**new_token_value, **{'listing': listing}}
        return new_token_value

    async def process_input(self,
                            job: Job,
                            token_value: Any) -> Token:
        if isinstance(token_value, MutableSequence):
            return ListToken(value=await asyncio.gather(*(
                asyncio.create_task(self.process_input(job, v)) for v in token_value)))
        elif isinstance(token_value, MutableMapping):
            if utils.get_token_class(token_value) in ['File', 'Directory']:
                return CWLFileToken(value=await self._process_file_token(job, token_value))
            else:
                token_tasks = {k: asyncio.create_task(self.process_input(job, v)) for k, v in token_value.items()}
                return ObjectToken(value=dict(zip(token_tasks.keys(), await asyncio.gather(*token_tasks.values()))))
        else:
            return Token(value=token_value)


class CWLTransferStep(TransferStep):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 job_port: JobPort,
                 writable: bool = False):
        super().__init__(name, workflow, job_port)
        self.writable: bool = writable

    async def _transfer_value(self, job: Job, token_value: Any) -> Any:
        if isinstance(token_value, Token):
            return token_value.update(await self._transfer_value(job, token_value.value))
        elif isinstance(token_value, MutableSequence):
            return await asyncio.gather(*(asyncio.create_task(
                self._transfer_value(job, element)) for element in token_value))
        elif isinstance(token_value, MutableMapping):
            if utils.get_token_class(token_value) in ['File', 'Directory']:
                return await self._update_file_token(job, token_value)
            else:
                return {k: v for k, v in zip(token_value.keys(), await asyncio.gather(*(
                    asyncio.create_task(self._transfer_value(job, element))
                    for element in token_value.values())))}
        else:
            return token_value

    async def _update_file_token(self,
                                 job: Job,
                                 token_value: MutableMapping[str, Any],
                                 dest_path: Optional[str] = None) -> MutableMapping[str, Any]:
        token_class = utils.get_token_class(token_value)
        # Get allocation and connector
        connector = self.workflow.context.scheduler.get_connector(job.name)
        # Extract location
        location = token_value.get('location', token_value.get('path'))
        if location and '://' in location:
            # Manage remote files
            scheme = urllib.parse.urlsplit(location).scheme
            if scheme in ['http', 'https']:
                location = await _download_file(job, location, self.workflow.context)
            elif scheme == 'file':
                location = urllib.parse.unquote(location[7:])
        # If basename is explicitly stated in the token, use it as destination path
        if 'basename' in token_value:
            path_processor = utils.get_path_processor(connector)
            dest_path = dest_path or path_processor.join(
                job.input_directory,
                random_name())
            dest_path = path_processor.join(dest_path, token_value['basename'])
        # Get destination coordinates
        dst_connector = self.workflow.context.scheduler.get_connector(job.name)
        dst_locations = self.workflow.context.scheduler.get_locations(job.name)
        path_processor = utils.get_path_processor(dst_connector)
        # If source data exist, get source locations
        if location and (
                selected_location := self.workflow.context.data_manager.get_source_location(
                    path=location,
                    dst_deployment=dst_connector.deployment_name)):
            # Build destination path
            filepath = dest_path or path_processor.join(
                job.input_directory,
                utils.random_name(),
                selected_location.relpath)
            # Perform and transfer
            src_connector = self.workflow.context.deployment_manager.get_connector(selected_location.deployment)
            if token_class == 'Directory':
                await remotepath.mkdir(dst_connector, dst_locations, filepath)
            await self.workflow.context.data_manager.transfer_data(
                src_deployment=src_connector.deployment_name,
                src_locations=[selected_location.location],
                src_path=selected_location.path,
                dst_deployment=dst_connector.deployment_name,
                dst_locations=dst_locations,
                dst_path=filepath,
                writable=self.writable)
            # Transform token value
            new_token_value = {
                'class': token_class,
                'path': filepath,
                'location': 'file://' + filepath,
                'basename': path_processor.basename(filepath),
                'dirname': path_processor.dirname(filepath)}
            # If token contains a file
            if token_class == 'File':
                # Perform remote checksum
                original_checksum = token_value['checksum']
                for location in dst_locations:
                    checksum = 'sha1${}'.format(await remotepath.checksum(
                        self.workflow.context, dst_connector, location, filepath))
                    if checksum != original_checksum:
                        raise WorkflowExecutionException("Error transferring file {} to location {}".format(
                            filepath, location))
                # Add size, checksum and format fields
                new_token_value = {**new_token_value, **{
                    'nameroot': token_value['nameroot'],
                    'nameext': token_value['nameext'],
                    'size': token_value['size'],
                    'checksum': original_checksum}}
                if 'format' in token_value:
                    new_token_value['format'] = token_value['format']
                if 'contents' in token_value:
                    new_token_value['contents'] = token_value['contents']
                # Check secondary files
                if 'secondaryFiles' in token_value:
                    new_token_value['secondaryFiles'] = await asyncio.gather(*(
                        self._update_file_token(
                            job=job,
                            token_value=element,
                            dest_path=path_processor.dirname(filepath))
                        for element in token_value['secondaryFiles']))
            # If token contains a directory, propagate listing if present
            elif token_class == 'Directory' and 'listing' in token_value:
                new_token_value['listing'] = await asyncio.gather(*(asyncio.create_task(
                    self._update_file_token(
                        job=job,
                        token_value=element,
                        dest_path=dest_path))
                    for element in token_value['listing']))
            return new_token_value
        # Otherwise, create elements remotely
        else:
            # Build destination path
            filepath = dest_path or path_processor.join(
                job.input_directory,
                utils.random_name())
            # If the token contains a directory, simply create it
            if token_class == 'Directory':
                await remotepath.mkdir(dst_connector, dst_locations, filepath)
            # Otherwise, create the parent directories structure and write file contents
            else:
                await remotepath.mkdir(dst_connector, dst_locations, path_processor.dirname(filepath))
                await utils.write_remote_file(
                        context=self.workflow.context,
                        job=job,
                        content=token_value.get('contents', ''),
                        path=filepath)
            # Build file token
            new_token_value = await utils.get_file_token(
                context=self.workflow.context,
                connector=dst_connector,
                locations=dst_locations,
                token_class=token_class,
                filepath=filepath,
                load_contents='contents' in token_value,
                load_listing=LoadListing.no_listing)
            # If listing is specified, recursively process its contents
            if 'listing' in token_value:
                new_token_value['listing'] = await asyncio.gather(*(asyncio.create_task(
                    self._update_file_token(
                        job=job,
                        token_value=t,
                        dest_path=dest_path
                    )) for t in token_value['listing']))
            # Return the new token value
            return new_token_value

    async def transfer(self, job: Job, token: Token) -> Token:
        return token.update(await self._transfer_value(job, token.value))
