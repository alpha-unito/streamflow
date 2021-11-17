from __future__ import annotations

import asyncio
import base64
import copy
import json
import logging
import posixpath
import shlex
import tempfile
import time
from abc import ABC
from asyncio.subprocess import STDOUT
from typing import Union, Optional, List, Any, IO, MutableMapping, MutableSequence

from streamflow.core.data import LOCAL_RESOURCE
from streamflow.core.exception import WorkflowExecutionException, WorkflowDefinitionException
from streamflow.core.utils import get_path_processor, flatten_list, get_resources, get_connector
from streamflow.core.workflow import Step, Command, Job, CommandOutput, Token, Status, TokenProcessor, Port
from streamflow.cwl import utils
from streamflow.cwl.token_processor import CWLMapTokenProcessor, CWLTokenProcessor
from streamflow.cwl.utils import eval_expression
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.workflow.port import DefaultOutputPort, ObjectTokenProcessor


def _adjust_inputs(inputs: MutableSequence[MutableMapping[str, Any]],
                   path_processor,
                   src_path: str,
                   dest_path: str) -> MutableSequence[MutableMapping[str, Any]]:
    for inp in inputs:
        if isinstance(inp, MutableMapping) and inp.get('class') in ('File', 'Directory'):
            path = utils.get_path_from_token(inp)
            if path == src_path:
                inp['path'] = dest_path
                dirname, basename = path_processor.split(dest_path)
                inp['dirname'] = dirname
                inp['basename'] = basename
                if inp['class'] == 'File':
                    nameroot, nameext = path_processor.splitext(basename)
                    inp['nameroot'] = nameroot
                    inp['nameext'] = nameext
            elif inp['class'] == 'Directory' and src_path.startswith(path) and 'listing' in inp:
                inp['listing'] = _adjust_inputs(inp['listing'], path_processor, src_path, dest_path)
    return inputs


def _check_command_token(command_token: CWLCommandToken, input_value: Any) -> bool:
    # CWLMapCommandToken is suitable for input lists
    if isinstance(command_token, CWLMapCommandToken):
        if isinstance(input_value, MutableSequence):
            return _check_command_token(command_token.value, input_value[0])
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
                if not (k in command_token.value and _check_command_token(command_token.value[k], v)):
                    return False
            return True
        else:
            return False
    # A default command token must match its type with the token value type
    else:
        if isinstance(input_value, Token):
            input_value = input_value.value
        return command_token.token_type == utils.infer_type_from_token(input_value)


async def _check_cwl_output(job: Job, result: Any) -> Any:
    path_processor = get_path_processor(job.step)
    cwl_output_path = path_processor.join(job.output_directory, 'cwl.output.json')
    connector = get_connector(job, job.step.context)
    resources = get_resources(job)
    for resource in resources:
        if await remotepath.exists(connector, resource, cwl_output_path):
            # If file exists, use its contents as token value
            result = json.loads(await remotepath.read(connector, resource, cwl_output_path))
            # Update step output ports at runtime if needed
            if isinstance(result, MutableMapping):
                workflow = job.step.workflow
                for out_name, out in result.items():
                    if out_name not in job.step.output_ports:
                        out_port = DefaultOutputPort(name=out_name, step=job.step)
                        out_port.token_processor = _infer_token_processor(out_port, out)
                        job.step.output_ports[out_name] = out_port
                        # Update workflow outputs if needed
                        if job.step.name in workflow.root_steps and out_name not in workflow.output_ports:
                            workflow.output_ports[out_name] = out_port
                for out_port in [p for p in job.step.output_ports if p not in result]:
                    result[out_port] = None
                    if job.step.name in workflow.root_steps and out_port in workflow.output_ports:
                        del workflow.output_ports[out_port]
            break
    return result


def _get_value(token: Any, item_separator: Optional[str]) -> Any:
    if isinstance(token, MutableSequence):
        value = []
        for element in token:
            value.append(_get_value(element, item_separator))
        if item_separator is not None:
            value = item_separator.join([str(v) for v in value])
        return value or None
    elif isinstance(token, MutableMapping):
        if (path := utils.get_path_from_token(token)) is not None:
            return path
        else:
            raise WorkflowExecutionException("Unsupported value " + str(token) + " from expression")
    else:
        return token


def _infer_token_processor(port: Port,
                           value: Any) -> TokenProcessor:
    port_type = utils.infer_type_from_token(value)
    if port_type == 'array':
        return CWLMapTokenProcessor(
            port=port,
            token_processor=_infer_token_processor(port, value[0] if len(value) > 1 else None))
    elif port_type == 'record':
        return ObjectTokenProcessor(
            port=port,
            processors={k: _infer_token_processor(port, v) for k, v in value})
    else:
        return CWLTokenProcessor(port=port, port_type=port_type)


def _escape_value(value: Any) -> Any:
    if isinstance(value, MutableSequence):
        return [_escape_value(v) for v in value]
    else:
        return shlex.quote(str(value))


def _merge_tokens(bindings_map: MutableMapping[str, MutableSequence[Any]]) -> MutableSequence[Any]:
    command = []
    for binding_position in sorted(bindings_map.keys()):
        for binding in bindings_map[binding_position]:
            command.extend(flatten_list(binding))
    return [str(token) for token in command]


class CWLCommandOutput(CommandOutput):
    __slots__ = 'exit_code'

    def __init__(self,
                 value: Any,
                 status: Status,
                 exit_code: int):
        super().__init__(value, status)
        self.exit_code: int = exit_code

    def update(self, value: Any):
        return CWLCommandOutput(value=value, status=self.status, exit_code=self.exit_code)


class CWLCommandToken(object):

    def __init__(self,
                 name: str,
                 value: Any,
                 token_type: Optional[str] = None,
                 is_shell_command: bool = False,
                 item_separator: Optional[str] = None,
                 position: Union[str, int] = 0,
                 prefix: Optional[str] = None,
                 separate: bool = True,
                 shell_quote: bool = True):
        self.name: str = name
        self.value: Any = value
        self.token_type: Optional[str] = token_type
        self.is_shell_command: bool = is_shell_command
        self.item_separator: Optional[str] = item_separator
        self.position: Union[str, int] = position
        self.prefix: Optional[str] = prefix
        self.separate: bool = separate
        self.shell_quote: bool = shell_quote

    def _compute_binding(self,
                         processed_token: Any,
                         context: MutableMapping[str, Any],
                         bindings_map: MutableMapping[str, MutableSequence[Any]],
                         full_js: bool = False,
                         expression_lib: Optional[MutableSequence[str]] = None
                         ) -> MutableMapping[str, MutableSequence[Any]]:
        # Obtain token value
        value = _get_value(processed_token, self.item_separator)
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
                    value = [self.prefix + str(value)]
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
                context['self'] = processed_token
                position = eval_expression(
                    expression=self.position,
                    context=context,
                    full_js=full_js,
                    expression_lib=expression_lib)
                try:
                    position = int(position) if position is not None else 0
                except ValueError:
                    pass
            else:
                position = int(self.position)
            sort_key = "".join([str(position), self.name]) if self.name else str(position)
            if sort_key not in bindings_map:
                bindings_map[sort_key] = []
            # Place value in proper position
            bindings_map[sort_key].append(value)
            return bindings_map

    def _process_token(self,
                       token_value: Any,
                       context: MutableMapping[str, Any],
                       full_js: bool = False,
                       expression_lib: Optional[MutableSequence[str]] = None) -> Any:
        if isinstance(token_value, CWLCommandToken):
            local_bindings = token_value.get_binding(
                context=context,
                bindings_map={},
                full_js=full_js,
                expression_lib=expression_lib)
            processed_token = _merge_tokens(local_bindings)
        elif isinstance(token_value, str):
            processed_token = eval_expression(
                expression=self.value,
                context=context,
                full_js=full_js,
                expression_lib=expression_lib)
        elif token_value is None:
            processed_token = context['inputs'][self.name]
            processed_token = processed_token.value if isinstance(processed_token, Token) else processed_token
        else:
            processed_token = token_value
        return processed_token

    def get_binding(self,
                    context: MutableMapping[str, Any],
                    bindings_map: MutableMapping[str, MutableSequence[Any]],
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[str]] = None) -> Any:
        processed_token = self._process_token(
            token_value=self.value,
            context=context,
            full_js=full_js,
            expression_lib=expression_lib)
        return self._compute_binding(
            processed_token=processed_token,
            context=context,
            bindings_map=bindings_map,
            full_js=full_js,
            expression_lib=expression_lib)


class CWLObjectCommandToken(CWLCommandToken):

    def _check_dict(self, value: Any):
        if not isinstance(value, MutableMapping):
            raise WorkflowDefinitionException(
                "A {this} object can only be used to process dict values".format(this=self.__class__.__name__))

    def get_binding(self,
                    context: MutableMapping[str, Any],
                    bindings_map: MutableMapping[str, MutableSequence[Any]],
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[str]] = None) -> Any:
        self._check_dict(self.value)
        context = copy.deepcopy(context)
        context['inputs'] = context['inputs'][self.name]
        for key, token in self.value.items():
            if key in context['inputs'] and token is not None:
                bindings_map = token.get_binding(
                    context=context,
                    bindings_map=bindings_map,
                    full_js=full_js,
                    expression_lib=expression_lib)
        return bindings_map


class CWLUnionCommandToken(CWLCommandToken):

    def get_binding(self,
                    context: MutableMapping[str, Any],
                    bindings_map: MutableMapping[str, MutableSequence[Any]],
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[str]] = None) -> Any:
        inputs = context['inputs'][self.name]
        command_token = self.get_command_token(inputs)
        return command_token.get_binding(
            context=context,
            bindings_map=bindings_map,
            full_js=full_js,
            expression_lib=expression_lib)

    def get_command_token(self, inputs: Any) -> CWLCommandToken:
        if self.value is not None:
            for command_token in self.value:
                if _check_command_token(command_token, inputs):
                    return command_token
            raise WorkflowDefinitionException("No suitable command token for input value " + str(inputs))
        else:
            return super()


class CWLMapCommandToken(CWLCommandToken):

    def _check_list(self, value: Any):
        if not isinstance(value, MutableSequence):
            raise WorkflowDefinitionException(
                "A {this} object can only be used to process list values".format(this=self.__class__.__name__))

    def get_binding(self,
                    context: MutableMapping[str, Any],
                    bindings_map: MutableMapping[str, MutableSequence[Any]],
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[str]] = None) -> Any:
        inputs = context['inputs'][self.name]
        if isinstance(inputs, Token):
            inputs = inputs.value
        self._check_list(inputs)
        context = copy.deepcopy(context)
        for value in inputs:
            context['inputs'][self.name] = value
            context['self'] = value
            bindings_map = super().get_binding(
                context=context,
                bindings_map=bindings_map,
                full_js=full_js,
                expression_lib=expression_lib)
        return bindings_map


class CWLBaseCommand(Command, ABC):

    def __init__(self,
                 step: Step,
                 absolute_initial_workdir_allowed: bool = False,
                 expression_lib: Optional[MutableSequence[str]] = None,
                 initial_work_dir: Optional[Union[str, MutableSequence[Any]]] = None,
                 inplace_update: bool = False,
                 full_js: bool = False,
                 time_limit: Optional[Union[int, str]] = None):
        super().__init__(step)
        self.absolute_initial_workdir_allowed: bool = absolute_initial_workdir_allowed
        self.expression_lib: Optional[MutableSequence[str]] = expression_lib
        self.full_js: bool = full_js
        self.initial_work_dir: Optional[Union[str, MutableSequence[Any]]] = initial_work_dir
        self.inplace_update: bool = inplace_update
        self.time_limit: Optional[Union[int, str]] = time_limit

    def _get_timeout(self, job: Job) -> Optional[int]:
        if isinstance(self.time_limit, int):
            timeout = self.time_limit
        elif isinstance(self.time_limit, str):
            context = utils.build_context(job)
            timeout = int(eval_expression(
                expression=self.time_limit,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib))
        else:
            timeout = 0
        return timeout if timeout > 0 else None

    async def _prepare_work_dir(self,
                                job: Job,
                                context: MutableMapping[str, Any],
                                element: Any,
                                base_path: Optional[str] = None,
                                dest_path: Optional[str] = None,
                                writable: bool = False) -> None:
        path_processor = get_path_processor(job.step)
        connector = get_connector(job, self.step.context)
        resources = get_resources(job)
        # Initialize base path to job output directory if present
        base_path = base_path or job.output_directory
        # If current element is a string, it must be an expression
        if isinstance(element, str):
            listing = eval_expression(
                expression=element,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib)
        else:
            listing = element
        # If listing is a list, each of its elements must be processed independently
        if isinstance(listing, MutableSequence):
            await asyncio.gather(*(asyncio.create_task(
                self._prepare_work_dir(
                    job=job,
                    context=context,
                    element=el,
                    base_path=base_path,
                    writable=writable)
            ) for el in listing))
        # If listing is a dictionary, it could be a File, a Directory, a Dirent or some other object
        elif isinstance(listing, MutableMapping):
            # If it is a File or Directory element, put the correspnding file in the output directory
            if 'class' in listing and listing['class'] in ['File', 'Directory']:
                src_path = utils.get_path_from_token(listing)
                src_found = False
                if src_path is not None:
                    if dest_path is None:
                        basename = path_processor.basename(src_path)
                        dest_path = path_processor.join(base_path, basename)
                    for resource in resources:
                        if await remotepath.exists(connector, resource, src_path):
                            await self.step.context.data_manager.transfer_data(
                                src=src_path,
                                src_job=job,
                                dst=dest_path,
                                dst_job=job,
                                writable=writable)
                            _adjust_inputs(
                                inputs=context['inputs'].values(),
                                path_processor=path_processor,
                                src_path=src_path,
                                dest_path=dest_path)
                            src_found = True
                            break
                # If the source path does not exist, create a File or a Directory in the remote path
                if not src_found:
                    if dest_path is None:
                        dest_path = base_path
                    if src_path is not None:
                        dest_path = path_processor.join(dest_path, path_processor.basename(src_path))
                    if listing['class'] == 'Directory':
                        await remotepath.mkdir(connector, resources, dest_path)
                    else:
                        await self._write_remote_file(
                            job=job,
                            content=listing['contents'] if 'contents' in listing else '',
                            dest_path=dest_path,
                            writable=writable)
                # If `listing` is present, recursively process folder contents
                if 'listing' in listing:
                    if 'basename' in listing:
                        folder_path = path_processor.join(base_path, listing['basename'])
                        await remotepath.mkdir(connector, resources, folder_path)
                    else:
                        folder_path = dest_path or base_path
                    await asyncio.gather(*(asyncio.create_task(
                        self._prepare_work_dir(
                            job=job,
                            context=context,
                            element=element,
                            base_path=folder_path,
                            writable=writable)
                    ) for element in listing['listing']))
                # If `secondaryFiles` is present, resursively process secondary files
                if 'secondaryFiles' in listing:
                    await asyncio.gather(*(asyncio.create_task(
                        self._prepare_work_dir(
                            job=job,
                            context=context,
                            element=element,
                            base_path=base_path,
                            writable=writable)
                    ) for element in listing['secondaryFiles']))
            # If it is a Dirent element, put or create the corresponding file according to the entryname field
            elif 'entry' in listing:
                entry = eval_expression(
                    expression=listing['entry'],
                    context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                    strip_whitespace=False)
                if 'entryname' in listing:
                    dest_path = eval_expression(
                        expression=listing['entryname'],
                        context=context,
                        full_js=self.full_js,
                        expression_lib=self.expression_lib)
                    if not path_processor.isabs(dest_path):
                        dest_path = posixpath.abspath(posixpath.join(job.output_directory, dest_path))
                    if not (dest_path.startswith(job.output_directory) or self.absolute_initial_workdir_allowed):
                        raise WorkflowDefinitionException("Entryname cannot be outside the job's output directory")
                    # The entryname field overrides the value of basename of the File or Directory object
                    if (isinstance(entry, MutableMapping) and
                            'path' not in entry and 'location' not in entry and
                            'basename' in entry):
                        entry['basename'] = dest_path
                    if not path_processor.isabs(dest_path):
                        dest_path = path_processor.join(job.output_directory, dest_path)
                writable = listing['writable'] if 'writable' in listing and not self.inplace_update else False
                # If entry is a string, a new text file must be created with the string as the file contents
                if isinstance(entry, str):
                    await self._write_remote_file(job, entry, dest_path or base_path, writable)
                # If entry is a list
                elif isinstance(entry, MutableSequence):
                    # If all elements are Files or Directories, each of them must be processed independently
                    if all('class' in t and t['class'] in ['File', 'Directory'] for t in entry):
                        await self._prepare_work_dir(
                            job=job,
                            context=context,
                            element=entry,
                            base_path=base_path,
                            dest_path=dest_path,
                            writable=writable)
                    # Otherwise, the content should be serialised to JSON
                    else:
                        await self._write_remote_file(job, json.dumps(entry), dest_path or base_path, writable)
                # If entry is a dict
                elif isinstance(entry, MutableMapping):
                    # If it is a File or Directory, it must be put in the destination path
                    if 'class' in entry and entry['class'] in ['File', 'Directory']:
                        await self._prepare_work_dir(
                            job=job,
                            context=context,
                            element=entry,
                            base_path=base_path,
                            dest_path=dest_path,
                            writable=writable)
                    # Otherwise, the content should be serialised to JSON
                    else:
                        await self._write_remote_file(job, json.dumps(entry), dest_path or base_path, writable)
                # Every object different from a string should be serialised to JSON
                elif entry is not None:
                    await self._write_remote_file(job, json.dumps(entry), dest_path or base_path, writable)

    async def _write_remote_file(self, job: Job,
                                 content: str,
                                 dest_path: Optional[str] = None,
                                 writable: bool = False):
        file_name = tempfile.mktemp()
        with open(file_name, mode='w') as file:
            file.write(content)
        await self.step.context.data_manager.transfer_data(file_name, None, dest_path, job, not writable)

    async def skip(self, job: Job):
        return CWLCommandOutput(
            value={t.name: None for t in job.inputs},
            status=Status.SKIPPED,
            exit_code=0)


class CWLCommand(CWLBaseCommand):

    def __init__(self,
                 step: Step,
                 absolute_initial_workdir_allowed: bool = False,
                 expression_lib: Optional[MutableSequence[str]] = None,
                 failure_codes: Optional[MutableSequence[int]] = None,
                 full_js: bool = False,
                 initial_work_dir: Optional[Union[str, MutableSequence[Any]]] = None,
                 inplace_update: bool = False,
                 is_shell_command: bool = False,
                 output_directory: Optional[str] = None,
                 success_codes: Optional[MutableSequence[int]] = None,
                 step_stderr: Optional[str] = None,
                 step_stdin: Optional[str] = None,
                 step_stdout: Optional[str] = None,
                 time_limit: Optional[Union[int, str]] = None):
        super().__init__(
            step=step,
            absolute_initial_workdir_allowed=absolute_initial_workdir_allowed,
            expression_lib=expression_lib,
            initial_work_dir=initial_work_dir,
            inplace_update=inplace_update,
            full_js=full_js,
            time_limit=time_limit)
        self.base_command: MutableSequence[str] = []
        self.command_tokens: MutableSequence[CWLCommandToken] = []
        self.environment: MutableMapping[str, str] = {}
        self.failure_codes: Optional[MutableSequence[int]] = failure_codes
        self.is_shell_command: bool = is_shell_command
        self.output_directory: Optional[str] = output_directory
        self.success_codes: Optional[MutableSequence[int]] = success_codes
        self.stderr: Union[str, IO] = step_stderr
        self.stdin: Union[str, IO] = step_stdin
        self.stdout: Union[str, IO] = step_stdout

    def _get_executable_command(self, context: MutableMapping[str, Any]) -> MutableSequence[str]:
        command = []
        bindings_map = {}
        # Process baseCommand
        if self.base_command:
            command.append(shlex.join(self.base_command))
        # Process tokens
        for command_token in self.command_tokens:
            if command_token.name is not None:
                context['self'] = context['inputs'][command_token.name]
                # If input is None, skip the command token
                if context['self'] is None:
                    continue
            bindings_map = command_token.get_binding(
                context=context,
                bindings_map=bindings_map,
                full_js=self.full_js,
                expression_lib=self.expression_lib)
        # Merge tokens
        command.extend(_merge_tokens(bindings_map))
        return command

    async def execute(self, job: Job) -> CWLCommandOutput:
        # Override output directory if needed (e.g. when dockerOutputDirectory is set)
        if self.output_directory:
            job.output_directory = self.output_directory
        # Build context
        context = utils.build_context(job)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Job {job} inputs: {inputs}".format(
                job=job.name, inputs=json.dumps(context['inputs'], indent=4, sort_keys=True)))
        # Build working directory
        if self.initial_work_dir is not None:
            await self._prepare_work_dir(job, context, self.initial_work_dir)
        # Build command string
        cmd = self._get_executable_command(context)
        # Build environment variables
        parsed_env = {k: str(eval_expression(
            expression=v,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib
        )) for (k, v) in self.environment.items()}
        if 'HOME' not in parsed_env:
            parsed_env['HOME'] = job.output_directory
        if 'TMPDIR' not in parsed_env:
            parsed_env['TMPDIR'] = job.tmp_directory
        # Get execution target
        connector = get_connector(job, self.step.context)
        resources = get_resources(job)
        cmd_string = ' \\\n\t'.join(["/bin/sh", "-c", "\"{cmd}\"".format(cmd=" ".join(cmd))]
                                    if self.is_shell_command else cmd)
        logger.info('Executing job {job} {resource} into directory {outdir}:\n{command}'.format(
            job=job.name,
            resource="locally" if resources[0] == LOCAL_RESOURCE else "on resource {res}".format(
                res=resources[0]),
            outdir=job.output_directory,
            command=cmd_string))
        # Persist command
        command_id = self.step.context.persistence_manager.db.add_command(
            step_id=self.step.persistent_id,
            cmd=cmd_string)
        # Escape shell command when needed
        if self.is_shell_command:
            cmd = ["/bin/sh", "-c", "\"$(echo {command} | base64 -d)\"".format(
                command=base64.b64encode(" ".join(cmd).encode('utf-8')).decode('utf-8'))]
        # If step is assigned to multiple resources, add the STREAMFLOW_HOSTS environment variable
        if len(resources) > 1:
            available_resources = await connector.get_available_resources(self.step.target.service)
            hosts = {k: v.hostname for k, v in available_resources.items() if k in resources}
            parsed_env['STREAMFLOW_HOSTS'] = ','.join(hosts.values())
        # Process streams
        stdin = eval_expression(
            expression=self.stdin,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib)
        stdout = eval_expression(
            expression=self.stdout,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib
        ) if self.stdout is not None else STDOUT
        stderr = eval_expression(
            expression=self.stderr,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib
        ) if self.stderr is not None else stdout
        # Execute remote command
        start_time = time.time_ns()
        result, exit_code = await asyncio.wait_for(
            connector.run(
                resources[0] if resources else None,
                cmd,
                environment=parsed_env,
                workdir=job.output_directory,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                capture_output=True,
                job_name=job.name),
            self._get_timeout(job))
        end_time = time.time_ns()
        # Handle exit codes
        if self.failure_codes and exit_code in self.failure_codes:
            status = Status.FAILED
        elif (self.success_codes and exit_code in self.success_codes) or exit_code == 0:
            status = Status.COMPLETED
            if result:
                logger.info(result)
        else:
            status = Status.FAILED
        # Update command persistence
        self.step.context.persistence_manager.db.update_command(command_id, {
            "status": status.value,
            "output": str(result),
            "start_time": start_time,
            "end_time": end_time
        })
        # Check if file `cwl.output.json` exists either locally on at least one resource
        result = await _check_cwl_output(job, result)
        return CWLCommandOutput(value=result, status=status, exit_code=exit_code)


class CWLExpressionCommand(CWLBaseCommand):

    def __init__(self,
                 step: Step,
                 expression: str,
                 absolute_initial_workdir_allowed: bool = False,
                 expression_lib: Optional[List[str]] = None,
                 full_js: bool = False,
                 initial_work_dir: Optional[Union[str, List[Any]]] = None,
                 inplace_update: bool = False):
        super().__init__(
            step=step,
            absolute_initial_workdir_allowed=absolute_initial_workdir_allowed,
            expression_lib=expression_lib,
            initial_work_dir=initial_work_dir,
            inplace_update=inplace_update,
            full_js=full_js)
        self.expression: str = expression

    async def execute(self, job: Job) -> CWLCommandOutput:
        context = utils.build_context(job)
        if self.initial_work_dir is not None:
            await self._prepare_work_dir(job, context, self.initial_work_dir)
        logger.info('Evaluating expression for job {job}'.format(job=job.name))
        # Persist command
        command_id = self.step.context.persistence_manager.db.add_command(self.step.persistent_id, self.expression)
        # Execute command
        start_time = time.time_ns()
        result = eval_expression(
            expression=self.expression,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib)
        end_time = time.time_ns()
        # Update command persistence
        self.step.context.persistence_manager.db.update_command(command_id, {
            "status": Status.COMPLETED.value,
            "output": str(result),
            "start_time": start_time,
            "end_time": end_time
        })
        # Return result
        return CWLCommandOutput(value=result, status=Status.COMPLETED, exit_code=0)


class CWLStepCommand(CWLBaseCommand):

    def __init__(self,
                 step: Step,
                 absolute_initial_workdir_allowed: bool = False,
                 expression_lib: Optional[MutableSequence[str]] = None,
                 full_js: bool = False,
                 initial_work_dir: Optional[Union[str, MutableSequence[Any]]] = None,
                 inplace_update: bool = False,
                 time_limit: Optional[Union[int, str]] = None):
        super().__init__(
            step=step,
            absolute_initial_workdir_allowed=absolute_initial_workdir_allowed,
            expression_lib=expression_lib,
            initial_work_dir=initial_work_dir,
            inplace_update=inplace_update,
            full_js=full_js,
            time_limit=time_limit)
        self.input_expressions: MutableMapping[str, str] = {}

    async def execute(self, job: Job) -> CommandOutput:
        context = utils.build_context(job)
        logger.info('Executing job {job}'.format(job=job.name))
        # Process expressions
        processed_inputs = {}
        for k, v in self.input_expressions.items():
            context = {**context, **{'self': context['inputs'][k]}}
            processed_inputs[k] = utils.eval_expression(
                expression=v,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib)
        context['inputs'] = {**context['inputs'], **processed_inputs}
        return CWLCommandOutput(
            value=context['inputs'],
            status=Status.COMPLETED,
            exit_code=0)
