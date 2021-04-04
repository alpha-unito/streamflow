from __future__ import annotations

import asyncio
import base64
import copy
import json
import logging
import sys
import tempfile
from abc import ABC
from asyncio.subprocess import STDOUT
from typing import Union, Optional, List, Any, IO, MutableMapping, MutableSequence

import shellescape
from typing_extensions import Text

from streamflow.core.exception import WorkflowExecutionException, WorkflowDefinitionException
from streamflow.core.utils import get_path_processor, flatten_list
from streamflow.core.workflow import Step, Command, Job, CommandOutput, Token, Status
from streamflow.cwl import utils
from streamflow.cwl.utils import eval_expression
from streamflow.data import remotepath
from streamflow.log_handler import logger


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


def _get_value(token: Any, item_separator: Optional[Text]) -> Any:
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


def _merge_tokens(bindings_map: MutableMapping[int, MutableSequence[Any]]) -> MutableSequence[Any]:
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
                 name: Text,
                 value: Any,
                 token_type: Optional[Text] = None,
                 item_separator: Optional[Text] = None,
                 position: Union[Text, int] = 0,
                 prefix: Optional[Text] = None,
                 separate: bool = True,
                 shell_quote: bool = True):
        self.name: Text = name
        self.value: Any = value
        self.token_type: Optional[Text] = token_type
        self.item_separator: Optional[Text] = item_separator
        self.position: Union[Text, int] = position
        self.prefix: Optional[Text] = prefix
        self.separate: bool = separate
        self.shell_quote: bool = shell_quote

    def _compute_binding(self,
                         processed_token: Any,
                         context: MutableMapping[Text, Any],
                         bindings_map: MutableMapping[int, MutableSequence[Any]],
                         is_shell_command: bool = False,
                         full_js: bool = False,
                         expression_lib: Optional[MutableSequence[Text]] = None
                         ) -> MutableMapping[int, MutableSequence[Any]]:
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
                    value = [self.prefix if value else '']
                elif self.separate:
                    if isinstance(value, MutableSequence):
                        value = [self.prefix, " ".join([str(v) for v in value])]
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
            # Process shell escape
            if is_shell_command and self.shell_quote:
                value = [shellescape.quote(v) for v in value]
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
                    position = 0
            else:
                position = int(self.position)
            if position not in bindings_map:
                bindings_map[position] = []
            # Place value in proper position
            bindings_map[position].append(value)
            return bindings_map

    def _process_token(self,
                       token_value: Any,
                       context: MutableMapping[Text, Any],
                       is_shell_command: bool = False,
                       full_js: bool = False,
                       expression_lib: Optional[MutableSequence[Text]] = None) -> Any:
        if isinstance(token_value, CWLCommandToken):
            local_bindings = token_value.get_binding(
                context=context,
                bindings_map={},
                is_shell_command=is_shell_command,
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
                    context: MutableMapping[Text, Any],
                    bindings_map: MutableMapping[int, MutableSequence[Any]],
                    is_shell_command: bool = False,
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[Text]] = None) -> Any:
        processed_token = self._process_token(
            token_value=self.value,
            context=context,
            is_shell_command=is_shell_command,
            full_js=full_js,
            expression_lib=expression_lib)
        return self._compute_binding(
            processed_token=processed_token,
            context=context,
            bindings_map=bindings_map,
            is_shell_command=is_shell_command,
            full_js=full_js,
            expression_lib=expression_lib)


class CWLObjectCommandToken(CWLCommandToken):

    def _check_dict(self, value: Any):
        if not isinstance(value, MutableMapping):
            raise WorkflowDefinitionException(
                "A {this} object can only be used to process dict values".format(this=self.__class__.__name__))

    def get_binding(self,
                    context: MutableMapping[Text, Any],
                    bindings_map: MutableMapping[int, MutableSequence[Any]],
                    is_shell_command: bool = False,
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[Text]] = None) -> Any:
        self._check_dict(self.value)
        context = copy.deepcopy(context)
        context['inputs'] = context['inputs'][self.name]
        for key, token in self.value.items():
            if key in context['inputs'] and token is not None:
                bindings_map = token.get_binding(
                    context=context,
                    bindings_map=bindings_map,
                    is_shell_command=is_shell_command,
                    full_js=full_js,
                    expression_lib=expression_lib)
        return bindings_map


class CWLUnionCommandToken(CWLCommandToken):

    def get_binding(self,
                    context: MutableMapping[Text, Any],
                    bindings_map: MutableMapping[int, MutableSequence[Any]],
                    is_shell_command: bool = False,
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[Text]] = None) -> Any:
        inputs = context['inputs'][self.name]
        command_token = self.get_command_token(inputs)
        return command_token.get_binding(
            context=context,
            bindings_map=bindings_map,
            is_shell_command=is_shell_command,
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
                    context: MutableMapping[Text, Any],
                    bindings_map: MutableMapping[int, MutableSequence[Any]],
                    is_shell_command: bool = False,
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[Text]] = None) -> Any:
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
                is_shell_command=is_shell_command,
                full_js=full_js,
                expression_lib=expression_lib)
        return bindings_map


class CWLBaseCommand(Command, ABC):

    def __init__(self,
                 step: Step,
                 expression_lib: Optional[MutableSequence[Text]] = None,
                 initial_work_dir: Optional[Union[Text, MutableSequence[Any]]] = None,
                 full_js: bool = False,
                 time_limit: Optional[Union[int, Text]] = None):
        super().__init__(step)
        self.expression_lib: Optional[MutableSequence[Text]] = expression_lib
        self.full_js: bool = full_js
        self.initial_work_dir: Optional[Union[Text, MutableSequence[Any]]] = initial_work_dir
        self.time_limit: Optional[Union[int, Text]] = time_limit

    def _get_timeout(self, job: Job) -> Optional[int]:
        if isinstance(self.time_limit, int):
            timeout = self.time_limit
        elif isinstance(self.time_limit, Text):
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
                                context: MutableMapping[Text, Any],
                                element: Any,
                                dest_path: Optional[Text] = None,
                                writable: bool = False) -> None:
        path_processor = get_path_processor(job.step)
        connector = job.step.get_connector()
        resources = job.get_resources() or [None]
        # If current element is a string, it must be an expression
        if isinstance(element, Text):
            listing = eval_expression(
                expression=element,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib)
        else:
            listing = element
        # If listing is a list, each of its elements must be processed independently
        if isinstance(listing, MutableSequence):
            await asyncio.gather(
                *[asyncio.create_task(self._prepare_work_dir(job, context, el, dest_path, writable)) for el in listing])
        # If listing is a dictionary, it could be a File, a Directory, a Dirent or some other object
        elif isinstance(listing, MutableMapping):
            # If it is a File or Directory element, put the correspnding file in the output directory
            if 'class' in listing and listing['class'] in ['File', 'Directory']:
                src_path = utils.get_path_from_token(listing)
                src_found = False
                if src_path is not None:
                    if dest_path is None:
                        if src_path.startswith(job.input_directory):
                            relpath = path_processor.relpath(src_path, job.input_directory)
                            dest_path = path_processor.join(job.output_directory, relpath)
                        else:
                            basename = path_processor.basename(src_path)
                            dest_path = path_processor.join(job.output_directory, basename)
                    for resource in resources:
                        if await remotepath.exists(connector, resource, src_path):
                            await self.step.context.data_manager.transfer_data(
                                src=src_path,
                                src_job=job,
                                dst=dest_path,
                                dst_job=job,
                                writable=writable)
                            src_found = True
                            break
                # If the source path does not exist, create a File or a Directory in the remote path
                if not src_found:
                    if dest_path is None:
                        dest_path = job.output_directory
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
                        dest_path = path_processor.join(dest_path, listing['basename'])
                        await remotepath.mkdir(connector, resources, dest_path)
                    await asyncio.gather(*[asyncio.create_task(
                        self._prepare_work_dir(job, context, element, dest_path, writable)
                    ) for element in listing['listing']])
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
                        dest_path = path_processor.join(job.output_directory, dest_path)
                writable = listing['writable'] if 'writable' in listing else False
                # If entry is a string, a new text file must be created with the string as the file contents
                if isinstance(entry, Text):
                    await self._write_remote_file(job, entry, dest_path, writable)
                # If entry is a list
                elif isinstance(entry, MutableSequence):
                    # If all elements are Files or Directories, each of them must be processed independently
                    if all('class' in t and t['class'] in ['File', 'Directory'] for t in entry):
                        await self._prepare_work_dir(job, context, entry, dest_path, writable)
                    # Otherwise, the content should be serialised to JSON
                    else:
                        await self._write_remote_file(job, json.dumps(entry), dest_path, writable)
                # If entry is a dict
                elif isinstance(entry, MutableMapping):
                    # If it is a File or Directory, it must be put in the destination path
                    if 'class' in entry and entry['class'] in ['File', 'Directory']:
                        await self._prepare_work_dir(job, context, entry, dest_path, writable)
                    # Otherwise, the content should be serialised to JSON
                    else:
                        await self._write_remote_file(job, json.dumps(entry), dest_path, writable)
                # Every object different from a string should be serialised to JSON
                else:
                    await self._write_remote_file(job, json.dumps(entry), dest_path, writable)

    async def _write_remote_file(self, job: Job,
                                 content: Text,
                                 dest_path: Optional[Text] = None,
                                 writable: bool = False):
        file_name = tempfile.mktemp()
        with open(file_name, mode='w') as file:
            file.write(content)
        await self.step.context.data_manager.transfer_data(file_name, None, dest_path, job, not writable)


class CWLCommand(CWLBaseCommand):

    def __init__(self,
                 step: Step,
                 expression_lib: Optional[MutableSequence[Text]] = None,
                 failure_codes: Optional[MutableSequence[int]] = None,
                 full_js: bool = False,
                 initial_work_dir: Optional[Union[Text, MutableSequence[Any]]] = None,
                 is_shell_command: bool = False,
                 success_codes: Optional[MutableSequence[int]] = None,
                 step_stderr: Optional[Text] = None,
                 step_stdin: Optional[Text] = None,
                 step_stdout: Optional[Text] = None,
                 time_limit: Optional[Union[int, Text]] = None):
        super().__init__(step, expression_lib, initial_work_dir, full_js, time_limit)
        self.base_command: MutableSequence[Text] = []
        self.command_tokens: MutableSequence[CWLCommandToken] = []
        self.environment: MutableMapping[Text, Text] = {}
        self.is_shell_command: bool = is_shell_command
        self.success_codes: Optional[MutableSequence[int]] = success_codes
        self.failure_codes: Optional[MutableSequence[int]] = failure_codes
        self.stderr: Union[Text, IO] = step_stderr
        self.stdin: Union[Text, IO] = step_stdin
        self.stdout: Union[Text, IO] = step_stdout

    def _get_executable_command(self, context: MutableMapping[Text, Any]) -> MutableSequence[Text]:
        command = []
        bindings_map = {}
        # Process baseCommand
        for command_token in self.base_command:
            command.append(command_token)
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
                is_shell_command=self.is_shell_command,
                full_js=self.full_js,
                expression_lib=self.expression_lib)
        # Merge tokens
        command.extend(_merge_tokens(bindings_map))
        return command

    def _get_stream(self,
                    job: Job,
                    context: MutableMapping[Text, Any],
                    stream: Optional[Text],
                    default_stream: IO,
                    is_input: bool = False) -> IO:
        if isinstance(stream, str):
            stream = eval_expression(
                expression=stream,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib)
            path_processor = get_path_processor(self.step)
            if not path_processor.isabs(stream):
                basedir = job.input_directory if is_input else job.output_directory
                stream = path_processor.join(basedir, stream)
            return open(stream, "rb" if is_input else "wb")
        else:
            return default_stream

    async def execute(self, job: Job) -> CWLCommandOutput:
        context = utils.build_context(job)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Job {job} inputs: {inputs}".format(
                job=job.name, inputs=json.dumps(context['inputs'], indent=4, sort_keys=True)))
        if self.initial_work_dir is not None:
            await self._prepare_work_dir(job, context, self.initial_work_dir)
        cmd = self._get_executable_command(context)
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
        if self.step.target is None:
            if self.is_shell_command:
                cmd = ["/bin/sh", "-c", " ".join(cmd)]
            # Open streams
            stderr = self._get_stream(job, context, self.stderr, sys.stderr)
            stdin = self._get_stream(job, context, self.stdin, sys.stdin, is_input=True)
            stdout = self._get_stream(job, context, self.stdout, sys.stderr)
            # Execute command
            logger.info('Executing job {job} into directory {outdir}: \n{command}'.format(
                job=job.name,
                outdir=job.output_directory,
                command=' \\\n\t'.join(cmd)
            ))
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=job.output_directory,
                env=parsed_env,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr
            )
            result, error = await asyncio.wait_for(proc.communicate(), self._get_timeout(job))
            exit_code = proc.returncode
            # Close streams
            if stdin is not sys.stdin:
                stdin.close()
            if stdout is not sys.stderr:
                stdout.close()
            if stderr is not sys.stderr:
                stderr.close()
        else:
            connector = self.step.get_connector()
            resources = job.get_resources()
            logger.info('Executing job {job} on resource {resource} into directory {outdir}:\n{command}'.format(
                job=job.name,
                resource=resources[0] if resources else None,
                outdir=job.output_directory,
                command=' \\\n\t'.join(["/bin/sh", "-c", "\"{cmd}\"".format(cmd=" ".join(cmd))]
                                       if self.is_shell_command else cmd)))
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
        # Handle exit codes
        if self.failure_codes is not None and exit_code in self.failure_codes:
            status = Status.FAILED
        elif (self.success_codes is not None and exit_code in self.success_codes) or exit_code == 0:
            status = Status.COMPLETED
            if result:
                logger.info(result)
        else:
            status = Status.FAILED
        return CWLCommandOutput(value=result, status=status, exit_code=exit_code)


class CWLExpressionCommand(CWLBaseCommand):

    def __init__(self,
                 step: Step,
                 expression: Text,
                 expression_lib: Optional[List[Text]] = None,
                 full_js: bool = False,
                 initial_work_dir: Optional[Union[Text, List[Any]]] = None,
                 time_limit: Optional[Union[int, Text]] = None):
        super().__init__(step, expression_lib, initial_work_dir, full_js, time_limit)
        self.expression: Text = expression

    async def execute(self, job: Job) -> CWLCommandOutput:
        context = utils.build_context(job)
        if self.initial_work_dir is not None:
            await self._prepare_work_dir(job, context, self.initial_work_dir)
        logger.info('Evaluating expression for job {job}'.format(job=job.name))
        timeout = self._get_timeout(job)
        result = eval_expression(
            expression=self.expression,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib,
            timeout=timeout)
        return CWLCommandOutput(value=result, status=Status.COMPLETED, exit_code=0)


class CWLStepCommand(CWLBaseCommand):

    def __init__(self,
                 step: Step,
                 expression_lib: Optional[MutableSequence[Text]] = None,
                 full_js: bool = False,
                 initial_work_dir: Optional[Union[Text, MutableSequence[Any]]] = None,
                 time_limit: Optional[Union[int, Text]] = None,
                 when_expression: Optional[Text] = None):
        super().__init__(step, expression_lib, initial_work_dir, full_js, time_limit)
        self.input_expressions: MutableMapping[Text, Text] = {}
        self.when_expression: Optional[Text] = when_expression

    def _evaulate_condition(self, context: MutableMapping[Text, Any]) -> bool:
        if self.when_expression is not None:
            condition = utils.eval_expression(
                expression=self.when_expression,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib)
            if condition is True or condition is False:
                return condition
            else:
                raise WorkflowDefinitionException("Conditional 'when' must evaluate to 'true' or 'false'")
        else:
            return True

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
        # If condition is satisfied, return the updated inputs
        if self._evaulate_condition(context):
            return CWLCommandOutput(
                value=context['inputs'],
                status=Status.COMPLETED,
                exit_code=0)
        # Otherwise, skip and return None
        else:
            return CWLCommandOutput(
                value={t.name: None for t in job.inputs},
                status=Status.SKIPPED,
                exit_code=0)
