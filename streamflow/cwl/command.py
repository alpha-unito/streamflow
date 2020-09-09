import asyncio
import json
import sys
import tempfile
from abc import ABC
from typing import Union, Optional, List, Any, IO, MutableMapping, MutableSequence, Tuple

import cwltool.expression
import shellescape
from typing_extensions import Text

import streamflow.core.utils
from streamflow.core.scheduling import JobStatus
from streamflow.core.workflow import Task, Command, Job
from streamflow.cwl import utils
from streamflow.log_handler import logger
from streamflow.workflow.exception import WorkflowExecutionException


def _get_value(token: Any, item_separator: Optional[Text]) -> Any:
    if isinstance(token, list):
        value = []
        for element in token:
            value.append(_get_value(element, item_separator))
        if item_separator is not None:
            value = item_separator.join(value)
        return value
    elif isinstance(token, dict):
        if 'path' in token:
            return token['path']
        else:
            raise WorkflowExecutionException("Unsupported value " + str(token) + " from expression")
    else:
        return str(token)


class CWLCommandToken(object):

    def __init__(self,
                 value: Any,
                 item_separator: Optional[Text] = None,
                 position: Union[Text, int] = 0,
                 prefix: Optional[Text] = None,
                 separate: bool = True,
                 shell_quote: bool = True):
        self.value: Any = value
        self.item_separator: Optional[Text] = item_separator
        self.position: Union[Text, int] = position
        self.prefix: Optional[Text] = prefix
        self.separate: bool = separate
        self.shell_quote: bool = shell_quote


class CWLBaseCommand(Command, ABC):

    def __init__(self,
                 task: Task,
                 expression_lib: Optional[List[Text]] = None,
                 initial_work_dir: Optional[Union[Text, List[Any]]] = None,
                 full_js: bool = False,
                 time_limit: Optional[Union[int, Text]] = None):
        super().__init__(task)
        self.base_command: List[Text] = []
        self.expression_lib: Optional[List[Text]] = expression_lib
        self.full_js: bool = full_js
        self.initial_work_dir: Optional[Union[Text, List[Any]]] = initial_work_dir
        self.time_limit: Optional[Union[int, Text]] = time_limit

    def _eval_expression(self,
                         expression: Text,
                         context: MutableMapping[Text, Any],
                         timeout: Optional[int] = None):
        return cwltool.expression.interpolate(
            expression,
            context,
            fullJS=self.full_js,
            jslib=cwltool.expression.jshead(
                self.expression_lib or [], context) if self.full_js else "",
            timeout=timeout if timeout is not None else cwltool.expression.default_timeout)

    def _get_timeout(self, job: Job) -> Optional[int]:
        if isinstance(self.time_limit, int):
            timeout = self.time_limit
        elif isinstance(self.time_limit, Text):
            if '$(' in self.time_limit or '${' in self.time_limit:
                context = utils.build_context(job)
                timeout = self._eval_expression(self.time_limit, context)
            else:
                timeout = int(self.time_limit)
        else:
            timeout = 0
        return timeout if timeout > 0 else None

    async def _prepare_work_dir(self,
                                job: Job,
                                context: MutableMapping[Text, Any],
                                element: Any,
                                dest_path: Optional[Text] = None,
                                writable: bool = False) -> None:
        # If current element is a string, it must be an expression
        if isinstance(element, Text):
            listing = self._eval_expression(element, context)
        else:
            listing = element
        # If listing is a list, each of its elements must be processed independently
        if isinstance(listing, MutableSequence):
            prepare_tasks = []
            for el in listing:
                prepare_tasks.append(
                    asyncio.create_task(self._prepare_work_dir(job, context, el, dest_path, writable)))
            await asyncio.gather(*prepare_tasks)
        # If listing is a dictionary, it could be a File, a Directory, a Dirent or some other object
        elif isinstance(listing, MutableMapping):
            # If it is a File or Directory element, put the correspnding file in the output directory
            if 'class' in listing and listing['class'] in ['File', 'Directory']:
                src_path = listing['path']
                dest_path = dest_path if dest_path is not None else job.output_directory
                await self.task.context.data_manager.transfer_data(src_path, job, dest_path, job, not writable)
            # If it is a Dirent element, put or create the corresponding file according to the entryname field
            elif 'entry' in listing:
                if '$(' in listing['entry'] or '${' in listing['entry']:
                    entry = self._eval_expression(listing['entry'], context)
                else:
                    entry = listing['entry']
                if 'entryname' in listing:
                    if '$(' in listing['entryname'] or '${' in listing['entryname']:
                        dest_path = self._eval_expression(listing['entryname'], context)
                    else:
                        dest_path = listing['entryname']
                    path_processor = streamflow.core.utils.get_path_processor(self.task)
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
        await self.task.context.data_manager.transfer_data(file_name, None, dest_path, job, not writable)


class CWLCommand(CWLBaseCommand):

    def __init__(self,
                 task: Task,
                 expression_lib: Optional[List[Text]] = None,
                 full_js: bool = False,
                 initial_work_dir: Optional[Union[Text, List[Any]]] = None,
                 is_shell_command: bool = False,
                 success_codes: Optional[List[int]] = None,
                 task_stderr: Optional[Text] = None,
                 task_stdin: Optional[Text] = None,
                 task_stdout: Optional[Text] = None,
                 time_limit: Optional[Union[int, Text]] = None):
        super().__init__(task, expression_lib, initial_work_dir, full_js, time_limit)
        self.command_tokens: List[CWLCommandToken] = []
        self.environment: MutableMapping[Text, Text] = {}
        self.is_shell_command: bool = is_shell_command
        self.success_codes: Optional[List[int]] = success_codes
        self.task_stderr: Union[Text, IO] = task_stderr
        self.task_stdin: Union[Text, IO] = task_stdin
        self.task_stdout: Union[Text, IO] = task_stdout

    def _get_executable_command(self, context: MutableMapping[Text, Any]) -> List[Text]:
        command = []
        bindings_map = {}
        # Process baseCommand
        for command_token in self.base_command:
            command.append(command_token)
        # Process tokens
        for command_token in self.command_tokens:
            # Obtain token value
            if isinstance(command_token.value, str) and ("$(" in command_token.value or "${" in command_token.value):
                processed_token = self._eval_expression(command_token.value, context)
            else:
                processed_token = command_token.value
            value = _get_value(processed_token, command_token.item_separator)
            # Obtain prefix if present
            if command_token.prefix is not None:
                if command_token.separate:
                    if isinstance(value, List):
                        value = [command_token.prefix] + value
                    else:
                        value = [command_token.prefix, value]
                else:
                    value = [command_token.prefix + value]
            # Ensure value is a list
            if not isinstance(value, list):
                value = [value]
            # Process shell escape
            if self.is_shell_command and command_token.shell_quote:
                value = [shellescape.quote(v) for v in value]
            # Obtain token position
            if isinstance(command_token.position, str) and not command_token.position.isnumeric():
                context['self'] = processed_token
                position = self._eval_expression(command_token.position, context)
            else:
                position = int(command_token.position)
            if position not in bindings_map:
                bindings_map[position] = []
            # Place value in proper position
            bindings_map[position].append(value)
        # Merge tokens
        for binding_position in sorted(bindings_map.keys()):
            for binding in bindings_map[binding_position]:
                command.extend(binding)
        return command

    def _get_stream(self,
                    context: MutableMapping[Text, Any],
                    stream: Optional[Text],
                    default_stream: IO,
                    is_input: bool = False) -> IO:
        if isinstance(stream, str):
            if "$(" in stream or "${" in stream:
                stream = self._eval_expression(stream, context)
            return open(stream, "rb" if is_input else "wb")
        else:
            return default_stream

    async def execute(self, job: Job) -> Tuple[Any, JobStatus]:
        context = utils.build_context(job)
        if self.initial_work_dir is not None:
            await self._prepare_work_dir(job, context, self.initial_work_dir)
        cmd = self._get_executable_command(context)
        if self.is_shell_command:
            cmd = ["/bin/sh", "-c", " ".join(cmd)]
        parsed_env = {k: str(self._eval_expression(v, context)) for (k, v) in self.environment.items()}
        if self.task.target is None:
            # Open streams
            stderr = self._get_stream(context, self.task_stderr, sys.stderr)
            stdin = self._get_stream(context, self.task_stdout, sys.stdin, is_input=True)
            stdout = self._get_stream(context, self.task_stdout, sys.stdout)
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
            result = await asyncio.wait_for(proc.wait(), self._get_timeout(job))
            exit_code = proc.returncode
            # Close streams
            if stdin is not sys.stdin:
                stdin.close()
            if stdout is not sys.stdout:
                stdout.close()
            if stderr is not sys.stderr:
                stderr.close()
        else:
            connector = self.task.get_connector()
            resources = job.get_resources()
            logger.info('Executing job {job} on resource {resource} into directory {outdir}:\n{command}'.format(
                job=job.name,
                resource=resources[0] if resources else None,
                outdir=job.output_directory,
                command=' \\\n\t'.join(cmd),
            ))
            # If task is assigned to multiple resources, add the STREAMFLOW_HOSTS environment variable
            if len(resources) > 1:
                available_resources = await connector.get_available_resources(self.task.target.service)
                hosts = {k: v.hostname for k, v in available_resources.items() if k in resources}
                parsed_env['STREAMFLOW_HOSTS'] = ','.join(hosts.values())
            # Execute remote command
            result, exit_code = await asyncio.wait_for(
                connector.run(
                    resources[0] if resources else None,
                    cmd,
                    environment=parsed_env,
                    workdir=job.output_directory,
                    capture_output=True,
                    task_command=True),
                self._get_timeout(job))
            # TODO: manage streams
        # Handle exit codes
        if (self.success_codes is not None and exit_code in self.success_codes) or exit_code == 0:
            status = JobStatus.COMPLETED
            if result:
                print(result)
        else:
            status = JobStatus.FAILED
        return result, status


class CWLExpressionCommand(CWLBaseCommand):

    def __init__(self,
                 task: Task,
                 expression: Text,
                 expression_lib: Optional[List[Text]] = None,
                 full_js: bool = False,
                 initial_work_dir: Optional[Union[Text, List[Any]]] = None,
                 time_limit: Optional[Union[int, Text]] = None):
        super().__init__(task, expression_lib, initial_work_dir, full_js, time_limit)
        self.expression: Text = expression

    async def execute(self, job: Job) -> Tuple[Any, JobStatus]:
        context = utils.build_context(job)
        if self.initial_work_dir is not None:
            await self._prepare_work_dir(job, context, self.initial_work_dir)
        logger.info('Evaluating expression for job {job}'.format(job=job.name))
        timeout = self._get_timeout(job)
        return self._eval_expression(self.expression, context, timeout), JobStatus.COMPLETED
