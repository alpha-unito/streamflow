from __future__ import annotations

import asyncio
from asyncio import FIRST_COMPLETED, Task, CancelledError
from typing import TYPE_CHECKING, cast, MutableSequence

from streamflow.core import utils
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.workflow import Executor, Status
from streamflow.log_handler import logger
from streamflow.workflow.token import TerminationToken

if TYPE_CHECKING:
    from streamflow.core.workflow import Workflow
    from typing import Any, MutableMapping


class StreamFlowExecutor(Executor):

    def __init__(self, workflow: Workflow):
        super().__init__(workflow)
        self.executions: MutableSequence[Task] = []
        self.output_tasks: MutableMapping[str, Task] = {}
        self.received: MutableSequence[str] = []
        self.closed: bool = False

    def _handle_exception(self, task: Task):
        try:
            if exc := task.exception():
                logger.exception(exc, exc_info=exc)
                if not self.closed:
                    self._shutdown()
        except CancelledError:
            pass

    def _shutdown(self):
        # Terminate all steps
        for step in self.workflow.steps.values():
            step.terminate(Status.CANCELLED)
        # Mark the executor as closed
        self.closed = True

    async def _wait_outputs(self,
                            output_consumer: str,
                            output_tokens: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        finished, unfinished = await asyncio.wait(self.output_tasks.values(), return_when=FIRST_COMPLETED)
        self.output_tasks = {t.get_name(): t for t in unfinished}
        for task in finished:
            if task.cancelled():
                continue
            task_name = cast(Task, task).get_name()
            if task_name not in self.workflow.output_ports:
                continue
            token = task.result()
            # If a TerminationToken is received, the corresponding port terminated its outputs
            if isinstance(token, TerminationToken):
                self.received.append(task_name)
                # When the last port terminates, the entire executor terminates
                if len(self.received) == len(self.workflow.output_ports):
                    self.closed = True
            else:
                # Collect result
                output_tokens[task_name] = utils.get_token_value(token)
                # Create a new task in place of the completed one if not terminated
                if task_name not in self.received:
                    self.output_tasks[task_name] = asyncio.create_task(
                        self.workflow.get_output_port(task_name).get(output_consumer), name=task_name)
                    self.output_tasks[task_name].add_done_callback(self._handle_exception)
        # Check if new output ports have been created
        for port_name, port in self.workflow.get_output_ports().items():
            if port_name not in self.output_tasks and port_name not in self.received:
                self.output_tasks[port_name] = asyncio.create_task(port.get(output_consumer), name=port_name)
                self.output_tasks[port_name].add_done_callback(self._handle_exception)
                self.closed = False
        # Return output tokens
        return output_tokens

    async def run(self) -> MutableMapping[str, Any]:
        try:
            output_tokens = {}
            # Execute workflow
            for step in self.workflow.steps.values():
                execution = asyncio.create_task(step.run(), name=step.name)
                execution.add_done_callback(self._handle_exception)
                self.executions.append(execution)
            # If workflow has output ports
            if self.workflow.output_ports:
                # Retreive output tokens
                output_consumer = utils.random_name()
                for port_name, port in self.workflow.get_output_ports().items():
                    self.output_tasks[port_name] = asyncio.create_task(port.get(output_consumer), name=port_name)
                    self.output_tasks[port_name].add_done_callback(self._handle_exception)
                while not self.closed:
                    output_tokens = await self._wait_outputs(output_consumer, output_tokens)
            # Otherwise simply wait for all tasks to finish
            else:
                await asyncio.gather(*self.executions)
            # Check if workflow terminated properly
            for step in self.workflow.steps.values():
                if step.status in [Status.FAILED, Status.CANCELLED]:
                    raise WorkflowExecutionException("Workflow execution terminated with status FAILED")
            # Print output tokens
            return output_tokens
        except BaseException:
            if not self.closed:
                self._shutdown()
            raise
