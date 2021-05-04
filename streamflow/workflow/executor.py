from __future__ import annotations

import asyncio
import json
import os
from asyncio import FIRST_COMPLETED, Task
from typing import TYPE_CHECKING, cast, MutableSequence

from streamflow.core import utils
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.workflow import Executor, TerminationToken, Status
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Workflow
    from typing import Any, MutableMapping, Optional
    from typing_extensions import Text


class StreamFlowExecutor(Executor):

    def __init__(self,
                 context: StreamFlowContext,
                 workflow: Workflow):
        super().__init__(workflow)
        self.context: StreamFlowContext = context
        self.executions: MutableSequence[Task] = []
        self.output_tasks: MutableSequence[Task] = []
        self.received: MutableSequence[Text] = []
        self.closed: bool = False

    async def _execute(self, step: Text):
        try:
            await self.workflow.steps[step].run()
        except Exception as e:
            logger.exception(e)
            self._shutdown()

    def _shutdown(self):
        # Terminate all steps
        for step in self.workflow.steps.values():
            step.terminate(Status.SKIPPED)
        # Mark the executor as closed
        self.closed = True

    async def _wait_outputs(self,
                            output_consumer: Text,
                            output_dir: Text,
                            output_tokens: MutableMapping[Text, Any]) -> MutableMapping[Text, Any]:
        finished, unfinished = await asyncio.wait(self.output_tasks, return_when=FIRST_COMPLETED)
        self.output_tasks = list(unfinished)
        for task in finished:
            if task.cancelled():
                continue
            task_name = cast(Task, task).get_name()
            token = task.result()
            # If a TerminationToken is received, the corresponding port terminated its outputs
            if isinstance(token, TerminationToken):
                self.received.append(task_name)
                # When the last port terminates, the entire executor terminates
                if len(self.received) == len(self.workflow.output_ports):
                    self.closed = True
                    return output_tokens
            else:
                # Collect outputs
                token_processor = self.workflow.output_ports[task_name].token_processor
                token = await token_processor.collect_output(token, output_dir)
                if token.value is not None:
                    output_tokens[task_name] = utils.get_token_value(token)
                # Create a new task in place of the completed one
                self.output_tasks.append(asyncio.create_task(
                    self.workflow.output_ports[task_name].get(output_consumer), name=task_name))
        return output_tokens

    async def run(self, output_dir: Optional[Text] = os.getcwd()):
        output_tokens = {}
        # Execute workflow
        for step in self.workflow.steps:
            execution = asyncio.create_task(self._execute(step), name=step)
            self.executions.append(execution)
        # If workflow has output ports
        if self.workflow.output_ports:
            # Retreive output tokens
            output_consumer = utils.random_name()
            for port_name, port in self.workflow.output_ports.items():
                self.output_tasks.append(asyncio.create_task(port.get(output_consumer), name=port_name))
            while not self.closed:
                output_tokens = await self._wait_outputs(output_consumer, output_dir, output_tokens)
        # Otherwise simply wait for all tasks to finish
        else:
            await asyncio.gather(*self.executions)
        # Check if workflow terminated properly
        for step in self.workflow.steps.values():
            if step.status == Status.FAILED:
                raise WorkflowExecutionException("Workflow execution failed")
        # Print output tokens
        print(json.dumps(output_tokens, sort_keys=True, indent=4))
