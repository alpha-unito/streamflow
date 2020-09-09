from __future__ import annotations

import asyncio
import os
from asyncio import FIRST_COMPLETED, Task
from typing import TYPE_CHECKING, Optional, cast

from streamflow.core import utils
from streamflow.core.scheduling import JobStatus
from streamflow.core.workflow import Executor, TerminationToken
from streamflow.log_handler import logger
from streamflow.workflow.task import BaseTask

if TYPE_CHECKING:
    from streamflow.core.context import StreamflowContext
    from streamflow.core.workflow import Workflow
    from typing import List
    from typing_extensions import Text


class StreamFlowExecutor(Executor):

    def __init__(self,
                 context: StreamflowContext,
                 workflow: Workflow):
        super().__init__(workflow)
        self.context: StreamflowContext = context
        self.executions: List[Task] = []
        self.output_tasks: List[Task] = []
        self.received: List[Text] = []
        self.closed: bool = False

    async def _execute(self, task: Text):
        try:
            await self.workflow.tasks[task].run()
        except Exception as e:
            logger.exception(e)
            self._shutdown()

    def _shutdown(self):
        # Terminate all tasks
        for task in self.workflow.tasks.values():
            task.terminate(JobStatus.CANCELLED)
        # Mark the executor as closed
        self.closed = True

    async def run(self, output_dir: Optional[Text] = os.getcwd()):
        # Execute workflow
        for task in self.workflow.tasks:
            execution = asyncio.create_task(self._execute(task), name=task)
            self.executions.append(execution)
        # If workflow has output ports
        if self.workflow.output_ports:
            # Retreive output tokens
            output_retriever = BaseTask(utils.random_name(), self.context)
            for port_name, port in self.workflow.output_ports.items():
                self.output_tasks.append(asyncio.create_task(port.get(output_retriever), name=port_name))
            while not self.closed:
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
                            return
                    else:
                        # Collect outputs
                        token_processor = self.workflow.output_ports[token.name].token_processor
                        await token_processor.collect_output(token, output_dir)
                        # Create a new task in place of the completed one
                        self.output_tasks.append(asyncio.create_task(
                            self.workflow.output_ports[task_name].get(output_retriever), name=task_name))
        # Otherwise simply wait for all tasks to finish
        else:
            await asyncio.gather(*self.executions)
