from __future__ import annotations

import asyncio
import logging
import os
import traceback
from asyncio import FIRST_COMPLETED, Task
from typing import TYPE_CHECKING, Optional, cast

from streamflow.core import utils
from streamflow.core.workflow import Executor, TerminationToken
from streamflow.workflow.exception import WorkflowExecutionException
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
        self.received: List[Text] = []
        self.context: StreamflowContext = context

    async def _execute(self, task: Text):
        try:
            await self.workflow.tasks[task].run()
        except Exception as e:
            traceback.print_exc()
            raise WorkflowExecutionException("An error occurred while executing task " + task) from e

    async def run(self, output_dir: Optional[Text] = os.getcwd()):
        executions = []
        try:
            # Execute workflow
            for task in self.workflow.tasks:
                execution = asyncio.create_task(self._execute(task), name=task)
                executions.append(execution)
            # If workflow has output ports
            if self.workflow.output_ports:
                # Retreive output tokens
                output_retriever = BaseTask(utils.random_name(), self.context)
                get_tasks = []
                for port_name, port in self.workflow.output_ports.items():
                    get_tasks.append(asyncio.create_task(port.get(output_retriever), name=port_name))
                while True:
                    finished, unfinished = await asyncio.wait(get_tasks, return_when=FIRST_COMPLETED)
                    get_tasks = list(unfinished)
                    for task in finished:
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
                            get_tasks.append(asyncio.create_task(
                                self.workflow.output_ports[task_name].get(output_retriever), name=task_name))
            # Otherwise simply wait for all tasks to finish
            else:
                await asyncio.gather(*executions)
        except BaseException as e:
            logging.exception(e)
            for task in executions:
                if not task.done():
                    task.cancel()
        finally:
            await self.context.deployment_manager.undeploy_all()
