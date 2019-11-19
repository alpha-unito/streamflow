import logging
import threading
from typing import Set, List, Union, Dict, Any

from cwltool.command_line_tool import CallbackJob
from cwltool.context import RuntimeContext
from cwltool.errors import WorkflowException
from cwltool.executors import JobExecutor, TMPDIR_LOCK
from cwltool.job import JobBase
from cwltool.process import Process
from cwltool.workflow import WorkflowJob

from streamflow.log_handler import _logger


class StreamflowJobExecutor(JobExecutor):

    def __init__(self):  # type: () -> None
        """Initialize."""
        super().__init__()
        self.threads: Set[threading.Thread] = set()
        self.exceptions: List[WorkflowException] = []
        self.pending_jobs: List[Union[JobBase, WorkflowJob]] = []
        self.pending_jobs_lock: threading.Lock = threading.Lock()

    def _runner(self,
                job: Union[JobBase, WorkflowJob, CallbackJob],
                runtime_context: RuntimeContext,
                tmpdir_lock: threading.Lock) -> None:
        try:
            _logger.debug("job: {}, runtime_context: {}, TMPDIR_LOCK: {}".format(job, runtime_context, tmpdir_lock))
            job.run(runtime_context, tmpdir_lock)
        except WorkflowException as err:
            _logger.exception("Got workflow error")
            self.exceptions.append(err)
        except Exception as err:  # pylint: disable=broad-except
            _logger.exception("Got workflow error")
            self.exceptions.append(WorkflowException(str(err)))
        finally:
            if runtime_context.workflow_eval_lock:
                with runtime_context.workflow_eval_lock:
                    self.threads.remove(threading.current_thread())
                    runtime_context.workflow_eval_lock.notifyAll()

    def run_job(self,
                job: Union[JobBase, WorkflowJob, None],
                runtime_context: RuntimeContext) -> None:
        if job is not None:
            with self.pending_jobs_lock:
                self.pending_jobs.append(job)

        with self.pending_jobs_lock:
            n = 0
            while (n + 1) <= len(self.pending_jobs):
                job = self.pending_jobs[n]
                thread = threading.Thread(target=self._runner, args=(job, runtime_context, TMPDIR_LOCK))
                thread.daemon = True
                self.threads.add(thread)
                thread.start()
                self.pending_jobs.remove(job)

    def run_jobs(self,
                 process: Process,
                 job_order_object: Dict[str, Any],
                 logger: logging.Logger,
                 runtime_context: RuntimeContext) -> None:
        jobiter = process.job(job_order_object, self.output_callback,
                              runtime_context)

        if runtime_context.workflow_eval_lock is None:
            raise WorkflowException(
                "runtimeContext.workflow_eval_lock must not be None")

        runtime_context.workflow_eval_lock.acquire()
        for job in jobiter:
            if job is not None:
                if isinstance(job, JobBase):
                    job.builder = runtime_context.builder or job.builder
                    if job.outdir is not None:
                        self.output_dirs.add(job.outdir)

            self.run_job(job, runtime_context)

            if job is None:
                if self.threads:
                    self.wait_for_next_completion(runtime_context)
                else:
                    logger.error("Workflow cannot make any more progress.")
                    break

        self.run_job(None, runtime_context)
        while self.threads:
            self.wait_for_next_completion(runtime_context)
            self.run_job(None, runtime_context)

        runtime_context.workflow_eval_lock.release()

    def wait_for_next_completion(self,
                                 runtime_context: RuntimeContext) -> None:
        if runtime_context.workflow_eval_lock is not None:
            runtime_context.workflow_eval_lock.wait()
        if self.exceptions:
            raise self.exceptions[0]
