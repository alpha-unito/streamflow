import os
import urllib.parse
from datetime import datetime
from typing import Type, Any, MutableMapping, Mapping, Callable, Generator, Union, Optional, Dict, Text, List

from cwltool.command_line_tool import CommandLineTool, CallbackJob
from cwltool.context import RuntimeContext
from cwltool.job import JobBase
from cwltool.pathmapper import PathMapper
from cwltool.provenance import ProvenanceProfile
from cwltool.utils import random_outdir
from cwltool.workflow import Workflow, WorkflowStep

from streamflow.cwl.context import SfRuntimeContext, SfLoadingContext
from streamflow.cwl.job import SfCommandLineJob
from streamflow.cwl.job_context import SfJobContext
from streamflow.cwl.remote_path_mapper import RemotePathMapper
from streamflow.data import remote_fs
from streamflow.log_handler import _logger
from streamflow.scheduling.scheduler import JobStatus


def _flatten_list(hierarchical_list):
    if not hierarchical_list:
        return hierarchical_list
    if isinstance(hierarchical_list[0], list):
        return _flatten_list(hierarchical_list[0]) + _flatten_list(hierarchical_list[1:])
    return hierarchical_list[:1] + _flatten_list(hierarchical_list[1:])


class DeploymentHelper(object):

    def __init__(self, deployment_names: List[str]) -> None:
        self.deployment_names = deployment_names
        self.deployment_manager = None
        self.original_callback = None

    def _output_callback(self, outputs, processStatus):
        if self.original_callback is not None:
            self.original_callback(outputs, processStatus)
        job_context = SfJobContext.current_context()
        if job_context is not None:
            if processStatus == 'success':
                _logger.info(
                    "Output data transfer started at {time}".format(name=job_context.name,
                                                                    time=datetime.now()))
                for output in outputs.values():
                    if not isinstance(output, list):
                        output = [output]
                    output = _flatten_list(output)
                    for element in output:
                        if hasattr(element, 'get') and (
                                element.get('class') == 'File' or element.get('class') == 'Directory'):
                            abs_path = urllib.parse.urlparse(element['location']).path
                            job_context.data_manager.collect_output(abs_path)

                _logger.info(
                    "Output data transfer terminated at {time}".format(name=job_context.name,
                                                                       time=datetime.now()))
            for deployment_name in self.deployment_names:
                self.deployment_manager.undeploy(deployment_name)

    def job(self,
            original_job: Callable[
                [Mapping[str, str], Callable[[Any, Any], Any], SfRuntimeContext],
                Generator[Union[JobBase, CallbackJob], None, None]],
            job_order: Mapping[str, str],  # type:
            output_callbacks: Callable[[Any, Any], Any],
            runtime_context: SfRuntimeContext
            ) -> Generator[Union[JobBase, CallbackJob], None, None]:
        self.original_callback = output_callbacks
        self.deployment_manager = runtime_context.deployment_manager
        yield from original_job(job_order, self._output_callback, runtime_context)


class SfCommandLineTool(CommandLineTool):

    def __init__(self,
                 toolpath_object: MutableMapping[str, Any],
                 loading_context: SfLoadingContext
                 ) -> None:
        self.deployment_helper = None
        self.step_path = loading_context.step_path
        deployments = loading_context.streamflow_config.get(loading_context.step_path, 'deployments')
        if deployments is not None:
            self.deployment_helper = DeploymentHelper(deployments.keys())
        super().__init__(toolpath_object, loading_context)

    def make_job_runner(self,
                        runtime_context: SfRuntimeContext
                        ) -> Type[JobBase]:
        deployment_target = runtime_context.streamflow_config.propagate(self.step_path, 'target')
        if deployment_target is not None:
            return SfCommandLineJob
        else:
            return super().make_job_runner(runtime_context)

    def make_path_mapper(self, reffiles, stagedir, runtimeContext,
                         separateDirs):  # type: (List[Any], Text, RuntimeContext, bool) -> PathMapper
        return RemotePathMapper(reffiles, runtimeContext.basedir, stagedir, separateDirs)

    def _init_job(self,
                  joborder: Mapping[str, str],
                  runtime_context: SfRuntimeContext):
        builder = super()._init_job(joborder, runtime_context)
        deployment_target = runtime_context.streamflow_config.propagate(self.step_path, 'target')
        if deployment_target is not None:
            builder.deployment_target = deployment_target
            deployments = runtime_context.streamflow_config.propagate(self.step_path, 'deployments')
            if 'model' in builder.deployment_target:
                model = deployments[builder.deployment_target['model']]
            elif len(deployments) == 1:
                model = next(iter(deployments.values()))
            else:
                raise Exception("Must specify model when there are multiple choices")
            builder.deployment_model = model
            builder.deployment_model['config']['config_file'] = runtime_context.streamflow_config.config_file
            deployment_dir_prefix = "/tmp/streamflow"
            builder.outdir = deployment_dir_prefix + random_outdir()
            builder.tmpdir = deployment_dir_prefix + os.path.sep + "tmp"
            builder.stagedir = deployment_dir_prefix + os.path.sep + "stage"
        return builder

    def job(self,
            job_order: Mapping[str, str],  # type:
            output_callbacks: Callable[[Any, Any], Any],
            runtime_context: SfRuntimeContext
            ) -> Generator[Union[JobBase, CallbackJob], None, None]:
        if self.deployment_helper is not None:
            yield from self.deployment_helper.job(super().job, job_order, output_callbacks, runtime_context)
        else:
            yield from super().job(job_order, output_callbacks, runtime_context)


class SfWorkflowStep(WorkflowStep):

    def __init__(self,
                 toolpath_object: Dict[Text, Any],
                 pos: int,
                 loading_context: SfLoadingContext,
                 parent_workflow_prov: Optional[ProvenanceProfile] = None):
        super().__init__(toolpath_object, pos, loading_context, parent_workflow_prov)
        self.step_path = loading_context.step_path

    def receive_output(self,
                       output_callback: Callable[..., Any],
                       jobout: Dict[Text, Any],
                       processStatus: Text):
        job_context = SfJobContext.current_context()
        if job_context is not None:
            task_description = job_context.scheduler.jobs[job_context.name].description
            for output in jobout.values():
                if not isinstance(output, list):
                    output = [output]
                for element in output:
                    if hasattr(element, 'get') and (
                            element.get('class') == 'File' or element.get('class') == 'Directory'):
                        abs_path = urllib.parse.urlparse(element['location']).path
                        task_description.add_output(abs_path)
                        remote_path = job_context.get_remote_path(abs_path)
                        if remote_fs.exists(job_context.connector, job_context.target_resource, remote_path):
                            job_context.data_manager.add_remote_path_mapping(
                                job_context.target_resource,
                                abs_path,
                                job_context.get_remote_path(abs_path))
                        if not os.path.exists(abs_path):
                            if element.get('class') == 'Directory':
                                os.makedirs(abs_path, exist_ok=True)
                            else:
                                open(abs_path, 'a').close()
        super().receive_output(output_callback, jobout, processStatus)
        if job_context is not None:
            if processStatus == 'success':
                job_context.scheduler.notify_status(job_context.name, JobStatus.completed)
            else:
                job_context.scheduler.notify_status(job_context.name, JobStatus.failed)


class SfWorkflow(Workflow):

    def __init__(self,
                 toolpath_object: MutableMapping[str, Any],
                 loading_context: SfLoadingContext
                 ) -> None:
        self.deployment_helper = None
        deployments = loading_context.streamflow_config.get(loading_context.step_path, 'deployments')
        if deployments is not None:
            self.deployment_helper = DeploymentHelper(deployments.keys())
        super().__init__(toolpath_object, loading_context)

    def job(self,
            job_order: Mapping[str, str],  # type:
            output_callbacks: Callable[[Any, Any], Any],
            runtime_context: SfRuntimeContext
            ) -> Generator[Union[JobBase, CallbackJob], None, None]:
        if self.deployment_helper is not None:
            yield from self.deployment_helper.job(super().job, job_order, output_callbacks, runtime_context)
        else:
            yield from super().job(job_order, output_callbacks, runtime_context)

    def make_workflow_step(self,
                           toolpath_object: Dict[str, Any],
                           pos: int,
                           loading_context: SfLoadingContext,
                           parent_workflow_prov: Optional[ProvenanceProfile] = None):
        step_id = toolpath_object.get('id')
        if step_id is not None:
            if '#' in step_id:
                step_name = step_id.split('#')[1]
                step_name = step_name.rsplit('/', 1)[1] if '/' in step_name else step_name
                loading_context.step_path = loading_context.step_path.joinpath(step_name)
        step = SfWorkflowStep(toolpath_object, pos, loading_context, parent_workflow_prov)
        if step_id is not None:
            loading_context.step_path = loading_context.step_path.parent
        return step
