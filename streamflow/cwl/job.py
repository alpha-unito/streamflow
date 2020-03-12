import functools
import logging
import os
import tempfile
import threading
from datetime import datetime
from pathlib import Path
from typing import cast, MutableMapping, List, Any

from cwltool.errors import WorkflowException
from cwltool.job import JobBase
from cwltool.pathmapper import PathMapper
from schema_salad.utils import json_dumps

from streamflow.connector.connector import Connector
from streamflow.cwl.context import SfRuntimeContext
from streamflow.cwl.job_context import SfJobContext
from streamflow.cwl.remote_fs_access import RemoteFsAccess
from streamflow.data import remote_fs
from streamflow.data.data_manager import DataManager
from streamflow.log_handler import _logger
from streamflow.scheduling.scheduler import Scheduler
from streamflow.scheduling.utils import TaskDescription


def _transfer_inputs(job_context: SfJobContext,
                     target: str,
                     pathmapper: PathMapper) -> None:
    _logger.info(
        "Input data transfer for job {name} started at {time}".format(name=job_context.name, time=datetime.now()))
    for key, vol in (itm for itm in pathmapper.items() if itm[1].staged):
        if vol.type in ("File", "Directory", "WritableFile", "WritableDirectory"):
            if vol.resolved.startswith("_:"):
                temp_dir = os.path.join(tempfile.mkdtemp(), os.path.basename(vol.target))
                os.mkdir(temp_dir)
                job_context.data_manager.transfer_data(temp_dir, vol.target, target)
            else:
                if vol.type in ("File", "Directory") and remote_fs.exists(job_context.connector, target, vol.resolved):
                    if not remote_fs.exists(job_context.connector, target, vol.target):
                        parent_dir = str(Path(vol.target).parent)
                        remote_fs.mkdir(job_context.connector, target, parent_dir)
                        remote_fs.symlink(job_context.connector, target, vol.resolved, vol.target)
                else:
                    job_context.data_manager.transfer_data(vol.resolved, vol.target, target)
    _logger.info(
        "Input data transfer for job {name} terminated at {time}".format(name=job_context.name, time=datetime.now()))


class SfCommandLineJob(JobBase):

    def _get_inputs(self):
        for key, vol in (itm for itm in self.pathmapper.items() if itm[1].staged):
            yield vol
        if self.generatemapper is not None:
            for key, vol in (itm for itm in self.generatemapper.items() if itm[1].staged):
                yield vol

    def _register_context(self,
                          connector: Connector,
                          scheduler: Scheduler,
                          data_manager: DataManager,
                          model_name: str,
                          target_resource: str) -> SfJobContext:
        job_context = SfJobContext(
            name=self.name,
            local_outdir=self.outdir,
            remote_outdir=self.builder.outdir,
            model_name=model_name,
            target_resource=target_resource,
            connector=connector,
            scheduler=scheduler,
            data_manager=data_manager
        )
        SfJobContext.register_context(job_context)
        return job_context

    def _setup(self, runtimeContext):  # type: (SfRuntimeContext) -> None
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

        for knownfile in self.pathmapper.files():
            if knownfile in self.pathmapper.remote_files():
                continue
            p = self.pathmapper.mapper(knownfile)
            if p.type == "File" and not os.path.isfile(p[0]) and p.staged:
                raise WorkflowException(
                    u"Input file %s (at %s) not found or is not a regular "
                    "file." % (knownfile, self.pathmapper.mapper(knownfile)[0]))

        if 'listing' in self.generatefiles:
            runtimeContext = runtimeContext.copy()
            runtimeContext.outdir = self.outdir
            self.generatemapper = self.make_path_mapper(
                cast(List[Any], self.generatefiles["listing"]),
                self.builder.outdir, runtimeContext, False)
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    u"[job %s] initial work dir %s", self.name,
                    json_dumps({p: self.generatemapper.mapper(p)
                                for p in self.generatemapper.files()}, indent=4))

    def _setup_files(self,
                     runtime_context: SfRuntimeContext,
                     connector: Connector) -> None:
        out_dir, out_prefix = os.path.split(
            runtime_context.tmp_outdir_prefix)
        self.outdir = runtime_context.outdir or tempfile.mkdtemp(prefix=out_prefix, dir=out_dir)
        self._setup(runtime_context)
        model_name = self.builder.deployment_model['name']
        target_service = self.builder.deployment_target['service']
        target_resource = self._schedule(model_name, target_service, runtime_context,
                                         connector.get_available_resources(target_service))
        remote_fs.mkdir(connector, target_resource, self.builder.outdir)
        remote_fs.mkdir(connector, target_resource, self.builder.tmpdir)
        job_context = self._register_context(connector, runtime_context.scheduler, runtime_context.data_manager,
                                             model_name, target_resource)
        _transfer_inputs(job_context, target_resource, self.pathmapper)
        if self.generatemapper is not None:
            _transfer_inputs(job_context, target_resource, self.generatemapper)

    def _schedule(self,
                  model_name: str,
                  target_service: str,
                  runtime_context: SfRuntimeContext,
                  available_resources: List[str]
                  ) -> str:
        task_description = TaskDescription(self.name)
        for data_dep in self._get_inputs():
            task_description.add_dependency(data_dep.resolved)
        return runtime_context.scheduler.schedule(
            task_description=task_description,
            available_resources=available_resources,
            remote_paths=runtime_context.data_manager.remote_paths,
            model_name=model_name,
            target_service=target_service
        )

    def run(self,
            runtime_context: SfRuntimeContext,
            tmpdir_lock: threading.Lock = None,
            ) -> None:
        model_name = self.builder.deployment_model['name']
        deployment_manager = runtime_context.deployment_manager
        connector = deployment_manager.deploy(model_name,
                                              self.builder.deployment_model['type'],
                                              self.builder.deployment_model['config'],
                                              self.builder.deployment_model.get('external', False))
        self._setup_files(runtime_context, connector)
        job_context = SfJobContext.current_context()
        target_workdir = self.builder.deployment_target.get('workdir', self.builder.outdir)
        runtime = connector.get_runtime(job_context.target_resource, self.environment,
                                        target_workdir).split()
        env = cast(MutableMapping[str, str], os.environ)
        monitor_function = functools.partial(self.process_monitor)
        _logger.info(
            "Job {name} started at {time}".format(name=job_context.name, time=datetime.now()))
        self._execute(runtime, env, runtime_context, monitor_function)
        _logger.info(
            "Job {name} terminated at {time}".format(name=job_context.name, time=datetime.now()))
