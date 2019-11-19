import os
import signal
import sys
from typing import MutableMapping, Any

import cwltool
from cwltool.argparser import arg_parser
from cwltool.command_line_tool import ExpressionTool
from cwltool.errors import WorkflowException
# noinspection PyProtectedMember
from cwltool.main import _terminate_processes, _signal_handler
from cwltool.process import Process

from streamflow.cwl.context import SfLoadingContext, SfRuntimeContext
from streamflow.cwl.executor import StreamflowJobExecutor
from streamflow.cwl.process import SfCommandLineTool, SfWorkflow
from streamflow.cwl.remote_fs_access import RemoteFsAccess


def sf_make_tool(toolpath_object: MutableMapping[str, Any],
                 loading_context: SfLoadingContext
                 ) -> Process:
    if not isinstance(toolpath_object, MutableMapping):
        raise WorkflowException(u"Not a dict: '%s'" % toolpath_object)
    if "class" in toolpath_object:
        if toolpath_object["class"] == "CommandLineTool":
            return SfCommandLineTool(toolpath_object, loading_context)
        if toolpath_object["class"] == "ExpressionTool":
            return ExpressionTool(toolpath_object, loading_context)
        if toolpath_object["class"] == "Workflow":
            return SfWorkflow(toolpath_object, loading_context)


def _parse_args(streamflow_config):
    cwl_config = streamflow_config.config
    streamflow_dir = streamflow_config.config_file['dirname']
    args = [os.path.join(streamflow_dir, cwl_config['file'])]
    if 'settings' in cwl_config:
        args.append(os.path.join(streamflow_dir, cwl_config['settings']))
    return args


def main(streamflow_config):
    args = _parse_args(streamflow_config)
    parsed_args = vars(arg_parser().parse_args(args))
    loading_context = SfLoadingContext(parsed_args)
    loading_context.construct_tool_object = sf_make_tool
    loading_context.streamflow_config = streamflow_config
    runtime_context = SfRuntimeContext(parsed_args)
    runtime_context.streamflow_config = streamflow_config
    runtime_context.make_fs_access = RemoteFsAccess
    runtime_context.compute_checksum = False

    try:
        cwltool.main.main(
            argsl=args,
            loadingContext=loading_context,
            runtimeContext=runtime_context,
            executor=StreamflowJobExecutor()
        )
    finally:
        runtime_context.deployment_manager.undeploy_all()


def run(*args, **kwargs):
    # type: (...) -> None
    """Run cwltool."""
    signal.signal(signal.SIGTERM, _signal_handler)
    try:
        sys.exit(main(*args, **kwargs))
    finally:
        _terminate_processes()
