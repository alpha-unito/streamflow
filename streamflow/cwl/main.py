import os
import signal
import sys

import cwltool.context
import cwltool.load_tool
import cwltool.process
import cwltool.process
import cwltool.workflow
import yaml
# noinspection PyProtectedMember
from cwltool.main import _terminate_processes, _signal_handler
from typing_extensions import Text

from streamflow.config.config import WorkflowConfig
from streamflow.core.context import StreamflowContext
from streamflow.cwl.translator import CWLTranslator
from streamflow.workflow.executor import StreamFlowExecutor


def _parse_args(workflow_config: WorkflowConfig, context: StreamflowContext):
    cwl_config = workflow_config.config
    args = [os.path.join(context.config_dir, cwl_config['file'])]
    if 'settings' in cwl_config:
        args.append(os.path.join(context.config_dir, cwl_config['settings']))
    return args


async def main(workflow_config: WorkflowConfig, context: StreamflowContext, outdir: Text):
    # Parse input arguments
    args = _parse_args(workflow_config, context)
    # Change current directory to CWL descriptors' parent dir
    os.chdir(os.path.dirname(args[0]))
    # Load CWL workflow definition
    loading_context = cwltool.context.LoadingContext()
    cwl_definition = cwltool.load_tool.load_tool(args[0], loading_context)
    with open(args[1], 'r') as stream:
        cwl_inputs = yaml.safe_load(stream)
    # Transpile CWL workflow to the StreamFlow representation
    translator = CWLTranslator(
        context=context,
        cwl_definition=cwl_definition,
        cwl_inputs=cwl_inputs,
        workflow_config=workflow_config,
        loading_context=loading_context)
    workflow = await translator.translate()
    # Execute workflow
    executor = StreamFlowExecutor(context, workflow)
    await executor.run(output_dir=outdir)


def run(*args, **kwargs):
    # type: (...) -> None
    """Run cwltool."""
    signal.signal(signal.SIGTERM, _signal_handler)
    try:
        sys.exit(main(*args, **kwargs))
    finally:
        _terminate_processes()
