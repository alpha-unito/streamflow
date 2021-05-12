import argparse
import logging
import os

import cwltool.context
import cwltool.load_tool
import cwltool.loghandler
import cwltool.process
import cwltool.process
import cwltool.workflow
from cwltool.resolver import tool_resolver

from streamflow.config.config import WorkflowConfig
from streamflow.core.context import StreamFlowContext
from streamflow.cwl.translator import CWLTranslator
from streamflow.workflow.executor import StreamFlowExecutor


def _parse_args(workflow_config: WorkflowConfig, context: StreamFlowContext):
    cwl_config = workflow_config.config
    args = [os.path.join(context.config_dir, cwl_config['file'])]
    if 'settings' in cwl_config:
        args.append(os.path.join(context.config_dir, cwl_config['settings']))
    return args


async def main(workflow_config: WorkflowConfig, context: StreamFlowContext, args: argparse.Namespace):
    # Parse input arguments
    cwl_args = _parse_args(workflow_config, context)
    # Configure log level
    if args.quiet:
        # noinspection PyProtectedMember
        cwltool.loghandler._logger.setLevel(logging.WARN)
    # Load CWL workflow definition
    loading_context = cwltool.context.LoadingContext()
    loading_context.resolver = tool_resolver
    loading_context.loader = cwltool.load_tool.default_loader(
        loading_context.fetcher_constructor
    )
    loading_context, workflowobj, uri = cwltool.load_tool.fetch_document(cwl_args[0], loading_context)
    loading_context, uri = cwltool.load_tool.resolve_and_validate_document(
        loading_context, workflowobj, uri
    )
    cwl_definition = cwltool.load_tool.make_tool(uri, loading_context)
    if len(cwl_args) == 2:
        cwl_inputs, _ = loading_context.loader.resolve_ref(cwl_args[1], checklinks=False)
    else:
        cwl_inputs = {}
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
    await executor.run(output_dir=args.outdir)
