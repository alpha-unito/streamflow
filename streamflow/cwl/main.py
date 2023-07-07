import argparse
import json
import logging
import os
from pathlib import PurePosixPath

import cwltool.context
import cwltool.load_tool
import cwltool.loghandler
import cwltool.main
import cwltool.utils

from streamflow.config.config import WorkflowConfig
from streamflow.core.context import StreamFlowContext
from streamflow.cwl.requirement.docker.translator import CWLDockerTranslatorConfig
from streamflow.cwl.translator import CWLTranslator
from streamflow.cwl.utils import load_cwl_inputs, load_cwl_workflow
from streamflow.log_handler import logger
from streamflow.workflow.executor import StreamFlowExecutor


def _parse_arg(path: str, context: StreamFlowContext):
    if "://" in path:
        return path
    elif not os.path.isabs(path):
        return os.path.join(os.path.dirname(context.config["path"]), path)
    else:
        return path


def _parse_args(
    workflow_config: WorkflowConfig,
    context: StreamFlowContext,
):
    cwl_config = workflow_config.config
    cwl_config["file"] = _parse_arg(cwl_config["file"], context)
    args = [cwl_config["file"]]
    if "settings" in cwl_config:
        cwl_config["settings"] = _parse_arg(cwl_config["settings"], context)
        args.append(cwl_config["settings"])
    for entry in cwl_config.get("docker", []):
        path = PurePosixPath(entry["step"])
        workflow_config.put(
            path,
            "docker",
            CWLDockerTranslatorConfig(
                name=str(path),
                type=entry["deployment"]["type"],
                config=entry["deployment"].get("config", {}),
                wrapper=entry["deployment"].get("wrapper", True),
            ),
        )
    return args


async def main(
    workflow_config: WorkflowConfig,
    context: StreamFlowContext,
    args: argparse.Namespace,
):
    # Parse input arguments
    cwl_args = _parse_args(workflow_config, context)
    # Configure log level
    if args.quiet:
        # noinspection PyProtectedMember
        cwltool.loghandler._logger.setLevel(logging.WARN)
    # Load CWL workflow definition
    cwl_definition, loading_context = load_cwl_workflow(cwl_args[0])
    if len(cwl_args) == 2:
        cwl_inputs = load_cwl_inputs(loading_context, cwl_definition, cwl_args[1])
    else:
        cwl_inputs = {}
    # Transpile CWL workflow to the StreamFlow representation
    if logger.isEnabledFor(logging.INFO):
        logger.info(f"Processing workflow {args.name}")
    translator = CWLTranslator(
        context=context,
        name=args.name,
        output_directory=args.outdir,
        cwl_definition=cwl_definition,
        cwl_inputs=cwl_inputs,
        workflow_config=workflow_config,
        loading_context=loading_context,
    )
    if logger.isEnabledFor(logging.INFO):
        logger.info("Building workflow execution plan")
    workflow = translator.translate()
    if getattr(args, "validate", False):
        return
    await workflow.save(context)
    if logger.isEnabledFor(logging.INFO):
        logger.info("COMPLETED Building of workflow execution plan")
    executor = StreamFlowExecutor(workflow)
    if logger.isEnabledFor(logging.INFO):
        logger.info(f"Running workflow {args.name}")
    output_tokens = await executor.run()
    if logger.isEnabledFor(logging.INFO):
        logger.info("COMPLETED Workflow execution")
    print(json.dumps(output_tokens, sort_keys=True, indent=4))
