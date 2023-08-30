import argparse
import asyncio
import logging
import os
import sys
import uuid

import streamflow.cwl.main
import streamflow.ext
from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.ext.utils import load_extensions
from streamflow.log_handler import logger
from streamflow.main import build_context

parser = argparse.ArgumentParser(description="cwl-runner interface")
parser.add_argument(
    "processfile",
    nargs="?",
    type=str,
    help="The CommandLineTool, ExpressionTool, or Workflow description to run. "
    "Optional if the jobfile has a `cwl:tool` field to indicate which process description to run",
)
parser.add_argument("jobfile", nargs="?", type=str, help="The input job document")
parser.add_argument(
    "--name",
    nargs="?",
    type=str,
    help="Name of the current workflow. Will be used for search and indexing",
)
parser.add_argument(
    "--outdir",
    default=os.getcwd(),
    type=str,
    help="Output directory, defaults to the current directory",
)
parser.add_argument(
    "--debug", action="store_true", help="Debug-level diagnostic output"
)
parser.add_argument("--quiet", action="store_true", help="No diagnostic output")
parser.add_argument(
    "--validate", action="store_true", help="Validate CWL document only"
)
parser.add_argument(
    "--version",
    action="store_true",
    help="Report the name and version, then quit without further processing",
)
parser.add_argument(
    "--streamflow-file",
    type=str,
    help="The path to a StreamFlow file specifying deployments and bindings for the workflow steps",
)


async def _async_main(args: argparse.Namespace):
    load_extensions()
    validator = SfValidator()
    args.name = args.name or str(uuid.uuid4())
    if args.streamflow_file:
        with open(args.streamflow_file) as f:
            streamflow_config = validator.yaml.load(f)
        workflows = streamflow_config.get("workflows", {})
        if len(workflows) == 1:
            workflow_name = list(workflows.keys())[0]
        elif len(workflows) == 0:
            workflow_name = "cwl-workflow"
            streamflow_config.setdefault("workflows", {})[workflow_name] = {}
        else:
            raise WorkflowDefinitionException(
                "A StreamFlow file must contain only one workflow definition when used with cwl-runner."
            )
    else:
        workflow_name = "cwl-workflow"
        streamflow_config = {"version": "v1.0", "workflows": {workflow_name: {}}}
    streamflow_config["workflows"][workflow_name]["type"] = "cwl"
    streamflow_config["workflows"][workflow_name].setdefault("config", {}).update(
        {"file": os.path.abspath(args.processfile)}
    )
    if args.jobfile:
        streamflow_config["workflows"][workflow_name]["config"][
            "settings"
        ] = os.path.abspath(args.jobfile)
    validator.validate(streamflow_config)
    streamflow_config["path"] = (
        args.streamflow_file if args.streamflow_file is not None else os.getcwd()
    )
    workflow_config = WorkflowConfig(workflow_name, streamflow_config)
    context = build_context(streamflow_config)
    try:
        await streamflow.cwl.main.main(
            workflow_config=workflow_config, context=context, args=args
        )
    finally:
        await context.close()


def main(args) -> int:
    try:
        args = parser.parse_args(args)
        if args.version:
            from streamflow.version import VERSION

            print(f"StreamFlow version {VERSION}")
            return 0
        if args.quiet:
            logger.setLevel(logging.WARN)
        elif args.debug:
            logger.setLevel(logging.DEBUG)
        asyncio.run(_async_main(args))
        return 0
    except SystemExit as se:
        if se.code != 0:
            logger.exception(se)
        return se.code
    except Exception as e:
        logger.exception(e)
        return 1
    except KeyboardInterrupt:
        return 1


def run():
    return main(sys.argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
