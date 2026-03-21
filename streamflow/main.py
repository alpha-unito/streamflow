from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import uuid
from collections.abc import MutableMapping, Sequence
from typing import TYPE_CHECKING, Any

from streamflow import report
from streamflow.config.config import WorkflowConfig
from streamflow.config.schema import SfSchema
from streamflow.config.validator import SfValidator
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowProvenanceException
from streamflow.core.provenance import ProvenanceManager
from streamflow.core.workflow import Workflow
from streamflow.cwl.main import main as cwl_main
from streamflow.data import data_manager_classes
from streamflow.deployment import deployment_manager_classes
from streamflow.ext.utils import (
    list_extensions,
    list_plugins,
    load_extensions,
    show_extension,
    show_plugin,
)
from streamflow.log_handler import CustomFormatter, HighlitingFilter, logger
from streamflow.parser import parser
from streamflow.persistence import database_classes
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.provenance import prov_classes
from streamflow.recovery import checkpoint_manager_classes, failure_manager_classes
from streamflow.scheduling import scheduler_classes
from streamflow.version import VERSION

if TYPE_CHECKING:
    from streamflow.core.config import SchemaEntityType


async def _async_ext(args: argparse.Namespace) -> None:
    match args.ext_context:
        case "list":
            list_extensions(args.name, args.type)
        case "show":
            show_extension(args.name, args.type)
        case _:
            parser.print_help(file=sys.stderr)
            raise SystemExit(1)


async def _async_list(args: argparse.Namespace) -> None:
    context = _get_context_from_config(args.file)
    try:
        if workflows := await context.database.get_workflows_list(args.name):
            max_sizes = {
                k: len(max(str(w[k]) for w in workflows)) + 1
                for k in workflows[0].keys()
            }
            if args.name is not None:
                format_string = (
                    "{:<"
                    + str(max(max_sizes["start_time"], 6))
                    + "}"
                    + "{:<"
                    + str(max(max_sizes["end_time"], 6))
                    + "}"
                    + "{:<"
                    + str(max(max_sizes["status"], 6))
                    + "}"
                )
                print(f"NAME: {args.name}")
                print(f"TYPE: {workflows[0]['type']}\n")
                print(format_string.format("START_TIME", "END_TIME", "STATUS"))
                for w in workflows:
                    print(
                        format_string.format(
                            w["start_time"], w["end_time"] or "-", w["status"]
                        )
                    )
            else:
                format_string = (
                    "{:<"
                    + str(max(max_sizes["name"], 4))
                    + "} "
                    + "{:<"
                    + str(max(max_sizes["type"], 4))
                    + "} "
                    + "{:<"
                    + str(max(max_sizes["num"], 4))
                    + "} "
                )
                print(format_string.format("NAME", "TYPE", "EXECUTIONS"))
                for w in workflows:
                    print(format_string.format(w["name"], w["type"], w["num"]))
        else:
            print("No workflow objects found.")
    finally:
        await context.close()


async def _async_plugin(args: argparse.Namespace) -> None:
    match args.plugin_context:
        case "list":
            list_plugins()
        case "show":
            show_plugin(args.plugin)
        case _:
            parser.print_help(file=sys.stderr)
            raise SystemExit(1)


async def _async_prov(args: argparse.Namespace) -> None:
    context = _get_context_from_config(args.file)
    try:
        db_context = DefaultDatabaseLoadingContext()
        workflows = await asyncio.gather(
            *(
                asyncio.create_task(
                    Workflow.load(
                        context=context,
                        persistent_id=w["id"],
                        loading_context=db_context,
                    )
                )
                for w in await context.database.get_workflows_by_name(
                    args.workflow, last_only=not args.all
                )
            )
        )
        wf_type = {w.type for w in workflows}
        if len(wf_type) != 1:
            raise WorkflowProvenanceException(
                "Cannot mix different provenance types in the same file. "
                f"Workflow {args.workflow} is associated to the following types: {','.join(wf_type)}"
            )
        wf_type = next(iter(wf_type))
        if args.type not in prov_classes:
            raise WorkflowProvenanceException(
                f"{args.type} provenance format is not supported."
            )
        elif wf_type not in prov_classes[args.type]:
            raise WorkflowProvenanceException(
                "{} provenance format is not supported for workflows of type {}.".format(
                    args.type, wf_type
                )
            )
        else:
            provenance_manager: ProvenanceManager = prov_classes[args.type][wf_type](
                context, db_context, workflows
            )
            await provenance_manager.create_archive(
                outdir=args.outdir,
                filename=args.name,
                config=args.file if os.path.exists(args.file) else None,
                additional_files=args.add_file,
                additional_properties=args.add_property,
            )
    finally:
        await context.close()


async def _async_report(args: argparse.Namespace) -> None:
    context = _get_context_from_config(args.file)
    try:
        await report.create_report(context, args)
    finally:
        await context.close()


async def _async_run(args: argparse.Namespace) -> None:
    args.name = args.name or str(uuid.uuid4())
    load_extensions()
    streamflow_config = SfValidator().validate_file(args.streamflow_file)
    streamflow_config["path"] = args.streamflow_file
    context = build_context(streamflow_config)
    try:
        workflow_tasks = []
        for workflow in streamflow_config.get("workflows", {}):
            workflow_config = WorkflowConfig(workflow, streamflow_config)
            if workflow_config.type == "cwl":
                workflow_tasks.append(
                    asyncio.create_task(cwl_main(workflow_config, context, args))
                )
            await asyncio.gather(*workflow_tasks)
    finally:
        await context.close()


def _get_context_from_config(streamflow_file: str | None) -> StreamFlowContext:
    if streamflow_file is not None and os.path.exists(streamflow_file):
        load_extensions()
        streamflow_config = SfValidator().validate_file(streamflow_file)
        streamflow_config["path"] = streamflow_file
        return build_context(streamflow_config)
    else:
        return build_context({"path": os.getcwd()})


def _get_class_from_config(
    streamflow_config: MutableMapping[str, Any],
    classes: MutableMapping[str, type[SchemaEntityType]],
    instance_type: str,
    enabled_by_default: bool = True,
) -> type[SchemaEntityType]:
    if (config := streamflow_config.get(instance_type)) is not None:
        enabled = config.get("enabled", enabled_by_default)
        class_name = config.get("type", "default" if enabled else "dummy")
    else:
        class_name = "default" if enabled_by_default else "dummy"
    return classes[class_name]


def build_context(config: MutableMapping[str, Any]) -> StreamFlowContext:
    return StreamFlowContext(
        config=config,
        checkpoint_manager_class=_get_class_from_config(
            config,
            checkpoint_manager_classes,
            "checkpointManager",
            enabled_by_default=False,
        ),
        database_class=_get_class_from_config(
            config,
            database_classes,
            "database",
        ),
        data_manager_class=_get_class_from_config(
            config,
            data_manager_classes,
            "dataManager",
        ),
        deployment_manager_class=_get_class_from_config(
            config,
            deployment_manager_classes,
            "deploymentManager",
        ),
        failure_manager_class=_get_class_from_config(
            config,
            failure_manager_classes,
            "failureManager",
            enabled_by_default=False,
        ),
        scheduler_class=_get_class_from_config(
            config.get("scheduling", {}), scheduler_classes, "scheduler"
        ),
    )


def main(args: Sequence[str]) -> int:
    try:
        parsed_args = parser.parse_args(args)
        match parsed_args.context:
            case "ext":
                asyncio.run(_async_ext(parsed_args))
            case "list":
                asyncio.run(_async_list(parsed_args))
            case "plugin":
                asyncio.run(_async_plugin(parsed_args))
            case "prov":
                asyncio.run(_async_prov(parsed_args))
            case "report":
                asyncio.run(_async_report(parsed_args))
            case "run":
                if parsed_args.quiet:
                    logger.setLevel(logging.WARNING)
                elif parsed_args.debug:
                    logger.setLevel(logging.DEBUG)
                if (
                    parsed_args.color
                    and hasattr(sys.stdout, "isatty")
                    and sys.stdout.isatty()
                ):
                    colored_stream_handler = logging.StreamHandler()
                    colored_stream_handler.setFormatter(CustomFormatter())
                    logger.handlers = []
                    logger.addHandler(colored_stream_handler)
                    logger.addFilter(HighlitingFilter())
                asyncio.run(_async_run(parsed_args))
            case "schema":
                load_extensions()
                print(SfSchema().dump(parsed_args.version, parsed_args.pretty))
            case "version":
                print(f"StreamFlow version {VERSION}")
            case _:
                parser.print_help(file=sys.stderr)
                return 1
        return 0
    except SystemExit as se:
        if se.code != 0:
            logger.exception(se)
        return int(se.code) if se.code is not None else 1
    except Exception as e:
        logger.exception(e)
        return 1


def run() -> int:
    return main(sys.argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
