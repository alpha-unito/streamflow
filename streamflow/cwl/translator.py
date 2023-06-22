from __future__ import annotations

import copy
import logging
import os
import posixpath
import urllib.parse
from enum import Enum
from pathlib import PurePosixPath
from types import ModuleType
from typing import (
    Any,
    MutableMapping,
    MutableSequence,
    cast,
)

import cwltool.command_line_tool
import cwltool.context
import cwltool.docker_id
import cwltool.process
import cwltool.workflow
from rdflib import Graph
from ruamel.yaml.comments import CommentedSeq

from streamflow.config.config import WorkflowConfig
from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    DeploymentConfig,
    LOCAL_LOCATION,
    LocalTarget,
    Location,
    Target,
)
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.workflow import (
    CommandOutputProcessor,
    Port,
    Step,
    Token,
    TokenProcessor,
    Workflow,
)
from streamflow.cwl import utils
from streamflow.cwl.combinator import ListMergeCombinator
from streamflow.cwl.command import (
    CWLCommand,
    CWLCommandToken,
    CWLExpressionCommand,
    CWLMapCommandToken,
    CWLObjectCommandToken,
    CWLUnionCommandToken,
)
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.processor import (
    CWLCommandOutputProcessor,
    CWLMapCommandOutputProcessor,
    CWLMapTokenProcessor,
    CWLObjectCommandOutputProcessor,
    CWLObjectTokenProcessor,
    CWLTokenProcessor,
    CWLUnionCommandOutputProcessor,
    CWLUnionTokenProcessor,
)
from streamflow.cwl.requirement.docker import cwl_docker_translator_classes
from streamflow.cwl.requirement.docker.translator import (
    CWLDockerTranslator,
    CWLDockerTranslatorConfig,
)
from streamflow.cwl.step import (
    CWLConditionalStep,
    CWLEmptyScatterConditionalStep,
    CWLInputInjectorStep,
    CWLLoopConditionalStep,
    CWLLoopOutputAllStep,
    CWLLoopOutputLastStep,
    CWLTransferStep,
)
from streamflow.cwl.transformer import (
    AllNonNullTransformer,
    CWLTokenTransformer,
    DefaultRetagTransformer,
    DefaultTransformer,
    FirstNonNullTransformer,
    ForwardTransformer,
    ListToElementTransformer,
    LoopValueFromTransformer,
    OnlyNonNullTransformer,
    ValueFromTransformer,
)
from streamflow.cwl.utils import LoadListing, SecondaryFile, resolve_dependencies
from streamflow.deployment.utils import get_binding_config
from streamflow.log_handler import logger
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.step import (
    Combinator,
    CombinatorStep,
    DeployStep,
    ExecuteStep,
    GatherStep,
    LoopCombinatorStep,
    ScatterStep,
    ScheduleStep,
    Transformer,
)
from streamflow.workflow.token import TerminationToken


def _create_command(
    cwl_element: cwltool.command_line_tool.CommandLineTool,
    cwl_name_prefix: str,
    schema_def_types: MutableMapping[str, Any],
    context: MutableMapping[str, Any],
    step: Step,
) -> CWLCommand:
    command = CWLCommand(step)
    # Process InitialWorkDirRequirement
    requirements = {**context["hints"], **context["requirements"]}
    if "InitialWorkDirRequirement" in requirements:
        command.initial_work_dir = requirements["InitialWorkDirRequirement"]["listing"]
        command.absolute_initial_workdir_allowed = (
            "DockerRequirement" in context["requirements"]
        )
    # Process InplaceUpdateRequirement
    if "InplaceUpdateRequirement" in requirements:
        command.inplace_update = requirements["InplaceUpdateRequirement"][
            "inplaceUpdate"
        ]
    # Process EnvVarRequirement
    if "EnvVarRequirement" in requirements:
        for env_entry in requirements["EnvVarRequirement"]["envDef"]:
            command.environment[env_entry["envName"]] = env_entry["envValue"]
    # Process ShellCommandRequirement
    if "ShellCommandRequirement" in requirements:
        command.is_shell_command = True
    # Process success and failure codes
    if "successCodes" in cwl_element.tool:
        command.success_codes = cwl_element.tool["successCodes"]
    if "permanentFailCodes" or "temporaryFailCodes" in cwl_element.tool:
        command.failure_codes = cwl_element.tool.get("permanentFailCodes", [])
        command.failure_codes.extend(cwl_element.tool.get("temporaryFailCodes", []))
    # Process baseCommand
    if "baseCommand" in cwl_element.tool:
        if isinstance(cwl_element.tool["baseCommand"], CommentedSeq):
            for command_token in cwl_element.tool["baseCommand"]:
                command.base_command.append(command_token)
        else:
            command.base_command.append(cwl_element.tool["baseCommand"])
    # Process arguments
    if "arguments" in cwl_element.tool:
        for argument in cwl_element.tool["arguments"]:
            command.command_tokens.append(
                _get_command_token(
                    binding=argument, is_shell_command=command.is_shell_command
                )
            )
    # Process inputs
    for input_port in cwl_element.tool["inputs"]:
        command_token = _get_command_token_from_input(
            cwl_element=input_port,
            port_type=input_port["type"],
            input_name=utils.get_name("", cwl_name_prefix, input_port["id"]),
            is_shell_command=command.is_shell_command,
            schema_def_types=schema_def_types,
        )
        if command_token is not None:
            command.command_tokens.append(command_token)
    return command


def _create_command_output_processor_base(
    port_name: str,
    workflow: Workflow,
    port_target: Target | None,
    port_type: Any,
    cwl_element: MutableMapping[str, Any],
    context: MutableMapping[str, Any],
    optional: bool = False,
) -> CWLCommandOutputProcessor:
    if not isinstance(port_type, MutableSequence):
        port_type = [port_type]
    # Normalize port type (Python does not distinguish among all CWL number types)
    port_type = [
        "long" if t == "int" else "double" if t == "float" else t for t in port_type
    ]
    # Process InlineJavascriptRequirement
    requirements = {**context["hints"], **context["requirements"]}
    expression_lib, full_js = _process_javascript_requirement(requirements)
    # Create OutputProcessor
    if "File" in port_type:
        return CWLCommandOutputProcessor(
            name=port_name,
            workflow=workflow,
            target=port_target,
            token_type=port_type[0] if len(port_type) == 1 else port_type,
            expression_lib=expression_lib,
            file_format=cwl_element.get("format"),
            full_js=full_js,
            glob=cwl_element.get("outputBinding", {}).get(
                "glob", cwl_element.get("path")
            ),
            load_contents=cwl_element.get(
                "loadContents",
                cwl_element.get("outputBinding", {}).get("loadContents", False),
            ),
            load_listing=_get_load_listing(cwl_element, context),
            optional=optional,
            output_eval=cwl_element.get("outputBinding", {}).get("outputEval"),
            secondary_files=_get_secondary_files(
                cwl_element.get("secondaryFiles"), default_required=False
            ),
            streamable=cwl_element.get("streamable", False),
        )
    else:
        return CWLCommandOutputProcessor(
            name=port_name,
            workflow=workflow,
            target=port_target,
            token_type=port_type[0] if len(port_type) == 1 else port_type,
            expression_lib=expression_lib,
            full_js=full_js,
            glob=cwl_element.get("outputBinding", {}).get(
                "glob", cwl_element.get("path")
            ),
            load_contents=cwl_element.get(
                "loadContents",
                cwl_element.get("outputBinding", {}).get("loadContents", False),
            ),
            load_listing=_get_load_listing(cwl_element, context),
            optional=optional,
            output_eval=cwl_element.get("outputBinding", {}).get("outputEval"),
        )


def _create_command_output_processor(
    port_name: str,
    workflow: Workflow,
    port_target: Target | None,
    port_type: Any,
    cwl_element: MutableMapping[str, Any],
    cwl_name_prefix: str,
    schema_def_types: MutableMapping[str, Any],
    context: MutableMapping[str, Any],
    optional: bool = False,
) -> CommandOutputProcessor:
    if isinstance(port_type, MutableMapping):
        if "type" in port_type:
            # Array type: -> MapCommandOutputProcessor
            if port_type["type"] == "array":
                return CWLMapCommandOutputProcessor(
                    name=port_name,
                    workflow=workflow,
                    processor=_create_command_output_processor(
                        port_name=port_name,
                        workflow=workflow,
                        port_target=port_target,
                        port_type=port_type["items"],
                        cwl_element=cwl_element,
                        cwl_name_prefix=cwl_name_prefix,
                        schema_def_types=schema_def_types,
                        context=context,
                        optional=optional,
                    ),
                )
            # Enum type: -> create command output processor
            elif port_type["type"] == "enum":
                # Process InlineJavascriptRequirement
                requirements = {**context["hints"], **context["requirements"]}
                expression_lib, full_js = _process_javascript_requirement(requirements)
                enum_prefix = (
                    utils.get_name(posixpath.sep, posixpath.sep, port_type["name"])
                    if "name" in port_type
                    else cwl_name_prefix
                )
                # Return OutputProcessor
                return CWLCommandOutputProcessor(
                    name=port_name,
                    workflow=workflow,
                    target=port_target,
                    token_type=port_type["type"],
                    enum_symbols=[
                        posixpath.relpath(
                            utils.get_name(posixpath.sep, posixpath.sep, s), enum_prefix
                        )
                        for s in port_type["symbols"]
                    ],
                    expression_lib=expression_lib,
                    full_js=full_js,
                    optional=optional,
                )
            # Record type: -> ObjectCommandOutputProcessor
            elif port_type["type"] == "record":
                # Process InlineJavascriptRequirement
                requirements = {**context["hints"], **context["requirements"]}
                expression_lib, full_js = _process_javascript_requirement(requirements)
                # Create processor
                record_name_prefix = utils.get_name(
                    posixpath.sep, posixpath.sep, port_type.get("name", port_name)
                )
                return CWLObjectCommandOutputProcessor(
                    name=port_name,
                    workflow=workflow,
                    processors={
                        utils.get_name(
                            "", record_name_prefix, port_type["name"]
                        ): _create_command_output_processor(
                            port_name=port_name,
                            port_target=port_target,
                            workflow=workflow,
                            port_type=port_type["type"],
                            cwl_element=port_type,
                            cwl_name_prefix=posixpath.join(
                                record_name_prefix,
                                utils.get_name(
                                    "", record_name_prefix, port_type["name"]
                                ),
                            ),
                            schema_def_types=schema_def_types,
                            context=context,
                        )
                        for port_type in port_type["fields"]
                    },
                    expression_lib=expression_lib,
                    full_js=full_js,
                    output_eval=cwl_element.get("outputBinding", {}).get("outputEval"),
                )
            # Unknown type -> raise Exception
            else:
                raise WorkflowDefinitionException(
                    f"Unsupported type {port_type['type']} for port {port_name}."
                )
        # Untyped object -> not supported
        else:
            raise WorkflowDefinitionException(
                "Unsupported dictionary type without explicit `type` key"
            )
    elif isinstance(port_type, MutableSequence):
        optional = "null" in port_type
        types = [t for t in filter(lambda x: x != "null", port_type)]
        # Optional type (e.g. ['null', Type] -> Equivalent to Type?
        if len(types) == 1:
            return _create_command_output_processor(
                port_name=port_name,
                workflow=workflow,
                port_target=port_target,
                port_type=types[0],
                cwl_element=cwl_element,
                cwl_name_prefix=cwl_name_prefix,
                schema_def_types=schema_def_types,
                context=context,
                optional=optional,
            )
        # List of types: -> UnionOutputProcessor
        else:
            types = [
                schema_def_types[t] if isinstance(t, str) and "#" in t else t
                for t in types
            ]
            complex_types = [t for t in types if isinstance(t, MutableMapping)]
            simple_types = [t for t in types if t not in complex_types]
            if complex_types:
                processors = [
                    _create_command_output_processor(
                        port_name=port_name,
                        workflow=workflow,
                        port_target=port_target,
                        port_type=port_type,
                        cwl_element=cwl_element,
                        cwl_name_prefix=cwl_name_prefix,
                        schema_def_types=schema_def_types,
                        context=context,
                        optional=optional,
                    )
                    for port_type in complex_types
                ]
            else:
                processors = []
            if simple_types:
                processors.append(
                    _create_command_output_processor_base(
                        port_name=port_name,
                        workflow=workflow,
                        port_target=port_target,
                        port_type=simple_types,
                        cwl_element=cwl_element,
                        context=context,
                        optional=optional,
                    )
                )
            return CWLUnionCommandOutputProcessor(
                name=port_name,
                workflow=workflow,
                processors=processors,
            )
    # Complex type -> Extract from schema definitions and propagate
    elif "#" in port_type:
        return _create_command_output_processor(
            port_name=port_name,
            workflow=workflow,
            port_target=port_target,
            port_type=schema_def_types[port_type],
            cwl_element=cwl_element,
            cwl_name_prefix=cwl_name_prefix,
            schema_def_types=schema_def_types,
            context=context,
            optional=optional,
        )
    # Simple type -> Create typed processor
    else:
        return _create_command_output_processor_base(
            port_name=port_name,
            workflow=workflow,
            port_target=port_target,
            port_type=port_type,
            cwl_element=cwl_element,
            context=context,
            optional=optional,
        )


def _create_context() -> MutableMapping[str, Any]:
    return {"default": {}, "requirements": {}, "hints": {}}


def _create_list_merger(
    name: str,
    workflow: Workflow,
    ports: MutableMapping[str, Port],
    output_port: Port | None = None,
    link_merge: str | None = None,
    pick_value: str | None = None,
) -> Step:
    combinator = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=ListMergeCombinator(
            name=utils.random_name(),
            workflow=workflow,
            input_names=list(ports.keys()),
            output_name=name,
            flatten=(link_merge == "merge_flattened"),
        ),
    )
    for port_name, port in ports.items():
        combinator.add_input_port(port_name, port)
        combinator.combinator.add_item(port_name)
    if pick_value == "first_non_null":
        combinator.add_output_port(name, workflow.create_port())
        transformer = workflow.create_step(
            cls=FirstNonNullTransformer, name=name + "-transformer"
        )
        transformer.add_input_port(name, combinator.get_output_port())
        transformer.add_output_port(name, output_port or workflow.create_port())
        return transformer
    elif pick_value == "the_only_non_null":
        combinator.add_output_port(name, workflow.create_port())
        transformer = workflow.create_step(
            cls=OnlyNonNullTransformer, name=name + "-transformer"
        )
        transformer.add_input_port(name, combinator.get_output_port())
        transformer.add_output_port(name, output_port or workflow.create_port())
        return transformer
    elif pick_value == "all_non_null":
        combinator.add_output_port(name, workflow.create_port())
        transformer = workflow.create_step(
            cls=AllNonNullTransformer, name=name + "-transformer"
        )
        if link_merge is None:
            list_to_element = workflow.create_step(
                cls=ListToElementTransformer, name=name + "-list-to-element"
            )
            list_to_element.add_input_port(name, combinator.get_output_port())
            list_to_element.add_output_port(name, workflow.create_port())
            transformer.add_input_port(name, list_to_element.get_output_port())
            transformer.add_output_port(name, output_port or workflow.create_port())
        else:
            transformer.add_input_port(name, combinator.get_output_port())
            transformer.add_output_port(name, output_port or workflow.create_port())
        return transformer
    elif link_merge is None:
        combinator.add_output_port(name, workflow.create_port())
        list_to_element = workflow.create_step(
            cls=ListToElementTransformer, name=name + "-list-to-element"
        )
        list_to_element.add_input_port(name, combinator.get_output_port())
        list_to_element.add_output_port(name, output_port or workflow.create_port())
        return list_to_element
    else:
        combinator.add_output_port(name, output_port or workflow.create_port())
        return combinator


def _create_residual_combinator(
    workflow: Workflow,
    step_name: str,
    inner_combinator: Combinator,
    inner_inputs: MutableSequence[str],
    input_ports: MutableMapping[str, Port],
) -> Combinator:
    dot_product_combinator = DotProductCombinator(
        workflow=workflow, name=step_name + "-dot-product-combinator"
    )
    if inner_combinator:
        dot_product_combinator.add_combinator(
            inner_combinator, inner_combinator.get_items(recursive=True)
        )
    else:
        for global_name in inner_inputs:
            dot_product_combinator.add_item(posixpath.relpath(global_name, step_name))
    for global_name in input_ports:
        if global_name not in inner_inputs:
            dot_product_combinator.add_item(posixpath.relpath(global_name, step_name))
    return dot_product_combinator


def _create_token_processor(
    port_name: str,
    workflow: Workflow,
    port_type: Any,
    cwl_element: MutableMapping[str, Any],
    cwl_name_prefix: str,
    schema_def_types: MutableMapping[str, Any],
    format_graph: Graph,
    context: MutableMapping[str, Any],
    optional: bool = False,
    default_required_sf: bool = True,
    check_type: bool = True,
    force_deep_listing: bool = False,
    only_propagate_secondary_files: bool = True,
) -> TokenProcessor:
    if isinstance(port_type, MutableMapping):
        if "type" in port_type:
            # Array type: -> MapTokenProcessor
            if port_type["type"] == "array":
                return CWLMapTokenProcessor(
                    name=port_name,
                    workflow=workflow,
                    processor=_create_token_processor(
                        port_name=port_name,
                        workflow=workflow,
                        port_type=port_type["items"],
                        cwl_element=cwl_element,
                        cwl_name_prefix=cwl_name_prefix,
                        schema_def_types=schema_def_types,
                        format_graph=format_graph,
                        context=context,
                        optional=optional,
                        check_type=check_type,
                        force_deep_listing=force_deep_listing,
                        only_propagate_secondary_files=only_propagate_secondary_files,
                    ),
                )
            # Enum type: -> create output processor
            elif port_type["type"] == "enum":
                # Process InlineJavascriptRequirement
                requirements = {**context["hints"], **context["requirements"]}
                expression_lib, full_js = _process_javascript_requirement(requirements)
                enum_prefix = (
                    utils.get_name(posixpath.sep, posixpath.sep, port_type["name"])
                    if "name" in port_type
                    else cwl_name_prefix
                )
                # Return TokenProcessor
                return CWLTokenProcessor(
                    name=port_name,
                    workflow=workflow,
                    token_type=port_type["type"],
                    check_type=check_type,
                    enum_symbols=[
                        posixpath.relpath(
                            utils.get_name(posixpath.sep, posixpath.sep, s), enum_prefix
                        )
                        for s in port_type["symbols"]
                    ],
                    expression_lib=expression_lib,
                    full_js=full_js,
                    optional=optional,
                )
            # Record type: -> ObjectTokenProcessor
            elif port_type["type"] == "record":
                record_name_prefix = utils.get_name(
                    posixpath.sep, posixpath.sep, port_type.get("name", port_name)
                )
                return CWLObjectTokenProcessor(
                    name=port_name,
                    workflow=workflow,
                    processors={
                        utils.get_name(
                            "", record_name_prefix, port_type["name"]
                        ): _create_token_processor(
                            port_name=port_name,
                            workflow=workflow,
                            port_type=port_type["type"],
                            cwl_element=port_type,
                            cwl_name_prefix=posixpath.join(
                                record_name_prefix,
                                utils.get_name(
                                    "", record_name_prefix, port_type["name"]
                                ),
                            ),
                            schema_def_types=schema_def_types,
                            format_graph=format_graph,
                            context=context,
                            check_type=check_type,
                            force_deep_listing=force_deep_listing,
                            only_propagate_secondary_files=only_propagate_secondary_files,
                        )
                        for port_type in port_type["fields"]
                    },
                )
            # Unknown type -> raise Exception
            else:
                raise WorkflowDefinitionException(
                    f"Unsupported type {port_type['type']} for port {port_name}."
                )
        # Untyped object -> not supported
        else:
            raise WorkflowDefinitionException(
                "Unsupported dictionary type without explicit `type` key"
            )
    elif isinstance(port_type, MutableSequence):
        optional = "null" in port_type
        types = [t for t in filter(lambda x: x != "null", port_type)]
        # Optional type (e.g. ['null', Type] -> Equivalent to Type?
        if len(types) == 1:
            return _create_token_processor(
                port_name=port_name,
                workflow=workflow,
                port_type=types[0],
                cwl_element=cwl_element,
                cwl_name_prefix=cwl_name_prefix,
                schema_def_types=schema_def_types,
                format_graph=format_graph,
                context=context,
                optional=optional,
                check_type=check_type,
                force_deep_listing=force_deep_listing,
                only_propagate_secondary_files=only_propagate_secondary_files,
            )
        # List of types: -> UnionOutputProcessor
        else:
            return CWLUnionTokenProcessor(
                name=port_name,
                workflow=workflow,
                processors=[
                    _create_token_processor(
                        port_name=port_name,
                        workflow=workflow,
                        port_type=port_type,
                        cwl_element=cwl_element,
                        cwl_name_prefix=cwl_name_prefix,
                        schema_def_types=schema_def_types,
                        format_graph=format_graph,
                        context=context,
                        optional=optional,
                        check_type=check_type,
                        force_deep_listing=force_deep_listing,
                        only_propagate_secondary_files=only_propagate_secondary_files,
                    )
                    for port_type in types
                ],
            )
    # Complex type -> Extract from schema definitions and propagate
    elif "#" in port_type:
        return _create_token_processor(
            port_name=port_name,
            workflow=workflow,
            port_type=schema_def_types[port_type],
            cwl_element=cwl_element,
            cwl_name_prefix=cwl_name_prefix,
            schema_def_types=schema_def_types,
            format_graph=format_graph,
            context=context,
            optional=optional,
            check_type=check_type,
            force_deep_listing=force_deep_listing,
            only_propagate_secondary_files=only_propagate_secondary_files,
        )
    # Simple type -> Create typed processor
    else:
        # Process InlineJavascriptRequirement
        requirements = {**context["hints"], **context["requirements"]}
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Create OutputProcessor
        if port_type == "File":
            return CWLTokenProcessor(
                name=port_name,
                workflow=workflow,
                token_type=port_type,
                check_type=check_type,
                expression_lib=expression_lib,
                file_format=cwl_element.get("format"),
                format_graph=format_graph,
                full_js=full_js,
                load_contents=cwl_element.get(
                    "loadContents",
                    cwl_element.get("inputBinding", {}).get("loadContents"),
                ),
                load_listing=(
                    LoadListing.deep_listing
                    if force_deep_listing
                    else _get_load_listing(cwl_element, context)
                ),
                only_propagate_secondary_files=only_propagate_secondary_files,
                optional=optional,
                secondary_files=_get_secondary_files(
                    cwl_element=cwl_element.get("secondaryFiles"),
                    default_required=default_required_sf,
                ),
                streamable=cwl_element.get("streamable", False),
            )
        else:
            # Normalize port type (Python does not distinguish among all CWL number types)
            port_type = (
                "long"
                if port_type == "int"
                else "double"
                if port_type == "float"
                else port_type
            )
            return CWLTokenProcessor(
                name=port_name,
                workflow=workflow,
                token_type=port_type,
                check_type=check_type,
                expression_lib=expression_lib,
                full_js=full_js,
                load_contents=cwl_element.get(
                    "loadContents",
                    cwl_element.get("inputBinding", {}).get("loadContents"),
                ),
                load_listing=(
                    LoadListing.deep_listing
                    if force_deep_listing
                    else _get_load_listing(cwl_element, context)
                ),
                optional=optional,
            )


def _create_token_transformer(
    name: str,
    port_name: str,
    workflow: Workflow,
    cwl_element: MutableMapping[str, Any],
    cwl_name_prefix: str,
    schema_def_types: MutableMapping[str, Any],
    format_graph: Graph,
    context: MutableMapping[str, Any],
    check_type: bool = True,
    only_propagate_secondary_files: bool = True,
) -> CWLTokenTransformer:
    token_transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name,
        port_name=port_name,
        processor=_create_token_processor(
            port_name=port_name,
            workflow=workflow,
            port_type=cwl_element["type"],
            cwl_element=cwl_element,
            cwl_name_prefix=cwl_name_prefix,
            schema_def_types=schema_def_types,
            format_graph=format_graph,
            context=context,
            check_type=check_type,
            only_propagate_secondary_files=only_propagate_secondary_files,
        ),
    )
    token_transformer.add_output_port(port_name, workflow.create_port())
    return token_transformer


def _get_command_token(
    binding: Any,
    is_shell_command: bool = False,
    input_name: str | None = None,
    token_type: str | None = None,
) -> CWLCommandToken:
    # Normalize type (Python does not distinguish among all CWL number types)
    token_type = (
        "long"
        if token_type == "int"  # nosec
        else "double"
        if token_type == "float"  # nosec
        else token_type
    )
    if isinstance(binding, MutableMapping):
        item_separator = binding.get("itemSeparator", None)
        position = binding.get("position", 0)
        prefix = binding.get("prefix", None)
        separate = binding.get("separate", True)
        shell_quote = binding.get("shellQuote", True)
        value = binding["valueFrom"] if "valueFrom" in binding else None
        return CWLCommandToken(
            name=input_name,
            value=value,
            token_type=token_type,
            is_shell_command=is_shell_command,
            item_separator=item_separator,
            position=position,
            prefix=prefix,
            separate=separate,
            shell_quote=shell_quote,
        )
    else:
        return CWLCommandToken(name=input_name, value=binding, token_type=token_type)


def _get_command_token_from_input(
    cwl_element: Any,
    port_type: Any,
    input_name: str,
    is_shell_command: bool = False,
    schema_def_types: MutableMapping[str, Any] | None = None,
):
    token = None
    command_line_binding = cwl_element.get("inputBinding", None)
    if isinstance(port_type, MutableMapping):
        if "type" in port_type:
            # Array type: -> CWLMapCommandToken
            if port_type["type"] == "array":
                token = _get_command_token_from_input(
                    cwl_element=port_type,
                    port_type=port_type["items"],
                    input_name=input_name,
                    is_shell_command=is_shell_command,
                    schema_def_types=schema_def_types,
                )
                if token is not None:
                    token = CWLMapCommandToken(
                        name=input_name, value=token, is_shell_command=is_shell_command
                    )
            # Enum type: -> substitute the type with string and reprocess
            elif port_type["type"] == "enum":
                return _get_command_token_from_input(
                    cwl_element=cwl_element,
                    port_type="string",
                    input_name=input_name,
                    is_shell_command=is_shell_command,
                    schema_def_types=schema_def_types,
                )
            # Object type: -> CWLObjectCommandToken
            elif port_type["type"] == "record":
                record_name_prefix = utils.get_name(
                    posixpath.sep, posixpath.sep, port_type.get("name", input_name)
                )
                command_tokens = {}
                for el in port_type["fields"]:
                    key = utils.get_name("", record_name_prefix, el["name"])
                    el_token = _get_command_token_from_input(
                        cwl_element=el,
                        port_type=el["type"],
                        input_name=key,
                        is_shell_command=is_shell_command,
                        schema_def_types=schema_def_types,
                    )
                    if el_token is not None:
                        command_tokens[key] = el_token
                if command_tokens:
                    token = CWLObjectCommandToken(
                        name=input_name,
                        value=command_tokens,
                        is_shell_command=True,
                        shell_quote=False,
                    )
            # Unknown type -> raise Exception
            else:
                raise WorkflowDefinitionException(
                    f"Unsupported type {port_type['type']} for port {input_name}."
                )
        # Untyped object -> not supported
        else:
            raise WorkflowDefinitionException(
                "Unsupported dictionary type without explicit `type` key"
            )
    elif isinstance(port_type, MutableSequence):
        types = [t for t in filter(lambda x: x != "null", port_type)]
        # Optional type (e.g. ['null', Type] -> propagate
        if len(types) == 1:
            return _get_command_token_from_input(
                cwl_element=cwl_element,
                port_type=types[0],
                input_name=input_name,
                is_shell_command=is_shell_command,
                schema_def_types=schema_def_types,
            )
        # List of types: -> CWLUnionCommandToken
        else:
            command_tokens = []
            for port_type in types:
                token = _get_command_token_from_input(
                    cwl_element=cwl_element,
                    port_type=port_type,
                    input_name=input_name,
                    is_shell_command=is_shell_command,
                    schema_def_types=schema_def_types,
                )
                if token is not None:
                    command_tokens.append(token)
            if command_tokens:
                token = CWLUnionCommandToken(
                    name=input_name,
                    value=command_tokens,
                    is_shell_command=True,
                    shell_quote=False,
                )
    elif isinstance(port_type, str):
        # Complex type -> Extract from schema definitions and propagate
        if "#" in port_type:
            return _get_command_token_from_input(
                cwl_element=cwl_element,
                port_type=schema_def_types[port_type],
                input_name=input_name,
                is_shell_command=is_shell_command,
                schema_def_types=schema_def_types,
            )
    # Simple type with `inputBinding` specified -> CWLCommandToken
    if command_line_binding is not None:
        if token is not None:
            # By default, do not escape composite command tokens
            if "shellQuote" not in command_line_binding:
                command_line_binding["shellQuote"] = False
                return _get_command_token(
                    binding={**command_line_binding, **{"valueFrom": token}},
                    is_shell_command=True,
                    input_name=input_name,
                )
            else:
                return _get_command_token(
                    binding={**command_line_binding, **{"valueFrom": token}},
                    is_shell_command=is_shell_command,
                    input_name=input_name,
                )
        else:
            return _get_command_token(
                binding=command_line_binding,
                is_shell_command=is_shell_command,
                input_name=input_name,
                token_type=port_type,
            )
    # Simple type without `inputBinding` specified -> token
    else:
        return token


def _get_hardware_requirement(
    cwl_version: str,
    requirements: MutableMapping[str, Any],
    expression_lib: MutableSequence[str] | None,
    full_js: bool,
) -> CWLHardwareRequirement:
    hardware_requirement = CWLHardwareRequirement(
        cwl_version=cwl_version, expression_lib=expression_lib, full_js=full_js
    )
    if "ResourceRequirement" in requirements:
        resource_requirement = requirements["ResourceRequirement"]
        hardware_requirement.cores = resource_requirement.get(
            "coresMin", resource_requirement.get("coresMax", hardware_requirement.cores)
        )
        hardware_requirement.memory = resource_requirement.get(
            "ramMin", resource_requirement.get("ramMax", hardware_requirement.memory)
        )
        hardware_requirement.tmpdir = resource_requirement.get(
            "tmpdirMin",
            resource_requirement.get("tmpdirMax", hardware_requirement.tmpdir),
        )
        hardware_requirement.outdir = resource_requirement.get(
            "outdirMin",
            resource_requirement.get("outdirMax", hardware_requirement.outdir),
        )
    return hardware_requirement


def _get_load_listing(
    port_description: MutableMapping[str, Any], context: MutableMapping[str, Any]
) -> LoadListing:
    requirements = {**context["hints"], **context["requirements"]}
    if "loadListing" in port_description:
        return LoadListing[port_description["loadListing"]]
    elif (
        "outputBinding" in port_description
        and "loadListing" in port_description["outputBinding"]
    ):
        return LoadListing[port_description["outputBinding"]["loadListing"]]
    elif (
        "LoadListingRequirement" in requirements
        and "loadListing" in requirements["LoadListingRequirement"]
    ):
        return LoadListing[requirements["LoadListingRequirement"]["loadListing"]]
    else:
        return LoadListing.no_listing


def _get_path(element_id: str) -> str:
    path = element_id
    if "#" in path:
        path = path.split("#")[-1]
    if path.startswith("file://"):
        path = urllib.parse.unquote(path[7:])
    return path


def _get_schema_def_types(
    requirements: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    return (
        {sd["name"]: sd for sd in requirements["SchemaDefRequirement"]["types"]}
        if "SchemaDefRequirement" in requirements
        else {}
    )


def _get_secondary_files(
    cwl_element, default_required: bool
) -> MutableSequence[SecondaryFile]:
    if not cwl_element:
        return []
    elif isinstance(cwl_element, MutableSequence):
        return [
            SecondaryFile(
                pattern=sf["pattern"],
                required=sf.get("required")
                if sf.get("required") is not None
                else default_required,
            )
            for sf in cwl_element
        ]
    elif isinstance(cwl_element, MutableMapping):
        return [
            SecondaryFile(
                pattern=cwl_element["pattern"],
                required=cwl_element.get("required")
                if cwl_element.get("required") is not None
                else default_required,
            )
        ]
    else:
        return []


def _percolate_port(port_name: str, *args) -> Port | None:
    for arg in args:
        if port_name in arg:
            port = arg[port_name]
            if isinstance(port, Port):
                return port
            else:
                return _percolate_port(port, *args)
    return None


def _process_docker_image(docker_requirement: MutableMapping[str, Any]) -> str:
    # Retrieve image
    if "dockerPull" in docker_requirement:
        image_name = docker_requirement["dockerPull"]
    elif "dockerImageId" in docker_requirement:
        image_name = docker_requirement["dockerImageId"]
    else:
        raise WorkflowDefinitionException(
            "DockerRequirements without `dockerPull` or `dockerImageId` are not supported yet"
        )
    return str(image_name)


def _process_docker_requirement(
    config_dir: str,
    config: CWLDockerTranslatorConfig,
    context: MutableMapping[str, Any],
    docker_requirement: MutableMapping[str, Any],
    network_access: bool,
    target: Target,
) -> Target:
    # Process output directory
    if "dockerOutputDirectory" in docker_requirement:
        output_directory = docker_requirement["dockerOutputDirectory"]
        context["output_directory"] = output_directory
    else:
        output_directory = None
    # Build CWLDockerTranslator
    if config.type not in cwl_docker_translator_classes:
        raise WorkflowDefinitionException(
            f"Container type `{config.type}` not supported"
        )
    translator_type = cwl_docker_translator_classes[config.type]
    translator = cast(
        CWLDockerTranslator,
        translator_type(config_dir=config_dir, wrapper=config.wrapper, **config.config),
    )
    return translator.get_target(
        image=_process_docker_image(docker_requirement=docker_requirement),
        output_directory=output_directory,
        network_access=network_access,
        target=target,
    )


def _process_input_value(
    path_processor: ModuleType, output_directory: str, target: Target, value: Any
) -> Any:
    if isinstance(value, MutableSequence):
        return [
            _process_input_value(path_processor, output_directory, target, v)
            for v in value
        ]
    elif isinstance(value, MutableMapping):
        if utils.get_token_class(value) in ["File", "Directory"]:
            if "location" in value:
                value["location"] = _remap_path(
                    path_processor=path_processor,
                    path=value["location"],
                    old_dir=output_directory,
                    new_dir=target.workdir,
                )
            if "path" in value:
                value["path"] = _remap_path(
                    path_processor=path_processor,
                    path=value["path"],
                    old_dir=output_directory,
                    new_dir=target.workdir,
                )
            if "secondaryFiles" in value:
                value["secondaryFiles"] = [
                    _process_input_value(path_processor, output_directory, target, sf)
                    for sf in value["secondaryFiles"]
                ]
            if "listing" in value:
                value["listing"] = [
                    _process_input_value(path_processor, output_directory, target, sf)
                    for sf in value["listing"]
                ]
            return value
        else:
            return {
                k: _process_input_value(path_processor, output_directory, target, v)
                for k, v in value.items()
            }
    else:
        return value


def _process_javascript_requirement(
    requirements: MutableMapping[str, Any]
) -> (MutableSequence[Any] | None, bool):
    expression_lib = None
    full_js = False
    if "InlineJavascriptRequirement" in requirements:
        full_js = True
        if "expressionLib" in requirements["InlineJavascriptRequirement"]:
            expression_lib = []
            for lib in requirements["InlineJavascriptRequirement"]["expressionLib"]:
                expression_lib.append(lib)
    return expression_lib, full_js


def _process_loop_transformers(
    step_name: str,
    input_ports: MutableMapping[str, Port],
    loop_input_ports: MutableMapping[str, Port],
    transformers: MutableMapping[str, LoopValueFromTransformer],
    input_dependencies: MutableMapping[str, set[str]],
):
    new_input_ports = {}
    for input_name, token_transformer in transformers.items():
        # Process inputs to attach ports
        for dep_name in input_dependencies[input_name]:
            if dep_name in input_ports:
                token_transformer.add_loop_input_port(
                    posixpath.relpath(dep_name, step_name), input_ports[dep_name]
                )
        if input_name in loop_input_ports:
            token_transformer.add_loop_source_port(
                posixpath.relpath(input_name, step_name), loop_input_ports[input_name]
            )
        # Put transformer output ports in input ports map
        new_input_ports[input_name] = token_transformer.get_output_port()
    return {**input_ports, **loop_input_ports, **new_input_ports}


def _process_transformers(
    step_name: str,
    input_ports: MutableMapping[str, Port],
    transformers: MutableMapping[str, Transformer],
    input_dependencies: MutableMapping[str, set[str]],
):
    new_input_ports = {}
    for input_name, token_transformer in transformers.items():
        # Process inputs to attach ports
        for dep_name in input_dependencies[input_name]:
            if dep_name in input_ports:
                token_transformer.add_input_port(
                    posixpath.relpath(dep_name, step_name), input_ports[dep_name]
                )
        # Put transformer output ports in input ports map
        new_input_ports[input_name] = token_transformer.get_output_port()
    return {**input_ports, **new_input_ports}


def _remap_path(
    path_processor: ModuleType, path: str, old_dir: str, new_dir: str
) -> str:
    if ":/" in path:
        scheme = urllib.parse.urlsplit(path).scheme
        if scheme == "file":
            return "file://{}".format(
                path_processor.join(
                    new_dir,
                    *os.path.relpath(urllib.parse.unquote(path[7:]), old_dir).split(
                        os.path.sep
                    ),
                )
            )
        else:
            return path
    else:
        return path_processor.join(
            new_dir,
            *os.path.relpath(urllib.parse.unquote(path[7:]), old_dir).split(
                os.path.sep
            ),
        )


class CWLTranslator:
    def __init__(
        self,
        context: StreamFlowContext,
        name: str,
        output_directory: str,
        cwl_definition: cwltool.process.Process,
        cwl_inputs: MutableMapping[str, Any],
        workflow_config: WorkflowConfig,
        loading_context: cwltool.context.LoadingContext,
    ):
        self.context: StreamFlowContext = context
        self.name: str = name
        self.output_directory: str = output_directory
        self.cwl_definition: cwltool.process.Process = cwl_definition
        self.cwl_inputs: MutableMapping[str, Any] = cwl_inputs
        self.default_map: MutableMapping[str, Any] = {}
        self.deployment_map: MutableMapping[str, DeployStep] = {}
        self.gather_map: MutableMapping[str, str] = {}
        self.input_ports: MutableMapping[str, Port] = {}
        self.loading_context: cwltool.context.LoadingContext = loading_context
        self.output_ports: MutableMapping[str, str | Port] = {}
        self.scatter: MutableMapping[str, Any] = {}
        self.workflow_config: WorkflowConfig = workflow_config

    def _get_deploy_step(
        self, deploymenty_config: DeploymentConfig, workflow: Workflow
    ):
        if deploymenty_config.name not in self.deployment_map:
            self.deployment_map[deploymenty_config.name] = workflow.create_step(
                cls=DeployStep,
                name=posixpath.join("__deploy__", deploymenty_config.name),
                deployment_config=deploymenty_config,
            )
        return self.deployment_map[deploymenty_config.name]

    def _get_input_port(
        self,
        workflow: Workflow,
        cwl_element: cwltool.workflow.Process,
        element_input: MutableMapping[str, Any],
        global_name: str,
        port_name: str,
    ) -> Port:
        # Retrieve or create input port
        if global_name not in self.input_ports:
            self.input_ports[global_name] = workflow.create_port()
        input_port = self.input_ports[global_name]
        # If there is a default value, construct a default port block
        if "default" in element_input:
            # Insert default port
            transformer_suffix = (
                "-wf-default-transformer"
                if isinstance(cwl_element, cwltool.workflow.Workflow)
                else "-cmd-default-transformer"
            )
            input_port = self._handle_default_port(
                global_name=global_name,
                port_name=port_name,
                transformer_suffix=transformer_suffix,
                port=input_port,
                workflow=workflow,
                value=element_input["default"],
            )
        # Return port
        return input_port

    def _get_source_port(self, workflow: Workflow, source_name: str) -> Port:
        if source_name in self.output_ports:
            return self.output_ports[source_name]
        if source_name not in self.input_ports:
            if source_name not in self.output_ports:
                self.output_ports[source_name] = workflow.create_port()
            return self.output_ports[source_name]
        else:
            return self.input_ports[source_name]

    def _handle_default_port(
        self,
        global_name: str,
        port_name: str,
        transformer_suffix: str,
        port: Port | None,
        workflow: Workflow,
        value: Any,
    ) -> Port:
        # Check output directory
        path = _get_path(self.cwl_definition.tool["id"])
        # Build default port
        default_port = workflow.create_port()
        self._inject_input(
            workflow=workflow,
            port_name="-".join([port_name, "default"]),
            global_name=global_name + transformer_suffix,
            port=default_port,
            output_directory=os.path.dirname(path),
            value=value,
        )
        if port is not None:
            # Add default transformer
            transformer = workflow.create_step(
                cls=DefaultTransformer,
                name=global_name + transformer_suffix,
                default_port=default_port,
            )
            transformer.add_input_port(port_name, port)
            transformer.add_output_port(port_name, workflow.create_port())
            return transformer.get_output_port()
        else:
            return default_port

    def _inject_input(
        self,
        workflow: Workflow,
        global_name: str,
        port_name,
        port: Port,
        output_directory: str,
        value: Any,
    ) -> None:
        # Retrieve the DeployStep for the port target
        binding_config = get_binding_config(global_name, "port", self.workflow_config)
        target = binding_config.targets[0]
        deploy_step = self._get_deploy_step(target.deployment, workflow)
        # Remap path if target's workdir is defined
        if (
            self.workflow_config.propagate(PurePosixPath(global_name), "port")
            is not None
        ):
            path_processor = os.path if target.deployment.type == "local" else posixpath
            value = _process_input_value(
                path_processor, output_directory, target, value
            )
        # Create a schedule step and connect it to the local DeployStep
        schedule_step = workflow.create_step(
            cls=ScheduleStep,
            name=posixpath.join(f"{global_name}-injector", "__schedule__"),
            job_prefix=f"{global_name}-injector",
            connector_ports={target.deployment.name: deploy_step.get_output_port()},
            input_directory=target.workdir or output_directory,
            output_directory=target.workdir or output_directory,
            tmp_directory=target.workdir or output_directory,
            binding_config=binding_config,
        )
        # Create a CWLInputInjector step to process the input
        injector_step = workflow.create_step(
            cls=CWLInputInjectorStep,
            name=global_name + "-injector",
            job_port=schedule_step.get_output_port(),
        )
        # Create an input port and inject values
        input_port = workflow.create_port()
        input_port.put(Token(value=value))
        input_port.put(TerminationToken())
        # Connect input and output ports to the injector step
        injector_step.add_input_port(port_name, input_port)
        injector_step.add_output_port(port_name, port)

    def _inject_inputs(self, workflow: Workflow):
        output_directory = None
        if self.cwl_inputs:
            # Compute output directory path
            path = _get_path(self.cwl_inputs["id"])
            output_directory = os.path.dirname(path)
        # Compute suffix
        default_suffix = (
            "-wf-default-transformer"
            if isinstance(self.cwl_definition, cwltool.workflow.Workflow)
            else "-cmd-default-transformer"
        )
        # Process externally provided inputs
        for global_name in self.input_ports:
            if step := workflow.steps.get(
                global_name + default_suffix,
                workflow.steps.get(global_name + "-token-transformer"),
            ):
                step_name = posixpath.dirname(global_name)
                port_name = posixpath.relpath(global_name, step_name)
                input_port = step.get_input_port(port_name)
                # If an input is given for port, inject it
                if step_name == posixpath.sep and port_name in self.cwl_inputs:
                    self._inject_input(
                        workflow=workflow,
                        global_name=global_name,
                        port_name=port_name,
                        port=input_port,
                        output_directory=output_directory,
                        value=self.cwl_inputs[port_name],
                    )
        # Search empty unbound input ports
        for input_port in workflow.ports.values():
            if input_port.empty() and not input_port.get_input_steps():
                input_port.put(Token(value=None))
                input_port.put(TerminationToken())

    def _recursive_translate(
        self,
        workflow: Workflow,
        cwl_element: cwltool.process.Process,
        context: MutableMapping[str, Any],
        name_prefix: str,
        cwl_name_prefix: str,
    ):
        # Update context
        current_context = copy.deepcopy(context)
        for hint in cwl_element.hints:
            current_context["hints"][hint["class"]] = hint
        for requirement in cwl_element.requirements:
            current_context["requirements"][requirement["class"]] = requirement
        # In the root process, override requirements when provided in the input file
        if name_prefix == posixpath.sep:
            req_string = "https://w3id.org/cwl/cwl#requirements"
            if req_string in self.cwl_inputs:
                current_context["requirements"] = {
                    req["class"]: req for req in self.cwl_inputs[req_string]
                }
        # Dispatch element
        if isinstance(cwl_element, cwltool.workflow.Workflow):
            self._translate_workflow(
                workflow=workflow,
                cwl_element=cwl_element,
                context=current_context,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
            )
        elif isinstance(cwl_element, cwltool.workflow.WorkflowStep):
            self._translate_workflow_step(
                workflow=workflow,
                cwl_element=cwl_element,
                context=current_context,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
            )
        elif isinstance(cwl_element, cwltool.command_line_tool.CommandLineTool):
            self._translate_command_line_tool(
                workflow=workflow,
                cwl_element=cwl_element,
                context=current_context,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
            )
        elif isinstance(cwl_element, cwltool.command_line_tool.ExpressionTool):
            self._translate_command_line_tool(
                workflow=workflow,
                cwl_element=cwl_element,
                context=current_context,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
            )
        else:
            raise WorkflowDefinitionException(
                "Definition of type "
                + type(cwl_element).__class__.__name__
                + " not supported"
            )

    def _translate_command_line_tool(
        self,
        workflow: Workflow,
        cwl_element: (
            cwltool.command_line_tool.CommandLineTool
            | cwltool.command_line_tool.ExpressionTool
        ),
        context: MutableMapping[str, Any],
        name_prefix: str,
        cwl_name_prefix: str,
    ):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Translating {cwl_element.__class__.__name__} {name_prefix}")
        # Extract custom types if present
        requirements = {**context["hints"], **context["requirements"]}
        schema_def_types = _get_schema_def_types(requirements)
        # Process InlineJavascriptRequirement
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Retrieve target
        binding_config = get_binding_config(name_prefix, "step", self.workflow_config)
        # Process DockerRequirement
        if "DockerRequirement" in requirements:
            network_access = (
                requirements["NetworkAccess"]["networkAccess"]
                if "NetworkAccess" in requirements
                else False
            )
            targets = []
            for target in binding_config.targets:
                # If original target is local, use a container
                if target.deployment.type == "local":
                    targets.append(
                        _process_docker_requirement(
                            config_dir=os.path.dirname(self.context.config["path"]),
                            config=self.workflow_config.propagate(
                                path=PurePosixPath(name_prefix),
                                name="docker",
                                default=CWLDockerTranslatorConfig(
                                    name="default", type="default", config={}
                                ),
                            ),
                            context=context,
                            docker_requirement=requirements["DockerRequirement"],
                            network_access=network_access,
                            target=target,
                        )
                    )
                # Otherwise, throw a warning and skip the DockerRequirement conversion
                else:
                    if logger.isEnabledFor(logging.WARN):
                        logger.warn(
                            f"Skipping DockerRequirement conversion for step `{name_prefix}` "
                            f"when executing on `{target.deployment.name}` deployment."
                        )
                    targets.append(target)
            binding_config.targets = targets
        # Create DeploySteps to initialise the execution environment
        deployments = {t.deployment.name: t.deployment for t in binding_config.targets}
        deploy_steps = {
            name: self._get_deploy_step(deployment, workflow)
            for name, deployment in deployments.items()
        }
        # Create a schedule step and connect it to the DeployStep
        schedule_step = workflow.create_step(
            cls=ScheduleStep,
            name=posixpath.join(name_prefix, "__schedule__"),
            job_prefix=name_prefix,
            connector_ports={
                name: step.get_output_port() for name, step in deploy_steps.items()
            },
            binding_config=binding_config,
            hardware_requirement=_get_hardware_requirement(
                cwl_version=self.loading_context.metadata["cwlVersion"],
                requirements=requirements,
                expression_lib=expression_lib,
                full_js=full_js,
            ),
            output_directory=context.get("output_directory"),
        )
        # Create the ExecuteStep and connect it to the ScheduleStep
        step = workflow.create_step(
            cls=ExecuteStep, name=name_prefix, job_port=schedule_step.get_output_port()
        )
        # Process inputs
        input_ports = {}
        token_transformers = []
        for element_input in cwl_element.tool["inputs"]:
            global_name = utils.get_name(
                name_prefix, cwl_name_prefix, element_input["id"]
            )
            port_name = posixpath.relpath(global_name, name_prefix)
            # Retrieve or create input port
            input_port = self._get_input_port(
                workflow=workflow,
                cwl_element=cwl_element,
                element_input=element_input,
                global_name=global_name,
                port_name=port_name,
            )
            # Add a token transformer step to process inputs
            token_transformer = _create_token_transformer(
                name=global_name + "-token-transformer",
                port_name=port_name,
                workflow=workflow,
                cwl_element=element_input,
                cwl_name_prefix=posixpath.join(cwl_name_prefix, port_name),
                schema_def_types=schema_def_types,
                format_graph=self.loading_context.loader.graph,
                context=context,
                only_propagate_secondary_files=(name_prefix != "/"),
            )
            # Add the output port as an input of the schedule step
            schedule_step.add_input_port(port_name, token_transformer.get_output_port())
            # Create a TransferStep
            transfer_step = workflow.create_step(
                cls=CWLTransferStep,
                name=posixpath.join(name_prefix, "__transfer__", port_name),
                job_port=schedule_step.get_output_port(),
            )
            transfer_step.add_input_port(port_name, token_transformer.get_output_port())
            transfer_step.add_output_port(port_name, workflow.create_port())
            # Connect the transfer step with the ExecuteStep
            step.add_input_port(port_name, transfer_step.get_output_port(port_name))
            # Store input port and token transformer
            input_ports[port_name] = input_port
            token_transformers.append(token_transformer)
        # Add input ports to token transformers
        for port_name, input_port in input_ports.items():
            for token_transformer in token_transformers:
                token_transformer.add_input_port(port_name, input_port)
        # Process outputs
        for element_output in cwl_element.tool["outputs"]:
            global_name = utils.get_name(
                name_prefix, cwl_name_prefix, element_output["id"]
            )
            port_name = posixpath.relpath(global_name, name_prefix)
            # Retrieve or create output port
            if global_name not in self.output_ports:
                self.output_ports[global_name] = workflow.create_port()
            output_port = self.output_ports[global_name]
            # If the port is bound to a remote target, add the connector dependency
            if self.workflow_config.propagate(PurePosixPath(global_name), "port"):
                binding_config = get_binding_config(
                    global_name, "port", self.workflow_config
                )
                port_target = binding_config.targets[0]
                output_deploy_step = self._get_deploy_step(
                    port_target.deployment, workflow
                )
                step.add_input_port(
                    port_name + "__connector__", output_deploy_step.get_output_port()
                )
                step.output_connectors[port_name] = port_name + "__connector__"
            else:
                port_target = None
            # In CWL <= v1.2, ExpressionTool output is never type-checked
            if isinstance(
                cwl_element, cwltool.command_line_tool.ExpressionTool
            ) and self.loading_context.metadata["cwlVersion"] in [
                "v1.0",
                "v1.1",
                "v1.2",
            ]:
                if isinstance(element_output["type"], MutableSequence):
                    port_type = element_output["type"]
                    if "null" not in port_type:
                        port_type.append("null")
                    if "Any" not in port_type:
                        port_type.append("Any")
                elif isinstance(element_output["type"], str):
                    port_type = [element_output["type"]]
                    if element_output["type"] != "null":
                        port_type.append("null")
                    if element_output["type"] != "Any":
                        port_type.append("Any")
                else:
                    port_type = ["null", element_output["type"], "Any"]
            else:
                port_type = element_output["type"]
            # Add output port to ExecuteStep
            step.add_output_port(
                name=port_name,
                port=output_port,
                output_processor=_create_command_output_processor(
                    port_name=port_name,
                    workflow=workflow,
                    port_target=port_target,
                    port_type=port_type,
                    cwl_element=element_output,
                    cwl_name_prefix=posixpath.join(cwl_name_prefix, port_name),
                    schema_def_types=schema_def_types,
                    context=context,
                ),
            )
        if isinstance(cwl_element, cwltool.command_line_tool.CommandLineTool):
            # Process command
            step.command = _create_command(
                cwl_element=cwl_element,
                cwl_name_prefix=cwl_name_prefix,
                schema_def_types=schema_def_types,
                context=context,
                step=step,
            )
            # Process ToolTimeLimit
            if "ToolTimeLimit" in requirements:
                step.command.time_limit = requirements["ToolTimeLimit"]["timelimit"]
        elif isinstance(cwl_element, cwltool.command_line_tool.ExpressionTool):
            if "expression" in cwl_element.tool:
                step.command = CWLExpressionCommand(
                    step, cwl_element.tool["expression"]
                )
        # Add JS requirements
        step.command.expression_lib = expression_lib
        step.command.full_js = full_js
        # Process streams
        if "stdin" in cwl_element.tool:
            step.command.stdin = cwl_element.tool["stdin"]
        if "stdout" in cwl_element.tool:
            step.command.stdout = cwl_element.tool["stdout"]
        if "stderr" in cwl_element.tool:
            step.command.stderr = cwl_element.tool["stderr"]

    def _translate_workflow(
        self,
        workflow: Workflow,
        cwl_element: cwltool.workflow.Workflow,
        context: MutableMapping[str, Any],
        name_prefix: str,
        cwl_name_prefix: str,
    ):
        step_name = name_prefix
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Translating Workflow {step_name}")
        # Extract custom types if present
        requirements = {**context["hints"], **context["requirements"]}
        schema_def_types = _get_schema_def_types(requirements)
        # Extract JavaScript requirements
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Process inputs to create steps
        input_ports = {}
        token_transformers = {}
        input_dependencies = {}
        for element_input in cwl_element.tool["inputs"]:
            global_name = utils.get_name(
                step_name, cwl_name_prefix, element_input["id"]
            )
            port_name = posixpath.relpath(global_name, step_name)
            # Retrieve or create input port
            input_ports[global_name] = self._get_input_port(
                workflow=workflow,
                cwl_element=cwl_element,
                element_input=element_input,
                global_name=global_name,
                port_name=port_name,
            )
            # Create token transformer step
            token_transformers[global_name] = _create_token_transformer(
                name=global_name + "-token-transformer",
                port_name=port_name,
                workflow=workflow,
                cwl_element=element_input,
                cwl_name_prefix=posixpath.join(cwl_name_prefix, port_name),
                schema_def_types=schema_def_types,
                format_graph=self.loading_context.loader.graph,
                context=context,
                only_propagate_secondary_files=(name_prefix != "/"),
            )
            # Process dependencies
            local_deps = resolve_dependencies(
                expression=element_input.get("format"),
                full_js=full_js,
                expression_lib=expression_lib,
            )
            if "secondaryFiles" in element_input:
                for secondary_file in element_input["secondaryFiles"]:
                    local_deps.update(
                        resolve_dependencies(
                            expression=secondary_file.get("pattern"),
                            full_js=full_js,
                            expression_lib=expression_lib,
                        ),
                        resolve_dependencies(
                            expression=secondary_file.get("required"),
                            full_js=full_js,
                            expression_lib=expression_lib,
                        ),
                    )
            input_dependencies[global_name] = set.union(
                {global_name}, {posixpath.join(step_name, d) for d in local_deps}
            )
        # Process inputs again to attach ports
        input_ports = _process_transformers(
            step_name=step_name,
            input_ports=input_ports,
            transformers=token_transformers,
            input_dependencies=input_dependencies,
        )
        # Save input ports in the global map
        for input_name in token_transformers:
            self.input_ports[input_name] = input_ports[input_name]
        # Process outputs
        for element_output in cwl_element.tool["outputs"]:
            global_name = utils.get_name(
                name_prefix, cwl_name_prefix, element_output["id"]
            )
            # If outputSource element is a list, the output element can depend on multiple ports
            if isinstance(element_output["outputSource"], MutableSequence):
                # If the list contains only one element and no `linkMerge` or `pickValue` are specified
                if (
                    len(element_output["outputSource"]) == 1
                    and "linkMerge" not in element_output
                    and "pickValue" not in element_output
                ):
                    # Treat it as a singleton
                    source_name = utils.get_name(
                        name_prefix, cwl_name_prefix, element_output["outputSource"][0]
                    )
                    # If the output source is an input port, link the output to the input
                    if source_name in self.input_ports:
                        self.output_ports[global_name] = self.input_ports[source_name]
                    # Otherwise, simply propagate the output port
                    else:
                        self.output_ports[source_name] = self._get_source_port(
                            workflow, global_name
                        )
                # Otherwise, create a ListMergeCombinator
                else:
                    if (
                        len(element_output["outputSource"]) > 1
                        and "MultipleInputFeatureRequirement" not in requirements
                    ):
                        raise WorkflowDefinitionException(
                            "Workflow contains multiple inbound links to a single parameter "
                            "but MultipleInputFeatureRequirement is not declared."
                        )
                    source_names = [
                        utils.get_name(name_prefix, cwl_name_prefix, src)
                        for src in element_output["outputSource"]
                    ]
                    ports = {
                        n: self._get_source_port(workflow, n) for n in source_names
                    }
                    _create_list_merger(
                        name=global_name,
                        workflow=workflow,
                        ports=ports,
                        output_port=self._get_source_port(workflow, global_name),
                        link_merge=element_output.get("linkMerge"),
                        pick_value=element_output.get("pickValue"),
                    )
            # Otherwise, the output element depends on a single output port
            else:
                source_name = utils.get_name(
                    name_prefix, cwl_name_prefix, element_output["outputSource"]
                )
                # If `pickValue` is specified, create a ListMergeCombinator
                if "pickValue" in element_output:
                    source_port = self._get_source_port(workflow, source_name)
                    _create_list_merger(
                        name=global_name,
                        workflow=workflow,
                        ports={source_name: source_port},
                        output_port=self._get_source_port(workflow, global_name),
                        link_merge=element_output.get("linkMerge"),
                        pick_value=element_output.get("pickValue"),
                    )
                else:
                    # If the output source is an input port, link the output to the input
                    if source_name in self.input_ports:
                        self.output_ports[global_name] = self.input_ports[source_name]
                    # Otherwise, simply propagate the output port
                    else:
                        if global_name not in self.output_ports:
                            self.output_ports[global_name] = workflow.create_port()
                        self.output_ports[source_name] = self.output_ports[global_name]
        # Process steps
        for step in cwl_element.steps:
            self._recursive_translate(
                workflow=workflow,
                cwl_element=step,
                context=context,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
            )

    def _translate_workflow_step(
        self,
        workflow: Workflow,
        cwl_element: cwltool.workflow.WorkflowStep,
        context: MutableMapping[str, Any],
        name_prefix: str,
        cwl_name_prefix: str,
    ):
        # Process content
        step_name = utils.get_name(name_prefix, cwl_name_prefix, cwl_element.id)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Translating WorkflowStep {step_name}")
        cwl_step_name = utils.get_name(
            name_prefix, cwl_name_prefix, cwl_element.id, preserve_cwl_prefix=True
        )
        # Extract requirements
        for hint in cwl_element.embedded_tool.hints:
            context["hints"][hint["class"]] = hint
        for requirement in cwl_element.embedded_tool.requirements:
            context["requirements"][requirement["class"]] = requirement
        requirements = {**context["hints"], **context["requirements"]}
        # Extract JavaScript requirements
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Find scatter elements
        if isinstance(cwl_element.tool.get("scatter"), str):
            scatter_inputs = [
                utils.get_name(
                    step_name, cwl_step_name, cwl_element.tool.get("scatter")
                )
            ]
        else:
            scatter_inputs = [
                utils.get_name(step_name, cwl_step_name, n)
                for n in cwl_element.tool.get("scatter", [])
            ]
        # Process inputs
        input_ports = {}
        default_ports = {}
        value_from_transformers = {}
        input_dependencies = {}
        for element_input in cwl_element.tool["inputs"]:
            self._translate_workflow_step_input(
                workflow=workflow,
                context=context,
                element_id=cwl_element.id,
                element_input=element_input,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
                scatter_inputs=scatter_inputs,
                requirements=requirements,
                input_ports=input_ports,
                default_ports=default_ports,
                value_from_transformers=value_from_transformers,
                input_dependencies=input_dependencies,
            )
        for default_name, default_port in default_ports.items():
            # If there are inputs, add default retag transformer
            if input_ports:
                global_name = posixpath.join(step_name, default_name)
                transformer = workflow.create_step(
                    cls=DefaultRetagTransformer,
                    name=global_name + "-step-retag-transformer",
                    default_port=default_port,
                )
                for port_name, port in input_ports.items():
                    transformer.add_input_port(
                        posixpath.relpath(port_name, step_name), port
                    )
                transformer.add_output_port(
                    posixpath.relpath(default_name, step_name), workflow.create_port()
                )
                default_ports[default_name] = transformer.get_output_port()
        input_ports = {**input_ports, **default_ports}
        # Process loop inputs
        element_requirements = {
            **{h["class"]: h for h in cwl_element.embedded_tool.hints},
            **{r["class"]: r for r in cwl_element.embedded_tool.requirements},
        }
        if "http://commonwl.org/cwltool#Loop" in element_requirements:
            loop_requirement = element_requirements["http://commonwl.org/cwltool#Loop"]
            # Build combinator
            loop_combinator = LoopCombinator(
                workflow=workflow, name=step_name + "-loop-combinator"
            )
            for global_name in input_ports:
                # Decouple loop ports through a forwarder
                port_name = posixpath.relpath(global_name, step_name)
                loop_forwarder = workflow.create_step(
                    cls=ForwardTransformer,
                    name=global_name + "-input-forward-transformer",
                )
                loop_forwarder.add_input_port(port_name, input_ports[global_name])
                input_ports[global_name] = workflow.create_port()
                loop_forwarder.add_output_port(port_name, input_ports[global_name])
                # Add item to combinator
                loop_combinator.add_item(posixpath.relpath(global_name, step_name))
            # Create a combinator step and add all inputs to it
            combinator_step = workflow.create_step(
                cls=LoopCombinatorStep,
                name=step_name + "-loop-combinator",
                combinator=loop_combinator,
            )
            for global_name in input_ports:
                port_name = posixpath.relpath(global_name, step_name)
                combinator_step.add_input_port(port_name, input_ports[global_name])
                combinator_step.add_output_port(port_name, workflow.create_port())
            # Create loop conditional step
            loop_conditional_step = workflow.create_step(
                cls=CWLLoopConditionalStep,
                name=step_name + "-loop-when",
                expression=loop_requirement["loopWhen"],
                expression_lib=expression_lib,
                full_js=full_js,
            )
            # Add inputs to conditional step
            for global_name in input_ports:
                port_name = posixpath.relpath(global_name, step_name)
                loop_conditional_step.add_input_port(
                    port_name, combinator_step.get_output_port(port_name)
                )
                input_ports[global_name] = workflow.create_port()
                loop_conditional_step.add_output_port(
                    port_name, input_ports[global_name]
                )
        # Retrieve scatter method (default to dotproduct)
        scatter_method = cwl_element.tool.get("scatterMethod", "dotproduct")
        # If there are scatter inputs
        if scatter_inputs:
            # If any scatter input is null, propagate an empty array on the output ports
            empty_scatter_conditional_step = workflow.create_step(
                cls=CWLEmptyScatterConditionalStep,
                name=step_name + "-empty-scatter-condition",
                scatter_method=scatter_method,
            )
            for global_name in scatter_inputs:
                port_name = posixpath.relpath(global_name, step_name)
                empty_scatter_conditional_step.add_input_port(
                    port_name, input_ports[global_name]
                )
                input_ports[global_name] = workflow.create_port()
                empty_scatter_conditional_step.add_output_port(
                    port_name, input_ports[global_name]
                )
            # If there are multiple scatter inputs, configure combinator
            scatter_combinator = None
            if len(scatter_inputs) > 1:
                # Build combinator
                if scatter_method == "dotproduct":
                    scatter_combinator = DotProductCombinator(
                        workflow=workflow, name=step_name + "-scatter-combinator"
                    )
                    for global_name in scatter_inputs:
                        scatter_combinator.add_item(
                            posixpath.relpath(global_name, step_name)
                        )
                else:
                    scatter_combinator = CartesianProductCombinator(
                        workflow=workflow, name=step_name + "-scatter-combinator"
                    )
                    for global_name in scatter_inputs:
                        scatter_combinator.add_item(
                            posixpath.relpath(global_name, step_name)
                        )
            # If there are both scatter and non-scatter inputs
            if len(scatter_inputs) < len(input_ports):
                scatter_combinator = _create_residual_combinator(
                    workflow=workflow,
                    step_name=step_name,
                    inner_combinator=scatter_combinator,
                    inner_inputs=scatter_inputs,
                    input_ports=input_ports,
                )
            # If there are scatter inputs, process them
            for global_name in scatter_inputs:
                port_name = posixpath.relpath(global_name, step_name)
                scatter_step = workflow.create_step(
                    cls=ScatterStep, name=global_name + "-scatter"
                )
                scatter_step.add_input_port(port_name, input_ports[global_name])
                input_ports[global_name] = workflow.create_port()
                scatter_step.add_output_port(port_name, input_ports[global_name])
            # If there is a scatter combinator, create a combinator step and add all inputs to it
            if scatter_combinator:
                combinator_step = workflow.create_step(
                    cls=CombinatorStep,
                    name=step_name + "-scatter-combinator",
                    combinator=scatter_combinator,
                )
                for global_name in input_ports:
                    port_name = posixpath.relpath(global_name, step_name)
                    combinator_step.add_input_port(port_name, input_ports[global_name])
                    input_ports[global_name] = workflow.create_port()
                    combinator_step.add_output_port(port_name, input_ports[global_name])
        # Process inputs again to attach ports to transformers
        input_ports = _process_transformers(
            step_name=step_name,
            input_ports=input_ports,
            transformers=value_from_transformers,
            input_dependencies=input_dependencies,
        )
        # Save input ports in the global map
        self.input_ports = {**self.input_ports, **input_ports}
        # Process condition
        conditional_step = None
        if "when" in cwl_element.tool:
            # Create conditional step
            conditional_step = workflow.create_step(
                cls=CWLConditionalStep,
                name=step_name + "-when",
                expression=cwl_element.tool["when"],
                expression_lib=expression_lib,
                full_js=full_js,
            )
            # Add inputs and outputs to conditional step
            for global_name in input_ports:
                port_name = posixpath.relpath(global_name, step_name)
                conditional_step.add_input_port(
                    port_name, self.input_ports[global_name]
                )
                self.input_ports[global_name] = workflow.create_port()
                conditional_step.add_output_port(
                    port_name, self.input_ports[global_name]
                )
        # Process outputs
        external_output_ports = {}
        internal_output_ports = {}
        for element_output in cwl_element.tool["outputs"]:
            global_name = utils.get_name(step_name, cwl_step_name, element_output["id"])
            port_name = posixpath.relpath(global_name, step_name)
            # Retrieve or create output port
            if global_name not in self.output_ports:
                self.output_ports[global_name] = workflow.create_port()
            external_output_ports[global_name] = self.output_ports[global_name]
            internal_output_ports[global_name] = self.output_ports[global_name]
            # If there are scatter inputs
            if scatter_inputs:
                # Perform a gather on the outputs
                if scatter_method == "nested_crossproduct":
                    gather_steps = []
                    internal_output_ports[global_name] = workflow.create_port()
                    gather_input_port = internal_output_ports[global_name]
                    for scatter_input in scatter_inputs:
                        scatter_port_name = posixpath.relpath(scatter_input, step_name)
                        gather_steps.append(
                            workflow.create_step(
                                cls=GatherStep,
                                name=global_name + "-gather-" + scatter_port_name,
                            )
                        )
                        gather_steps[-1].add_input_port(port_name, gather_input_port)
                        gather_steps[-1].add_output_port(
                            name=port_name,
                            port=(
                                external_output_ports[global_name]
                                if len(gather_steps) == len(scatter_inputs)
                                else workflow.create_port()
                            ),
                        )
                        gather_input_port = gather_steps[-1].get_output_port()
                else:
                    gather_step = workflow.create_step(
                        cls=GatherStep,
                        name=global_name + "-gather",
                        depth=1
                        if scatter_method == "dotproduct"
                        else len(scatter_inputs),
                    )
                    internal_output_ports[global_name] = workflow.create_port()
                    gather_step.add_input_port(
                        port_name, internal_output_ports[global_name]
                    )
                    gather_step.add_output_port(
                        port_name, external_output_ports[global_name]
                    )
                # Add the output port as a skip port in the empty scatter conditional step
                empty_scatter_conditional_step = cast(
                    CWLConditionalStep,
                    workflow.steps[step_name + "-empty-scatter-condition"],
                )
                empty_scatter_conditional_step.add_skip_port(
                    port_name, external_output_ports[global_name]
                )
            # Add skip ports if there is a condition
            if "when" in cwl_element.tool:
                cast(CWLConditionalStep, conditional_step).add_skip_port(
                    port_name, internal_output_ports[global_name]
                )
        # Process loop outputs
        if "http://commonwl.org/cwltool#Loop" in element_requirements:
            loop_requirement = element_requirements["http://commonwl.org/cwltool#Loop"]
            output_method = loop_requirement.get("outputMethod", "last")
            # Retrieve loop steps
            loop_conditional_step = cast(
                CWLLoopConditionalStep, workflow.steps[step_name + "-loop-when"]
            )
            combinator_step = workflow.steps[step_name + "-loop-combinator"]
            # Create a loop termination combinator
            loop_terminator_combinator = LoopTerminationCombinator(
                workflow=workflow, name=step_name + "-loop-termination-combinator"
            )
            loop_terminator_step = workflow.create_step(
                cls=CombinatorStep,
                name=step_name + "-loop-terminator",
                combinator=loop_terminator_combinator,
            )
            for port_name, port in combinator_step.get_input_ports().items():
                loop_terminator_step.add_output_port(port_name, port)
                loop_terminator_combinator.add_output_item(port_name)
            # Add outputs to conditional step
            for global_name in internal_output_ports:
                port_name = posixpath.relpath(global_name, step_name)
                # Create loop forwarder
                loop_forwarder = workflow.create_step(
                    cls=ForwardTransformer,
                    name=global_name + "-output-forward-transformer",
                )
                internal_output_ports[global_name] = workflow.create_port()
                loop_forwarder.add_input_port(
                    port_name, internal_output_ports[global_name]
                )
                self.output_ports[global_name] = workflow.create_port()
                loop_forwarder.add_output_port(
                    port_name, self.output_ports[global_name]
                )
                # Create loop output step
                loop_output_step = workflow.create_step(
                    cls=CWLLoopOutputLastStep
                    if output_method == "last"
                    else CWLLoopOutputAllStep,
                    name=global_name + "-loop-output",
                )
                loop_output_step.add_input_port(
                    port_name, loop_forwarder.get_output_port()
                )
                loop_conditional_step.add_skip_port(
                    port_name, loop_forwarder.get_output_port()
                )
                loop_output_step.add_output_port(
                    port_name, external_output_ports[global_name]
                )
                loop_terminator_step.add_input_port(
                    port_name, external_output_ports[global_name]
                )
                loop_terminator_combinator.add_item(port_name)
            # Process inputs
            loop_input_ports = {}
            loop_default_ports = {}
            loop_value_from_transformers = {}
            loop_input_dependencies = {}
            for loop_input in loop_requirement.get("loop", []):
                # Pre-process the `loopSource` field to avoid inconsistencies
                loop_input["source"] = loop_input.get("loopSource", loop_input["id"])
                self._translate_workflow_step_input(
                    workflow=workflow,
                    context=context,
                    element_id=cwl_element.id,
                    element_input=loop_input,
                    name_prefix=name_prefix,
                    cwl_name_prefix=cwl_name_prefix,
                    scatter_inputs=scatter_inputs,
                    requirements=requirements,
                    input_ports=loop_input_ports,
                    default_ports=loop_default_ports,
                    value_from_transformers=loop_value_from_transformers,
                    input_dependencies=loop_input_dependencies,
                    inner_steps_prefix="-loop",
                    value_from_transformer_cls=LoopValueFromTransformer,
                )
            for default_name, default_port in loop_default_ports.items():
                # If there are inputs, add default retag transformer
                if loop_input_ports:
                    global_name = posixpath.join(step_name, default_name)
                    transformer = workflow.create_step(
                        cls=DefaultRetagTransformer,
                        name=global_name + "-loop-step-retag-transformer",
                        default_port=default_port,
                    )
                    for port_name, port in loop_input_ports.items():
                        transformer.add_input_port(
                            posixpath.relpath(port_name, step_name), port
                        )
                    transformer.add_output_port(
                        posixpath.relpath(default_name, step_name),
                        workflow.create_port(),
                    )
                    loop_default_ports[default_name] = transformer.get_output_port()
            loop_input_ports = {**loop_input_ports, **loop_default_ports}
            # Process inputs again to attach ports to transformers
            loop_input_ports = _process_loop_transformers(
                step_name=step_name,
                input_ports=input_ports,
                loop_input_ports=loop_input_ports,
                transformers=loop_value_from_transformers,
                input_dependencies=loop_input_dependencies,
            )
            # Connect loop outputs to loop inputs
            for global_name in input_ports:
                # Create loop output step
                port_name = posixpath.relpath(global_name, step_name)
                loop_forwarder = workflow.create_step(
                    cls=ForwardTransformer,
                    name=global_name + "-back-propagation-transformer",
                )
                loop_forwarder.add_input_port(
                    port_name,
                    loop_input_ports.get(
                        global_name, loop_conditional_step.get_output_port(port_name)
                    ),
                )
                loop_forwarder.add_output_port(
                    port_name, combinator_step.get_input_port(port_name)
                )
        # Add skip ports if there is a condition
        if "when" in cwl_element.tool:
            for element_output in cwl_element.tool["outputs"]:
                global_name = utils.get_name(
                    step_name, cwl_step_name, element_output["id"]
                )
                port_name = posixpath.relpath(global_name, step_name)
                skip_port = (
                    external_output_ports[global_name]
                    if "http://commonwl.org/cwltool#Loop" in element_requirements
                    else internal_output_ports[global_name]
                )
                cast(CWLConditionalStep, conditional_step).add_skip_port(
                    port_name, skip_port
                )
        # Update output ports with the internal ones
        self.output_ports = {**self.output_ports, **internal_output_ports}
        # Process inner element
        inner_cwl_name_prefix = utils.get_inner_cwl_prefix(
            cwl_name_prefix, name_prefix, cwl_element
        )
        self._recursive_translate(
            workflow=workflow,
            cwl_element=cwl_element.embedded_tool,
            context=context,
            name_prefix=step_name,
            cwl_name_prefix=inner_cwl_name_prefix,
        )
        # Update output ports with the external ones
        self.output_ports = {**self.output_ports, **external_output_ports}

    def _translate_workflow_step_input(
        self,
        workflow: Workflow,
        context: MutableMapping[str, Any],
        element_id: str,
        element_input: MutableMapping[str, Any],
        name_prefix: str,
        cwl_name_prefix: str,
        scatter_inputs: MutableSequence[str],
        requirements: MutableMapping[str, Any],
        input_ports: MutableMapping[str, Port],
        default_ports: MutableMapping[str, Port],
        value_from_transformers: MutableMapping[str, ValueFromTransformer],
        input_dependencies: MutableMapping[str, Any],
        inner_steps_prefix: str = "",
        value_from_transformer_cls: type[ValueFromTransformer] = ValueFromTransformer,
    ):
        # Extract custom types if present
        schema_def_types = _get_schema_def_types(requirements)
        # Extract JavaScript requirements
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Extract names
        step_name = utils.get_name(name_prefix, cwl_name_prefix, element_id)
        cwl_step_name = utils.get_name(
            name_prefix, cwl_name_prefix, element_id, preserve_cwl_prefix=True
        )
        global_name = utils.get_name(step_name, cwl_step_name, element_input["id"])
        port_name = posixpath.relpath(global_name, step_name)
        # Adjust type to handle scatter
        if global_name in scatter_inputs:
            element_input = {
                **element_input,
                **{"type": element_input["type"]["items"]},
            }
        # If element contains `valueFrom` directive
        if "valueFrom" in element_input:
            # Check if StepInputExpressionRequirement is specified
            if "StepInputExpressionRequirement" not in requirements:
                raise WorkflowDefinitionException(
                    "Workflow step contains valueFrom but StepInputExpressionRequirement not in requirements"
                )
            # Create a ValueFromTransformer
            value_from_transformers[global_name] = workflow.create_step(
                cls=value_from_transformer_cls,
                name=global_name + inner_steps_prefix + "-value-from-transformer",
                processor=_create_token_processor(
                    port_name=port_name,
                    workflow=workflow,
                    port_type=element_input.get("type", "Any"),
                    cwl_element=element_input,
                    cwl_name_prefix=posixpath.join(cwl_step_name, port_name),
                    schema_def_types=schema_def_types,
                    format_graph=self.loading_context.loader.graph,
                    context=context,
                    check_type=False,
                ),
                port_name=port_name,
                expression_lib=expression_lib,
                full_js=full_js,
                value_from=element_input["valueFrom"],
            )
            value_from_transformers[global_name].add_output_port(
                port_name, workflow.create_port()
            )
            # Retrieve dependencies
            local_deps = resolve_dependencies(
                expression=element_input.get("valueFrom"),
                full_js=full_js,
                expression_lib=expression_lib,
            )
            input_dependencies[global_name] = set.union(
                {global_name}
                if "source" in element_input or "default" in element_input
                else set(),
                {posixpath.join(step_name, d) for d in local_deps},
            ) or {global_name}
        # If `source` entry is present, process output dependencies
        if "source" in element_input:
            # If source element is a list, the input element can depend on multiple ports
            if isinstance(element_input["source"], MutableSequence):
                # If the list contains only one element and no `linkMerge` is specified, treat it as a singleton
                if (
                    len(element_input["source"]) == 1
                    and "linkMerge" not in element_input
                    and "pickValue" not in element_input
                ):
                    source_name = utils.get_name(
                        name_prefix, cwl_name_prefix, element_input["source"][0]
                    )
                    source_port = self._get_source_port(workflow, source_name)
                    # If there is a default value, construct a default port block
                    if "default" in element_input:
                        # Insert default port
                        source_port = self._handle_default_port(
                            global_name=global_name,
                            port_name=port_name,
                            transformer_suffix=inner_steps_prefix
                            + "-step-default-transformer",
                            port=source_port,
                            workflow=workflow,
                            value=element_input["default"],
                        )
                    # Add source port to the list of input ports for the current step
                    input_ports[global_name] = source_port
                # Otherwise, create a ListMergeCombinator
                else:
                    if (
                        len(element_input["source"]) > 1
                        and "MultipleInputFeatureRequirement" not in requirements
                    ):
                        raise WorkflowDefinitionException(
                            "Workflow contains multiple inbound links to a single parameter "
                            "but MultipleInputFeatureRequirement is not declared."
                        )
                    source_names = [
                        utils.get_name(name_prefix, cwl_name_prefix, src)
                        for src in element_input["source"]
                    ]
                    ports = {
                        n: self._get_source_port(workflow, n) for n in source_names
                    }
                    list_merger = _create_list_merger(
                        name=global_name
                        + inner_steps_prefix
                        + "-list-merge-combinator",
                        workflow=workflow,
                        ports=ports,
                        link_merge=element_input.get("linkMerge"),
                        pick_value=element_input.get("pickValue"),
                    )
                    # Add ListMergeCombinator output port to the list of input ports for the current step
                    input_ports[global_name] = list_merger.get_output_port()
            # Otherwise, the input element depends on a single output port
            else:
                source_name = utils.get_name(
                    name_prefix, cwl_name_prefix, element_input["source"]
                )
                source_port = self._get_source_port(workflow, source_name)
                # If there is a default value, construct a default port block
                if "default" in element_input:
                    # Insert default port
                    source_port = self._handle_default_port(
                        global_name=global_name,
                        port_name=port_name,
                        transformer_suffix=inner_steps_prefix
                        + "-step-default-transformer",
                        port=source_port,
                        workflow=workflow,
                        value=element_input["default"],
                    )
                # Add source port to the list of input ports for the current step
                input_ports[global_name] = source_port
        # Otherwise, search for default values
        elif "default" in element_input:
            default_ports[global_name] = self._handle_default_port(
                global_name=global_name,
                port_name=port_name,
                transformer_suffix=inner_steps_prefix + "-step-default-transformer",
                port=None,
                workflow=workflow,
                value=element_input["default"],
            )
        # Otherwise, inject a synthetic port into the workflow
        else:
            input_ports[global_name] = workflow.create_port()

    def translate(self) -> Workflow:
        workflow = Workflow(
            context=self.context,
            type="cwl",
            config=self.workflow_config.config,
            name=self.name,
        )
        # Create context
        context = _create_context()
        # Compute root prefix
        workflow_id = self.cwl_definition.tool["id"]
        cwl_root_prefix = (
            utils.get_name(posixpath.sep, posixpath.sep, workflow_id)
            if "#" in workflow_id
            else posixpath.sep
        )
        # Register data locations for config files
        path = _get_path(self.cwl_definition.tool["id"])
        self.context.data_manager.register_path(
            location=Location(deployment=LOCAL_LOCATION, name=LOCAL_LOCATION),
            path=path,
            relpath=os.path.basename(path),
        )
        if self.cwl_inputs:
            path = _get_path(self.cwl_inputs["id"])
            self.context.data_manager.register_path(
                location=Location(deployment=LOCAL_LOCATION, name=LOCAL_LOCATION),
                path=path,
                relpath=os.path.basename(path),
            )
        # Build workflow graph
        self._recursive_translate(
            workflow=workflow,
            cwl_element=self.cwl_definition,
            context=context,
            name_prefix=posixpath.sep,
            cwl_name_prefix=cwl_root_prefix,
        )
        # Inject initial inputs
        self._inject_inputs(workflow)
        # Extract requirements
        for hint in self.cwl_definition.hints:
            context["hints"][hint["class"]] = hint
        for requirement in self.cwl_definition.requirements:
            context["requirements"][requirement["class"]] = requirement
        requirements = {**context["hints"], **context["requirements"]}
        # Extract workflow outputs
        cwl_elements = {
            utils.get_name("/", cwl_root_prefix, element["id"]): element
            for element in self.cwl_definition.tool.get("outputs", [])
        }
        for output_name in self.output_ports:
            if output_name.lstrip(posixpath.sep).count(posixpath.sep) == 0:
                if port := _percolate_port(output_name, self.output_ports):
                    port_name = output_name.lstrip(posixpath.sep)
                    # Retrieve a local DeployStep
                    target = LocalTarget()
                    deploy_step = self._get_deploy_step(target.deployment, workflow)
                    # Create a transformer to enforce deep listing in folders
                    expression_lib, full_js = _process_javascript_requirement(
                        requirements
                    )
                    # Search for dependencies in format expression
                    format_deps = resolve_dependencies(
                        expression=cwl_elements[output_name].get("format"),
                        full_js=full_js,
                        expression_lib=expression_lib,
                    )
                    # Build transformer step
                    transformer_step = workflow.create_step(
                        cls=CWLTokenTransformer,
                        name=posixpath.join(port_name + "-collector-transformer"),
                        port_name=port_name,
                        processor=_create_token_processor(
                            port_name=port_name,
                            workflow=workflow,
                            port_type=cwl_elements[output_name]["type"],
                            cwl_element=cwl_elements[output_name],
                            cwl_name_prefix=posixpath.join(cwl_root_prefix, port_name),
                            schema_def_types=_get_schema_def_types(requirements),
                            format_graph=self.loading_context.loader.graph,
                            context=context,
                            check_type=False,
                            default_required_sf=False,
                            force_deep_listing=True,
                        ),
                    )
                    # Add port as transformer input
                    transformer_step.add_input_port(port_name, port)
                    # If there are format dependencies, search for inputs in the port's input steps
                    for dep in format_deps:
                        port_found = False
                        for step in port.get_input_steps():
                            if dep in step.input_ports:
                                transformer_step.add_input_port(
                                    dep, step.get_input_port(dep)
                                )
                                port_found = True
                                break
                        if not port_found:
                            raise WorkflowDefinitionException(
                                f"Cannot retrieve {dep} input port."
                            )
                    # Create an output port for the transformer
                    transformer_step.add_output_port(port_name, workflow.create_port())
                    # Create a schedule step and connect it to the local DeployStep
                    schedule_step = workflow.create_step(
                        cls=ScheduleStep,
                        name=posixpath.join(f"{port_name}-collector", "__schedule__"),
                        job_prefix=f"{port_name}-collector",
                        connector_ports={
                            target.deployment.name: deploy_step.get_output_port()
                        },
                        input_directory=self.output_directory,
                        binding_config=BindingConfig(targets=[target]),
                    )
                    # Add the port as an input of the schedule step
                    schedule_step.add_input_port(
                        port_name, transformer_step.get_output_port()
                    )
                    # Add TransferStep to transfer the output in the output_dir
                    transfer_step = workflow.create_step(
                        cls=CWLTransferStep,
                        name=port_name + "-collector",
                        job_port=schedule_step.get_output_port(),
                        writable=True,
                    )
                    transfer_step.add_input_port(
                        port_name, transformer_step.get_output_port()
                    )
                    transfer_step.add_output_port(port_name, workflow.create_port())
                    # Add the output port of the TransferStep to the workflow output ports
                    workflow.output_ports[
                        port_name
                    ] = transfer_step.get_output_port().name
        # Return the final workflow object
        return workflow


class LinkMergeMethod(Enum):
    merge_nested = 1
    merge_flattened = 2
