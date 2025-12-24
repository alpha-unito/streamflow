from __future__ import annotations

import copy
import logging
import os
import posixpath
import urllib.parse
from collections.abc import MutableMapping, MutableSequence
from pathlib import Path, PurePosixPath
from typing import Any, cast, get_args

import cwl_utils.parser
import cwl_utils.parser.utils
from schema_salad.exceptions import ValidationException

from streamflow.config.config import WorkflowConfig
from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    DeploymentConfig,
    ExecutionLocation,
    LocalTarget,
    Target,
)
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.processor import (
    CommandOutputProcessor,
    MapTokenProcessor,
    NullTokenProcessor,
    ObjectTokenProcessor,
    PopCommandOutputProcessor,
    TokenProcessor,
    UnionCommandOutputProcessor,
    UnionTokenProcessor,
)
from streamflow.core.utils import random_name
from streamflow.core.workflow import (
    CommandTokenProcessor,
    Port,
    Step,
    Token,
    Workflow,
)
from streamflow.cwl import utils
from streamflow.cwl.combinator import ListMergeCombinator
from streamflow.cwl.command import (
    CWLCommand,
    CWLCommandTokenProcessor,
    CWLExpressionCommand,
    CWLForwardCommandTokenProcessor,
    CWLMapCommandTokenProcessor,
    CWLObjectCommandTokenProcessor,
)
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.processor import (
    CWLCommandOutputProcessor,
    CWLExpressionToolOutputProcessor,
    CWLObjectCommandOutputProcessor,
    CWLTokenProcessor,
)
from streamflow.cwl.requirement.docker import cwl_docker_translator_classes
from streamflow.cwl.requirement.docker.translator import (
    CWLDockerTranslator,
    CWLDockerTranslatorConfig,
)
from streamflow.cwl.step import (
    CWLConditionalStep,
    CWLEmptyScatterConditionalStep,
    CWLExecuteStep,
    CWLInputInjectorStep,
    CWLLoopConditionalStep,
    CWLLoopOutputAllStep,
    CWLLoopOutputLastStep,
    CWLScheduleStep,
    CWLTransferStep,
)
from streamflow.cwl.transformer import (
    AllNonNullTransformer,
    CartesianProductSizeTransformer,
    CloneTransformer,
    CWLTokenTransformer,
    DefaultRetagTransformer,
    DotProductSizeTransformer,
    FirstNonNullTransformer,
    ForwardTransformer,
    ListToElementTransformer,
    LoopValueFromTransformer,
    OnlyNonNullTransformer,
    ValueFromTransformer,
)
from streamflow.cwl.utils import (
    LoadListing,
    SecondaryFile,
    process_embedded_tool,
    remap_token_value,
    resolve_dependencies,
)
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.deployment.utils import get_binding_config
from streamflow.log_handler import logger
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.command import UnionCommandTokenProcessor
from streamflow.workflow.step import (
    Combinator,
    CombinatorStep,
    DeployStep,
    GatherStep,
    LoopCombinatorStep,
    ScatterStep,
    ScheduleStep,
    Transformer,
)
from streamflow.workflow.token import TerminationToken
from streamflow.workflow.utils import check_bindings


def _adjust_default_ports(
    workflow: CWLWorkflow,
    step_name: str,
    default_ports: MutableMapping[str, Port],
    input_ports: MutableMapping[str, Port],
    transformer_prefix: str,
    dependent_ports: MutableSequence[str] | None = None,
) -> None:
    dependent_ports = dependent_ports or {}
    if filtered_ports := {
        port_name: port
        for port_name, port in input_ports.items()
        if port_name not in default_ports.keys() and port_name not in dependent_ports
    }:
        for default_name in default_ports.keys():
            transformer = workflow.steps.get(
                posixpath.join(step_name, default_name)
                + f"-{transformer_prefix}-default-transformer"
            )
            for port_name, port in filtered_ports.items():
                transformer.add_input_port(
                    (
                        posixpath.relpath(port_name, step_name)
                        if os.path.isabs(port_name)
                        else port_name
                    ),
                    port,
                )
            default_ports[default_name] = transformer.get_output_port()


def _copy_context(context: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    return {
        "elements": copy.copy(context["elements"]),
        "requirements": copy.copy(context["requirements"]),
        "hints": copy.copy(context["hints"]),
        "version": context["version"],
    }


def _create_command(
    cwl_element: cwl_utils.parser.CommandLineTool,
    cwl_name_prefix: str,
    schema_def_types: MutableMapping[str, Any],
    context: MutableMapping[str, Any],
    step: Step,
) -> CWLCommand:
    requirements = context["hints"] | context["requirements"]
    # Process ShellCommandRequirement
    is_shell_command = "ShellCommandRequirement" in requirements
    # Create command
    command = CWLCommand(
        step=step,
        base_command=(
            cwl_element.baseCommand
            if isinstance(cwl_element.baseCommand, MutableSequence)
            else (
                [cwl_element.baseCommand] if cwl_element.baseCommand is not None else []
            )
        ),
        processors=[
            _get_command_token_processor(binding=a, is_shell_command=is_shell_command)
            for a in (cwl_element.arguments or [])
        ],
        success_codes=cwl_element.successCodes,
        failure_codes=(cwl_element.permanentFailCodes or []).extend(
            cwl_element.permanentFailCodes or []
        )
        or None,
        is_shell_command=is_shell_command,
        step_stdin=cwl_element.stdin,
        step_stdout=cwl_element.stdout,
        step_stderr=cwl_element.stderr,
    )
    # Process InitialWorkDirRequirement
    if "InitialWorkDirRequirement" in requirements:
        command.initial_work_dir = _inject_value(
            requirements["InitialWorkDirRequirement"].listing
        )
        command.absolute_initial_workdir_allowed = (
            "DockerRequirement" in context["requirements"]
        )
    # Process InplaceUpdateRequirement
    if "InplaceUpdateRequirement" in requirements:
        command.inplace_update = requirements["InplaceUpdateRequirement"].inplaceUpdate
    # Process EnvVarRequirement
    if "EnvVarRequirement" in requirements:
        for env_entry in requirements["EnvVarRequirement"].envDef:
            command.environment[env_entry.envName] = env_entry.envValue
    # Process inputs
    for input_port in cwl_element.inputs:
        command.processors.append(
            _get_command_token_processor_from_input(
                cwl_element=input_port,
                port_type=input_port.type_,
                input_name=utils.get_name("", cwl_name_prefix, input_port.id),
                is_shell_command=command.is_shell_command,
                schema_def_types=schema_def_types,
            )
        )
    return command


def _create_command_output_processor(
    port_name: str,
    workflow: CWLWorkflow,
    port_target: Target | None,
    port_type: (
        str
        | cwl_utils.parser.InputSchema
        | cwl_utils.parser.OutputSchema
        | MutableSequence[
            str,
            cwl_utils.parser.OutputSchema,
            cwl_utils.parser.InputSchema,
        ]
    ),
    cwl_element: (
        cwl_utils.parser.CommandOutputParameter
        | cwl_utils.parser.OutputRecordField
        | cwl_utils.parser.ExpressionToolOutputParameter
    ),
    cwl_name_prefix: str,
    schema_def_types: MutableMapping[str, Any],
    context: MutableMapping[str, Any],
    optional: bool = False,
    single: bool = True,
) -> CommandOutputProcessor:
    # Array type: -> single is False
    if isinstance(port_type, get_args(cwl_utils.parser.ArraySchema)):
        return _create_command_output_processor(
            port_name=port_name,
            workflow=workflow,
            port_target=port_target,
            port_type=cast(cwl_utils.parser.OutputArraySchema, port_type).items,
            cwl_element=cwl_element,
            cwl_name_prefix=cwl_name_prefix,
            schema_def_types=schema_def_types,
            context=context,
            optional=True,
            single=False,
        )
    # Enum type: -> create command output processor
    elif isinstance(port_type, get_args(cwl_utils.parser.EnumSchema)):
        # Process InlineJavascriptRequirement
        requirements = context["hints"] | context["requirements"]
        expression_lib, full_js = _process_javascript_requirement(requirements)
        if type_name := cast(cwl_utils.parser.OutputEnumSchema, port_type).name:
            if type_name.startswith("_:"):
                enum_prefix = cwl_name_prefix
            else:
                enum_prefix = utils.get_name(posixpath.sep, posixpath.sep, type_name)
        else:
            enum_prefix = cwl_name_prefix
        # Return OutputProcessor
        if isinstance(
            cwl_element, get_args(cwl_utils.parser.ExpressionToolOutputParameter)
        ):
            return CWLExpressionToolOutputProcessor(
                name=port_name,
                workflow=workflow,
                token_type=port_type[0] if len(port_type) == 1 else port_type,
                enum_symbols=[
                    posixpath.relpath(
                        utils.get_name(posixpath.sep, posixpath.sep, s), enum_prefix
                    )
                    for s in cast(cwl_utils.parser.OutputEnumSchema, port_type).symbols
                ],
                file_format=getattr(cwl_element, "format", None),
                optional=optional,
                streamable=getattr(cwl_element, "streamable", None),
            )
        else:
            return CWLCommandOutputProcessor(
                name=port_name,
                workflow=workflow,
                target=port_target,
                token_type=cast(cwl_utils.parser.OutputEnumSchema, port_type).type_,
                enum_symbols=[
                    posixpath.relpath(
                        utils.get_name(posixpath.sep, posixpath.sep, s), enum_prefix
                    )
                    for s in cast(cwl_utils.parser.OutputEnumSchema, port_type).symbols
                ],
                expression_lib=expression_lib,
                full_js=full_js,
                optional=optional,
            )
    # Record type: -> ObjectCommandOutputProcessor
    elif isinstance(port_type, get_args(cwl_utils.parser.RecordSchema)):
        # Process InlineJavascriptRequirement
        requirements = context["hints"] | context["requirements"]
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Create processor
        if (type_name := getattr(port_type, "name", port_name)).startswith("_:"):
            type_name = port_name
        record_name_prefix = utils.get_name(posixpath.sep, posixpath.sep, type_name)
        return CWLObjectCommandOutputProcessor(
            name=port_name,
            workflow=workflow,
            processors={
                utils.get_name(
                    "", record_name_prefix, port_type.name
                ): _create_command_output_processor(
                    port_name=port_name,
                    port_target=port_target,
                    workflow=workflow,
                    port_type=port_type.type_,
                    cwl_element=port_type,
                    cwl_name_prefix=posixpath.join(
                        record_name_prefix,
                        utils.get_name("", record_name_prefix, port_type.name),
                    ),
                    schema_def_types=schema_def_types,
                    context=context,
                )
                for port_type in cast(
                    cwl_utils.parser.OutputRecordSchema, port_type
                ).fields
            },
            expression_lib=expression_lib,
            full_js=full_js,
            output_eval=(
                cwl_element.outputBinding.outputEval
                if getattr(cwl_element, "outputBinding", None)
                else None
            ),
            single=single,
        )
    elif isinstance(port_type, MutableSequence):
        optional = "null" in port_type
        # Optional type (e.g. ['null', Type]) -> Equivalent to Type?
        if len(types := [t for t in filter(lambda x: x != "null", port_type)]) == 1:
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
                single=single,
            )
        # Any type (e.g. ['Any', Type]) -> Equivalent to Any
        elif "Any" in types:
            return _create_command_output_processor(
                port_name=port_name,
                workflow=workflow,
                port_target=port_target,
                port_type="Any",
                cwl_element=cwl_element,
                cwl_name_prefix=cwl_name_prefix,
                schema_def_types=schema_def_types,
                context=context,
                optional=optional,
                single=single,
            )
        # List of types: -> UnionOutputProcessor
        else:
            types = [
                schema_def_types[t] if isinstance(t, str) and "#" in t else t
                for t in types
            ]
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
                    single=single,
                )
                for port_type in [t for t in types if not isinstance(t, str)]
            ]
            if simple_types := [t for t in types if isinstance(t, str)]:
                processors.append(
                    create_command_output_processor_base(
                        port_name=port_name,
                        workflow=workflow,
                        port_target=port_target,
                        port_type=simple_types,
                        cwl_element=cwl_element,
                        context=context,
                        optional=optional,
                        single=single,
                    )
                )
            if len(processors) > 1:
                return UnionCommandOutputProcessor(
                    name=port_name,
                    workflow=workflow,
                    processors=processors,
                )
            else:
                return processors[0]
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
            single=single,
        )
    # Simple type -> Create typed processor
    else:
        return create_command_output_processor_base(
            port_name=port_name,
            workflow=workflow,
            port_target=port_target,
            port_type=port_type,
            cwl_element=cwl_element,
            context=context,
            optional=optional,
            single=single,
        )


def _get_command_token_processor(
    binding: Any | None = None,
    processor: CommandTokenProcessor | None = None,
    is_shell_command: bool = False,
    input_name: str | None = None,
    token_type: Any | None = None,
) -> CWLCommandTokenProcessor:
    # Normalize type (Python does not distinguish among all CWL number types)
    token_type = (
        "long"
        if token_type == "int"  # nosec
        else "double" if token_type == "float" else token_type  # nosec
    )
    if isinstance(binding, get_args(cwl_utils.parser.CommandLineBinding)):
        return CWLCommandTokenProcessor(
            name=input_name,
            processor=processor,
            expression=binding.valueFrom if binding.valueFrom is not None else None,
            token_type=token_type,
            is_shell_command=is_shell_command,
            item_separator=binding.itemSeparator,
            position=binding.position if binding.position is not None else 0,
            prefix=binding.prefix,
            separate=binding.separate if binding.separate is not None else True,
            shell_quote=binding.shellQuote if binding.shellQuote is not None else True,
        )
    else:
        return CWLCommandTokenProcessor(
            name=input_name,
            processor=processor,
            expression=binding,
            token_type=(
                token_type.save()
                if isinstance(token_type, get_args(cwl_utils.parser.Saveable))
                else token_type
            ),
        )


def _get_command_token_processor_from_input(
    cwl_element: Any,
    port_type: Any,
    input_name: str,
    is_shell_command: bool = False,
    schema_def_types: MutableMapping[str, Any] | None = None,
) -> CommandTokenProcessor:
    processor = None
    command_line_binding = cwl_element.inputBinding
    # Array type: -> CWLMapCommandToken
    if isinstance(port_type, get_args(cwl_utils.parser.ArraySchema)):
        processor = CWLMapCommandTokenProcessor(
            name=input_name,
            processor=_get_command_token_processor_from_input(
                cwl_element=port_type,
                port_type=port_type.items,
                input_name=input_name,
                is_shell_command=is_shell_command,
                schema_def_types=schema_def_types,
            ),
        )
    # Enum type: -> substitute the type with string and reprocess
    elif isinstance(port_type, get_args(cwl_utils.parser.EnumSchema)):
        return _get_command_token_processor_from_input(
            cwl_element=cwl_element,
            port_type="string",
            input_name=input_name,
            is_shell_command=is_shell_command,
            schema_def_types=schema_def_types,
        )
    # Object type: -> CWLObjectCommandToken
    elif isinstance(port_type, get_args(cwl_utils.parser.RecordSchema)):
        if (type_name := getattr(port_type, "name", input_name)).startswith("_:"):
            type_name = input_name
        record_name_prefix = utils.get_name(posixpath.sep, posixpath.sep, type_name)
        processors = {}
        for el in port_type.fields:
            key = utils.get_name("", record_name_prefix, el.name)
            processors[key] = _get_command_token_processor_from_input(
                cwl_element=el,
                port_type=el.type_,
                input_name=key,
                is_shell_command=is_shell_command,
                schema_def_types=schema_def_types,
            )
        processor = CWLObjectCommandTokenProcessor(
            name=input_name,
            processors=processors,
        )
    elif isinstance(port_type, MutableSequence):
        types = [t for t in filter(lambda x: x != "null", port_type)]
        # Optional type (e.g. ['null', Type]) -> propagate
        if len(types) == 1:
            return _get_command_token_processor_from_input(
                cwl_element=cwl_element,
                port_type=types[0],
                input_name=input_name,
                is_shell_command=is_shell_command,
                schema_def_types=schema_def_types,
            )
        # List of types: -> UnionCommandToken
        else:
            return UnionCommandTokenProcessor(
                name=input_name,
                processors=[
                    _get_command_token_processor_from_input(
                        cwl_element=cwl_element,
                        port_type=port_type,
                        input_name=input_name,
                        is_shell_command=is_shell_command,
                        schema_def_types=schema_def_types,
                    )
                    for port_type in types
                ],
            )
    elif isinstance(port_type, str):
        # Complex type -> Extract from schema definitions and propagate
        if "#" in port_type:
            return _get_command_token_processor_from_input(
                cwl_element=cwl_element,
                port_type=schema_def_types[port_type],
                input_name=input_name,
                is_shell_command=is_shell_command,
                schema_def_types=schema_def_types,
            )
    # Simple type with `inputBinding` specified -> CWLCommandToken
    if command_line_binding is not None:
        if processor is not None:
            # By default, do not escape composite command tokens
            if command_line_binding.shellQuote is None:
                command_line_binding.shellQuote = False
                is_shell_command = True
            processor = _get_command_token_processor(
                binding=command_line_binding,
                processor=processor,
                is_shell_command=is_shell_command,
                input_name=input_name,
            )
        else:
            processor = _get_command_token_processor(
                binding=command_line_binding,
                is_shell_command=is_shell_command,
                input_name=input_name,
                token_type=port_type if isinstance(port_type, str) else port_type.type_,
            )
    # Simple type without `inputBinding` specified -> CWLForwardCommandToken
    elif processor is None:
        processor = CWLForwardCommandTokenProcessor(
            name=input_name,
            token_type=port_type if isinstance(port_type, str) else port_type.type_,
        )
    return processor


def _create_context(version: str) -> MutableMapping[str, Any]:
    return {
        "elements": {},
        "requirements": {},
        "hints": {},
        "version": version,
    }


def _create_list_merger(
    name: str,
    workflow: CWLWorkflow,
    ports: MutableMapping[str, Port],
    output_port: Port | None = None,
    link_merge: str | None = None,
    pick_value: str | None = None,
) -> Step:
    output_port_name = _get_source_name(name)
    combinator = workflow.create_step(
        cls=CombinatorStep,
        name=f"{name}-combinator",
        combinator=ListMergeCombinator(
            name=random_name(),
            workflow=workflow,
            input_names=[_get_source_name(p) for p in ports.keys()],
            output_name=output_port_name,
            flatten=(link_merge == "merge_flattened"),
        ),
    )
    for input_port_name, port in ports.items():
        input_port_name = _get_source_name(input_port_name)
        combinator.add_input_port(input_port_name, port)
        combinator.combinator.add_item(input_port_name)
    match pick_value:
        case "first_non_null":
            combinator.add_output_port(output_port_name, workflow.create_port())
            transformer = workflow.create_step(
                cls=FirstNonNullTransformer, name=name + "-transformer"
            )
            transformer.add_input_port(output_port_name, combinator.get_output_port())
            transformer.add_output_port(
                output_port_name, output_port or workflow.create_port()
            )
            return transformer
        case "the_only_non_null":
            combinator.add_output_port(output_port_name, workflow.create_port())
            transformer = workflow.create_step(
                cls=OnlyNonNullTransformer, name=name + "-transformer"
            )
            transformer.add_input_port(output_port_name, combinator.get_output_port())
            transformer.add_output_port(
                output_port_name, output_port or workflow.create_port()
            )
            return transformer
        case "all_non_null":
            combinator.add_output_port(output_port_name, workflow.create_port())
            transformer = workflow.create_step(
                cls=AllNonNullTransformer, name=name + "-transformer"
            )
            transformer.add_input_port(output_port_name, combinator.get_output_port())
            transformer.add_output_port(
                output_port_name, output_port or workflow.create_port()
            )
            return transformer
        case _:
            if link_merge is None:
                combinator.add_output_port(output_port_name, workflow.create_port())
                list_to_element = workflow.create_step(
                    cls=ListToElementTransformer, name=name + "-list-to-element"
                )
                list_to_element.add_input_port(
                    output_port_name, combinator.get_output_port()
                )
                list_to_element.add_output_port(
                    output_port_name, output_port or workflow.create_port()
                )
                return list_to_element
            else:
                combinator.add_output_port(
                    output_port_name, output_port or workflow.create_port()
                )
                return combinator


def _create_loop_condition(
    condition: str,
    expression_lib: MutableSequence[str] | None,
    full_js: bool,
    input_ports: MutableMapping[str, Port],
    step_name: str,
    workflow: CWLWorkflow,
) -> CWLLoopConditionalStep:
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
        expression=condition,
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
        loop_conditional_step.add_output_port(port_name, input_ports[global_name])
    return loop_conditional_step


def _create_nested_size_tag(
    size_ports: MutableMapping[str, Port],
    replicas_port: MutableMapping[str, Port],
    step_name: str,
    workflow: Workflow,
) -> MutableSequence[Port]:
    if len(size_ports) == 0:
        return [next(iter(replicas_port.values()))]
    new_replicas_port = {}
    new_size_ports = {}
    for port_name, port in size_ports.items():
        output_port_name = f"{port_name}-{next(iter(replicas_port.keys()))}"
        transformer = workflow.create_step(
            cls=CloneTransformer,
            name=f"{step_name}-{output_port_name}-scatter-size-transformer",
            replicas_port=next(iter(replicas_port.values())),
        )
        transformer.add_input_port(port_name, port)
        output_port = workflow.create_port()
        transformer.add_output_port(output_port_name, output_port)
        if not new_replicas_port:
            new_replicas_port[output_port_name] = output_port
        else:
            new_size_ports[output_port_name] = output_port
    size_ports_list = _create_nested_size_tag(
        new_size_ports,
        new_replicas_port,
        step_name,
        workflow,
    )
    size_ports_list.append(next(iter(replicas_port.values())))
    return size_ports_list


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
    workflow: CWLWorkflow,
    port_type: Any,
    cwl_element: (
        cwl_utils.parser.InputParameter
        | cwl_utils.parser.OutputParameter
        | cwl_utils.parser.WorkflowStepInput
    ),
    cwl_name_prefix: str,
    schema_def_types: MutableMapping[str, Any],
    context: MutableMapping[str, Any],
    optional: bool = False,
    default_required_sf: bool = True,
    force_deep_listing: bool = False,
    only_propagate_secondary_files: bool = True,
) -> TokenProcessor:
    # Array type: -> MapTokenProcessor
    if isinstance(port_type, get_args(cwl_utils.parser.ArraySchema)):
        return _create_token_processor_optional(
            processor=MapTokenProcessor(
                name=port_name,
                workflow=workflow,
                processor=_create_token_processor(
                    port_name=port_name,
                    workflow=workflow,
                    port_type=port_type.items,
                    cwl_element=cwl_element,
                    cwl_name_prefix=cwl_name_prefix,
                    schema_def_types=schema_def_types,
                    context=context,
                    optional=optional,
                    force_deep_listing=force_deep_listing,
                    only_propagate_secondary_files=only_propagate_secondary_files,
                ),
            ),
            optional=optional,
        )
    # Enum type: -> create output processor
    elif isinstance(port_type, get_args(cwl_utils.parser.EnumSchema)):
        # Process InlineJavascriptRequirement
        requirements = context["hints"] | context["requirements"]
        expression_lib, full_js = _process_javascript_requirement(requirements)
        if type_name := port_type.name:
            if type_name.startswith("_:"):
                enum_prefix = cwl_name_prefix
            else:
                enum_prefix = utils.get_name(posixpath.sep, posixpath.sep, type_name)
        else:
            enum_prefix = cwl_name_prefix
        # Return TokenProcessor
        return CWLTokenProcessor(
            name=port_name,
            workflow=workflow,
            token_type=port_type.type_,
            enum_symbols=[
                posixpath.relpath(
                    utils.get_name(posixpath.sep, posixpath.sep, s), enum_prefix
                )
                for s in port_type.symbols
            ],
            expression_lib=expression_lib,
            full_js=full_js,
        )
    # Record type: -> ObjectTokenProcessor
    elif isinstance(port_type, get_args(cwl_utils.parser.RecordSchema)):
        if (type_name := getattr(port_type, "name", port_name)).startswith("_:"):
            type_name = cwl_name_prefix
        record_name_prefix = utils.get_name(posixpath.sep, posixpath.sep, type_name)
        return _create_token_processor_optional(
            processor=ObjectTokenProcessor(
                name=port_name,
                workflow=workflow,
                processors={
                    utils.get_name(
                        "", record_name_prefix, port_type.name
                    ): _create_token_processor(
                        port_name=port_name,
                        workflow=workflow,
                        port_type=port_type.type_,
                        cwl_element=port_type,
                        cwl_name_prefix=posixpath.join(
                            record_name_prefix,
                            utils.get_name("", record_name_prefix, port_type.name),
                        ),
                        schema_def_types=schema_def_types,
                        context=context,
                        force_deep_listing=force_deep_listing,
                        only_propagate_secondary_files=only_propagate_secondary_files,
                    )
                    for port_type in port_type.fields
                },
            ),
            optional=optional,
        )
    elif isinstance(port_type, MutableSequence):
        optional = "null" in port_type
        types = [t for t in filter(lambda x: x != "null", port_type)]
        # Optional type (e.g. ['null', Type]) -> Equivalent to Type?
        if len(types) == 1:
            return _create_token_processor(
                port_name=port_name,
                workflow=workflow,
                port_type=types[0],
                cwl_element=cwl_element,
                cwl_name_prefix=cwl_name_prefix,
                schema_def_types=schema_def_types,
                context=context,
                optional=optional,
                force_deep_listing=force_deep_listing,
                only_propagate_secondary_files=only_propagate_secondary_files,
            )
        # Any type (e.g. ['Any', Type]) -> Equivalent to Any
        elif "Any" in types:
            return _create_token_processor(
                port_name=port_name,
                workflow=workflow,
                port_type="Any",
                cwl_element=cwl_element,
                cwl_name_prefix=cwl_name_prefix,
                schema_def_types=schema_def_types,
                context=context,
                optional=optional,
                force_deep_listing=force_deep_listing,
                only_propagate_secondary_files=only_propagate_secondary_files,
            )
        # List of types: -> UnionOutputProcessor
        else:
            types = [
                schema_def_types[t] if isinstance(t, str) and "#" in t else t
                for t in types
            ]
            processors = [
                _create_token_processor(
                    port_name=port_name,
                    workflow=workflow,
                    port_type=port_type,
                    cwl_element=cwl_element,
                    cwl_name_prefix=cwl_name_prefix,
                    schema_def_types=schema_def_types,
                    context=context,
                    optional=False,
                    force_deep_listing=force_deep_listing,
                    only_propagate_secondary_files=only_propagate_secondary_files,
                )
                for port_type in [t for t in types if not isinstance(t, str)]
            ]
            if simple_types := [t for t in types if isinstance(t, str)]:
                processors.append(
                    _create_token_processor_base(
                        port_name=port_name,
                        workflow=workflow,
                        port_type=simple_types,
                        cwl_element=cwl_element,
                        context=context,
                        default_required_sf=default_required_sf,
                        force_deep_listing=force_deep_listing,
                        only_propagate_secondary_files=only_propagate_secondary_files,
                    )
                )
            if optional:
                processors.append(NullTokenProcessor(name=port_name, workflow=workflow))
            if len(processors) > 1:
                return UnionTokenProcessor(
                    name=port_name,
                    workflow=workflow,
                    processors=processors,
                )
            else:
                return processors[0]
    # Complex type -> Extract from schema definitions and propagate
    elif "#" in port_type:
        return _create_token_processor(
            port_name=port_name,
            workflow=workflow,
            port_type=schema_def_types[port_type],
            cwl_element=cwl_element,
            cwl_name_prefix=cwl_name_prefix,
            schema_def_types=schema_def_types,
            context=context,
            optional=optional,
            force_deep_listing=force_deep_listing,
            only_propagate_secondary_files=only_propagate_secondary_files,
        )
    # Simple type -> Create typed processor
    else:
        return _create_token_processor_optional(
            processor=_create_token_processor_base(
                port_name=port_name,
                workflow=workflow,
                port_type=port_type,
                cwl_element=cwl_element,
                context=context,
                default_required_sf=default_required_sf,
                force_deep_listing=force_deep_listing,
                only_propagate_secondary_files=only_propagate_secondary_files,
            ),
            optional=optional,
        )


def _create_token_processor_base(
    port_name: str,
    workflow: CWLWorkflow,
    port_type: Any,
    cwl_element: (
        cwl_utils.parser.InputParameter
        | cwl_utils.parser.OutputParameter
        | cwl_utils.parser.WorkflowStepInput
    ),
    context: MutableMapping[str, Any],
    default_required_sf: bool = True,
    force_deep_listing: bool = False,
    only_propagate_secondary_files: bool = True,
) -> CWLTokenProcessor:
    if not isinstance(port_type, MutableSequence):
        port_type = [port_type]
    # Normalize port type (Python does not distinguish among all CWL number types)
    port_type = [
        "long" if t == "int" else "double" if t == "float" else t for t in port_type
    ]
    # Process InlineJavascriptRequirement
    requirements = context["hints"] | context["requirements"]
    expression_lib, full_js = _process_javascript_requirement(requirements)
    # Create OutputProcessor
    if "File" in port_type:
        return CWLTokenProcessor(
            name=port_name,
            workflow=workflow,
            token_type=port_type[0] if len(port_type) == 1 else port_type,
            expression_lib=expression_lib,
            file_format=getattr(cwl_element, "format", None),
            full_js=full_js,
            load_contents=_get_load_contents(cwl_element, only_input=True),
            load_listing=(
                LoadListing.deep_listing
                if force_deep_listing
                else _get_load_listing(cwl_element, context)
            ),
            only_propagate_secondary_files=only_propagate_secondary_files,
            secondary_files=_get_secondary_files(
                cwl_element=getattr(cwl_element, "secondaryFiles", None),
                default_required=default_required_sf,
            ),
            streamable=getattr(cwl_element, "streamable", None),
        )
    else:
        return CWLTokenProcessor(
            name=port_name,
            workflow=workflow,
            token_type=port_type[0] if len(port_type) == 1 else port_type,
            expression_lib=expression_lib,
            full_js=full_js,
            load_contents=_get_load_contents(cwl_element, only_input=True),
            load_listing=(
                LoadListing.deep_listing
                if force_deep_listing
                else _get_load_listing(cwl_element, context)
            ),
        )


def _create_token_processor_optional(
    processor: TokenProcessor, optional: bool
) -> TokenProcessor:
    return (
        UnionTokenProcessor(
            name=processor.name,
            workflow=processor.workflow,
            processors=[
                NullTokenProcessor(
                    name=processor.name,
                    workflow=processor.workflow,
                ),
                processor,
            ],
        )
        if optional
        else processor
    )


def _create_token_transformer(
    name: str,
    port_name: str,
    workflow: CWLWorkflow,
    cwl_element: cwl_utils.parser.InputParameter,
    cwl_name_prefix: str,
    schema_def_types: MutableMapping[str, Any],
    context: MutableMapping[str, Any],
    only_propagate_secondary_files: bool = True,
) -> CWLTokenTransformer:
    token_transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name,
        port_name=port_name,
        processor=_create_token_processor(
            port_name=port_name,
            workflow=workflow,
            port_type=cwl_element.type_,
            cwl_element=cwl_element,
            cwl_name_prefix=cwl_name_prefix,
            schema_def_types=schema_def_types,
            context=context,
            only_propagate_secondary_files=only_propagate_secondary_files,
        ),
    )
    token_transformer.add_output_port(port_name, workflow.create_port())
    return token_transformer


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
        hardware_requirement.cores = (
            resource_requirement.coresMin
            if resource_requirement.coresMin is not None
            else (
                resource_requirement.coresMax
                if resource_requirement.coresMax is not None
                else hardware_requirement.cores
            )
        )
        hardware_requirement.memory = (
            resource_requirement.ramMin
            if resource_requirement.ramMin is not None
            else (
                resource_requirement.ramMax
                if resource_requirement.ramMax is not None
                else hardware_requirement.memory
            )
        )
        hardware_requirement.tmpdir = (
            resource_requirement.tmpdirMin
            if resource_requirement.tmpdirMin is not None
            else (
                resource_requirement.tmpdirMax
                if resource_requirement.tmpdirMax is not None
                else hardware_requirement.tmpdir
            )
        )
        hardware_requirement.outdir = (
            resource_requirement.outdirMin
            if resource_requirement.outdirMin is not None
            else (
                resource_requirement.outdirMax
                if resource_requirement.outdirMax is not None
                else hardware_requirement.outdir
            )
        )
    return hardware_requirement


def _get_load_contents(
    port_description: (
        cwl_utils.parser.InputParameter
        | cwl_utils.parser.OutputParameter
        | cwl_utils.parser.WorkflowStepInput
    ),
    only_input: bool = False,
) -> bool | None:
    if getattr(port_description, "loadContents", None) is not None:
        return port_description.loadContents
    elif (
        getattr(port_description, "inputBinding", None)
        and port_description.inputBinding.loadContents is not None
    ):
        return port_description.inputBinding.loadContents
    elif (
        getattr(port_description, "outputBinding", None)
        and port_description.outputBinding.loadContents is not None
        and not only_input
    ):
        return port_description.outputBinding.loadContents
    else:
        return None


def _get_load_listing(
    port_description: (
        cwl_utils.parser.InputParameter
        | cwl_utils.parser.OutputParameter
        | cwl_utils.parser.WorkflowStepInput
    ),
    context: MutableMapping[str, Any],
) -> LoadListing:
    requirements = context["hints"] | context["requirements"]
    if getattr(port_description, "loadListing", None):
        return LoadListing[port_description.loadListing]
    elif (
        getattr(port_description, "outputBinding", None)
        and getattr(port_description.outputBinding, "loadListing", None) is not None
    ):
        return LoadListing[port_description.outputBinding.loadListing]
    elif (
        "LoadListingRequirement" in requirements
        and requirements["LoadListingRequirement"].loadListing
    ):
        return LoadListing[requirements["LoadListingRequirement"].loadListing]
    else:
        return (
            LoadListing.deep_listing
            if context["version"] == "v1.0"
            else LoadListing.no_listing
        )


def _get_loop(
    cwl_element: cwl_utils.parser.WorkflowStep, requirements: MutableMapping[str, Any]
) -> MutableMapping[str, Any] | None:
    if "Loop" in requirements:
        loop = cast(cwl_utils.parser.cwl_v1_2.Loop, requirements["Loop"])
        return {
            "loop": loop.loop,
            "outputMethod": (
                loop.outputMethod == "all_iterations"
                if loop.outputMethod == "all"
                else "last_iteration"
            ),
            "when": loop.loopWhen,
        }
    elif isinstance(cwl_element, cwl_utils.parser.LoopWorkflowStep):
        return {
            "outputMethod": cwl_element.outputMethod or "last_iteration",
            "loop": cwl_element.loop,
            "when": None,
        }
    else:
        return None


def _get_path(element_id: str) -> str:
    path = element_id
    if "#" in path:
        path = path.split("#")[0]
    if path.startswith("file://"):
        path = urllib.parse.unquote(path[7:])
    return path


def _get_schedule_step(step: Step) -> ScheduleStep:
    if job_port := step.get_input_port("__job__"):
        return cast(ScheduleStep, next(iter(job_port.get_input_steps())))
    else:
        raise WorkflowDefinitionException(f"Step {step.name} has no job input")


def _get_schema_def_types(
    requirements: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    return (
        {sd.name: sd for sd in requirements["SchemaDefRequirement"].types}
        if "SchemaDefRequirement" in requirements
        else {}
    )


def _get_secondary_files(
    cwl_element, default_required: bool
) -> MutableSequence[SecondaryFile]:
    if not cwl_element:
        return []
    secondary_files = []
    for sf in cwl_element:
        if isinstance(sf, str):
            secondary_files.append(SecondaryFile(pattern=sf, required=default_required))
        elif isinstance(sf, get_args(cwl_utils.parser.SecondaryFileSchema)):
            secondary_files.append(
                SecondaryFile(
                    pattern=sf.pattern,
                    required=(
                        sf.required if sf.required is not None else default_required
                    ),
                )
            )
    return secondary_files


def _inject_value(value: Any):
    if isinstance(value, MutableSequence):
        return [_inject_value(v) for v in value]
    elif isinstance(value, MutableMapping):
        if utils.get_token_class(value) is not None:
            return value
        else:
            return {k: _inject_value(v) for k, v in value.items()}
    elif isinstance(value, get_args(cwl_utils.parser.File)):
        dict_value = {"class": value.class_}
        if value.basename is not None:
            dict_value["basename"] = value.basename
        if value.checksum is not None:
            dict_value["checksum"] = value.checksum
        if value.contents is not None:
            dict_value["contents"] = value.contents
        if value.dirname is not None:
            dict_value["dirname"] = value.dirname
        if value.format is not None:
            dict_value["format"] = value.format
        if value.location is not None:
            dict_value["location"] = value.location
        if value.nameext is not None:
            dict_value["nameext"] = value.nameext
        if value.nameroot is not None:
            dict_value["nameroot"] = value.nameroot
        if value.path is not None:
            dict_value["path"] = value.path
        if value.secondaryFiles is not None:
            dict_value["secondaryFiles"] = [
                _inject_value(sf) for sf in value.secondaryFiles
            ]
        if value.size is not None:
            dict_value["size"] = value.size
        return dict_value
    elif isinstance(value, get_args(cwl_utils.parser.Directory)):
        dict_value = {"class": value.class_}
        if value.basename is not None:
            dict_value["basename"] = value.basename
        if value.listing is not None:
            dict_value["listing"] = [_inject_value(sf) for sf in value.listing]
        if value.location is not None:
            dict_value["location"] = value.location
        if value.path is not None:
            dict_value["path"] = value.path
        return dict_value
    elif isinstance(value, get_args(cwl_utils.parser.Dirent)):
        dict_value = {"entry": value.entry}
        if value.entryname is not None:
            dict_value["entryname"] = value.entryname
        if value.writable is not None:
            dict_value["writable"] = value.writable
        return dict_value
    else:
        return value


def _is_optional_port(
    port_type: (
        str
        | cwl_utils.parser.InputSchema
        | cwl_utils.parser.OutputSchema
        | MutableSequence[
            str,
            cwl_utils.parser.OutputSchema,
            cwl_utils.parser.InputSchema,
        ]
    ),
) -> bool:
    if isinstance(port_type, MutableSequence):
        return "null" in port_type
    elif isinstance(port_type, str):
        return "null" == port_type
    else:
        return False


def _percolate_port(port_name: str, *args) -> Port | None:
    for arg in args:
        if port_name in arg:
            port = arg[port_name]
            if isinstance(port, Port):
                return port
            else:
                return _percolate_port(port, *args)
    return None


def _process_docker_image(
    docker_requirement: cwl_utils.parser.DockerRequirement,
) -> str:
    # Retrieve image
    if docker_requirement.dockerPull is not None:
        return cast(str, docker_requirement.dockerPull)
    elif docker_requirement.dockerImageId is not None:
        return cast(str, docker_requirement.dockerImageId)
    else:
        raise WorkflowDefinitionException(
            "DockerRequirements without `dockerPull` or `dockerImageId` are not supported yet"
        )


def _process_docker_requirement(
    config_dir: str,
    config: CWLDockerTranslatorConfig,
    context: MutableMapping[str, Any],
    docker_requirement: cwl_utils.parser.DockerRequirement,
    network_access: bool,
    target: Target,
) -> Target:
    if docker_requirement.dockerOutputDirectory is not None:
        output_directory = docker_requirement.dockerOutputDirectory
        context["output_directory"] = output_directory
    else:
        output_directory = None
    if config.type not in cwl_docker_translator_classes:
        raise WorkflowDefinitionException(
            f"Container type `{config.type}` not supported"
        )
    translator_type = cwl_docker_translator_classes[config.type]
    translator = cast(
        CWLDockerTranslator,
        translator_type(
            config_dir=config_dir,
            wrapper=(config.wrapper if target.deployment.type != "local" else False),
            **config.config,
        ),
    )
    return translator.get_target(
        image=_process_docker_image(docker_requirement=docker_requirement),
        output_directory=output_directory,
        network_access=network_access,
        target=target,
    )


def _process_javascript_requirement(
    requirements: MutableMapping[str, Any],
) -> tuple[MutableSequence[str] | None, bool]:
    expression_lib = None
    full_js = False
    if "InlineJavascriptRequirement" in requirements:
        full_js = True
        if requirements["InlineJavascriptRequirement"].expressionLib is not None:
            expression_lib = list(
                requirements["InlineJavascriptRequirement"].expressionLib
            )
    return expression_lib, full_js


def _process_loop_transformers(
    step_name: str,
    input_ports: MutableMapping[str, Port],
    loop_input_ports: MutableMapping[str, Port],
    transformers: MutableMapping[str, LoopValueFromTransformer],
    input_dependencies: MutableMapping[str, set[str]],
) -> MutableMapping[str, Port]:
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
    return (
        cast(dict[str, Port], input_ports)
        | cast(dict[str, Port], loop_input_ports)
        | new_input_ports
    )


def _process_transformers(
    step_name: str,
    input_ports: MutableMapping[str, Port],
    transformers: MutableMapping[str, Transformer],
    input_dependencies: MutableMapping[str, set[str]],
) -> MutableMapping[str, Port]:
    new_input_ports = {}
    for input_name, token_transformer in transformers.items():
        # If transformer has true dependencies, use them
        if not (dependencies := input_dependencies[input_name]):
            # Otherwise, if there are other inputs, use them as dependencies to preserve token tag
            if not (
                dependencies := {
                    k
                    for k in input_ports.keys()
                    if k != input_name and k not in input_dependencies.keys()
                }
            ):
                # Otherwise, if there is only one input without dependencies, use it as its own dependency
                dependencies = {input_name}
        # Process inputs to attach ports
        for dep_name in dependencies:
            if dep_name in input_ports:
                token_transformer.add_input_port(
                    posixpath.relpath(dep_name, step_name), input_ports[dep_name]
                )
        # Put transformer output ports in input ports map
        new_input_ports[input_name] = token_transformer.get_output_port()
    return cast(dict[str, Port], input_ports) | new_input_ports


def _get_source_name(global_name: str) -> str:
    return posixpath.relpath(global_name, PurePosixPath(global_name).parent.parent)


def create_command_output_processor_base(
    port_name: str,
    workflow: CWLWorkflow,
    port_target: Target | None,
    port_type: str | MutableSequence[str],
    cwl_element: (
        cwl_utils.parser.CommandOutputParameter
        | cwl_utils.parser.OutputRecordField
        | cwl_utils.parser.ExpressionToolOutputParameter
    ),
    context: MutableMapping[str, Any],
    optional: bool = False,
    single: bool = True,
) -> CommandOutputProcessor:
    if not isinstance(port_type, MutableSequence):
        port_type = [port_type]
    # Normalize port type (Python does not distinguish among all CWL number types)
    port_type = [
        "long" if t == "int" else "double" if t == "float" else t for t in port_type
    ]
    # Process InlineJavascriptRequirement
    requirements = context["hints"] | context["requirements"]
    expression_lib, full_js = _process_javascript_requirement(requirements)
    # Create OutputProcessor
    if isinstance(
        cwl_element, get_args(cwl_utils.parser.ExpressionToolOutputParameter)
    ):
        return CWLExpressionToolOutputProcessor(
            name=port_name,
            workflow=workflow,
            token_type=port_type[0] if len(port_type) == 1 else port_type,
            file_format=getattr(cwl_element, "format", None),
            optional=optional,
            streamable=getattr(cwl_element, "streamable", None),
        )
    elif "File" in port_type:
        return CWLCommandOutputProcessor(
            name=port_name,
            workflow=workflow,
            target=port_target,
            token_type=port_type[0] if len(port_type) == 1 else port_type,
            expression_lib=expression_lib,
            file_format=getattr(cwl_element, "format", None),
            full_js=full_js,
            glob=(
                cwl_element.outputBinding.glob
                if getattr(cwl_element, "outputBinding", None)
                else None
            ),
            load_contents=_get_load_contents(cwl_element),
            load_listing=_get_load_listing(cwl_element, context),
            optional=optional,
            output_eval=(
                cwl_element.outputBinding.outputEval
                if getattr(cwl_element, "outputBinding", None)
                else None
            ),
            secondary_files=_get_secondary_files(
                cwl_element=getattr(cwl_element, "secondaryFiles", None),
                default_required=False,
            ),
            single=single,
            streamable=getattr(cwl_element, "streamable", None),
        )
    else:
        return CWLCommandOutputProcessor(
            name=port_name,
            workflow=workflow,
            target=port_target,
            token_type=port_type[0] if len(port_type) == 1 else port_type,
            expression_lib=expression_lib,
            full_js=full_js,
            glob=(
                cwl_element.outputBinding.glob
                if getattr(cwl_element, "outputBinding", None)
                else None
            ),
            load_contents=_get_load_contents(cwl_element),
            load_listing=_get_load_listing(cwl_element, context),
            optional=optional,
            output_eval=(
                cwl_element.outputBinding.outputEval
                if getattr(cwl_element, "outputBinding", None)
                else None
            ),
            single=single,
        )


class CWLTranslator:
    def __init__(
        self,
        context: StreamFlowContext,
        name: str,
        output_directory: str,
        cwl_definition: (
            cwl_utils.parser.CommandLineTool
            | cwl_utils.parser.ExpressionTool
            | cwl_utils.parser.Workflow
        ),
        cwl_inputs: MutableMapping[str, Any],
        cwl_inputs_path: str | None,
        workflow_config: WorkflowConfig,
    ):
        self.context: StreamFlowContext = context
        self.name: str = name
        self.output_directory: str = output_directory
        self.cwl_definition: (
            cwl_utils.parser.CommandLineTool
            | cwl_utils.parser.ExpressionTool
            | cwl_utils.parser.Workflow
        ) = cwl_definition
        self.cwl_inputs: MutableMapping[str, Any] = cwl_inputs

        if cwl_inputs_path is not None:
            cwl_inputs_path = _get_path(Path(cwl_inputs_path).resolve().as_uri())
        self.cwl_inputs_path: str | None = cwl_inputs_path
        self.default_map: MutableMapping[str, Any] = {}
        self.deployment_map: MutableMapping[str, DeployStep] = {}
        self.gather_map: MutableMapping[str, str] = {}
        self.input_ports: MutableMapping[str, Port] = {}
        self.output_ports: MutableMapping[str, str | Port] = {}
        self.scatter: MutableMapping[str, Any] = {}
        self.workflow_config: WorkflowConfig = workflow_config

    def _get_deploy_step(self, deployment_config: DeploymentConfig, workflow: Workflow):
        if deployment_config.name not in self.deployment_map:
            self.deployment_map[deployment_config.name] = workflow.create_step(
                cls=DeployStep,
                name=posixpath.join("__deploy__", deployment_config.name),
                deployment_config=deployment_config,
            )
        return self.deployment_map[deployment_config.name]

    def _handle_inner_inputs(
        self,
        cwl_element: cwl_utils.parser.WorkflowStep,
        inner_cwl_element: cwl_utils.parser.Process,
        cwl_name_prefix: str,
        inner_cwl_name_prefix: str,
        default_ports: MutableMapping[str, Port],
        name_prefix: str,
        step_name: str,
        workflow: CWLWorkflow,
    ) -> None:
        # Get inner CWL object input names
        inner_input_ports = {
            posixpath.relpath(
                utils.get_name(step_name, inner_cwl_name_prefix, el.id), step_name
            )
            for el in inner_cwl_element.inputs
        }
        # Get WorkflowStep input names
        outer_input_ports = {
            posixpath.relpath(
                utils.get_name(
                    step_name,
                    utils.get_name(
                        name_prefix,
                        cwl_name_prefix,
                        cwl_element.id,
                        preserve_cwl_prefix=True,
                    ),
                    el.id,
                ),
                step_name,
            )
            for el in cwl_element.in_
        }
        # Create a `DefaultTransformer` for each internal input port
        # that is not present in outer input ports
        for port_name in inner_input_ports - outer_input_ports:
            global_name = os.path.join(step_name, port_name)
            default_ports[global_name] = self._handle_default_port(
                global_name=global_name,
                port_name=port_name,
                transformer_suffix="-step-default-transformer",
                port=workflow.create_port(),
                workflow=workflow,
                value=None,
            )

    def _get_input_port(
        self,
        workflow: Workflow,
        cwl_element: cwl_utils.parser.Process,
        element_input: cwl_utils.parser.InputParameter,
        global_name: str,
        port_name: str,
        default_ports: MutableMapping[str, Port],
        default_key: str,
    ) -> Port:
        # Retrieve or create input port
        if global_name not in self.input_ports:
            self.input_ports[global_name] = workflow.create_port()
        input_port = self.input_ports[global_name]
        # If there is a default value, construct a default port block
        if element_input.default is not None or _is_optional_port(element_input.type_):
            # Insert default port
            transformer_suffix = (
                "-wf-default-transformer"
                if isinstance(cwl_element, get_args(cwl_utils.parser.Workflow))
                else "-cmd-default-transformer"
            )
            input_port = self._handle_default_port(
                global_name=global_name,
                port_name=port_name,
                transformer_suffix=transformer_suffix,
                port=input_port,
                workflow=workflow,
                value=element_input.default,
            )
            default_ports[default_key] = input_port
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
        port: Port,
        workflow: Workflow,
        value: Any | None,
    ) -> Port:
        # Check output directory
        path = _get_path(self.cwl_definition.id)
        # Build default port
        default_port = workflow.create_port()
        if value is not None:
            self._inject_input(
                workflow=workflow,
                port_name="-".join([port_name, "default"]),
                global_name=global_name + transformer_suffix,
                port=default_port,
                output_directory=os.path.dirname(path),
                value=value,
            )
        # Add default transformer
        transformer = workflow.create_step(
            cls=DefaultRetagTransformer,
            name=global_name + transformer_suffix,
            default_port=default_port,
            primary_port=port_name,
        )
        transformer.add_input_port(port_name, port)
        transformer.add_output_port(port_name, workflow.create_port())
        return transformer.get_output_port()

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
            value = remap_token_value(
                path_processor, output_directory, target.workdir, _inject_value(value)
            )
        else:
            value = _inject_value(value)
        # Create a schedule step and connect it to the local DeployStep
        schedule_step = workflow.create_step(
            cls=ScheduleStep,
            name=posixpath.join(f"{global_name}-injector", "__schedule__"),
            job_prefix=f"{global_name}-injector",
            connector_ports={target.deployment.name: deploy_step.get_output_port()},
            binding_config=binding_config,
            input_directory=target.workdir or self.output_directory,
            output_directory=target.workdir or self.output_directory,
            tmp_directory=target.workdir or self.output_directory,
        )
        # Create a CWLInputInjector step to process the input
        injector_step = workflow.create_step(
            cls=CWLInputInjectorStep,
            name=global_name + "-injector",
            job_port=schedule_step.get_output_port(),
        )
        # Create an input port and inject values
        input_port = workflow.create_port()
        input_port.put(Token(value=value, recoverable=True))
        input_port.put(TerminationToken())
        # Connect input and output ports to the injector step
        injector_step.add_input_port(port_name, input_port)
        injector_step.add_output_port(port_name, port)
        # Add the input port of the InputInjectorStep to the workflow input ports
        workflow.input_ports[port_name] = input_port.name

    def _inject_inputs(self, workflow: Workflow) -> None:
        output_directory = None
        if self.cwl_inputs:
            # Compute output directory path
            output_directory = os.path.dirname(self.cwl_inputs_path)
        # Compute suffix
        default_suffix = (
            "-wf-default-transformer"
            if isinstance(self.cwl_definition, get_args(cwl_utils.parser.Workflow))
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
                input_port.put(Token(value=None, recoverable=True))
                input_port.put(TerminationToken())

    def _recursive_translate(
        self,
        workflow: CWLWorkflow,
        cwl_element: (
            cwl_utils.parser.CommandLineTool
            | cwl_utils.parser.ExpressionTool
            | cwl_utils.parser.Workflow
            | cwl_utils.parser.WorkflowStep
        ),
        context: MutableMapping[str, Any],
        name_prefix: str,
        cwl_name_prefix: str,
    ) -> None:
        # Update context
        current_context = _copy_context(context)
        for hint in cwl_element.hints or []:
            if not isinstance(hint, MutableMapping):
                current_context["hints"][hint.class_] = hint
            else:
                # Fail if `cwltool:Loop` is defined as a hint
                if hint["class"] == "cwltool:Loop":
                    raise WorkflowDefinitionException(
                        "The `cwltool:Loop` clause is valid only under requirements."
                    )
        for requirement in cwl_element.requirements or []:
            if requirement.class_ == "Loop":
                if not isinstance(cwl_element, get_args(cwl_utils.parser.WorkflowStep)):
                    raise WorkflowDefinitionException(
                        "The `cwltool:Loop` clause is not compatible "
                        f"with the `{cwl_element.__class__.__name__}` class."
                    )
                if (
                    cast(cwl_utils.parser.ScatterWorkflowStep, cwl_element).scatter
                    is not None
                ):
                    raise WorkflowDefinitionException(
                        "The `cwltool:Loop` clause is not compatible with the `scatter` directive."
                    )
                if cwl_element.when is not None:
                    raise WorkflowDefinitionException(
                        "The `cwltool:Loop` clause is not compatible with the `when` directive."
                    )
            current_context["requirements"][requirement.class_] = requirement
        # In the root process, override requirements when provided in the input file
        if name_prefix == posixpath.sep:
            req_string = "cwl:requirements"
            if req_string in self.cwl_inputs:
                if context["version"] == "v1.0":
                    raise WorkflowDefinitionException(
                        "`cwl:requirements` in the input object is not part of CWL v1.0."
                    )
                else:
                    for requirement in self.cwl_inputs[req_string]:
                        current_context["requirements"][
                            requirement.class_
                        ] = requirement
        # Dispatch element
        if isinstance(cwl_element, get_args(cwl_utils.parser.Workflow)):
            self._translate_workflow(
                workflow=workflow,
                cwl_element=cwl_element,
                context=current_context,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
            )
        elif isinstance(cwl_element, get_args(cwl_utils.parser.WorkflowStep)):
            self._translate_workflow_step(
                workflow=workflow,
                cwl_element=cwl_element,
                context=current_context,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
            )
        elif isinstance(cwl_element, get_args(cwl_utils.parser.CommandLineTool)):
            self._translate_command_line_tool(
                workflow=workflow,
                cwl_element=cwl_element,
                context=current_context,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
            )
        elif isinstance(cwl_element, get_args(cwl_utils.parser.ExpressionTool)):
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
                + cwl_element.__class__.__name__
                + " not supported"
            )

    def _translate_command_line_tool(
        self,
        workflow: CWLWorkflow,
        cwl_element: cwl_utils.parser.CommandLineTool | cwl_utils.parser.ExpressionTool,
        context: MutableMapping[str, Any],
        name_prefix: str,
        cwl_name_prefix: str,
    ):
        context["elements"][cwl_element.id] = cwl_element
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Translating {cwl_element.__class__.__name__} {name_prefix}")
        # Extract custom types if present
        requirements = context["hints"] | context["requirements"]
        schema_def_types = _get_schema_def_types(requirements)
        # Process InlineJavascriptRequirement
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Retrieve target
        binding_config = get_binding_config(name_prefix, "step", self.workflow_config)
        # Process DockerRequirement
        if "DockerRequirement" in requirements:
            network_access = (
                requirements["NetworkAccess"].networkAccess
                if "NetworkAccess" in requirements
                else False
            )
            binding_config.targets = [
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
                for target in binding_config.targets
            ]
        # Create DeploySteps to initialise the execution environment
        deployments = {t.deployment.name: t.deployment for t in binding_config.targets}
        deploy_steps = {
            name: self._get_deploy_step(deployment, workflow)
            for name, deployment in deployments.items()
        }
        # Create a schedule step and connect it to the DeployStep
        schedule_step = workflow.create_step(
            cls=CWLScheduleStep,
            name=posixpath.join(name_prefix, "__schedule__"),
            job_prefix=name_prefix,
            connector_ports={
                name: step.get_output_port() for name, step in deploy_steps.items()
            },
            binding_config=binding_config,
            hardware_requirement=_get_hardware_requirement(
                cwl_version=context["version"],
                requirements=requirements,
                expression_lib=expression_lib,
                full_js=full_js,
            ),
            output_directory=context.get("output_directory"),
        )
        # Create the ExecuteStep and connect it to the ScheduleStep
        step = workflow.create_step(
            cls=CWLExecuteStep,
            name=name_prefix,
            job_port=schedule_step.get_output_port(),
            recoverable=(
                requirements["WorkReuse"].enableReuse
                if "WorkReuse" in requirements
                else True
            ),
            expression_lib=expression_lib,
            full_js=full_js,
        )
        # Process inputs
        input_ports = {}
        token_transformers = []
        default_ports = {}
        for element_input in cwl_element.inputs:
            global_name = utils.get_name(name_prefix, cwl_name_prefix, element_input.id)
            port_name = posixpath.relpath(global_name, name_prefix)
            # Retrieve or create input port
            input_ports[port_name] = self._get_input_port(
                workflow=workflow,
                cwl_element=cwl_element,
                element_input=element_input,
                global_name=global_name,
                port_name=port_name,
                default_ports=default_ports,
                default_key=port_name,
            )
        _adjust_default_ports(workflow, name_prefix, default_ports, input_ports, "cmd")
        input_ports |= default_ports
        for element_input in cwl_element.inputs:
            global_name = utils.get_name(name_prefix, cwl_name_prefix, element_input.id)
            port_name = posixpath.relpath(global_name, name_prefix)
            # Add a token transformer step to process inputs
            token_transformer = _create_token_transformer(
                name=global_name + "-token-transformer",
                port_name=port_name,
                workflow=workflow,
                cwl_element=element_input,
                cwl_name_prefix=posixpath.join(cwl_name_prefix, port_name),
                schema_def_types=schema_def_types,
                context=context,
                only_propagate_secondary_files=(name_prefix != "/"),
            )
            # Add the output port as an input of the schedule step
            schedule_step.add_input_port(port_name, token_transformer.get_output_port())
            if isinstance(cwl_element, get_args(cwl_utils.parser.CommandLineTool)):
                # Create a TransferStep
                transfer_step = workflow.create_step(
                    cls=CWLTransferStep,
                    name=posixpath.join(name_prefix, "__transfer__", port_name),
                    job_port=schedule_step.get_output_port(),
                )
                transfer_step.add_input_port(
                    port_name, token_transformer.get_output_port()
                )
                transfer_step.add_output_port(port_name, workflow.create_port())
                # Connect the transfer step with the ExecuteStep
                step.add_input_port(port_name, transfer_step.get_output_port(port_name))
            elif isinstance(cwl_element, get_args(cwl_utils.parser.ExpressionTool)):
                # Connect the token transformer step with the ExecuteStep
                step.add_input_port(port_name, token_transformer.get_output_port())
            # Store input port and token transformer
            token_transformers.append(token_transformer)
        # Add input ports to token transformers
        for port_name, input_port in input_ports.items():
            for token_transformer in token_transformers:
                token_transformer.add_input_port(port_name, input_port)
        # Process outputs
        for element_output in cwl_element.outputs:
            global_name = utils.get_name(
                name_prefix, cwl_name_prefix, element_output.id
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
                cwl_element, get_args(cwl_utils.parser.ExpressionTool)
            ) and context["version"] in [
                "v1.0",
                "v1.1",
                "v1.2",
            ]:
                if isinstance(element_output.type_, MutableSequence):
                    port_type = element_output.type_
                    if "null" not in port_type:
                        port_type.append("null")
                    if "Any" not in port_type:
                        port_type.append("Any")
                elif isinstance(element_output.type_, str):
                    port_type = [element_output.type_]
                    if element_output.type_ != "null":
                        port_type.append("null")
                    if element_output.type_ != "Any":
                        port_type.append("Any")
                else:
                    port_type = ["null", element_output.type_, "Any"]
            else:
                port_type = element_output.type_
            # Add output port to ExecuteStep
            output_processor = _create_command_output_processor(
                port_name=port_name,
                workflow=workflow,
                port_target=port_target,
                port_type=port_type,
                cwl_element=element_output,
                cwl_name_prefix=posixpath.join(cwl_name_prefix, port_name),
                schema_def_types=schema_def_types,
                context=context,
            )
            pop_processor = PopCommandOutputProcessor(
                name=port_name,
                workflow=workflow,
                processor=output_processor,
            )
            if isinstance(output_processor, UnionCommandOutputProcessor):
                output_processor.processors.append(pop_processor)
            else:
                output_processor = UnionCommandOutputProcessor(
                    name=port_name,
                    workflow=workflow,
                    processors=[
                        pop_processor,
                        output_processor,
                    ],
                )
            step.add_output_port(
                name=port_name,
                port=output_port,
                output_processor=output_processor,
            )
        if isinstance(cwl_element, get_args(cwl_utils.parser.CommandLineTool)):
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
                step.command.time_limit = requirements["ToolTimeLimit"].timelimit
        elif isinstance(cwl_element, get_args(cwl_utils.parser.ExpressionTool)):
            step.command = CWLExpressionCommand(step, cwl_element.expression)
        # Add JS requirements
        step.command.expression_lib = expression_lib
        step.command.full_js = full_js

    def _translate_workflow(
        self,
        workflow: CWLWorkflow,
        cwl_element: cwl_utils.parser.Workflow,
        context: MutableMapping[str, Any],
        name_prefix: str,
        cwl_name_prefix: str,
    ) -> None:
        try:
            cwl_utils.parser.utils.static_checker(cwl_element)
        except ValidationException as ve:
            raise WorkflowDefinitionException from ve
        context["elements"][cwl_element.id] = cwl_element
        step_name = name_prefix
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Translating Workflow {step_name}")
        # Extract custom types if present
        requirements = context["hints"] | context["requirements"]
        schema_def_types = _get_schema_def_types(requirements)
        # Extract JavaScript requirements
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Process inputs to create steps
        default_ports: MutableMapping[str, Port] = {}
        input_ports: MutableMapping[str, Port] = {}
        token_transformers: MutableMapping[str, Transformer] = {}
        input_dependencies: MutableMapping[str, set[str]] = {}
        for element_input in cwl_element.inputs:
            global_name = utils.get_name(step_name, cwl_name_prefix, element_input.id)
            port_name = posixpath.relpath(global_name, step_name)
            # Retrieve or create input port
            input_ports[global_name] = self._get_input_port(
                workflow=workflow,
                cwl_element=cwl_element,
                element_input=element_input,
                global_name=global_name,
                port_name=port_name,
                default_ports=default_ports,
                default_key=global_name,
            )
        _adjust_default_ports(workflow, name_prefix, default_ports, input_ports, "wf")
        input_ports |= default_ports
        for element_input in cwl_element.inputs:
            global_name = utils.get_name(step_name, cwl_name_prefix, element_input.id)
            port_name = posixpath.relpath(global_name, step_name)
            # Create token transformer step
            token_transformers[global_name] = _create_token_transformer(
                name=global_name + "-token-transformer",
                port_name=port_name,
                workflow=workflow,
                cwl_element=element_input,
                cwl_name_prefix=posixpath.join(cwl_name_prefix, port_name),
                schema_def_types=schema_def_types,
                context=context,
                only_propagate_secondary_files=(name_prefix != "/"),
            )
            # Process dependencies
            local_deps = resolve_dependencies(
                expression=element_input.format,
                full_js=full_js,
                expression_lib=expression_lib,
            )
            for secondary_file in _get_secondary_files(
                element_input.secondaryFiles, True
            ):
                local_deps.update(
                    resolve_dependencies(
                        expression=secondary_file.pattern,
                        full_js=full_js,
                        expression_lib=expression_lib,
                    ),
                    resolve_dependencies(
                        expression=secondary_file.required,
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
        for element_output in cwl_element.outputs:
            global_name = utils.get_name(
                name_prefix, cwl_name_prefix, element_output.id
            )
            link_merge = element_output.linkMerge
            pick_value = (
                None
                if context["version"] in ["v1.0", "v1.1"]
                else element_output.pickValue
            )
            # If outputSource element is a list, the output element can depend on multiple ports
            if isinstance(element_output.outputSource, MutableSequence):
                # If the list contains only one element and no `linkMerge` or `pickValue` are specified
                if (
                    len(element_output.outputSource) == 1
                    and link_merge is None
                    and pick_value is None
                ):
                    # Treat it as a singleton
                    source_name = utils.get_name(
                        name_prefix, cwl_name_prefix, element_output.outputSource[0]
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
                        len(element_output.outputSource) > 1
                        and "MultipleInputFeatureRequirement" not in requirements
                    ):
                        raise WorkflowDefinitionException(
                            "Workflow contains multiple inbound links to a single parameter "
                            "but MultipleInputFeatureRequirement is not declared."
                        )
                    source_names = [
                        utils.get_name(name_prefix, cwl_name_prefix, src)
                        for src in element_output.outputSource
                    ]
                    ports = {
                        n: self._get_source_port(workflow, n) for n in source_names
                    }
                    _create_list_merger(
                        name=global_name,
                        workflow=workflow,
                        ports=ports,
                        output_port=self._get_source_port(workflow, global_name),
                        link_merge=link_merge,
                        pick_value=pick_value,
                    )
            # Otherwise, the output element depends on a single output port
            else:
                source_name = utils.get_name(
                    name_prefix, cwl_name_prefix, element_output.outputSource
                )
                # If `pickValue` is specified, create a ListMergeCombinator
                if pick_value is not None:
                    source_port = self._get_source_port(workflow, source_name)
                    _create_list_merger(
                        name=global_name,
                        workflow=workflow,
                        ports={source_name: source_port},
                        output_port=self._get_source_port(workflow, global_name),
                        link_merge=link_merge,
                        pick_value=pick_value,
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
        workflow: CWLWorkflow,
        cwl_element: cwl_utils.parser.WorkflowStep,
        context: MutableMapping[str, Any],
        name_prefix: str,
        cwl_name_prefix: str,
    ) -> None:
        # Process content
        context["elements"][cwl_element.id] = cwl_element
        step_name = utils.get_name(name_prefix, cwl_name_prefix, cwl_element.id)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Translating WorkflowStep {step_name}")
        cwl_step_name = utils.get_name(
            name_prefix, cwl_name_prefix, cwl_element.id, preserve_cwl_prefix=True
        )
        requirements = context["hints"] | context["requirements"]
        # Extract JavaScript requirements
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Find scatter elements
        if isinstance(cwl_element, get_args(cwl_utils.parser.ScatterWorkflowStep)):
            if isinstance(cwl_element.scatter, str):
                scatter_inputs = [
                    utils.get_name(step_name, cwl_step_name, cwl_element.scatter)
                ]
            else:
                scatter_inputs = [
                    utils.get_name(step_name, cwl_step_name, n)
                    for n in cwl_element.scatter or []
                ]
        else:
            scatter_inputs = []
        # Check if `ScatterFeatureRequirement` is defined when a step defines the `scatter` property
        if scatter_inputs and "ScatterFeatureRequirement" not in requirements:
            raise WorkflowDefinitionException(
                "Workflow contains scatter but ScatterFeatureRequirement "
                "not in requirements"
            )

        # Process inner element
        run_command, inner_cwl_name_prefix, inner_context = process_embedded_tool(
            cwl_element=cwl_element,
            step_name=step_name,
            name_prefix=name_prefix,
            cwl_name_prefix=cwl_name_prefix,
            context=context,
        )
        # If the inner command is a workflow, check if `SubworkflowFeatureRequirement` is defined
        if isinstance(cwl_element, get_args(cwl_utils.parser.Workflow)):
            if "SubworkflowFeatureRequirement" not in requirements:
                raise WorkflowDefinitionException(
                    "Workflow contains embedded workflow but "
                    "SubworkflowFeatureRequirement not in requirements"
                )
        # Handle optional input variables
        default_ports: MutableMapping[str, Port] = {}
        # Process inputs
        input_ports: MutableMapping[str, Port] = {}
        value_from_transformers: MutableMapping[str, ValueFromTransformer] = {}
        input_dependencies: MutableMapping[str, set[str]] = {}
        self._handle_inner_inputs(
            cwl_element=cwl_element,
            inner_cwl_element=run_command,
            cwl_name_prefix=cwl_name_prefix,
            inner_cwl_name_prefix=inner_cwl_name_prefix,
            default_ports=default_ports,
            name_prefix=name_prefix,
            step_name=step_name,
            workflow=workflow,
        )
        for element_input in cwl_element.in_:
            self._translate_workflow_step_input(
                workflow=workflow,
                context=context,
                element_id=cwl_element.id,
                element_input=element_input,
                element_source=element_input.source,
                name_prefix=name_prefix,
                cwl_name_prefix=cwl_name_prefix,
                requirements=requirements,
                input_ports=input_ports,
                default_ports=default_ports,
                value_from_transformers=value_from_transformers,
                input_dependencies=input_dependencies,
            )
        _adjust_default_ports(
            workflow,
            name_prefix,
            default_ports,
            input_ports,
            "step",
            list(input_dependencies.keys()),
        )
        input_ports |= default_ports
        # If there are scatter inputs
        if scatter_inputs:
            # Retrieve scatter method (default to dotproduct)
            scatter_method = (
                cast(cwl_utils.parser.ScatterWorkflowStep, cwl_element).scatterMethod
                or "dotproduct"
            )
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
            size_ports = {}
            scatter_combinator: Combinator | None = None
            scatter_size_transformer: Transformer | None = None
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
                    scatter_size_transformer = workflow.create_step(
                        cls=DotProductSizeTransformer,
                        name=step_name + "-scatter-size-transformer",
                    )
                else:
                    scatter_combinator = CartesianProductCombinator(
                        workflow=workflow, name=step_name + "-scatter-combinator"
                    )
                    for global_name in scatter_inputs:
                        scatter_combinator.add_item(
                            posixpath.relpath(global_name, step_name)
                        )
                    if scatter_method == "flat_crossproduct":
                        scatter_size_transformer = workflow.create_step(
                            cls=CartesianProductSizeTransformer,
                            name=step_name + "-scatter-size-transformer",
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
                size_ports[global_name] = scatter_step.get_size_port()
            if len(scatter_inputs) == 1:
                # use the size_port of ScatterStep as the size_port of GatherStep
                size_port = next(iter(size_ports.values()))

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
                if scatter_size_transformer:
                    output_port_names = []
                    for global_name in scatter_inputs:
                        port_name = posixpath.relpath(global_name, step_name)
                        scatter_size_transformer.add_input_port(
                            port_name, size_ports[global_name]
                        )
                        output_port_names.append(port_name)
                    size_port = workflow.create_port()
                    scatter_size_transformer.add_output_port(
                        "-".join(output_port_names), size_port
                    )

        # Process inputs again to attach ports to `valueFrom` transformers
        input_ports = _process_transformers(
            step_name=step_name,
            input_ports=input_ports,
            transformers=value_from_transformers,
            input_dependencies=input_dependencies,
        )

        # Get loop if present
        if (loop := _get_loop(cwl_element, requirements)) is not None:
            # Create loop conditional step
            if loop["when"] is not None:
                _create_loop_condition(
                    condition=loop["when"],
                    expression_lib=expression_lib,
                    full_js=full_js,
                    input_ports=input_ports,
                    step_name=step_name,
                    workflow=workflow,
                )

        # Save input ports in the global map
        self.input_ports |= input_ports
        # Process condition
        conditional_step = None
        cwl_condition = (
            None if context["version"] in ["v1.0", "v1.1"] else cwl_element.when
        )
        if cwl_condition is not None:
            if loop is not None:
                # Create loop conditional step
                conditional_step = _create_loop_condition(
                    condition=cwl_condition,
                    expression_lib=expression_lib,
                    full_js=full_js,
                    input_ports=input_ports,
                    step_name=step_name,
                    workflow=workflow,
                )
                self.input_ports |= input_ports
            else:
                # Create conditional step
                conditional_step = workflow.create_step(
                    cls=CWLConditionalStep,
                    name=step_name + "-when",
                    expression=cwl_condition,
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
        for element_output in cwl_element.out:
            global_name = utils.get_name(step_name, cwl_step_name, element_output)
            port_name = posixpath.relpath(global_name, step_name)
            # Retrieve or create output port
            if global_name not in self.output_ports:
                self.output_ports[global_name] = workflow.create_port()
            external_output_ports[global_name] = self.output_ports[global_name]
            internal_output_ports[global_name] = self.output_ports[global_name]
            # If there are scatter inputs
            if scatter_inputs:
                # Retrieve scatter method (default to dotproduct)
                scatter_method = (
                    cast(
                        cwl_utils.parser.ScatterWorkflowStep, cwl_element
                    ).scatterMethod
                    or "dotproduct"
                )
                # Perform a gather on the outputs
                if scatter_method == "nested_crossproduct":
                    gather_steps = []
                    internal_output_ports[global_name] = workflow.create_port()
                    gather_input_port = internal_output_ports[global_name]

                    # build clone size transformers
                    scatter_step = cast(
                        ScatterStep, workflow.steps[scatter_inputs[0] + "-scatter"]
                    )
                    ext_port_sizes = {}
                    for ext_scatter_input in scatter_inputs[1:]:
                        ext_scatter_step = cast(
                            ScatterStep,
                            workflow.steps[ext_scatter_input + "-scatter"],
                        )
                        ext_port_name = ext_scatter_step.get_input_port_name()
                        ext_port_sizes[ext_port_name] = ext_scatter_step.get_size_port()
                    size_ports_list = _create_nested_size_tag(
                        ext_port_sizes,
                        {
                            scatter_step.get_input_port_name(): scatter_step.get_size_port()
                        },
                        step_name,
                        workflow,
                    )

                    for scatter_input, size_port in zip(
                        scatter_inputs, size_ports_list, strict=True
                    ):
                        scatter_port_name = posixpath.relpath(scatter_input, step_name)
                        gather_step = workflow.create_step(
                            cls=GatherStep,
                            name=global_name + "-gather-" + scatter_port_name,
                            size_port=size_port,
                        )
                        gather_steps.append(gather_step)
                        gather_step.add_input_port(port_name, gather_input_port)
                        gather_step.add_output_port(
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
                        size_port=size_port,
                        depth=(
                            1 if scatter_method == "dotproduct" else len(scatter_inputs)
                        ),
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
                    CWLEmptyScatterConditionalStep,
                    workflow.steps[step_name + "-empty-scatter-condition"],
                )
                empty_scatter_conditional_step.add_skip_port(
                    port_name, external_output_ports[global_name]
                )
            # Add skip ports if there is a condition without a loop
            if cwl_condition and loop is None:
                cast(CWLConditionalStep, conditional_step).add_skip_port(
                    port_name, internal_output_ports[global_name]
                )
        # Process loop outputs
        if loop is not None:
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
                    cls=(
                        CWLLoopOutputLastStep
                        if loop["outputMethod"] == "last_iteration"
                        else CWLLoopOutputAllStep
                    ),
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
            for loop_input in loop["loop"] or []:
                # Extract element source
                if "Loop" in requirements:
                    element_source = (
                        loop_input.loopSource
                        if loop_input.loopSource is not None
                        else loop_input.id
                    )
                else:
                    element_source = (
                        loop_input.outputSource
                        if loop_input.outputSource is not None
                        else loop_input.id
                    )
                self._translate_workflow_step_input(
                    workflow=workflow,
                    context=context,
                    element_id=cwl_element.id,
                    element_input=loop_input,
                    element_source=element_source,
                    name_prefix=name_prefix,
                    cwl_name_prefix=cwl_name_prefix,
                    requirements=requirements,
                    input_ports=loop_input_ports,
                    default_ports=loop_default_ports,
                    value_from_transformers=loop_value_from_transformers,
                    input_dependencies=loop_input_dependencies,
                    inner_steps_prefix="-loop",
                    value_from_transformer_cls=LoopValueFromTransformer,
                )
            _adjust_default_ports(
                workflow,
                name_prefix,
                loop_default_ports,
                loop_input_ports,
                "loop-step",
                list(loop_input_dependencies.keys()),
            )
            loop_input_ports |= loop_default_ports
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
        # Add skip ports if there is a condition without a loop
        if cwl_condition and loop is None:
            for element_output in cwl_element.out:
                global_name = utils.get_name(step_name, cwl_step_name, element_output)
                port_name = posixpath.relpath(global_name, step_name)
                skip_port = (
                    external_output_ports[global_name]
                    if loop is not None
                    else internal_output_ports[global_name]
                )
                cast(CWLConditionalStep, conditional_step).add_skip_port(
                    port_name, skip_port
                )
        # Update output ports with the internal ones
        self.output_ports |= internal_output_ports
        self._recursive_translate(
            workflow=workflow,
            cwl_element=run_command,
            context=inner_context
            | {"requirements": {k: v for k, v in requirements.items() if k != "Loop"}},
            name_prefix=step_name,
            cwl_name_prefix=inner_cwl_name_prefix,
        )
        # Update output ports with the external ones
        self.output_ports |= external_output_ports

    def _translate_workflow_step_input(
        self,
        workflow: CWLWorkflow,
        context: MutableMapping[str, Any],
        element_id: str,
        element_input: cwl_utils.parser.WorkflowStepInput,
        element_source: str,
        name_prefix: str,
        cwl_name_prefix: str,
        requirements: MutableMapping[str, Any],
        input_ports: MutableMapping[str, Port],
        default_ports: MutableMapping[str, Port],
        value_from_transformers: MutableMapping[str, ValueFromTransformer],
        input_dependencies: MutableMapping[str, set[str]],
        inner_steps_prefix: str = "",
        value_from_transformer_cls: type[ValueFromTransformer] = ValueFromTransformer,
    ) -> None:
        # Extract custom types if present
        schema_def_types = _get_schema_def_types(requirements)
        # Extract JavaScript requirements
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Extract names
        step_name = utils.get_name(name_prefix, cwl_name_prefix, element_id)
        cwl_step_name = utils.get_name(
            name_prefix, cwl_name_prefix, element_id, preserve_cwl_prefix=True
        )
        global_name = utils.get_name(step_name, cwl_step_name, element_input.id)
        port_name = posixpath.relpath(global_name, step_name)
        # If element contains `valueFrom` directive
        if element_input.valueFrom:
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
                    port_type=["null", "Any"],
                    cwl_element=element_input,
                    cwl_name_prefix=posixpath.join(cwl_step_name, port_name),
                    schema_def_types=schema_def_types,
                    context=context,
                ),
                port_name=port_name,
                expression_lib=expression_lib,
                full_js=full_js,
                value_from=element_input.valueFrom,
            )
            value_from_transformers[global_name].add_output_port(
                port_name, workflow.create_port()
            )

            # Retrieve dependencies
            local_deps = resolve_dependencies(
                expression=element_input.valueFrom,
                full_js=full_js,
                expression_lib=expression_lib,
            )
            input_dependencies[global_name] = set.union(
                (
                    {global_name}
                    if element_source is not None or element_input.default is not None
                    else set()
                ),
                {posixpath.join(step_name, d) for d in local_deps},
            )
        # If `source` entry is present, process output dependencies
        if element_source is not None:
            link_merge = element_input.linkMerge
            pick_value = (
                None
                if context["version"] in ["v1.0", "v1.1"]
                else cast(
                    cwl_utils.parser.cwl_v1_2.WorkflowStepInput, element_input
                ).pickValue
            )
            # If source element is a list, the input element can depend on multiple ports
            if isinstance(element_source, MutableSequence):
                # If the list contains only one element and no `linkMerge` or `pickValue`
                # are specified, treat it as a singleton
                if (
                    len(element_source) == 1
                    and link_merge is None
                    and pick_value is None
                ):
                    source_name = utils.get_name(
                        name_prefix, cwl_name_prefix, next(iter(element_source))
                    )
                    source_port = self._get_source_port(workflow, source_name)
                    # If there is a default value, construct a default port block
                    if element_input.default is not None:
                        # Insert default port
                        source_port = self._handle_default_port(
                            global_name=global_name,
                            port_name=port_name,
                            transformer_suffix=inner_steps_prefix
                            + "-step-default-transformer",
                            port=source_port,
                            workflow=workflow,
                            value=element_input.default,
                        )
                    # Add source port to the list of input ports for the current step
                    input_ports[global_name] = source_port
                # Otherwise, create a ListMergeCombinator
                else:
                    if (
                        len(element_source) > 1
                        and "MultipleInputFeatureRequirement" not in requirements
                    ):
                        raise WorkflowDefinitionException(
                            "Workflow contains multiple inbound links to a single parameter "
                            "but MultipleInputFeatureRequirement is not declared."
                        )
                    source_names = [
                        utils.get_name(name_prefix, cwl_name_prefix, src)
                        for src in element_source
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
                        link_merge=link_merge,
                        pick_value=pick_value,
                    )
                    # Add ListMergeCombinator output port to the list of input ports for the current step
                    input_ports[global_name] = list_merger.get_output_port()
            # Otherwise, the input element depends on a single output port
            else:
                source_name = utils.get_name(
                    name_prefix, cwl_name_prefix, cast(str, element_source)
                )
                source_port = self._get_source_port(workflow, source_name)
                # If there is a default value, construct a default port block
                if element_input.default is not None:
                    # Insert default port
                    source_port = self._handle_default_port(
                        global_name=global_name,
                        port_name=port_name,
                        transformer_suffix=inner_steps_prefix
                        + "-step-default-transformer",
                        port=source_port,
                        workflow=workflow,
                        value=element_input.default,
                    )
                # Add source port to the list of input ports for the current step
                input_ports[global_name] = source_port
        # Otherwise, search for default values
        elif element_input.default is not None:
            default_ports[global_name] = self._handle_default_port(
                global_name=global_name,
                port_name=port_name,
                transformer_suffix=inner_steps_prefix + "-step-default-transformer",
                port=workflow.create_port(),
                workflow=workflow,
                value=element_input.default,
            )
        # Otherwise, inject a synthetic port into the workflow
        else:
            input_ports[global_name] = workflow.create_port()

    def translate(self) -> Workflow:
        # Parse streams
        cwl_utils.parser.utils.convert_stdstreams_to_files(self.cwl_definition)
        # Create workflow
        workflow = CWLWorkflow(
            context=self.context,
            config=self.workflow_config.config,
            name=self.name,
            cwl_version=cast(str, self.cwl_definition.cwlVersion),
            format_graph=self.cwl_definition.loadingOptions.graph,
        )
        # Create context
        context = _create_context(version=cast(str, self.cwl_definition.cwlVersion))
        # Compute root prefix
        workflow_id = self.cwl_definition.id
        cwl_root_prefix = (
            utils.get_name(posixpath.sep, posixpath.sep, workflow_id)
            if "#" in workflow_id
            else posixpath.sep
        )
        # Register data locations for config files
        deployment_name = LocalTarget.deployment_name
        path = _get_path(self.cwl_definition.id)
        self.context.data_manager.register_path(
            location=ExecutionLocation(
                deployment=deployment_name, local=True, name="__LOCAL__"
            ),
            path=path,
            relpath=os.path.basename(path),
        )
        if self.cwl_inputs:
            path = self.cwl_inputs_path
            self.context.data_manager.register_path(
                location=ExecutionLocation(
                    deployment=deployment_name, local=True, name="__LOCAL__"
                ),
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
        check_bindings(self.workflow_config, workflow)
        # Extract requirements
        for hint in self.cwl_definition.hints or []:
            if not isinstance(hint, MutableMapping):
                context["hints"][hint.class_] = hint
        for requirement in self.cwl_definition.requirements or []:
            context["requirements"][requirement.class_] = requirement
        requirements = context["hints"] | context["requirements"]
        # Extract workflow outputs
        cwl_elements = {
            utils.get_name("/", cwl_root_prefix, element.id): element
            for element in self.cwl_definition.outputs or []
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
                        expression=cwl_elements[output_name].format,
                        full_js=full_js,
                        expression_lib=expression_lib,
                    )
                    # Build transformer step
                    transformer_step = workflow.create_step(
                        cls=CWLTokenTransformer,
                        name=f"{output_name}-collector-transformer",
                        port_name=port_name,
                        processor=_create_token_processor(
                            port_name=port_name,
                            workflow=workflow,
                            port_type=cwl_elements[output_name].type_,
                            cwl_element=cwl_elements[output_name],
                            cwl_name_prefix=posixpath.join(cwl_root_prefix, port_name),
                            schema_def_types=_get_schema_def_types(requirements),
                            context=context,
                            optional=True,
                            default_required_sf=False,
                            force_deep_listing=True,
                        ),
                    )
                    # Add port as transformer input
                    transformer_step.add_input_port(port_name, port)
                    # If there are format dependencies, search for inputs in the port's input steps
                    for dep in format_deps:
                        for step in port.get_input_steps():
                            if dep in step.input_ports:
                                transformer_step.add_input_port(
                                    dep, step.get_input_port(dep)
                                )
                                break
                        else:
                            raise WorkflowDefinitionException(
                                f"Cannot retrieve {dep} input port."
                            )
                    # Create an output port for the transformer
                    transformer_step.add_output_port(port_name, workflow.create_port())
                    # Create a schedule step and connect it to the local DeployStep
                    schedule_step = workflow.create_step(
                        cls=ScheduleStep,
                        name=posixpath.join(f"{output_name}-collector", "__schedule__"),
                        job_prefix=f"{output_name}-collector",
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
                        name=f"{output_name}-collector",
                        job_port=schedule_step.get_output_port(),
                        prefix_path=False,
                        writable=True,
                    )
                    transfer_step.add_input_port(
                        port_name, transformer_step.get_output_port()
                    )
                    transfer_step.add_output_port(port_name, workflow.create_port())
                    # Add the output port of the TransferStep to the workflow output ports
                    workflow.output_ports[port_name] = (
                        transfer_step.get_output_port().name
                    )
        # Return the final workflow object
        return workflow
