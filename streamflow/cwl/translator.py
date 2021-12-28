from __future__ import annotations

import asyncio
import copy
import json
import os
import posixpath
import sys
import tempfile
from enum import Enum
from pathlib import PurePosixPath
from typing import MutableMapping, TYPE_CHECKING, Optional, MutableSequence, cast, Callable

import cwltool.command_line_tool
import cwltool.context
import cwltool.docker_id
import cwltool.load_tool
import cwltool.process
import cwltool.workflow
from rdflib import Graph
from ruamel.yaml.comments import CommentedSeq

from streamflow.config.config import WorkflowConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import ModelConfig
from streamflow.core.exception import WorkflowDefinitionException, WorkflowExecutionException
from streamflow.core.utils import random_name, get_local_target, flatten_list, get_tag
from streamflow.core.workflow import Port, OutputPort, Workflow, Target, Token, TerminationToken, \
    InputCombinator, InputPort, Status, OutputCombinator
from streamflow.cwl.command import CWLCommand, CWLExpressionCommand, CWLMapCommandToken, \
    CWLUnionCommandToken, CWLObjectCommandToken, CWLCommandToken, CWLCommandOutput, CWLStepCommand
from streamflow.cwl.condition import CWLCondition
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.token_processor import LoadListing, SecondaryFile, CWLTokenProcessor, CWLUnionTokenProcessor, \
    CWLMapTokenProcessor, CWLSkipTokenProcessor
from streamflow.cwl.utils import resolve_dependencies
from streamflow.workflow.combinator import DotProductInputCombinator, CartesianProductInputCombinator, \
    DotProductOutputCombinator, NondeterminateMergeOutputCombinator
from streamflow.workflow.port import DefaultInputPort, DefaultOutputPort, ScatterInputPort, GatherOutputPort, \
    ObjectTokenProcessor, DefaultTokenProcessor
from streamflow.workflow.step import BaseStep, BaseJob

if TYPE_CHECKING:
    from streamflow.core.workflow import Step, TokenProcessor
    from typing import Union, Any, Set


class LinkMergeMethod(Enum):
    merge_nested = 1
    merge_flattened = 2


class ScatterPredecessor(object):
    __slots__ = ('step', 'scatter_step')

    def __init__(self,
                 step: str,
                 scatter_step: str):
        self.step: str = step
        self.scatter_step: str = scatter_step


def _build_command(
        cwl_element: cwltool.command_line_tool.CommandLineTool,
        schema_def_types: MutableMapping[str, Any],
        context: MutableMapping[str, Any],
        step: Step) -> CWLCommand:
    command = CWLCommand(step)
    # Process InitialWorkDirRequirement
    requirements = {**context['hints'], **context['requirements']}
    if 'InitialWorkDirRequirement' in requirements:
        command.initial_work_dir = requirements['InitialWorkDirRequirement']['listing']
        command.absolute_initial_workdir_allowed = 'DockerRequirement' in context['requirements']
    # Process InplaceUpdateRequirement
    if 'InplaceUpdateRequirement' in requirements:
        command.inplace_update = requirements['InplaceUpdateRequirement']['inplaceUpdate']
    # Process EnvVarRequirement
    if 'EnvVarRequirement' in requirements:
        for env_entry in requirements['EnvVarRequirement']['envDef']:
            command.environment[env_entry['envName']] = env_entry['envValue']
    # Process ShellCommandRequirement
    if 'ShellCommandRequirement' in requirements:
        command.is_shell_command = True
    # Process success and failure codes
    if 'successCodes' in cwl_element.tool:
        command.success_codes = cwl_element.tool['successCodes']
    if 'permanentFailCodes' or 'temporaryFailCodes' in cwl_element.tool:
        command.failure_codes = cwl_element.tool.get('permanentFailCodes', [])
        command.failure_codes.extend(cwl_element.tool.get('temporaryFailCodes', []))
    # Process baseCommand
    if 'baseCommand' in cwl_element.tool:
        if isinstance(cwl_element.tool['baseCommand'], CommentedSeq):
            for command_token in cwl_element.tool['baseCommand']:
                command.base_command.append(command_token)
        else:
            command.base_command.append(cwl_element.tool['baseCommand'])
    # Process arguments
    if 'arguments' in cwl_element.tool:
        for argument in cwl_element.tool['arguments']:
            command.command_tokens.append(_get_command_token(
                binding=argument,
                is_shell_command=command.is_shell_command))
    # Process inputs
    for input_port in cwl_element.tool['inputs']:
        command_token = _get_command_token_from_input(
            cwl_element=input_port,
            port_type=input_port['type'],
            input_name=_get_name("", input_port['id'], last_element_only=True),
            is_shell_command=command.is_shell_command,
            schema_def_types=schema_def_types)
        if command_token is not None:
            command.command_tokens.append(command_token)
    # Process output directory
    command.output_directory = context.get('output_directory')
    return command


def _build_dependee(default_map: MutableMapping[str, Any],
                    global_name: str,
                    output_port: OutputPort) -> OutputPort:
    if global_name in default_map:
        default_port = default_map[global_name]
        return _create_output_combinator(
            name=random_name(),
            ports={output_port.name: output_port,
                   default_port.name: default_port},
            step=default_port.step,
            merge_strategy=_get_merge_strategy(None, 'first_non_null'))
    else:
        return output_port


def _check_scatter(ports: MutableSequence[InputPort]):
    visited_steps = [p.step.name for p in ports]
    while ports:
        port = ports.pop()
        if isinstance(port, ScatterInputPort):
            return True
        if isinstance(port.dependee, GatherOutputPort):
            return False
        elif isinstance(port.dependee, OutputCombinator):
            for p in port.dependee.ports.values():
                if isinstance(p, GatherOutputPort):
                    return False
        if port.dependee is not None:
            if isinstance(port.dependee, OutputCombinator):
                for port in port.dependee.ports.values():
                    step = port.step
                    if step is not None and step.name not in visited_steps:
                        visited_steps.append(step.name)
                    for p in step.input_ports.values():
                        ports.append(p)
            else:
                step = port.dependee.step
                if step is not None and step.name not in visited_steps:
                    visited_steps.append(step.name)
                    for p in step.input_ports.values():
                        ports.append(p)
    return None


def _create_context() -> MutableMapping[str, Any]:
    return {
        'default': {},
        'requirements': {},
        'hints': {}
    }


async def _create_default_port(port: Port,
                               value: Any) -> OutputPort:
    default_port = DefaultOutputPort(
        name="-".join([port.name, "default"]),
        step=port.step)
    default_port.token_processor = _infer_token_processor(
        port=default_port,
        token_processor=port.token_processor)
    await _inject_input(
        outdir=os.getcwd(),
        port=default_port,
        value=value)
    return default_port


async def _create_input_port(
        port_name: str,
        port_step: Step,
        global_name: str,
        default_map: MutableMapping[str, Any],
        port_description: MutableMapping[str, Any],
        schema_def_types: MutableMapping[str, Any],
        format_graph: Graph,
        context: MutableMapping[str, Any]) -> InputPort:
    # Create port
    port = DefaultInputPort(name=port_name, step=port_step)
    port.token_processor = _create_token_processor(
        port=port,
        port_type=port_description['type'],
        port_description=port_description,
        schema_def_types=schema_def_types,
        format_graph=format_graph,
        context=context)
    # Save default value
    if 'default' in port_description:
        default_map[global_name] = await _create_default_port(
            port=port,
            value=port_description['default'])
    port_step.input_ports[port_name] = port
    return port


def _create_output_combinator(name: str,
                              ports: MutableMapping[str, OutputPort],
                              step: Optional[Step] = None,
                              merge_strategy: Optional[
                                  Callable[[MutableSequence[Token]], MutableSequence[Token]]] = None):
    combinator = DotProductOutputCombinator(
        name=name,
        ports=ports,
        step=step,
        merge_strategy=merge_strategy)
    combinator.token_processor = DefaultTokenProcessor(port=combinator)
    return combinator


def _create_output_port(
        port_name: str,
        port_description: MutableMapping,
        schema_def_types: MutableMapping[str, Any],
        format_graph: Graph,
        context: MutableMapping[str, Any]) -> OutputPort:
    port = DefaultOutputPort(port_name)
    port.token_processor = _create_token_processor(
        port=port,
        port_type=port_description['type'],
        port_description=port_description,
        schema_def_types=schema_def_types,
        format_graph=format_graph,
        context=context)
    return port


def _create_skip_link(port: OutputPort,
                      step: Step) -> OutputPort:
    skip_port_name = random_name()
    skip_port = DefaultOutputPort(
        name=skip_port_name,
        step=step)
    skip_port.token_processor = CWLSkipTokenProcessor(port=skip_port)
    step.output_ports[skip_port_name] = skip_port
    combinator = NondeterminateMergeOutputCombinator(
        name=random_name(),
        step=step,
        ports={p.name: p for p in [port, skip_port]})
    combinator.token_processor = CWLUnionTokenProcessor(
        port=combinator,
        processors=[port.token_processor, skip_port.token_processor])
    return combinator


def _create_token_processor(
        port: Port,
        port_type: Any,
        port_description: MutableMapping[str, Any],
        schema_def_types: MutableMapping[str, Any],
        format_graph: Graph,
        context: MutableMapping[str, Any],
        optional: bool = False) -> TokenProcessor:
    if isinstance(port_type, MutableMapping):
        if 'type' in port_type:
            # Array type: -> MapTokenProcessor
            if port_type['type'] == 'array':
                processor = _create_token_processor(
                    port=port,
                    port_type=port_type['items'],
                    port_description=port_description,
                    schema_def_types=schema_def_types,
                    format_graph=format_graph,
                    context=context,
                    optional=optional)
                return CWLMapTokenProcessor(
                    port=port,
                    token_processor=processor,
                    optional=optional)
            # Enum type: -> substitute with string and propagate the description
            elif port_type['type'] == 'enum':
                return _create_token_processor(
                    port=port,
                    port_type='string',
                    port_description=port_description,
                    schema_def_types=schema_def_types,
                    format_graph=format_graph,
                    context=context,
                    optional=optional)
            # Generic object type -> propagate the type
            else:
                return _create_token_processor(
                    port=port,
                    port_type=port_type['type'],
                    port_description=port_type,
                    schema_def_types=schema_def_types,
                    format_graph=format_graph,
                    context=context,
                    optional=optional)
        # Untyped object -> not supported
        else:
            raise WorkflowDefinitionException("Unsupported dictionary type without explicit `type` key")
    elif isinstance(port_type, MutableSequence):
        optional = 'null' in port_type
        types = [t for t in filter(lambda x: x != 'null', port_type)]
        # Optional type (e.g. ['null', Type] -> Equivalent to Type?
        if len(types) == 1:
            return _create_token_processor(
                port=port,
                port_type=types[0],
                port_description=port_description,
                schema_def_types=schema_def_types,
                format_graph=format_graph,
                context=context,
                optional=optional)
        # List of types: -> UnionTokenProcessor
        else:
            token_processors = []
            for i, port_type in enumerate(types):
                token_processors.append(_create_token_processor(
                    port=port,
                    port_type=port_type,
                    port_description=port_description,
                    schema_def_types=schema_def_types,
                    format_graph=format_graph,
                    context=context))
            return CWLUnionTokenProcessor(
                port=port,
                processors=token_processors,
                optional=optional)
    # Record type: -> ObjectTokenProcessor
    elif port_type == 'record':
        token_processors = {}
        for port_type in port_description['fields']:
            key = _get_name("", port_type['name'], last_element_only=True)
            token_processors[key] = _create_token_processor(
                port=port,
                port_type=port_type['type'],
                port_description=port_type,
                schema_def_types=schema_def_types,
                format_graph=format_graph,
                context=context)
        return ObjectTokenProcessor(port, token_processors)
    # Optional type -> Propagate with optional = True
    elif port_type.endswith('?'):
        return _create_token_processor(
            port=port,
            port_type=port_type[:-1].strip(),
            port_description=port_description,
            schema_def_types=schema_def_types,
            format_graph=format_graph,
            context=context,
            optional=True)
    # Complex type -> Extract from schema definitions and propagate
    elif '#' in port_type:
        return _create_token_processor(
            port=port,
            port_type=schema_def_types[port_type],
            port_description=port_description,
            schema_def_types=schema_def_types,
            format_graph=format_graph,
            context=context,
            optional=optional)
    # Simple type -> Create typed token processor
    else:
        # Process InlineJavascriptRequirement
        requirements = {**context['hints'], **context['requirements']}
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Create token processor
        if port_type == 'File':
            file_format = port_description.get('format', None)
            streamable = port_description.get('streamable', False)
            location = port_description.get('path', None)
            processor = CWLTokenProcessor(
                port=port,
                port_type=port_type,
                expression_lib=expression_lib,
                file_format=file_format,
                format_graph=format_graph,
                full_js=full_js,
                glob=location,
                optional=optional,
                streamable=streamable)
        elif port_type == 'Directory':
            location = port_description.get('path', None)
            processor = CWLTokenProcessor(
                port=port,
                port_type='Directory',
                expression_lib=expression_lib,
                full_js=full_js,
                glob=location,
                optional=optional)
        else:
            # Normalize port type (Python does not distinguish among all CWL number types)
            port_type = 'long' if port_type == 'int' else 'double' if port_type == 'float' else port_type
            processor = CWLTokenProcessor(
                port=port,
                port_type=port_type,
                optional=optional,
                expression_lib=expression_lib,
                full_js=full_js)
        # Load listing
        processor.load_listing = _get_load_listing(port_description, context)
        # Load contents
        if 'loadContents' in port_description:
            processor.load_contents = port_description['loadContents']
        elif 'inputBinding' in port_description and 'loadContents' in port_description['inputBinding']:
            processor.load_contents = port_description['inputBinding']['loadContents']
        elif 'outputBinding' in port_description and 'loadContents' in port_description['outputBinding']:
            processor.load_contents = port_description['outputBinding']['loadContents']
        # Output binding
        if 'outputBinding' in port_description:
            output_binding = port_description['outputBinding']
            if 'glob' in output_binding:
                processor.glob = output_binding['glob']
            if 'outputEval' in output_binding:
                processor.output_eval = output_binding['outputEval']
        # Secondary files
        if 'secondaryFiles' in port_description:
            processor.secondary_files = _get_secondary_files(
                port_description['secondaryFiles'], default_required=False)
        return processor


def _dict_to_lists(groups: MutableMapping[str, Any], port_name: str) -> Token:
    if isinstance(groups, MutableMapping):
        groups_list = [_dict_to_lists(g, port_name) for g in groups.values()]
    else:
        groups_list = groups
    return Token(
        name=port_name,
        job=flatten_list([t.job for t in groups_list]),
        tag='.'.join(get_tag(groups_list).split('.')[:-1]),
        value=sorted(groups_list, key=lambda t: int(t.tag.split('.')[-1])))


def _get_command_token(binding: Any,
                       is_shell_command: bool = False,
                       input_name: Optional[str] = None,
                       token_type: Optional[str] = None) -> CWLCommandToken:
    # Normalize type (Python does not distinguish among all CWL number types)
    token_type = 'long' if token_type == 'int' else 'double' if token_type == 'float' else token_type
    if isinstance(binding, MutableMapping):
        item_separator = binding.get('itemSeparator', None)
        position = binding.get('position', 0)
        prefix = binding.get('prefix', None)
        separate = binding.get('separate', True)
        shell_quote = binding.get('shellQuote', True)
        value = binding['valueFrom'] if 'valueFrom' in binding else None
        return CWLCommandToken(
            name=input_name,
            value=value,
            token_type=token_type,
            is_shell_command=is_shell_command,
            item_separator=item_separator,
            position=position,
            prefix=prefix,
            separate=separate,
            shell_quote=shell_quote)
    else:
        return CWLCommandToken(
            name=input_name,
            value=binding,
            token_type=token_type)


def _get_command_token_from_input(cwl_element: Any,
                                  port_type: Any,
                                  input_name: str,
                                  is_shell_command: bool = False,
                                  schema_def_types: Optional[MutableMapping[str, Any]] = None):
    token = None
    command_line_binding = cwl_element.get('inputBinding', None)
    if isinstance(port_type, MutableMapping):
        if 'type' in port_type:
            # Array type: -> CWLMapCommandToken
            if port_type['type'] == 'array':
                token = _get_command_token_from_input(
                    cwl_element=port_type,
                    port_type=port_type['items'],
                    input_name=input_name,
                    is_shell_command=is_shell_command,
                    schema_def_types=schema_def_types)
                if token is not None:
                    token = CWLMapCommandToken(
                        name=input_name,
                        value=token,
                        is_shell_command=is_shell_command)
            # Enum type: -> substitute the type with string and reprocess
            elif port_type['type'] == 'enum':
                return _get_command_token_from_input(
                    cwl_element=cwl_element,
                    port_type='string',
                    input_name=input_name,
                    is_shell_command=is_shell_command,
                    schema_def_types=schema_def_types)
            # Generic typed object: -> propagate
            else:
                token = _get_command_token_from_input(
                    cwl_element=port_type,
                    port_type=port_type['type'],
                    input_name=input_name,
                    is_shell_command=is_shell_command,
                    schema_def_types=schema_def_types)
        else:
            raise WorkflowDefinitionException("Unsupported dictionary type without explicit `type` key")
    elif isinstance(port_type, MutableSequence):
        types = [t for t in filter(lambda x: x != 'null', port_type)]
        # Optional type (e.g. ['null', Type] -> propagate
        if len(types) == 1:
            return _get_command_token_from_input(
                cwl_element=cwl_element,
                port_type=types[0],
                input_name=input_name,
                is_shell_command=is_shell_command,
                schema_def_types=schema_def_types)
        # List of types: -> CWLUnionCommandToken
        else:
            command_tokens = []
            for i, port_type in enumerate(types):
                token = _get_command_token_from_input(
                    cwl_element=cwl_element,
                    port_type=port_type,
                    input_name=input_name,
                    is_shell_command=is_shell_command,
                    schema_def_types=schema_def_types)
                if token is not None:
                    command_tokens.append(token)
            if command_tokens:
                token = CWLUnionCommandToken(
                    name=input_name,
                    value=command_tokens,
                    is_shell_command=True,
                    shell_quote=False)
    elif isinstance(port_type, str):
        # Complex type -> Extract from schema definitions and propagate
        if '#' in port_type:
            return _get_command_token_from_input(
                cwl_element=cwl_element,
                port_type=schema_def_types[port_type],
                input_name=input_name,
                is_shell_command=is_shell_command,
                schema_def_types=schema_def_types)
        # Object type: -> CWLObjectCommandToken
        elif port_type == 'record':
            command_tokens = {}
            for el in cwl_element['fields']:
                key = _get_name("", el['name'], last_element_only=True)
                el_token = _get_command_token_from_input(
                    cwl_element=el,
                    port_type=el['type'],
                    input_name=key,
                    is_shell_command=is_shell_command,
                    schema_def_types=schema_def_types)
                if el_token is not None:
                    command_tokens[key] = el_token
            if command_tokens:
                token = CWLObjectCommandToken(
                    name=input_name,
                    value=command_tokens,
                    is_shell_command=True,
                    shell_quote=False)
    # Simple type with `inputBinding` specified -> CWLCommandToken
    if command_line_binding is not None:
        if token is not None:
            # By default, do not escape composite command tokens
            if 'shellQuote' not in command_line_binding:
                command_line_binding['shellQuote'] = False
                return _get_command_token(
                    binding={**command_line_binding, **{'valueFrom': token}},
                    is_shell_command=True,
                    input_name=input_name)
            else:
                return _get_command_token(
                    binding={**command_line_binding, **{'valueFrom': token}},
                    is_shell_command=is_shell_command,
                    input_name=input_name)
        else:
            return _get_command_token(
                binding=command_line_binding,
                is_shell_command=is_shell_command,
                input_name=input_name,
                token_type=port_type)
    # Simple type without `inputBinding` specified -> token
    else:
        return token


def _get_empty_scatter_return_value(
        element_output: MutableMapping[str, Any],
        step_name: str,
        empty_scatter_step: Step,
        scatter_method: str) -> Any:
    global_name = _get_name(step_name, element_output['id'], last_element_only=True)
    port_name = posixpath.relpath(global_name, step_name)
    if scatter_method == 'nested_crossproduct':
        return port_name, [[] for _ in empty_scatter_step.input_ports]
    else:
        return port_name, []


def _get_hardware_requirement(
        requirements: MutableMapping[str, Any],
        expression_lib: Optional[MutableSequence[str]],
        full_js: bool):
    hardware_requirement = CWLHardwareRequirement(
        expression_lib=expression_lib,
        full_js=full_js)
    if 'ResourceRequirement' in requirements:
        resource_requirement = requirements['ResourceRequirement']
        hardware_requirement.cores = resource_requirement.get(
            'coresMin', resource_requirement.get('coresMax', hardware_requirement.cores))
        hardware_requirement.memory = resource_requirement.get(
            'ramMin', resource_requirement.get('ramMax', hardware_requirement.memory))
        hardware_requirement.tmpdir = resource_requirement.get(
            'tmpdirMin', resource_requirement.get('tmpdirMax', hardware_requirement.tmpdir))
        hardware_requirement.outdir = resource_requirement.get(
            'outdirMin', resource_requirement.get('outdirMax', hardware_requirement.outdir))
    return hardware_requirement


def _get_load_listing(port_description: MutableMapping[str, Any],
                      context: MutableMapping[str, Any]) -> LoadListing:
    requirements = {**context['hints'], **context['requirements']}
    if 'loadListing' in port_description:
        return LoadListing[port_description['loadListing']]
    elif 'outputBinding' in port_description and 'loadListing' in port_description['outputBinding']:
        return LoadListing[port_description['outputBinding']['loadListing']]
    elif 'LoadListingRequirement' in requirements and 'loadListing' in requirements['LoadListingRequirement']:
        return LoadListing[requirements['LoadListingRequirement']['loadListing']]
    else:
        return LoadListing.no_listing


def _get_merge_strategy(
        linkMerge: Optional[str],
        pickValue: Optional[str]) -> Optional[Callable[[MutableSequence[Token]], MutableSequence[Token]]]:
    if pickValue == 'first_non_null':
        def merge_strategy(token_list):
            token_list = _flatten_token_list(token_list) if linkMerge == 'merge_flattened' else token_list
            for t in token_list:
                if t.value is not None:
                    return t
            raise WorkflowExecutionException("All sources are null")
    elif pickValue == 'the_only_non_null':
        def merge_strategy(token_list):
            ret = None
            token_list = _flatten_token_list(token_list) if linkMerge == 'merge_flattened' else token_list
            for t in token_list:
                if t.value is not None:
                    if ret is not None:
                        raise WorkflowExecutionException("Expected only one source to be non-null")
                    ret = t
            if ret is None:
                raise WorkflowExecutionException("All sources are null")
            return ret
    elif pickValue == 'all_non_null':
        def merge_strategy(token_list):
            if linkMerge == 'merge_flattened':
                token_list = _flatten_token_list(token_list)
            elif len(token_list) == 1 and linkMerge is None:
                token_list = token_list[0].value
            return [t for t in token_list if t.value is not None]
    else:
        def merge_strategy(token_list):
            if linkMerge == 'merge_flattened':
                return _flatten_token_list(token_list)
            elif len(token_list) == 1 and linkMerge is None:
                return token_list[0]
            else:
                return token_list
    return merge_strategy


def _get_name(name_prefix: str, element_id: str, last_element_only: bool = False) -> str:
    name = element_id.split('#')[-1]
    if last_element_only and '/' in name:
        name = name.split('/')[-1]
    if name_prefix and posixpath.join('/', name).startswith(os.path.join(name_prefix, '')):
        return posixpath.join('/', name)
    else:
        return posixpath.join(name_prefix, name)


def _get_schema_def_types(requirements: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    return ({sd['name']: sd for sd in requirements['SchemaDefRequirement']['types']}
            if 'SchemaDefRequirement' in requirements else {})


def _get_secondary_files(cwl_element, default_required: bool) -> MutableSequence[SecondaryFile]:
    if isinstance(cwl_element, MutableSequence):
        return [SecondaryFile(sf['pattern'], sf.get('required')
        if sf.get('required') is not None else default_required) for sf in cwl_element]
    elif isinstance(cwl_element, MutableMapping):
        return [SecondaryFile(cwl_element['pattern'], cwl_element.get('required')
        if cwl_element.get('required') is not None else default_required)]


def _get_type_from_array(port_type: str):
    if isinstance(port_type, MutableMapping):
        if port_type['type'] == 'array':
            return _get_type_from_array(port_type['items'])
        else:
            return port_type['type']
    else:
        return port_type


def _flatten_token_list(outputs: MutableSequence[Token]):
    flattened_list = []
    for token in sorted(outputs, key=lambda t: int(t.tag.split('.')[-1])):
        if isinstance(token.job, MutableSequence):
            flattened_list.extend(_flatten_token_list(token.value))
        else:
            flattened_list.append(token)
    return flattened_list


def _infer_token_processor(port: Port,
                           token_processor: TokenProcessor) -> TokenProcessor:
    if isinstance(token_processor, CWLMapTokenProcessor):
        return CWLMapTokenProcessor(
            port=port,
            token_processor=_infer_token_processor(port, token_processor.processor))
    elif isinstance(token_processor, ObjectTokenProcessor):
        return ObjectTokenProcessor(
            port=port,
            processors={k: _infer_token_processor(port, v) for k, v in token_processor.processors.items()})
    elif isinstance(token_processor, CWLTokenProcessor):
        return CWLTokenProcessor(
            port=port,
            port_type=token_processor.port_type)
    else:
        return DefaultTokenProcessor(port=port)


async def _inject_input(outdir: str,
                        port: OutputPort,
                        value: Any) -> None:
    # Create a dummy job
    input_injector = BaseJob(
        name=random_name(),
        step=BaseStep(
            name=random_name(),
            context=port.step.context,
            target=get_local_target()),
        inputs=[])
    input_injector.output_directory = outdir
    try:
        await port.step.context.scheduler.schedule(input_injector)
        port.step = input_injector.step
        port.put(await port.token_processor.compute_token(
            job=input_injector,
            command_output=CWLCommandOutput(value, Status.COMPLETED, 0)))
        port.put(TerminationToken(name=port.name))
    finally:
        await port.step.context.scheduler.notify_status(input_injector.name, Status.COMPLETED)


def _percolate_port(port_name: str, *args) -> OutputPort:
    for arg in args:
        if port_name in arg:
            port = arg[port_name]
            if isinstance(port, OutputPort):
                return port
            else:
                return _percolate_port(port, *args)


def _process_javascript_requirement(requirements: MutableMapping[str, Any]) -> (Optional[MutableSequence[Any]], bool):
    expression_lib = None
    full_js = False
    if 'InlineJavascriptRequirement' in requirements:
        full_js = True
        if 'expressionLib' in requirements['InlineJavascriptRequirement']:
            expression_lib = []
            for lib in requirements['InlineJavascriptRequirement']['expressionLib']:
                expression_lib.append(lib)
    return expression_lib, full_js


def _process_docker_requirement(step: Step,
                                context: MutableMapping[str, Any],
                                docker_requirement: MutableMapping[str, Any],
                                network_access: bool) -> Target:
    # Retrieve image
    if 'dockerPull' in docker_requirement:
        image_name = docker_requirement['dockerPull']
    elif 'dockerImageId' in docker_requirement:
        image_name = docker_requirement['dockerImageId']
    else:
        raise WorkflowDefinitionException(
            "DockerRequirements without `dockerPull` or `dockerImageId` are not supported yet")
    # Build configuration
    docker_config = {
        'image': image_name,
        'logDriver': 'none',
        'network': 'default' if network_access else 'none'
    }
    if step.workdir:
        docker_config['volume'] = ['{host}:{container}'.format(
            host=step.workdir,
            container=step.workdir)]
    else:
        docker_config['volume'] = ['{host}:{container}'.format(
            host=os.path.join(tempfile.gettempdir(), 'streamflow'),
            container=posixpath.join('/tmp', 'streamflow'))]
    if 'dockerOutputDirectory' in docker_requirement:
        docker_config['workdir'] = docker_requirement['dockerOutputDirectory']
        context['output_directory'] = docker_config['workdir']
        local_dir = os.path.join(tempfile.gettempdir(), 'streamflow', random_name())
        os.makedirs(local_dir, exist_ok=True)
        docker_config['volume'].append('{host}:{container}'.format(
            host=local_dir,
            container=docker_config['workdir']))
    # Build step target
    return Target(
        model=ModelConfig(
            name='docker-requirement-{id}'.format(id=random_name()),
            connector_type='docker',
            config=docker_config,
            external=False),
        service=image_name)


class CWLTranslator(object):

    def __init__(self,
                 context: StreamFlowContext,
                 cwl_definition: cwltool.process.Process,
                 cwl_inputs: MutableMapping[str, Any],
                 workflow_config: WorkflowConfig,
                 loading_context: cwltool.context.LoadingContext):
        self.context: StreamFlowContext = context
        self.cwl_definition: cwltool.process.Process = cwl_definition
        self.cwl_inputs: Optional[MutableMapping[str, Any]] = cwl_inputs
        self.default_map: MutableMapping[str, Any] = {}
        self.gather_map: MutableMapping[str, str] = {}
        self.input_dependencies: MutableMapping[str, Set[str]] = {}
        self.loading_context: cwltool.context.LoadingContext = loading_context
        self.output_ports: MutableMapping[str, Union[str, OutputPort]] = {}
        self.scatter: MutableMapping[str, Any] = {}
        self.workflow_config: WorkflowConfig = workflow_config

    def _apply_config(self, workflow: Workflow):
        for step in workflow.steps.values():
            step_path = PurePosixPath(step.name)
            step_target = self.workflow_config.propagate(step_path, 'target')
            step_workdir = self.workflow_config.propagate(step_path, 'workdir')
            for group_name, scheduling_group in self.workflow_config.scheduling_groups.items():
                if step.name in scheduling_group:
                    step.scheduling_group = group_name
            if step_workdir is not None:
                step.workdir = step_workdir
            if step_target is not None:
                target_model = self.workflow_config.models[step_target['model']]
                step.target = Target(
                    model=ModelConfig(
                        name=target_model['name'],
                        connector_type=target_model['type'],
                        config=target_model['config'],
                        external=target_model.get('external', False)),
                    resources=step_target.get('resources', 1),
                    service=step_target.get('service'))
            elif step.target is None:
                step.target = get_local_target(step_workdir)
        self.context.scheduler.scheduling_groups = self.workflow_config.scheduling_groups

    def _create_input_combinator(self,
                                 step: Step) -> InputCombinator:
        cartesian_combinator = None
        other_ports = step.input_ports.values()
        scatter_inputs = self.scatter[step.name]['inputs'] if step.name in self.scatter else None
        scatter_method = self.scatter[step.name]['method'] if step.name in self.scatter else None
        if scatter_inputs:
            other_ports = [p.name for p in other_ports if p.name not in scatter_inputs]
            if scatter_method is None or scatter_method == 'dotproduct':
                scatter_name = random_name()
                scatter_combinator = DotProductInputCombinator(scatter_name)
            else:
                scatter_name = random_name()
                scatter_combinator = CartesianProductInputCombinator(scatter_name)

                def tag_strategy(token_list: MutableSequence[Token]) -> MutableSequence[Token]:
                    tag = [t.tag.split('.')[-1] for t in token_list]
                    return [t.retag('.'.join(t.tag.split('.')[:-1] + tag)) for t in token_list]

                scatter_combinator.tag_strategy = tag_strategy
            scatter_combinator.ports = {p: step.input_ports[p] for p in scatter_inputs}
            if other_ports:
                cartesian_combinator = CartesianProductInputCombinator(random_name())
                cartesian_combinator.ports[scatter_name] = scatter_combinator
            else:
                return scatter_combinator
        scatter_ports = {}
        for p in other_ports:
            if p.dependee is not None:
                if p.dependee.step and (predecessors := self._get_scatter_predecessors(p.dependee.step)):
                    scatter_step = max((pred.scatter_step for pred in predecessors), key=len)
                else:
                    scatter_step = '/'
                if scatter_step not in scatter_ports:
                    scatter_ports[scatter_step] = []
                scatter_ports[scatter_step].append(p)
        inner_combinator = None
        for level in sorted(scatter_ports, reverse=True, key=len):
            if len(scatter_ports[level]) > 1:
                input_combinator = DotProductInputCombinator(random_name())
                input_combinator.ports = {p.name: p for p in scatter_ports[level]}
                port = input_combinator
            else:
                port = scatter_ports[level][0]
            if inner_combinator:
                combinator = CartesianProductInputCombinator(random_name())
                if len(scatter_ports[level]) > 1:

                    def tag_strategy(token_list: MutableSequence[Token]) -> MutableSequence[Token]:
                        token_list = flatten_list(token_list)
                        group_by_scatter_step = {}
                        for t in token_list:
                            for s, ports in scatter_ports.items():
                                if next((p for p in ports if p.name == t.name), None):
                                    if s not in group_by_scatter_step:
                                        group_by_scatter_step[s] = []
                                    group_by_scatter_step[s].append(t)
                        token_list = []
                        tag = []
                        for s in sorted(group_by_scatter_step, key=len):
                            tag.extend(group_by_scatter_step[s][0].tag.split('.')[-len(group_by_scatter_step[s]):])
                            token_list.extend([t.retag('.'.join(t.tag.split('.')[:-len(group_by_scatter_step[s])]))
                                               for t in group_by_scatter_step[s]])
                        return [t.retag('.'.join(t.tag.split('.') + tag)) for t in token_list]

                    combinator.tag_strategy = tag_strategy
                combinator.ports[port.name] = port
                combinator.ports[inner_combinator.name] = inner_combinator
                inner_combinator = combinator
            else:
                inner_combinator = port
        if cartesian_combinator:
            cartesian_combinator.ports[inner_combinator.name] = inner_combinator
            return cartesian_combinator
        elif isinstance(inner_combinator, InputCombinator):
            return inner_combinator
        else:
            combinator = DotProductInputCombinator(random_name())
            if inner_combinator is not None:
                combinator.ports[inner_combinator.name] = inner_combinator
            return combinator

    def _get_scatter_predecessors(self,
                                  step: Step,
                                  scatter_step: str = '',
                                  gather_blacklist: MutableSequence[str] = None):
        predecessors = {ScatterPredecessor(step=step.name, scatter_step=scatter_step)}
        if any(isinstance(p, ScatterInputPort) for p in step.input_ports.values()):
            if not gather_blacklist or step.name not in gather_blacklist:
                scatter_step = step.name
        for p in step.input_ports.values():
            if p.dependee is not None:
                if isinstance(p.dependee, OutputCombinator):
                    for port in p.dependee.ports.values():
                        if port.step is not None:
                            blacklist = copy.copy(gather_blacklist) if gather_blacklist else []
                            if blacklist_scatter := self.gather_map.get(
                                    posixpath.join(port.step.name, port.name)):
                                blacklist.append(blacklist_scatter)
                            predecessors = predecessors.union(self._get_scatter_predecessors(
                                port.step, scatter_step, blacklist))
                elif p.dependee.step is not None:
                    if isinstance(p.dependee, GatherOutputPort):
                        blacklist = copy.copy(gather_blacklist) if gather_blacklist else []
                        if blacklist_scatter := self.gather_map.get(
                                posixpath.join(p.dependee.step.name, p.dependee.name)):
                            blacklist.append(blacklist_scatter)
                    predecessors = predecessors.union(self._get_scatter_predecessors(
                        p.dependee.step, scatter_step, gather_blacklist))
        return predecessors

    def _get_source_port(self, workflow: Workflow, source_name: str) -> OutputPort:
        source_step, source_port = posixpath.split(source_name)
        if source_step in workflow.steps and source_port in workflow.steps[source_step].output_ports:
            return workflow.steps[source_step].output_ports[source_port]
        else:
            input_step = posixpath.join(source_step, 'in', source_port)
            if input_step in workflow.steps and source_port in workflow.steps[input_step].output_ports:
                return workflow.steps[input_step].output_ports[source_port]
            else:
                return _percolate_port(source_name, self.output_ports)

    async def _inject_inputs(self,
                             workflow: Workflow,
                             root_prefix: str):
        if self.cwl_inputs:
            # Compute outdir path
            path = self.cwl_inputs['id']
            if '#' in path:
                path = path.split('#')[-1]
            if path.startswith('file://'):
                path = path[7:]
            outdir = os.path.dirname(path)
            # Inject input values into initial ports
            inject_tasks = []
            for key, value in self.cwl_inputs.items():
                root_port_name = posixpath.join(root_prefix, 'in', key)
                if (input_port := (
                        workflow.steps[root_prefix].input_ports.get(key) if root_prefix in workflow.steps else
                        workflow.steps[root_port_name].input_ports.get(key) if root_port_name in workflow.steps else
                        None)):
                    # Create dependee
                    port = DefaultOutputPort(random_name())
                    port.step = BaseStep(name=random_name(), context=self.context)
                    port.token_processor = _infer_token_processor(port, input_port.token_processor)
                    input_port.dependee = _build_dependee(
                        default_map=self.default_map,
                        global_name=posixpath.join(root_prefix, input_port.name),
                        output_port=port)
                    # Inject input value
                    inject_tasks.append(asyncio.create_task(_inject_input(
                        outdir=outdir,
                        port=port,
                        value=value)))
            await asyncio.gather(*inject_tasks)
        # Create null-valued tokens for unbound input ports
        for step in workflow.steps.values():
            for input_name, input_port in step.input_ports.items():
                if input_port.dependee is None:
                    global_name = posixpath.join(step.name, input_port.name)
                    input_port.dependee = self.default_map.get(global_name, DefaultOutputPort(random_name()))
                if input_port.dependee.empty() and input_port.dependee.step is None:
                    input_port.dependee.put(Token(name=input_port.dependee.name, value=None))
                    input_port.dependee.put(TerminationToken(name=input_port.dependee.name))

    def _is_cwl10(self):
        return (self.cwl_definition.tool['cwlVersion'] == 'v1.0' or
                self.cwl_definition.tool['http://commonwl.org/cwltool#original_cwlVersion'] == 'v1.0')

    def _process_dependencies(self,
                              workflow: Workflow,
                              cwl_element: cwltool.workflow.WorkflowStep,
                              name_prefix: str):
        step_name = _get_name(name_prefix, cwl_element.id)
        for element_input in cwl_element.tool['inputs']:
            # If the input element depends on one or more output ports
            if 'source' in element_input:
                global_name = _get_name(step_name, element_input['id'], last_element_only=True)
                port_name = posixpath.relpath(global_name, step_name)
                input_name = posixpath.join(step_name, 'in', port_name)
                steps = {workflow.steps[k] for k, v in self.input_dependencies.items() if input_name in v}
                # If source element is a list, the input element can depend on multiple ports
                if isinstance(element_input['source'], MutableSequence):
                    # If the list contains only one element and no `linkMerge` is specified, treat it as a singleton
                    if (len(element_input['source']) == 1 and
                            'linkMerge' not in element_input and
                            'pickValue' not in element_input):
                        source_name = _get_name(name_prefix, element_input['source'][0])
                        for step in steps:
                            step.input_ports[port_name].dependee = _build_dependee(
                                default_map=self.default_map,
                                global_name=global_name,
                                output_port=self._get_source_port(workflow, source_name))
                    # Otherwise, create a DotrProductOutputCombinator
                    else:
                        source_names = [_get_name(name_prefix, src) for src in element_input['source']]
                        ports = [self._get_source_port(workflow, n) for n in source_names]
                        for step in steps:
                            port = step.input_ports[port_name]
                            port.dependee = _build_dependee(
                                default_map=self.default_map,
                                global_name=global_name,
                                output_port=_create_output_combinator(
                                    name=port.name,
                                    step=step,
                                    ports={p.name: p for p in ports},
                                    merge_strategy=_get_merge_strategy(
                                        element_input.get('linkMerge'),
                                        element_input.get('pickValue'))))
                # Otherwise, the input element depends on a single output port
                else:
                    source_name = _get_name(name_prefix, element_input['source'])
                    source_port = self._get_source_port(workflow, source_name)
                    for step in steps:
                        port = step.input_ports[port_name]
                        port.dependee = _build_dependee(
                            default_map=self.default_map,
                            global_name=global_name,
                            output_port=source_port)
                        # Percolate secondary files
                        current_processor = port.token_processor
                        if isinstance(current_processor, CWLTokenProcessor) and current_processor.secondary_files:
                            if isinstance(source_port, DotProductOutputCombinator):
                                for src_port in source_port.ports.values():
                                    if (isinstance(src_port.step.command, CWLStepCommand) and
                                            isinstance(src_port.token_processor, CWLTokenProcessor)):
                                        port.token_processor.secondary_files = list(set(
                                            (current_processor.secondary_files or []) +
                                            (src_port.token_processor.secondary_files or [])))
                            elif (isinstance(source_port.step.command, CWLStepCommand) and
                                  isinstance(source_port.token_processor, CWLTokenProcessor)):
                                port.token_processor.secondary_files = list(set(
                                    (current_processor.secondary_files or []) +
                                    (source_port.token_processor.secondary_files or [])))

    async def _recursive_translate(self,
                                   workflow: Workflow,
                                   cwl_element: cwltool.process.Process,
                                   context: MutableMapping[str, Any],
                                   name_prefix: str = "/"):
        # Update context
        current_context = copy.deepcopy(context)
        for hint in cwl_element.hints:
            current_context['hints'][hint['class']] = hint
        for requirement in cwl_element.requirements:
            current_context['requirements'][requirement['class']] = requirement
        # In the root process, override requirements when provided in the input file
        workflow_id = self.cwl_definition.tool['id']
        if name_prefix == (_get_name('/', workflow_id) if '#' in workflow_id else '/'):
            req_string = 'https://w3id.org/cwl/cwl#requirements'
            if req_string in self.cwl_inputs:
                current_context['requirements'] = {req['class']: req for req in self.cwl_inputs[req_string]}
        # Dispatch element
        if isinstance(cwl_element, cwltool.workflow.Workflow):
            await self._translate_workflow(workflow, cwl_element, current_context, name_prefix)
        elif isinstance(cwl_element, cwltool.workflow.WorkflowStep):
            await self._translate_workflow_step(workflow, cwl_element, current_context, name_prefix)
        elif isinstance(cwl_element, cwltool.command_line_tool.CommandLineTool):
            await self._translate_command_line_tool(workflow, cwl_element, current_context, name_prefix)
        elif isinstance(cwl_element, cwltool.command_line_tool.ExpressionTool):
            await self._translate_command_line_tool(workflow, cwl_element, current_context, name_prefix)
        else:
            raise WorkflowDefinitionException(
                "Definition of type " + type(cwl_element).__class__.__name__ + " not supported")

    async def _translate_command_line_tool(self,
                                           workflow: Workflow,
                                           cwl_element: Union[cwltool.command_line_tool.CommandLineTool,
                                                              cwltool.command_line_tool.ExpressionTool],
                                           context: MutableMapping[str, Any],
                                           name_prefix: str):
        step = BaseStep(name_prefix, self.context)
        # Extract custom types if present
        requirements = {**context['hints'], **context['requirements']}
        schema_def_types = _get_schema_def_types(requirements)
        # Process InlineJavascriptRequirement
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Process hardware requirements
        step.hardware_requirement = _get_hardware_requirement(requirements, expression_lib, full_js)
        # Process inputs
        for element_input in cwl_element.tool['inputs']:
            global_name = _get_name(name_prefix, element_input['id'], last_element_only=True)
            port_name = posixpath.relpath(global_name, name_prefix)
            # Create input port
            await _create_input_port(
                port_name=port_name,
                port_step=step,
                global_name=global_name,
                default_map=self.default_map,
                port_description=element_input,
                schema_def_types=schema_def_types,
                format_graph=self.loading_context.loader.graph,
                context=context)
        # Process outputs
        for element_output in cwl_element.tool['outputs']:
            global_name = _get_name(name_prefix, element_output['id'], last_element_only=True)
            port_name = posixpath.relpath(global_name, name_prefix)
            port = _create_output_port(
                port_name=port_name,
                port_description=element_output,
                schema_def_types=schema_def_types,
                format_graph=self.loading_context.loader.graph,
                context=context)
            port.step = step
            self.output_ports[global_name] = port
            step.output_ports[port_name] = port
        # Process DockerRequirement
        if 'DockerRequirement' in requirements:
            network_access = (requirements['NetworkAccess']['networkAccess'] if 'NetworkAccess' in requirements
                              else False)
            step.target = _process_docker_requirement(step, context, requirements['DockerRequirement'], network_access)
        # Process command
        if isinstance(cwl_element, cwltool.command_line_tool.CommandLineTool):
            step.command = _build_command(
                cwl_element=cwl_element,
                schema_def_types=schema_def_types,
                context=context,
                step=step)
            # Process ToolTimeLimit
            if 'ToolTimeLimit' in requirements:
                step.command.time_limit = requirements['ToolTimeLimit']['timelimit']
                if step.command.time_limit < 0:
                    raise WorkflowDefinitionException('Invalid time limit for step {step}'.format(step=name_prefix))
        elif isinstance(cwl_element, cwltool.command_line_tool.ExpressionTool):
            if 'expression' in cwl_element.tool:
                step.command = CWLExpressionCommand(step, cwl_element.tool['expression'])
        else:
            WorkflowDefinitionException(
                "Command generation for " + type(cwl_element).__class__.__name__ + " is not suported")
        step.command.expression_lib = expression_lib
        step.command.full_js = full_js
        # Process streams
        if 'stdin' in cwl_element.tool:
            step.command.stdin = cwl_element.tool['stdin']
        if 'stdout' in cwl_element.tool:
            step.command.stdout = cwl_element.tool['stdout']
        if 'stderr' in cwl_element.tool:
            step.command.stderr = cwl_element.tool['stderr']
        # Add step to workflow
        workflow.add_step(step)
        step.persistent_id = self.context.persistence_manager.db.add_step(
            name=posixpath.join('/', *[s for s in step.name.split(posixpath.sep) if s != 'run']),
            status=step.status.value)

    async def _translate_workflow(self,
                                  workflow: Workflow,
                                  cwl_element: cwltool.workflow.Workflow,
                                  context: MutableMapping[str, Any],
                                  name_prefix: str):
        step_name = name_prefix
        # Extract custom types if present
        requirements = {**context['hints'], **context['requirements']}
        schema_def_types = _get_schema_def_types(requirements)
        # Extract JavaScript requirements
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Process inputs to create steps
        cwl_inputs, input_steps = {}, {}
        for element_input in cwl_element.tool['inputs']:
            global_name = _get_name(step_name, element_input['id'])
            port_name = posixpath.relpath(global_name, step_name)
            input_name = posixpath.join(step_name, 'in', port_name)
            # Index CWL step description
            cwl_inputs[input_name] = element_input
            # Create input step
            input_step = BaseStep(input_name, self.context)
            input_steps[input_name] = input_step
            # Add command
            input_step.command = CWLStepCommand(
                step=input_step,
                expression_lib=expression_lib,
                full_js=full_js)
            # Create output port
            output_port = _create_output_port(
                port_name=port_name,
                port_description=element_input,
                schema_def_types=schema_def_types,
                format_graph=self.loading_context.loader.graph,
                context=context)
            output_port.step = input_step
            input_step.output_ports[port_name] = output_port
            # Process dependencies
            local_deps = resolve_dependencies(
                expression=element_input.get('format'),
                full_js=full_js,
                expression_lib=expression_lib)
            if 'secondaryFiles' in element_input:
                for secondary_file in element_input['secondaryFiles']:
                    local_deps.update(
                        resolve_dependencies(
                            expression=secondary_file.get('pattern'),
                            full_js=full_js,
                            expression_lib=expression_lib),
                        resolve_dependencies(
                            expression=secondary_file.get('required'),
                            full_js=full_js,
                            expression_lib=expression_lib))
            self.input_dependencies[input_name] = set.union(
                {input_name},
                {posixpath.join(step_name, 'in', d) for d in local_deps})
        # Process inputs again to attach ports
        for input_name, input_step in input_steps.items():
            for dep_name in self.input_dependencies[input_name]:
                element_input = cwl_inputs[dep_name]
                global_name = _get_name(step_name, element_input['id'])
                port_name = posixpath.relpath(global_name, step_name)
                # Create an input port
                await _create_input_port(
                    port_name=port_name,
                    port_step=input_step,
                    global_name=posixpath.join(input_name, port_name),
                    default_map=self.default_map,
                    port_description=element_input,
                    schema_def_types=schema_def_types,
                    format_graph=self.loading_context.loader.graph,
                    context=context)
            # Add input step to workflow
            workflow.add_step(input_step)
        # Process steps
        await asyncio.gather(*(asyncio.create_task(
            self._recursive_translate(
                workflow=workflow,
                cwl_element=step,
                context=context,
                name_prefix=name_prefix)
        ) for step in cwl_element.steps))
        # Link step dependencies
        for step in cwl_element.steps:
            self._process_dependencies(
                workflow=workflow,
                cwl_element=step,
                name_prefix=name_prefix)
        # Process outputs
        for element_output in cwl_element.tool['outputs']:
            name = _get_name(name_prefix, element_output['id'])
            # If outputSource element is a list, the output element can depend on multiple ports
            if isinstance(element_output['outputSource'], MutableSequence):
                # If the list contains only one element and no `linkMerge` or `pickValue` are specified
                if (len(element_output['outputSource']) == 1 and
                        'linkMerge' not in element_output and
                        'pickValue' not in element_output):
                    # Treat it as a singleton
                    source_name = _get_name(name_prefix, element_output['outputSource'][0])
                    self.output_ports[name] = self._get_source_port(workflow, source_name)
                # Otherwise, create a DotrProductOutputCombinator
                else:
                    source_names = [_get_name(name_prefix, src) for src in element_output['outputSource']]
                    ports = {n: self._get_source_port(workflow, n) for n in source_names}
                    self.output_ports[name] = _create_output_combinator(
                        name=name,
                        step=BaseStep(
                            name=random_name(),
                            context=self.context),
                        ports=ports,
                        merge_strategy=_get_merge_strategy(
                            element_output.get('linkMerge'),
                            element_output.get('pickValue')))
            # Otherwise, the output element depends on a single output port
            else:
                source_name = _get_name(name_prefix, element_output['outputSource'])
                source_port = self._get_source_port(workflow, source_name)
                if 'pickValue' in element_output:
                    self.output_ports[name] = _create_output_combinator(
                        name=name,
                        step=BaseStep(
                            name=random_name(),
                            context=self.context),
                        ports={source_port.name: source_port},
                        merge_strategy=_get_merge_strategy(
                            element_output.get('linkMerge'),
                            element_output.get('pickValue')))
                else:
                    self.output_ports[name] = source_port

    async def _translate_workflow_step(self,
                                       workflow: Workflow,
                                       cwl_element: cwltool.workflow.WorkflowStep,
                                       context: MutableMapping[str, Any],
                                       name_prefix: str):
        # Process content
        step_name = _get_name(name_prefix, cwl_element.id)
        run_command = cwl_element.tool['run']
        loading_context = self.loading_context.copy()
        loading_context.requirements = list(context['requirements'].values())
        loading_context.hints = list(context['hints'].values())
        if isinstance(run_command, MutableMapping):
            step_definition = self.loading_context.construct_tool_object(run_command, loading_context)
            inner_step_name = (_get_name(name_prefix, step_definition.tool['id']) if self._is_cwl10() else
                               posixpath.join(step_name, 'run'))
            # Recusrively process the step
            await self._recursive_translate(
                workflow=workflow,
                cwl_element=step_definition,
                context=context,
                name_prefix=inner_step_name)
        else:
            step_definition = cwltool.load_tool.load_tool(run_command, loading_context)
            inner_step_name = posixpath.join(step_name, 'run')
            await self._recursive_translate(
                workflow=workflow,
                cwl_element=step_definition,
                context=context,
                name_prefix=inner_step_name)
        # Merge requirements with the underlying step definition
        context = copy.deepcopy(context)
        for hint in step_definition.hints:
            context['hints'][hint['class']] = hint
        for requirement in step_definition.requirements:
            context['requirements'][requirement['class']] = requirement
        requirements = {**context['hints'], **context['requirements']}
        # Extract custom types if present
        schema_def_types = _get_schema_def_types(requirements)
        # Extract JavaScript requirements
        expression_lib, full_js = _process_javascript_requirement(requirements)
        # Check global dependencies
        when_deps = resolve_dependencies(
            expression=cwl_element.tool.get('when'),
            full_js=full_js,
            expression_lib=expression_lib)
        # Find scatter elements
        scatter_method = cwl_element.tool.get('scatterMethod')
        if isinstance(cwl_element.tool.get('scatter'), str):
            scatter_inputs = [_get_name(step_name, cwl_element.tool.get('scatter'), last_element_only=True)]
        else:
            scatter_inputs = [_get_name(step_name, n, last_element_only=True)
                              for n in cwl_element.tool.get('scatter', [])]
        input_steps = {}
        # Build scatter-related steps first
        scatter_step, empty_scatter_step = None, None
        if scatter_inputs:
            # Create dedicated step for scatter inputs
            scatter_step = BaseStep(posixpath.join(step_name, 'scatter'), self.context)
            self.scatter[scatter_step.name] = {
                'inputs': [posixpath.relpath(p, step_name) for p in scatter_inputs],
                'method': scatter_method}
            self.input_dependencies[scatter_step.name] = set()
            scatter_step.command = CWLStepCommand(
                step=scatter_step,
                expression_lib=expression_lib,
                full_js=full_js)
            workflow.add_step(scatter_step)
            # Manage the empty scatter case with a dedicated step
            empty_scatter_step = BaseStep(posixpath.join(step_name, 'empty-scatter'), self.context)
        # When there is a condition, manage skip of inner steps with a dedicated step
        skip_step = None
        if 'when' in cwl_element.tool:
            skip_step = BaseStep(name=posixpath.join(step_name, 'skip-step'), context=self.context)
            input_steps[skip_step.name] = skip_step
            # Add command
            skip_step.command = CWLStepCommand(
                step=skip_step,
                expression_lib=expression_lib,
                full_js=full_js)
            # Add condition
            skip_step.condition = CWLCondition(
                step=skip_step,
                expression=cwl_element.tool['when'],
                expression_lib=expression_lib,
                full_js=full_js)
            # Add dependencies
            self.input_dependencies[skip_step.name] = {posixpath.join(step_name, 'in', d) for d in when_deps}
        # Process inputs to create steps
        cwl_inputs = {}
        scatter_port_names = []
        for element_input in cwl_element.tool['inputs']:
            global_name = _get_name(step_name, element_input['id'], last_element_only=True)
            port_name = posixpath.relpath(global_name, step_name)
            input_name = posixpath.join(step_name, 'in', port_name)
            scatter = global_name in scatter_inputs
            # Index CWL step description
            cwl_inputs[input_name] = element_input
            # If element is not in scatter and is connected with an inner step
            if scatter or not element_input.get('not_connected', False):
                # Create input step
                input_step = BaseStep(input_name, self.context)
                input_steps[input_name] = input_step
                # Add condition if present
                if 'when' in cwl_element.tool:
                    input_step.condition = CWLCondition(
                        step=input_step,
                        expression=cwl_element.tool['when'],
                        expression_lib=expression_lib,
                        full_js=full_js)
                # Add command
                input_step.command = CWLStepCommand(
                    step=input_step,
                    expression_lib=expression_lib,
                    full_js=full_js)
                if 'valueFrom' in element_input:
                    input_step.command.input_expressions[port_name] = element_input['valueFrom']
                # If is a scatter step
                if scatter:
                    # Adjust the the element type
                    element_input = {**element_input, **{'type': _get_type_from_array(element_input['type'])}}
                    # Create scatter input port into the scatter step
                    scatter_port_names.append(port_name)
                    port = ScatterInputPort(name=port_name, step=scatter_step)
                    port.token_processor = CWLTokenProcessor(port=port, port_type=element_input['type'])
                    scatter_step.input_ports[port_name] = port
                    # Save default value
                    if 'default' in element_input:
                        self.default_map[posixpath.join(scatter_step.name, port_name)] = await _create_default_port(
                            port=port,
                            value=element_input['default'])
                        self.default_map[posixpath.join(empty_scatter_step.name, port_name)] = self.default_map[
                            posixpath.join(scatter_step.name, port_name)]
                    # Manage empty scatter ports
                    empty_scatter_port = DefaultInputPort(
                        name=port_name, step=empty_scatter_step)
                    empty_scatter_step.input_ports[port_name] = empty_scatter_port
                    empty_scatter_step.input_ports[port_name].token_processor = CWLMapTokenProcessor(
                        port=empty_scatter_port, token_processor=port.token_processor)
                    # Create output on the scatter step
                    scatter_output = DefaultOutputPort(name=port_name, step=scatter_step)
                    scatter_output.token_processor = CWLTokenProcessor(port=port, port_type=element_input['type'])
                    scatter_step.output_ports[port_name] = scatter_output
                    # Add to scatter dependencies
                    self.input_dependencies[scatter_step.name].add(input_name)
                # Create output port
                output_port = _create_output_port(
                    port_name=port_name,
                    port_description=element_input,
                    schema_def_types=schema_def_types,
                    format_graph=self.loading_context.loader.graph,
                    context=context)
                output_port.step = input_step
                input_step.output_ports[port_name] = output_port
                # Process local dependencies
                local_deps = resolve_dependencies(
                    expression=element_input.get('valueFrom'),
                    full_js=full_js,
                    expression_lib=expression_lib)
                self.input_dependencies[input_name] = set.union(
                    {input_name},
                    {posixpath.join(step_name, 'in', d) for d in when_deps},
                    {posixpath.join(step_name, 'in', d) for d in local_deps})
        # Assign scatter dependencies to the empty scatter step
        if scatter_inputs:
            self.input_dependencies[empty_scatter_step.name] = self.input_dependencies[scatter_step.name]
        # Process inputs again to attach ports
        for input_name, input_step in input_steps.items():
            for dep_name in [d for d in self.input_dependencies[input_name]]:
                element_input = cwl_inputs[dep_name]
                global_name = _get_name(step_name, element_input['id'], last_element_only=True)
                port_name = posixpath.relpath(global_name, step_name)
                scatter = global_name in scatter_inputs
                # Create input port
                if scatter:
                    element_input = {**element_input, **{'type': _get_type_from_array(element_input['type'])}}
                    port = DefaultInputPort(name=port_name, step=input_step)
                    port.token_processor = _create_token_processor(
                        port=port,
                        port_type=element_input['type'],
                        port_description=element_input,
                        schema_def_types=schema_def_types,
                        format_graph=self.loading_context.loader.graph,
                        context=context)
                    input_step.input_ports[port_name] = port
                    # Link to scatter output port and remove port from dependencies
                    port.dependee = scatter_step.output_ports[port_name]
                    self.input_dependencies[input_name].remove(dep_name)
                else:
                    # Create input port
                    await _create_input_port(
                        port_name=port_name,
                        port_step=input_step,
                        global_name=global_name,
                        default_map=self.default_map,
                        port_description=element_input,
                        schema_def_types=schema_def_types,
                        format_graph=self.loading_context.loader.graph,
                        context=context)
            # Link ports to inner steps and propagate secondary files
            if input_name in [posixpath.join(step_name, 'empty-scatter'), posixpath.join(step_name, 'skip-step')]:
                port_names = []
            else:
                port_names = list(input_step.output_ports.keys())
            for port_name in port_names:
                element_input = cwl_inputs[posixpath.join(step_name, 'in', port_name)]
                global_name = _get_name(step_name, element_input['id'], last_element_only=True)
                # Link output port to inner step's input port
                if inner_step_name not in workflow.steps:
                    inner_step = workflow.steps[posixpath.join(inner_step_name, 'in', port_name)]
                else:
                    inner_step = workflow.steps[inner_step_name]
                if port_name in inner_step.input_ports:
                    inner_step.input_ports[port_name].dependee = _build_dependee(
                        default_map=self.default_map,
                        global_name=global_name,
                        output_port=input_step.output_ports[port_name])
                    # Percolate secondary files
                    inner_processor = inner_step.input_ports[port_name].token_processor
                    if isinstance(inner_processor, CWLTokenProcessor) and inner_processor.secondary_files:
                        current_processor = cast(CWLTokenProcessor, input_step.input_ports[port_name].token_processor)
                        current_processor.secondary_files = list(set(
                            (current_processor.secondary_files or []) + (inner_processor.secondary_files or [])))
            # Add input step to workflow
            workflow.add_step(input_step)
        if scatter_inputs:
            # Set empty scatter condition
            empty_scatter_condition = ' && '.join(
                ['(Array.isArray(inputs.{name}) && inputs.{name}.length)'.format(name=p) for p in scatter_port_names])
            empty_scatter_step.condition = CWLCondition(
                step=empty_scatter_step,
                expression='${return !(' + empty_scatter_condition + ')}',
                full_js=True)
            # Build empty scatter output expression
            empty_scatter_return_value = {
                k: v for k, v in [_get_empty_scatter_return_value(output, step_name, empty_scatter_step, scatter_method)
                                  for output in cwl_element.tool['outputs']]}
            empty_scatter_step.command = CWLExpressionCommand(
                step=empty_scatter_step,
                expression='${return ' + json.dumps(empty_scatter_return_value) + '}',
                full_js=True)
            # Add empty scatter step
            workflow.add_step(empty_scatter_step)
        # Process outputs
        for element_output in cwl_element.tool['outputs']:
            inner_name = _get_name(inner_step_name, element_output['id'], last_element_only=True)
            outer_name = _get_name(step_name, element_output['id'], last_element_only=True)
            port_name = posixpath.relpath(outer_name, step_name)
            inner_port = _percolate_port(inner_name, self.output_ports)
            if scatter_step is not None:
                if isinstance(inner_port, DotProductOutputCombinator):
                    predecessors = set()
                    for s in [p.step for p in inner_port.ports.values()]:
                        predecessors = predecessors.union(self._get_scatter_predecessors(s)) if s else set()
                else:
                    predecessors = self._get_scatter_predecessors(inner_port.step) if inner_port.step else set()
                predecessors = {p.step for p in predecessors}
                gather = scatter_step.name in predecessors
            else:
                gather = False
            # If output port comes from a scatter input, place a port in the gather and empty scatter steps
            if gather:
                # Process port type
                element_type = element_output['type']
                if isinstance(element_type, MutableSequence):
                    element_type = [t for t in filter(lambda x: x != 'null', element_type)]
                    if len(element_type) == 1:
                        port_type = _get_type_from_array(element_type[0])
                    else:
                        raise WorkflowDefinitionException('Invalid type for scatter port')
                else:
                    port_type = _get_type_from_array(element_type)
                # Manage gathering of scatter ports with a dedicated step
                gather_step = BaseStep(posixpath.join(step_name, 'gather', port_name), self.context)
                gather_step.command = CWLStepCommand(
                    step=gather_step,
                    expression_lib=expression_lib,
                    full_js=full_js)
                self.gather_map[gather_step.name] = scatter_step.name
                # Create input port
                input_port = DefaultInputPort(port_name, gather_step)
                input_port.token_processor = _create_token_processor(
                    port=input_port,
                    port_type=_get_type_from_array(port_type) if scatter_method == 'nested_crossproduct' else port_type,
                    port_description=element_output,
                    schema_def_types=schema_def_types,
                    format_graph=self.loading_context.loader.graph,
                    context=context)
                source_port = self._get_source_port(workflow, inner_name)
                if 'when' in cwl_element.tool:
                    source_port = _create_skip_link(
                        port=source_port,
                        step=skip_step)
                input_port.dependee = _build_dependee(
                    default_map=self.default_map,
                    global_name=posixpath.join(gather_step.name, port_name),
                    output_port=source_port)
                gather_step.input_ports[port_name] = input_port
                # Add gather step
                workflow.add_step(gather_step)
                # Create output port
                output_port = GatherOutputPort(port_name, gather_step)
                if scatter_method == 'nested_crossproduct':
                    def merge_strategy(token_list):
                        groups = {}
                        for t in token_list:
                            tags = t.tag.split('.')[-len(scatter_inputs):]
                            g = groups
                            for tag in tags[:-2]:
                                if tag not in g:
                                    g[tag] = {}
                                g = g[tag]
                            if tags[-2] not in g:
                                g[tags[-2]] = []
                            g[tags[-2]].append(t)
                        return [_dict_to_lists(groups, port_name)]

                    output_port.merge_strategy = merge_strategy
                elif scatter_method == 'flat_crossproduct':
                    def merge_strategy(token_list):
                        token_list = sorted(token_list, key=lambda t: t.tag)
                        token_dict = {}
                        for t in token_list:
                            tag = '.'.join(t.tag.split('.')[:-len(scatter_inputs)])
                            if tag not in token_dict:
                                token_dict[tag] = []
                            token_dict[tag].append(t)
                        return [Token(
                            name=port_name,
                            job=[t.job for t in token_list],
                            tag='.'.join(get_tag(token_list).split('.')[:-len(scatter_inputs)]),
                            value=token_list)]

                    output_port.merge_strategy = merge_strategy
                output_port.token_processor = _create_token_processor(
                    port=output_port,
                    port_type=_get_type_from_array(port_type) if scatter_method == 'nested_crossproduct' else port_type,
                    port_description=element_output,
                    schema_def_types=schema_def_types,
                    format_graph=self.loading_context.loader.graph,
                    context=context)
                gather_step.output_ports[port_name] = output_port
                # Create the empty version of output port
                empty_scatter_step.output_ports[port_name] = DefaultOutputPort(
                    name=port_name, step=empty_scatter_step)
                empty_scatter_step.output_ports[port_name].token_processor = output_port.token_processor
                combinator = NondeterminateMergeOutputCombinator(
                    name=random_name(),
                    step=output_port.step,
                    ports={output_port.name: output_port,
                           output_port.name + '-empty': empty_scatter_step.output_ports[port_name]})
                combinator.token_processor = output_port.token_processor
                # Remap output port
                gather_name = _get_name(gather_step.name, element_output['id'], last_element_only=True)
                self.output_ports[gather_name] = combinator
                self.output_ports[outer_name] = gather_name
            else:
                # Remap output port
                source_port = self._get_source_port(workflow, inner_name)
                if 'when' in cwl_element.tool:
                    source_port = _create_skip_link(
                        port=source_port,
                        step=skip_step)
                self.output_ports[outer_name] = source_port

    async def translate(self) -> Workflow:
        workflow = Workflow()
        # Create context
        context = _create_context()
        # Compute root prefix
        workflow_id = self.cwl_definition.tool['id']
        root_prefix = _get_name('/', workflow_id) if '#' in workflow_id else '/'
        # Build workflow graph
        await self._recursive_translate(workflow, self.cwl_definition, context, root_prefix)
        # Mark root steps
        workflow.root_steps.append(root_prefix)
        # Inject initial inputs
        await self._inject_inputs(workflow, root_prefix)
        # Add input combinators
        for step in workflow.steps.values():
            if step.name in self.scatter:
                step.input_combinator = self._create_input_combinator(step)
            else:
                step.input_combinator = self._create_input_combinator(step)
        # Extract workflow outputs
        root_prefix = posixpath.join(root_prefix, '')
        for output_name, output_value in self.output_ports.items():
            if output_name[len(root_prefix):].count('/') == 0:
                if port := _percolate_port(output_name, self.output_ports):
                    workflow.output_ports[output_name[len(root_prefix):]] = port
        # Apply StreamFlow config
        self._apply_config(workflow)
        # Return the final workflow object
        return workflow
