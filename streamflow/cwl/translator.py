from __future__ import annotations

import asyncio
import copy
import os
import posixpath
from enum import Enum
from pathlib import PurePosixPath
from typing import MutableMapping, TYPE_CHECKING, Optional, MutableSequence, cast, Callable

import cwltool.command_line_tool
import cwltool.context
import cwltool.load_tool
import cwltool.process
import cwltool.workflow
from ruamel.yaml.comments import CommentedSeq
from typing_extensions import Text

from streamflow.config.config import WorkflowConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import ModelConfig
from streamflow.core.exception import WorkflowDefinitionException, WorkflowExecutionException
from streamflow.core.utils import random_name
from streamflow.core.workflow import Port, OutputPort, Workflow, Target, Token, TerminationToken, \
    InputCombinator, InputPort, Job, Status
from streamflow.cwl.command import CWLCommand, CWLExpressionCommand, CWLMapCommandToken, \
    CWLUnionCommandToken, CWLObjectCommandToken, CWLCommandToken, CWLCommandOutput, CWLStepCommand
from streamflow.cwl.token_processor import LoadListing, SecondaryFile, CWLTokenProcessor, CWLUnionTokenProcessor, \
    CWLMapTokenProcessor, CWLSkipTokenProcessor
from streamflow.workflow.combinator import DotProductInputCombinator, CartesianProductInputCombinator, \
    DotProductOutputCombinator, NondeterminateMergeOutputCombinator
from streamflow.workflow.port import DefaultInputPort, DefaultOutputPort, ScatterInputPort, GatherOutputPort, \
    ObjectTokenProcessor
from streamflow.workflow.step import BaseStep, BaseJob

if TYPE_CHECKING:
    from streamflow.core.workflow import Step, TokenProcessor
    from typing import Union, Any, Set


class LinkMergeMethod(Enum):
    merge_nested = 1
    merge_flattened = 2


def _build_command(
        cwl_element: cwltool.command_line_tool.CommandLineTool,
        schema_def_types: MutableMapping[Text, Any],
        context: MutableMapping[Text, Any],
        step: Step) -> CWLCommand:
    command = CWLCommand(step)
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
            command.command_tokens.append(_get_command_token(argument))
    # Process inputs
    for input_port in cwl_element.tool['inputs']:
        command_token = _get_command_token_from_input(
            cwl_element=input_port,
            port_type=input_port['type'],
            input_name=_get_name("", input_port['id'], last_element_only=True),
            schema_def_types=schema_def_types)
        if command_token is not None:
            command.command_tokens.append(command_token)
    # Process InitialWorkDirRequirement
    requirements = {**context['hints'], **context['requirements']}
    if 'InitialWorkDirRequirement' in requirements:
        command.initial_work_dir = requirements['InitialWorkDirRequirement']['listing']
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
    return command


def _check_scatter(step: Step, scatter_inputs: Set[Text], visited_steps: MutableSequence[Text] = None):
    visited_steps.append(step.name)
    for port_name in [posixpath.join(step.name, p) for p in step.input_ports]:
        if port_name in scatter_inputs:
            return True
    for port in step.input_ports.values():
        if port.dependee is not None and port.dependee.step not in visited_steps:
            if _check_scatter(port.dependee.step, scatter_inputs, visited_steps):
                return True
    return False


def _create_context() -> MutableMapping[Text, Any]:
    return {
        'requirements': {},
        'hints': {}
    }


def _create_input_port(
        port_name: Text,
        port_description: MutableMapping[Text, Any],
        schema_def_types: MutableMapping[Text, Any],
        context: MutableMapping[Text, Any],
        scatter: bool = False) -> InputPort:
    # Create port
    if scatter:
        port = ScatterInputPort(port_name)
    else:
        port = DefaultInputPort(port_name)
    port.token_processor = _create_token_processor(
        port=port,
        port_type=port_description['type'],
        port_description=port_description,
        schema_def_types=schema_def_types,
        context=context)
    return port


def _create_output_port(
        port_name: Text,
        port_description: MutableMapping,
        schema_def_types: MutableMapping[Text, Any],
        context: MutableMapping[Text, Any]) -> OutputPort:
    port = DefaultOutputPort(port_name)
    port.token_processor = _create_token_processor(
        port=port,
        port_type=port_description['type'],
        port_description=port_description,
        schema_def_types=schema_def_types,
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
        port_description: MutableMapping,
        schema_def_types: MutableMapping[Text, Any],
        context: MutableMapping[Text, Any],
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
                    context=context,
                    optional=optional)
                return CWLMapTokenProcessor(
                    port=port,
                    token_processor=processor,
                    default_value=port_description.get('default'),
                    optional=optional)
            # Enum type: -> substitute with string and propagate the description
            elif port_type['type'] == 'enum':
                return _create_token_processor(
                    port=port,
                    port_type='string',
                    port_description=port_description,
                    schema_def_types=schema_def_types,
                    context=context,
                    optional=optional)
            # Generic object type -> propagate the type
            else:
                return _create_token_processor(
                    port=port,
                    port_type=port_type['type'],
                    port_description=port_type,
                    schema_def_types=schema_def_types,
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
                    context=context))
            default_value = port_description.get('default', None)
            return CWLUnionTokenProcessor(
                port=port,
                processors=token_processors,
                default_value=default_value,
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
                context=context)
        return ObjectTokenProcessor(port, token_processors)
    # Optional type -> Propagate with optional = True
    elif port_type.endswith('?'):
        return _create_token_processor(
            port=port,
            port_type=port_type[:-1].strip(),
            port_description=port_description,
            schema_def_types=schema_def_types,
            context=context,
            optional=True)
    # Complex type -> Extract from schema definitions and propagate
    elif '#' in port_type:
        return _create_token_processor(
            port=port,
            port_type=schema_def_types[port_type],
            port_description=port_description,
            schema_def_types=schema_def_types,
            context=context,
            optional=optional)
    # Simple type -> Create typed token processor
    else:
        # Get default value and valueFrom fields
        default_value = port_description.get('default', None)
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
                default_value=default_value,
                expression_lib=expression_lib,
                file_format=file_format,
                full_js=full_js,
                glob=location,
                optional=optional,
                streamable=streamable)
        elif port_type == 'Directory':
            location = port_description.get('path', None)
            processor = CWLTokenProcessor(
                port=port,
                port_type='Directory',
                default_value=default_value,
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
                default_value=default_value,
                expression_lib=expression_lib,
                full_js=full_js)
        if isinstance(port, InputPort):
            processor.load_listing = _get_load_listing(port_description, context)
            if 'loadContents' in port_description:
                processor.load_contents = port_description['loadContents']
            elif 'inputBinding' in port_description and 'loadContents' in port_description['inputBinding']:
                processor.load_contents = port_description['inputBinding']['loadContents']
            if 'secondaryFiles' in port_description:
                processor.secondary_files = _get_secondary_files(
                    port_description['secondaryFiles'], default_required=True)
        elif isinstance(port, OutputPort):
            if 'loadContents' in port_description:
                processor.load_contents = port_description['loadContents']
            if 'outputBinding' in port_description:
                output_binding = port_description['outputBinding']
                processor.load_contents = output_binding.get('loadContents', False)
                processor.load_listing = _get_load_listing(port_description, context)
                if 'glob' in output_binding:
                    processor.glob = output_binding['glob']
                if 'outputEval' in output_binding:
                    processor.output_eval = output_binding['outputEval']
            if 'secondaryFiles' in port_description:
                processor.secondary_files = _get_secondary_files(
                    port_description['secondaryFiles'], default_required=False)
        return processor


def _get_command_token(binding: Any,
                       input_name: Optional[Text] = None,
                       token_type: Optional[Text] = None) -> CWLCommandToken:
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
                                  input_name: Text,
                                  schema_def_types: Optional[MutableMapping[Text, Any]] = None):
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
                    schema_def_types=schema_def_types)
                if token is not None:
                    token = CWLMapCommandToken(input_name, token)
            # Enum type: -> substitute the type with string and reprocess
            elif port_type['type'] == 'enum':
                return _get_command_token_from_input(
                    cwl_element=cwl_element,
                    port_type='string',
                    input_name=input_name,
                    schema_def_types=schema_def_types)
            # Generic typed object: -> propagate
            else:
                token = _get_command_token_from_input(
                    cwl_element=port_type,
                    port_type=port_type['type'],
                    input_name=input_name,
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
                schema_def_types=schema_def_types)
        # List of types: -> CWLUnionCommandToken
        else:
            command_tokens = []
            for i, port_type in enumerate(types):
                token = _get_command_token_from_input(
                    cwl_element=cwl_element,
                    port_type=port_type,
                    input_name=input_name,
                    schema_def_types=schema_def_types)
                if token is not None:
                    command_tokens.append(token)
            if command_tokens:
                token = CWLUnionCommandToken(input_name, command_tokens)
    elif isinstance(port_type, str):
        # Complex type -> Extract from schema definitions and propagate
        if '#' in port_type:
            return _get_command_token_from_input(
                cwl_element=cwl_element,
                port_type=schema_def_types[port_type],
                input_name=input_name,
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
                    schema_def_types=schema_def_types)
                if el_token is not None:
                    command_tokens[key] = el_token
            if command_tokens:
                token = CWLObjectCommandToken(
                    name=input_name,
                    value=command_tokens)
    # Simple type with `inputBinding` specified -> CWLCommandToken
    if command_line_binding is not None:
        if token is not None:
            return _get_command_token({**command_line_binding, **{'valueFrom': token}}, input_name)
        else:
            return _get_command_token(command_line_binding, input_name, port_type)
    # Simple type without `inputBinding` specified -> token
    else:
        return token


def _get_input_combinator(step: Step,
                          scatter_inputs: Optional[Set[Text]] = None,
                          scatter_method: Optional[Text] = None) -> InputCombinator:
    scatter_inputs = scatter_inputs or set()
    # If there are no scatter ports in this step, create a single DotProduct combinator
    if not [n for n in scatter_inputs if n.startswith(step.name)]:
        input_combinator = DotProductInputCombinator(random_name())
        for port in step.input_ports.values():
            input_combinator.ports[port.name] = port
        return input_combinator
    # If there are scatter ports
    else:
        other_ports = dict(step.input_ports)
        cartesian_combinator = CartesianProductInputCombinator(random_name())
        # Separate scatter ports from the other ones
        scatter_ports = {}
        for port_name, port in step.input_ports.items():
            if posixpath.join(step.name, port_name) in scatter_inputs:
                scatter_ports[port_name] = port
                del other_ports[port_name]
        # Choose the right combinator for the scatter ports, based on the `scatterMethod` property
        if scatter_method == 'dotproduct':
            scatter_name = random_name()
            scatter_combinator = DotProductInputCombinator(scatter_name)
        else:
            scatter_name = random_name()
            scatter_combinator = CartesianProductInputCombinator(scatter_name)
        scatter_combinator.ports = scatter_ports
        cartesian_combinator.ports[scatter_name] = scatter_combinator
        # Create a CartesianProduct combinator between the scatter ports and the DotProduct of the others
        if other_ports:
            dotproduct_name = random_name()
            dotproduct_combinator = DotProductInputCombinator(dotproduct_name)
            dotproduct_combinator.ports = other_ports
            cartesian_combinator.ports[dotproduct_name] = dotproduct_combinator
        return cartesian_combinator


def _get_merge_strategy(pickValue: Text) -> Optional[Callable[[MutableSequence[Token]], MutableSequence[Token]]]:
    if pickValue == 'first_non_null':
        def merge_strategy(token_list):
            for t in token_list:
                if t.value is not None:
                    return t
            raise WorkflowExecutionException("All sources are null")
    elif pickValue == 'the_only_non_null':
        def merge_strategy(token_list):
            ret = None
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
            return [t for t in token_list if t.value is not None]
    else:
        merge_strategy = None
    return merge_strategy


def _get_name(name_prefix: Text, element_id: Text, last_element_only: bool = False) -> Text:
    name = element_id.split('#')[-1]
    if last_element_only and '/' in name:
        name = name.split('/')[-1]
    if name_prefix and posixpath.join('/', name).startswith(os.path.join(name_prefix, '')):
        return posixpath.join('/', name)
    else:
        return posixpath.join(name_prefix, name)


def _get_output_combinator(name: Text,
                           ports: MutableMapping[Text, OutputPort],
                           step: Optional[Step] = None,
                           merge_strategy: Optional[Callable[[MutableSequence[Token]], MutableSequence[Token]]] = None):
    combinator = DotProductOutputCombinator(
        name=name,
        ports=ports,
        step=step,
        merge_strategy=merge_strategy)
    combinator.token_processor = CWLUnionTokenProcessor(
        port=combinator,
        processors=[p.token_processor for p in ports.values()])
    return combinator


def _get_load_listing(port_description: MutableMapping[Text, Any],
                      context: MutableMapping[Text, Any]) -> LoadListing:
    requirements = {**context['hints'], **context['requirements']}
    if 'loadListing' in port_description:
        return LoadListing[port_description['loadListing']]
    elif 'LoadListingRequirement' in requirements and 'loadListing' in requirements['LoadListingRequirement']:
        return LoadListing[requirements['LoadListingRequirement']['loadListing']]
    else:
        return LoadListing.no_listing


def _get_schema_def_types(requirements: MutableMapping[Text, Any]) -> MutableMapping[Text, Any]:
    return ({sd['name']: sd for sd in requirements['SchemaDefRequirement']['types']}
            if 'SchemaDefRequirement' in requirements else {})


def _get_secondary_files(cwl_element, default_required: bool) -> MutableSequence[SecondaryFile]:
    if isinstance(cwl_element, MutableSequence):
        return [SecondaryFile(sf['pattern'], sf.get('required')
        if sf.get('required') is not None else default_required) for sf in cwl_element]
    elif isinstance(cwl_element, MutableMapping):
        return [SecondaryFile(cwl_element['pattern'], cwl_element.get('required')
        if cwl_element.get('required') is not None else default_required)]


async def _inject_input(job: Job, port: OutputPort, value: Any) -> None:
    port.step = job.step
    port.put(await port.token_processor.compute_token(
        job=job,
        command_output=CWLCommandOutput(value, Status.COMPLETED, 0)))
    port.put(TerminationToken(name=port.name))


def _percolate_port(port_name: Text, *args) -> OutputPort:
    for arg in args:
        if port_name in arg:
            port = arg[port_name]
            if isinstance(port, OutputPort):
                return port
            else:
                return _percolate_port(port, *args)


def _process_javascript_requirement(requirements: MutableMapping[Text, Any]) -> (Optional[MutableSequence[Any]], bool):
    expression_lib = None
    full_js = False
    if 'InlineJavascriptRequirement' in requirements:
        full_js = True
        if 'expressionLib' in requirements['InlineJavascriptRequirement']:
            expression_lib = []
            for lib in requirements['InlineJavascriptRequirement']['expressionLib']:
                expression_lib.append(lib)
    return expression_lib, full_js


def _process_docker_requirement(docker_requirement: MutableMapping[Text, Any]) -> Target:
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
        'network': 'none'
    }
    if 'dockerOutputDirectory' in docker_requirement:
        docker_config['workdir'] = docker_requirement['dockerOutputDirectory']
    # Build step target
    return Target(
        model=ModelConfig(
            name='docker-requirement-{id}'.format(id=random_name()),
            connector_type='docker',
            config=docker_config,
            external=False
        ),
        service=image_name
    )


class CWLTranslator(object):

    def __init__(self,
                 context: StreamFlowContext,
                 cwl_definition: cwltool.process.Process,
                 cwl_inputs: MutableMapping[Text, Any],
                 workflow_config: WorkflowConfig,
                 loading_context: cwltool.context.LoadingContext):
        self.context: StreamFlowContext = context
        self.cwl_definition: cwltool.process.Process = cwl_definition
        self.cwl_inputs: Optional[MutableMapping[Text, Any]] = cwl_inputs
        self.workflow_config: WorkflowConfig = workflow_config
        self.loading_context: cwltool.context.LoadingContext = loading_context
        self.output_ports: MutableMapping[Text, Union[Text, OutputPort]] = {}

    def _apply_config(self, workflow: Workflow):
        for step in workflow.steps.values():
            step_path = PurePosixPath(step.name)
            step_target = self.workflow_config.propagate(step_path, 'target')
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
            step_workdir = self.workflow_config.propagate(step_path, 'workdir')
            if step_workdir is not None:
                step.workdir = step_workdir

    def _get_source_port(self, workflow: Workflow, source_name: Text):
        source_step, source_port = posixpath.split(source_name)
        if source_step in workflow.steps and source_port in workflow.steps[source_step].output_ports:
            return workflow.steps[source_step].output_ports[source_port]
        else:
            return _percolate_port(source_name, self.output_ports)

    async def _inject_inputs(self, workflow: Workflow, root_prefix: Text):
        # Create a dummy job and an empty context
        input_injector = BaseJob(
            name=random_name(),
            step=BaseStep(random_name(), self.context),
            inputs=[])
        input_injector.output_directory = os.getcwd()
        # Inject input values into initial ports
        inject_tasks = []
        if self.cwl_inputs:
            for key, value in self.cwl_inputs.items():
                if key in workflow.steps[root_prefix].input_ports:
                    # Retrieve input port
                    input_port = workflow.steps[root_prefix].input_ports[key]
                    # Create dependee
                    port = DefaultOutputPort(random_name())
                    port.token_processor = input_port.token_processor
                    input_port.dependee = port
                    # Inject input value
                    inject_tasks.append(asyncio.create_task(_inject_input(input_injector, port, value)))
        await asyncio.gather(*inject_tasks)
        # Create null-valued tokens for unbound input ports
        for step in workflow.steps.values():
            for input_name, input_port in step.input_ports.items():
                if input_port.dependee is None:
                    port = DefaultOutputPort(random_name())
                    input_port.dependee = port
                if input_port.dependee.empty() and input_port.dependee.step is None:
                    input_port.dependee.put(Token(name=input_port.dependee.name, value=None))
                    input_port.dependee.put(TerminationToken(name=input_port.dependee.name))

    def _process_dependencies(self,
                              workflow: Workflow,
                              cwl_element: cwltool.workflow.WorkflowStep,
                              name_prefix: Text):
        step_name = _get_name(name_prefix, cwl_element.id)
        step = workflow.steps[step_name]
        for element_input in cwl_element.tool['inputs']:
            # If the input element depends on one or more output ports
            if 'source' in element_input:
                global_name = _get_name(step_name, element_input['id'], last_element_only=True)
                port = step.input_ports[posixpath.relpath(global_name, step_name)]
                # If source element is a list, the input element can depend on multiple ports
                if isinstance(element_input['source'], MutableSequence):
                    # If the list contains only one element and no `linkMerge` is specified, treat it as a singleton
                    if (len(element_input['source']) == 1 and
                            'linkMerge' not in element_input and
                            'pickValue' not in element_input):
                        source_name = _get_name(name_prefix, element_input['source'][0])
                        port.dependee = self._get_source_port(workflow, source_name)
                    # Otherwise, create a DotrProductOutputCombinator
                    else:
                        source_names = [_get_name(name_prefix, src) for src in element_input['source']]
                        ports = [self._get_source_port(workflow, n) for n in source_names]
                        port.dependee = _get_output_combinator(
                            name=port.name,
                            step=step,
                            ports={p.name: p for p in ports},
                            merge_strategy=_get_merge_strategy(element_input.get('pickValue')))
                # Otherwise, the input element depends on a single output port
                else:
                    source_name = _get_name(name_prefix, element_input['source'])
                    source_port = self._get_source_port(workflow, source_name)
                    port.dependee = source_port
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

    def _recursive_translate(self,
                             workflow: Workflow,
                             cwl_element: cwltool.process.Process,
                             context: MutableMapping[Text, Any],
                             name_prefix: Text = "/"):
        # Update context
        current_context = copy.deepcopy(context)
        for hint in cwl_element.hints:
            current_context['hints'][hint['class']] = hint
        for requirement in cwl_element.requirements:
            current_context['requirements'][requirement['class']] = requirement
        # In the root process, override requirements when provided in the input file
        workflow_id = self.cwl_definition.tool['id']
        if name_prefix == (_get_name('/', workflow_id) if '#' in workflow_id else '/'):
            if 'requirements' in self.cwl_inputs:
                current_context['requirements'] = {req['class']: req for req in self.cwl_inputs['requirements']}
        # Dispatch element
        if isinstance(cwl_element, cwltool.workflow.Workflow):
            self._translate_workflow(workflow, cwl_element, current_context, name_prefix)
        elif isinstance(cwl_element, cwltool.workflow.WorkflowStep):
            self._translate_workflow_step(workflow, cwl_element, current_context, name_prefix)
        elif isinstance(cwl_element, cwltool.command_line_tool.CommandLineTool):
            self._translate_command_line_tool(workflow, cwl_element, current_context, name_prefix)
        elif isinstance(cwl_element, cwltool.command_line_tool.ExpressionTool):
            self._translate_command_line_tool(workflow, cwl_element, current_context, name_prefix)
        else:
            raise WorkflowDefinitionException(
                "Definition of type " + type(cwl_element).__class__.__name__ + " not supported")

    def _translate_command_line_tool(self,
                                     workflow: Workflow,
                                     cwl_element: Union[cwltool.command_line_tool.CommandLineTool,
                                                        cwltool.command_line_tool.ExpressionTool],
                                     context: MutableMapping[Text, Any],
                                     name_prefix: Text):
        step = BaseStep(name_prefix, self.context)
        # Extract custom types if present
        requirements = {**context['hints'], **context['requirements']}
        schema_def_types = _get_schema_def_types(requirements)
        # Process inputs
        for element_input in cwl_element.tool['inputs']:
            global_name = _get_name(name_prefix, element_input['id'], last_element_only=True)
            port_name = posixpath.relpath(global_name, name_prefix)
            port = _create_input_port(
                port_name=port_name,
                port_description=element_input,
                schema_def_types=schema_def_types,
                context=context)
            port.step = step
            step.input_ports[port_name] = port
        # Add input combinator
        step.input_combinator = _get_input_combinator(step)
        # Process outputs
        for element_output in cwl_element.tool['outputs']:
            global_name = _get_name(name_prefix, element_output['id'], last_element_only=True)
            port_name = posixpath.relpath(global_name, name_prefix)
            port = _create_output_port(
                port_name=port_name,
                port_description=element_output,
                schema_def_types=schema_def_types,
                context=context)
            port.step = step
            self.output_ports[global_name] = port
            step.output_ports[port_name] = port
        # Process DockerRequirement
        if 'DockerRequirement' in requirements:
            step.target = _process_docker_requirement(requirements['DockerRequirement'])
        # Process command
        if isinstance(cwl_element, cwltool.command_line_tool.CommandLineTool):
            step.command = _build_command(
                cwl_element=cwl_element,
                schema_def_types=schema_def_types,
                context=context,
                step=step)
        elif isinstance(cwl_element, cwltool.command_line_tool.ExpressionTool):
            if 'expression' in cwl_element.tool:
                step.command = CWLExpressionCommand(step, cwl_element.tool['expression'])
        else:
            WorkflowDefinitionException(
                "Command generation for " + type(cwl_element).__class__.__name__ + " is not suported")
        # Process InlineJavascriptRequirement
        step.command.expression_lib, step.command.full_js = _process_javascript_requirement(requirements)
        # Process ToolTimeLimit
        if 'ToolTimeLimit' in requirements:
            step.command.time_limit = requirements['ToolTimeLimit']['timelimit']
            if step.command.time_limit < 0:
                raise WorkflowDefinitionException('Invalid time limit for step {step}'.format(step=name_prefix))
        # Process streams
        if 'stdin' in cwl_element.tool:
            step.command.stdin = cwl_element.tool['stdin']
        if 'stdout' in cwl_element.tool:
            step.command.stdout = cwl_element.tool['stdout']
        if 'stderr' in cwl_element.tool:
            step.command.stderr = cwl_element.tool['stderr']
        # Add step to workflow
        workflow.steps[step.name] = step

    def _translate_workflow(self,
                            workflow: Workflow,
                            cwl_element: cwltool.workflow.Workflow,
                            context: MutableMapping[Text, Any],
                            name_prefix: Text):
        # Extract custom types if present
        requirements = {**context['hints'], **context['requirements']}
        schema_def_types = _get_schema_def_types(requirements)
        # Create input step
        step_name = name_prefix
        input_step = BaseStep(step_name, self.context)
        # Add command
        expression_lib, full_js = _process_javascript_requirement(requirements)
        input_step.command = CWLStepCommand(
            step=input_step,
            expression_lib=expression_lib,
            full_js=full_js)
        # Process inputs
        for element_input in cwl_element.tool['inputs']:
            global_name = _get_name(step_name, element_input['id'])
            port_name = posixpath.relpath(global_name, step_name)
            # Create an input port
            port = _create_input_port(
                port_name=port_name,
                port_description=element_input,
                schema_def_types=schema_def_types,
                context=context)
            port.step = input_step
            input_step.input_ports[port_name] = port
            # Create output port
            output_port = _create_output_port(
                port_name=port_name,
                port_description=element_input,
                schema_def_types=schema_def_types,
                context=context)
            output_port.step = input_step
            input_step.output_ports[port_name] = output_port
        # Add input combinator
        input_step.input_combinator = _get_input_combinator(input_step)
        # Add input step to workflow
        workflow.steps[input_step.name] = input_step
        # Process steps
        for step in cwl_element.steps:
            self._recursive_translate(workflow, step, context, name_prefix)
        # Link step dependencies
        for step in cwl_element.steps:
            self._process_dependencies(workflow, step, name_prefix)
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
                    self.output_ports[name] = _get_output_combinator(
                        name=name,
                        step=BaseStep(
                            name=random_name(),
                            context=self.context),
                        ports=ports,
                        merge_strategy=_get_merge_strategy(element_output.get('pickValue')))
            # Otherwise, the output element depends on a single output port
            else:
                source_name = _get_name(name_prefix, element_output['outputSource'])
                source_port = self._get_source_port(workflow, source_name)
                if 'pickValue' in element_output:
                    self.output_ports[name] = _get_output_combinator(
                        name=name,
                        step=BaseStep(
                            name=random_name(),
                            context=self.context),
                        ports={source_port.name: source_port},
                        merge_strategy=_get_merge_strategy(element_output.get('pickValue')))
                else:
                    self.output_ports[name] = source_port

    def _translate_workflow_step(self,
                                 workflow: Workflow,
                                 cwl_element: cwltool.workflow.WorkflowStep,
                                 context: MutableMapping[Text, Any],
                                 name_prefix: Text):
        # Process content
        step_name = _get_name(name_prefix, cwl_element.id)
        run_command = cwl_element.tool['run']
        loading_context = self.loading_context.copy()
        loading_context.requirements = list(context['requirements'].values())
        loading_context.hints = list(context['hints'].values())
        if isinstance(run_command, MutableMapping):
            step_definition = self.loading_context.construct_tool_object(run_command, loading_context)
            # Recusrively process the step
            self._recursive_translate(workflow, step_definition, context, posixpath.join(step_name, 'run'))
        else:
            step_definition = cwltool.load_tool.load_tool(run_command, loading_context)
            self._recursive_translate(workflow, step_definition, context, posixpath.join(step_name, 'run'))
        # Merge requirements with the underlying step definition
        context = copy.deepcopy(context)
        for hint in step_definition.hints:
            context['hints'][hint['class']] = hint
        for requirement in step_definition.requirements:
            context['requirements'][requirement['class']] = requirement
        requirements = {**context['hints'], **context['requirements']}
        # Create input step
        input_step = BaseStep(step_name, self.context)
        # Extract custom types if present
        schema_def_types = _get_schema_def_types(requirements)
        # Add command
        expression_lib, full_js = _process_javascript_requirement(requirements)
        input_step.command = CWLStepCommand(
            step=input_step,
            expression_lib=expression_lib,
            full_js=full_js,
            when_expression=cwl_element.tool.get('when'))
        # Find scatter elements
        scatter_method = cwl_element.tool.get('scatterMethod')
        if isinstance(cwl_element.tool.get('scatter'), Text):
            scatter_inputs = {_get_name(step_name, cwl_element.tool.get('scatter'), last_element_only=True)}
        else:
            scatter_inputs = {_get_name(step_name, n, last_element_only=True)
                              for n in cwl_element.tool.get('scatter', [])}
        # Process inputs
        for element_input in cwl_element.tool['inputs']:
            global_name = _get_name(step_name, element_input['id'], last_element_only=True)
            port_name = posixpath.relpath(global_name, step_name)
            scatter = global_name in scatter_inputs
            if scatter:
                element_input = {**element_input, **{'type': element_input['type']['items']}}
            # Create an input port
            port = _create_input_port(
                port_name=port_name,
                port_description=element_input,
                schema_def_types=schema_def_types,
                context=context,
                scatter=scatter)
            port.step = input_step
            input_step.input_ports[port_name] = port
            # If the input element contains a `valueFrom` option, create also a command token
            if 'valueFrom' in element_input:
                input_step.command.input_expressions[port_name] = element_input['valueFrom']
            # If input is propagated inside the step, create output port
            if not element_input.get('not_connected', False):
                output_port = _create_output_port(
                    port_name=port_name,
                    port_description=element_input,
                    schema_def_types=schema_def_types,
                    context=context)
                output_port.step = input_step
                input_step.output_ports[port_name] = output_port
                # Link output port to inner step's input port
                inner_step = workflow.steps[posixpath.join(step_name, 'run')]
                inner_step.input_ports[port_name].dependee = output_port
                # Percolate secondary files
                inner_processor = inner_step.input_ports[port_name].token_processor
                if isinstance(inner_processor, CWLTokenProcessor) and inner_processor.secondary_files:
                    current_processor = cast(CWLTokenProcessor, input_step.input_ports[port_name].token_processor)
                    current_processor.secondary_files = list(set(
                        (current_processor.secondary_files or []) + (inner_processor.secondary_files or [])))
        # Add input combinator
        input_step.input_combinator = _get_input_combinator(input_step, scatter_inputs, scatter_method)
        # Add input step to workflow
        workflow.steps[input_step.name] = input_step
        # Manage gathering of scatter ports with a dedicated step
        gather_step = None
        if scatter_inputs:
            gather_step = BaseStep(posixpath.join(step_name, 'gather'), self.context)
            gather_step.command = CWLStepCommand(
                step=gather_step,
                expression_lib=expression_lib,
                full_js=full_js)
        # Process outputs
        for element_output in cwl_element.tool['outputs']:
            inner_name = _get_name(posixpath.join(step_name, 'run'), element_output['id'], last_element_only=True)
            outer_name = _get_name(step_name, element_output['id'], last_element_only=True)
            port_name = posixpath.relpath(outer_name, step_name)
            inner_port = _percolate_port(inner_name, self.output_ports)
            if isinstance(inner_port, DotProductOutputCombinator):
                inner_steps = [p.step for p in inner_port.ports.values()]
            else:
                inner_steps = [inner_port.step]
            # If output port comes from a scatter input, place a port in the gathering step
            if any(_check_scatter(inner_step, scatter_inputs, []) for inner_step in inner_steps):
                # Process port type
                element_type = element_output['type']
                if isinstance(element_type, MutableSequence):
                    element_type = [t for t in filter(lambda x: x != 'null', element_type)]
                    if len(element_type) == 1:
                        port_type = element_type[0]['items']
                    else:
                        raise WorkflowDefinitionException('Invalid type for scatter port')
                else:
                    port_type = element_type['items']
                # Create input port
                input_port = DefaultInputPort(port_name, gather_step)
                input_port.token_processor = _create_token_processor(
                    port=input_port,
                    port_type=port_type['items'] if scatter_method == 'nested_crossproduct' else port_type,
                    port_description=element_output,
                    schema_def_types=schema_def_types,
                    context=context)
                source_port = self._get_source_port(workflow, inner_name)
                if 'when' in cwl_element.tool:
                    source_port = _create_skip_link(
                        port=source_port,
                        step=input_step)
                input_port.dependee = source_port
                gather_step.input_ports[port_name] = input_port
                # Create output port
                output_port = GatherOutputPort(port_name, gather_step)
                if scatter_method == 'nested_crossproduct':
                    def merge_strategy(token_list):
                        groups = {}
                        for t in token_list:
                            if t.tag not in groups:
                                groups[t.tag] = []
                            groups[t.tag].append(t)
                        return list(groups.values())
                    output_port.merge_strategy = merge_strategy
                output_port.token_processor = _create_token_processor(
                    port=output_port,
                    port_type=port_type['items'] if scatter_method == 'nested_crossproduct' else port_type,
                    port_description=element_output,
                    schema_def_types=schema_def_types,
                    context=context)
                gather_step.output_ports[port_name] = output_port
                # Remap output port
                gather_name = _get_name(gather_step.name, element_output['id'], last_element_only=True)
                self.output_ports[gather_name] = output_port
                self.output_ports[outer_name] = gather_name
            else:
                # Remap output port
                source_port = self._get_source_port(workflow, inner_name)
                if 'when' in cwl_element.tool:
                    source_port = _create_skip_link(
                        port=source_port,
                        step=input_step)
                self.output_ports[outer_name] = source_port
        if gather_step is not None:
            gather_step.input_combinator = _get_input_combinator(gather_step)
            workflow.steps[gather_step.name] = gather_step

    async def translate(self) -> Workflow:
        workflow = Workflow()
        # Create context
        context = _create_context()
        # Compute root prefix
        workflow_id = self.cwl_definition.tool['id']
        root_prefix = _get_name('/', workflow_id) if '#' in workflow_id else '/'
        # Build workflow graph
        self._recursive_translate(workflow, self.cwl_definition, context, root_prefix)
        # Inject initial inputs
        await self._inject_inputs(workflow, root_prefix)
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
