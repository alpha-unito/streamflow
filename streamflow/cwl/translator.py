from __future__ import annotations

import copy
import os
import posixpath
from pathlib import PurePosixPath
from typing import MutableMapping, TYPE_CHECKING, Optional, List

import cwltool.command_line_tool
import cwltool.context
import cwltool.load_tool
import cwltool.process
import cwltool.workflow
from ruamel.yaml.comments import CommentedMap, CommentedSeq

from streamflow.config.config import WorkflowConfig
from streamflow.core import utils
from streamflow.core.context import StreamflowContext
from streamflow.core.deployment import ModelConfig
from streamflow.core.workflow import Port, InputPort, OutputPort, Workflow, Target, Token, TerminationToken, \
    InputCombinator
from streamflow.cwl.command import CWLCommand, CWLCommandToken, CWLExpressionCommand
from streamflow.cwl.condition import CWLCondition
from streamflow.cwl.token_processor import FileType, LoadListing, SecondaryFile, CWLFileProcessor, CWLValueProcessor
from streamflow.workflow.combinator import DotProductInputCombinator, CartesianProductInputCombinator
from streamflow.workflow.exception import WorkflowDefinitionException
from streamflow.workflow.port import DefaultInputPort, DefaultOutputPort, ScatterInputPort, GatherOutputPort
from streamflow.workflow.task import BaseTask

if TYPE_CHECKING:
    from streamflow.core.workflow import Task, TokenProcessor
    from streamflow.cwl.command import CWLBaseCommand
    from typing import Union, Any
    from typing_extensions import Text


def _build_command(
        cwl_element: cwltool.command_line_tool.CommandLineTool,
        context: MutableMapping[Text, Any],
        task: Task) -> CWLCommand:
    command = CWLCommand(task)
    # Process baseCommand
    if 'baseCommand' in cwl_element.tool:
        for command_token in cwl_element.tool['baseCommand']:
            command.base_command.append(command_token)
    # Process inputs
    for input_port in cwl_element.tool['inputs']:
        if 'inputBinding' in input_port:
            input_binding = input_port['inputBinding']
            if 'valueFrom' not in input_binding:
                input_binding['valueFrom'] = "".join(
                    ["$(inputs.", _get_name("", input_port['id'], last_element_only=True), ")"])
            command.command_tokens.append(_get_token(input_binding))
    # Process arguments
    if 'arguments' in cwl_element.tool:
        for argument in cwl_element.tool['arguments']:
            if isinstance(argument, MutableMapping):
                command.command_tokens.append(_get_token(argument))
            else:
                command.command_tokens.append(CWLCommandToken(argument))
    # Process InitialWorkDirRequirement
    requirements = context['requirements']
    if 'InitialWorkDirRequirement' in requirements:
        command.initial_work_dir = requirements['InitialWorkDirRequirement']['listing']
    # Process EnvVarRequirement
    if 'EnvVarRequirement' in requirements:
        for env_entry in requirements['EnvVarRequirement']['envDef']:
            command.environment[env_entry['envName']] = env_entry['envValue']
    # Process ShellCommandRequirement
    if 'ShellCommandRequirement' in requirements:
        command.is_shell_command = True
    return command


def _build_condition(context: MutableMapping[Text, Any], task: Task):
    requirements = context['requirements']
    when_expression = context['condition']
    if when_expression is not None:
        # Process InlineJavascriptRequirement
        expression_lib, full_js = _process_javascript_requirement(requirements)
        return CWLCondition(
            task=task,
            when_expression=when_expression,
            expression_lib=expression_lib,
            full_js=full_js)
    else:
        return None


def _create_input_port(
        name: Text,
        port_description: MutableMapping,
        context: MutableMapping[Text, Any],
        scatter: bool) -> InputPort:
    if scatter:
        port = ScatterInputPort(name)
    else:
        port = DefaultInputPort(name)
    processor = _create_token_processor(port, port_description, context)
    if isinstance(processor, CWLFileProcessor):
        processor.load_listing = _get_load_listing(port_description, context)
        if 'loadContents' in port_description:
            processor.load_contents = port_description['loadContents']
        elif 'inputBinding' in port_description and 'loadContents' in port_description['inputBinding']:
            processor.load_contents = port_description['inputBinding']['loadContents']
        if 'secondaryFiles' in port_description:
            processor.secondary_files = _get_secondary_files(port_description['secondaryFiles'], default_required=True)
    port.token_processor = processor
    return port


def _create_output_port(
        name: Text,
        port_description: MutableMapping,
        context: MutableMapping[Text, Any],
        gather: bool) -> OutputPort:
    if gather:
        port = GatherOutputPort(name)
    else:
        port = DefaultOutputPort(name)
    processor = _create_token_processor(port, port_description, context)
    if 'secondaryFiles' in port_description and isinstance(processor, CWLFileProcessor):
        if 'secondaryFiles' in port_description:
            processor.secondary_files = _get_secondary_files(port_description['secondaryFiles'], default_required=False)
    if 'outputBinding' in port_description:
        output_binding = port_description['outputBinding']
        if isinstance(processor, CWLFileProcessor):
            processor.load_contents = output_binding.get('loadContents', False)
            processor.load_listing = _get_load_listing(port_description, context)
        if 'glob' in output_binding:
            processor.glob = output_binding['glob']
        if 'outputEval' in output_binding:
            processor.output_eval = output_binding['outputEval']
    port.token_processor = processor
    return port


def _create_token_processor(
        port: Port,
        port_description: MutableMapping,
        context: MutableMapping[Text, Any]) -> TokenProcessor:
    # Get port type
    port_type = port_description['type']
    is_array = False
    while not isinstance(port_type, str):
        if isinstance(port_type, CommentedMap):
            is_array = True
            port_type = port_type['items']
        if isinstance(port_type, CommentedSeq):
            is_array = True
            if len(port_type) == 1:
                port_type = port_type[0]
            else:
                WorkflowDefinitionException("Output " + port.name + " is in an unsupported format")
    if port_type.endswith('[]'):
        is_array = True
        port_type = port_type[:-2].strip()
    # Get default value
    default_value = port_description.get('default', None)
    # Process InlineJavascriptRequirement
    expression_lib, full_js = _process_javascript_requirement(context['requirements'])
    # Create port
    if port_type == 'File':
        file_format = port_description.get('format', None)
        streamable = port_description.get('streamable', False)
        location = port_description.get('path', None)
        return CWLFileProcessor(
            port,
            is_array=is_array,
            file_type=FileType.FILE,
            default_path=default_value,
            expression_lib=expression_lib,
            file_format=file_format,
            full_js=full_js,
            glob=location,
            streamable=streamable)
    elif port_type == 'Directory':
        location = port_description.get('path', None)
        return CWLFileProcessor(
            port,
            is_array=is_array,
            file_type=FileType.DIRECTORY,
            default_path=default_value,
            expression_lib=expression_lib,
            full_js=full_js,
            glob=location)
    else:
        return CWLValueProcessor(
            port,
            is_array=is_array,
            port_type=port_type,
            default_value=default_value,
            expression_lib=expression_lib,
            full_js=full_js)


def _get_input_combinator(task: Task, scatter_context: MutableMapping[Text, Any]) -> InputCombinator:
    # If there are no scatter ports in this task, create a single DotProduct combinator
    if 'inputs' not in scatter_context or not [n for n in scatter_context['inputs'] if n.startswith(task.name)]:
        input_combinator = DotProductInputCombinator(utils.random_name())
        for port in task.input_ports.values():
            input_combinator.ports[port.name] = port
        return input_combinator
    else:
        # If there is a single scatter port, create a CartesianProduct
        # between the scatter port and the DotProduct of the others
        if 'method' not in scatter_context:
            cartesian_combinator = CartesianProductInputCombinator(utils.random_name())
            dotproduct_name = utils.random_name()
            dotproduct_combinator = DotProductInputCombinator(dotproduct_name)
            for port_name, port in task.input_ports.items():
                name = posixpath.join(task.name, port_name)
                if name in scatter_context['inputs']:
                    cartesian_combinator.ports[port.name] = port
                else:
                    dotproduct_combinator.ports[port.name] = port
            cartesian_combinator.ports[dotproduct_name] = dotproduct_combinator
            return cartesian_combinator
        else:
            pass
            # TODO: manage scatterMethod with port combinators


def _get_name(name_prefix: Text, element_id: Text, last_element_only: bool = False) -> Text:
    name = element_id.split('#')[-1]
    if last_element_only and '/' in name:
        name = name.split('/')[-1]
    return posixpath.join(name_prefix, name)


def _get_load_listing(port_description: MutableMapping[Text, Any],
                      context: MutableMapping[Text, Any]) -> LoadListing:
    requirements = context['requirements']
    if 'loadListing' in port_description:
        return LoadListing[port_description['loadListing']]
    elif 'LoadListingRequirement' in requirements and 'loadListing' in requirements['LoadListingRequirement']:
        return LoadListing[requirements['LoadListingRequirement']['loadListing']]
    else:
        return LoadListing.no_listing


def _get_secondary_files(cwl_element, default_required: bool) -> List[SecondaryFile]:
    return []  # TODO: manage secondary files


def _get_token(command_line_binding: MutableMapping):
    item_separator = command_line_binding.get('itemSeparator', None)
    position = command_line_binding.get('position', 0)
    prefix = command_line_binding.get('prefix', None)
    separate = command_line_binding.get('separate', True)
    shell_quote = command_line_binding.get('shellQuote', True)
    value = command_line_binding['valueFrom']
    return CWLCommandToken(
        value=value,
        item_separator=item_separator,
        position=position,
        prefix=prefix,
        separate=separate,
        shell_quote=shell_quote)


def _process_javascript_requirement(requirements: MutableMapping[Text, Any]) -> (Optional[List[Any]], bool):
    expression_lib = None
    full_js = False
    if 'InlineJavascriptRequirement' in requirements:
        full_js = True
        if 'expressionLib' in requirements['InlineJavascriptRequirement']:
            expression_lib = []
            for lib in requirements['InlineJavascriptRequirement']['expressionLib']:
                expression_lib.append(lib)
    return expression_lib, full_js


def _process_requirements(command: CWLBaseCommand,
                          requirements: MutableMapping[Text, Any]) -> CWLBaseCommand:
    # Process InlineJavascriptRequirement
    command.expression_lib, command.full_js = _process_javascript_requirement(requirements)
    # Process ToolTimeLimit
    if 'ToolTimeLimit' in requirements:
        command.time_limit = requirements['ToolTimeLimit']['timelimit']
    return command


class CWLTranslator(object):

    def __init__(self,
                 context: StreamflowContext,
                 cwl_definition: cwltool.process.Process,
                 cwl_inputs: MutableMapping[Text, Any],
                 workflow_config: WorkflowConfig,
                 loading_context: cwltool.context.LoadingContext):
        self.context: StreamflowContext = context
        self.cwl_definition: cwltool.process.Process = cwl_definition
        self.cwl_inputs: MutableMapping[Text, Any] = cwl_inputs
        self.workflow_config: WorkflowConfig = workflow_config
        self.loading_context: cwltool.context.LoadingContext = loading_context
        self.input_ports: MutableMapping[Text, Union[Text, OutputPort]] = {}
        self.output_ports: MutableMapping[Text, Union[Text, OutputPort]] = {}

    def _apply_config(self, workflow: Workflow):
        for task in workflow.tasks.values():
            task_path = PurePosixPath(task.name)
            task_target = self.workflow_config.propagate(task_path, 'target')
            if task_target is not None:
                target_model = self.workflow_config.models[task_target['model']]
                task.target = Target(
                    model=ModelConfig(
                        name=target_model['name'],
                        connector_type=target_model['type'],
                        config=target_model['config'],
                        external=target_model.get('external', False)
                    ),
                    service=task_target['service']
                )

    def _inject_inputs(self):
        for key, value in self.cwl_inputs.items():
            port_name = posixpath.join('/', key)
            port = self.input_ports[port_name]
            processor = port.token_processor
            # If value is a dictionary, extract the actual value
            if isinstance(value, dict):
                if 'class' not in value:
                    raise WorkflowDefinitionException(
                        "Dictionaries without explicit class declaration are not supported")
                if value['class'] in ['File', 'Directory'] and isinstance(processor, CWLFileProcessor):
                    weight = utils.get_size(value['path']) if os.path.exists(value['path']) else 0
                    port.put(Token(name=port.name, value=value['path'], weight=weight))
                    port.put(TerminationToken(name=port.name))
                else:
                    raise WorkflowDefinitionException("Inputs of type " + value['class'] + " are not supported")
            elif isinstance(processor, CWLValueProcessor):
                port.put(Token(name=port.name, value=value, weight=0))
                port.put(TerminationToken(name=port.name))
            else:
                raise WorkflowDefinitionException("Incorrect value specification for " + key)

    async def _percolate_inputs(self, workflow: Workflow):
        for task in workflow.tasks.values():
            for input_name, input_value in task.input_ports.items():
                current_name = posixpath.join(task.name, input_name)
                while current_name in self.input_ports:
                    current_value = self.input_ports[current_name]
                    if isinstance(current_value, OutputPort):
                        input_value.dependee = current_value
                        break
                    else:
                        current_name = current_value
                while current_name in self.output_ports:
                    current_value = self.output_ports[current_name]
                    if isinstance(current_value, OutputPort):
                        input_value.dependee = current_value
                        break
                    else:
                        current_name = current_value

    def _percolate_outputs(self, workflow: Workflow):
        for output_name, output_value in self.output_ports.items():
            if output_name.count('/') == 1:
                current_name = output_name
                while current_name in self.output_ports:
                    current_value = self.output_ports[current_name]
                    if isinstance(current_value, OutputPort):
                        workflow.output_ports[current_value.name] = current_value
                        break
                    else:
                        current_name = current_value

    def _recursive_translate(self,
                             workflow: Workflow,
                             cwl_element: cwltool.process.Process,
                             context: MutableMapping[Text, Any],
                             name_prefix: Text = "/"):
        # Update context
        current_context = copy.deepcopy(context)
        for requirement in cwl_element.requirements:
            current_context['requirements'][requirement['class']] = requirement
        # Dispatch element
        if isinstance(cwl_element, cwltool.workflow.Workflow):
            self._translate_workflow(workflow, cwl_element, name_prefix, current_context)
        elif isinstance(cwl_element, cwltool.command_line_tool.CommandLineTool):
            self._translate_command_line_tool(workflow, cwl_element, name_prefix, current_context)
        elif isinstance(cwl_element, cwltool.command_line_tool.ExpressionTool):
            self._translate_command_line_tool(workflow, cwl_element, name_prefix, current_context)
        else:
            raise WorkflowDefinitionException(
                "Definition of type " + type(cwl_element).__class__.__name__ + " not supported")

    def _translate_command_line_tool(self,
                                     workflow: Workflow,
                                     cwl_element: Union[cwltool.command_line_tool.CommandLineTool,
                                                        cwltool.command_line_tool.ExpressionTool],
                                     name_prefix: Text,
                                     context: MutableMapping[Text, Any]
                                     ):
        task = BaseTask(name_prefix, self.context)
        # Process input
        for element_input in cwl_element.tool['inputs']:
            name = _get_name(name_prefix, element_input['id'], last_element_only=True)
            scatter = 'inputs' in context['scatter'] and name in context['scatter']['inputs']
            port = _create_input_port(posixpath.relpath(name, name_prefix), element_input, context, scatter)
            port.task = task
            task.input_ports[posixpath.relpath(name, name_prefix)] = port
        # Add input combinator
        task.input_combinator = _get_input_combinator(task, context['scatter'])
        # Process outputs
        for element_output in cwl_element.tool['outputs']:
            name = _get_name(name_prefix, element_output['id'], last_element_only=True)
            gather = 'outputs' in context['scatter'] and name in context['scatter']['outputs']
            port = _create_output_port(posixpath.relpath(name, name_prefix), element_output, context, gather)
            port.task = task
            self.output_ports[name] = port
            task.output_ports[posixpath.relpath(name, name_prefix)] = port
        # Process command
        if isinstance(cwl_element, cwltool.command_line_tool.CommandLineTool):
            task.command = _build_command(cwl_element, context, task)
        elif isinstance(cwl_element, cwltool.command_line_tool.ExpressionTool):
            if 'expression' in cwl_element.tool:
                task.command = CWLExpressionCommand(task, cwl_element.tool['expression'])
        else:
            WorkflowDefinitionException(
                "Command generation for " + type(cwl_element).__class__.__name__ + " is not suported")
        task.command = _process_requirements(task.command, context['requirements'])
        task.command.task = task
        # Process condition
        task.condition = _build_condition(context, task)
        # Process streams
        if 'stdin' in cwl_element.tool:
            task.command.task_stdin = cwl_element.tool['stdin']
        if 'stdout' in cwl_element.tool:
            task.command.task_stdout = cwl_element.tool['stdout']
        if 'stderr' in cwl_element.tool:
            task.command.task_stdout = cwl_element.tool['stderr']
        # Add task to workflow
        workflow.tasks[task.name] = task

    def _translate_workflow(self,
                            workflow: Workflow,
                            cwl_element: cwltool.workflow.Workflow,
                            name_prefix: Text,
                            context: MutableMapping[Text, Any]):
        # Process inputs
        for element_input in cwl_element.tool['inputs']:
            name = _get_name(name_prefix, element_input['id'], last_element_only=True)
            port = DefaultOutputPort(posixpath.relpath(name, name_prefix))
            port.token_processor = _create_token_processor(port, element_input, context)
            self.input_ports[name] = port
        # Process steps
        for step in cwl_element.steps:
            self._translate_workflow_step(workflow, step, name_prefix, context)
        # Process outputs
        for element_output in cwl_element.tool['outputs']:
            name = _get_name(name_prefix, element_output['id'])
            self.output_ports[name] = _get_name(name_prefix, element_output['outputSource'])

    def _translate_workflow_step(self,
                                 workflow: Workflow,
                                 cwl_element: cwltool.workflow.WorkflowStep,
                                 name_prefix: Text,
                                 context: MutableMapping[Text, Any]):
        step_name = _get_name(name_prefix, cwl_element.id)
        # Check for condition
        if 'when' in cwl_element.tool:
            context['condition'] = cwl_element.tool['when']
        # Check for scatter
        if 'scatter' in cwl_element.tool:
            if isinstance(cwl_element.tool['scatter'], List):
                context['scatter']['inputs'] = [_get_name(name_prefix, n) for n in cwl_element.tool['scatter']]
                context['scatter']['method'] = cwl_element.tool['scatterMethod']
            else:
                context['scatter']['inputs'] = [_get_name(name_prefix, cwl_element.tool['scatter'])]
            context['scatter']['outputs'] = [_get_name(name_prefix, out['id']) for out in cwl_element.tool['outputs']]
        # Process content
        step_command = cwl_element.tool['run']
        if isinstance(step_command, MutableMapping):
            step_definition = self.loading_context.construct_tool_object(step_command, self.loading_context)
            self._recursive_translate(workflow, step_definition, context, step_name)
        else:
            step_definition = cwltool.load_tool.load_tool(step_command, self.loading_context)
            self._recursive_translate(workflow, step_definition, context, step_name)
        # Process inputs
        for element_input in cwl_element.tool['in']:
            name = _get_name(name_prefix, element_input['id'])
            self.input_ports[name] = _get_name(name_prefix, element_input['source'])

    async def translate(self) -> Workflow:
        workflow = Workflow()
        context = {
            'requirements': {},
            'condition': None,
            'scatter': {}
        }
        self._recursive_translate(workflow, self.cwl_definition, context)
        self._inject_inputs()
        await self._percolate_inputs(workflow)
        self._percolate_outputs(workflow)
        self._apply_config(workflow)

        return workflow
