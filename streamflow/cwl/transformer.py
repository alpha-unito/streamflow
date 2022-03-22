from typing import MutableMapping, Optional, MutableSequence

from streamflow.core.exception import WorkflowExecutionException, WorkflowDefinitionException
from streamflow.core.utils import get_tag
from streamflow.core.workflow import Token, Workflow, TokenProcessor, Port
from streamflow.cwl import utils
from streamflow.workflow.token import ListToken
from streamflow.workflow.transformer import OneToOneTransformer, ManyToOneTransformer


class AllNonNullTransformer(OneToOneTransformer):

    def _transform(self, name: str, token: Token) -> Token:
        if isinstance(token, ListToken):
            return token.update([t for t in token.value if utils.get_token_value(t) is not None])
        elif isinstance(token.value, Token):
            return token.update(self._transform(name, token.value))
        else:
            raise WorkflowExecutionException("Invalid value for token {}".format(name))

    async def transform(self, inputs: MutableMapping[str, Token]) -> MutableMapping[str, Token]:
        return {self.get_output_name(): self._transform(*next(iter(inputs.items())))}


class CWLDefaultTransformer(ManyToOneTransformer):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 default_port: Port):
        super().__init__(name, workflow)
        self.add_input_port('__default__', default_port)

    def add_input_port(self, name: str, port: Port) -> None:
        input_ports = {k: p for k, p in self.input_ports .items()if k != '__default__'}
        if not input_ports:
            super().add_input_port(name, port)
        else:
            raise WorkflowDefinitionException("{} step must contain a single input port.".format(self.name))

    async def transform(self, inputs: MutableMapping[str, Token]) -> MutableMapping[str, Token]:
        if len(inputs) != 2:
            raise WorkflowDefinitionException("{} step must contain a single input port.".format(self.name))
        if '__default__' not in inputs:
            raise WorkflowDefinitionException("{} step must contain a default port.".format(self.name))
        primary_token = next(iter(inputs[k] for k in inputs if k != '__default__'))
        if utils.get_token_value(primary_token) is not None:
            return {self.get_output_name(): primary_token}
        else:
            return {self.get_output_name(): inputs['__default__']}


class CWLTokenTransformer(ManyToOneTransformer):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 port_name: str,
                 processor: TokenProcessor):
        super().__init__(name, workflow)
        self.port_name: str = port_name
        self.processor: TokenProcessor = processor

    async def transform(self, inputs: MutableMapping[str, Token]) -> MutableMapping[str, Token]:
        return {self.get_output_name(): await self.processor.process(inputs, inputs[self.port_name])}


class FirstNonNullTransformer(OneToOneTransformer):

    def _transform(self, name: str, token: Token) -> Token:
        if isinstance(token, ListToken):
            for t in token.value:
                if utils.get_token_value(t) is not None:
                    return t
            raise WorkflowExecutionException("All sources are null in token {}".format(name))
        elif isinstance(token.value, Token):
            return token.update(self._transform(name, token.value))
        else:
            raise WorkflowExecutionException("Invalid value for token {}".format(name))

    async def transform(self, inputs: MutableMapping[str, Token]) -> MutableMapping[str, Token]:
        return {self.get_output_name(): self._transform(*next(iter(inputs.items())))}


class ListToElementTransformer(OneToOneTransformer):

    def _transform(self, token: Token) -> Token:
        if isinstance(token, ListToken):
            if len(token.value) == 1:
                return token.value[0]
            else:
                return token
        elif isinstance(token.value, Token):
            return token.update(self._transform(token.value))
        else:
            return token

    async def transform(self, inputs: MutableMapping[str, Token]) -> MutableMapping[str, Token]:
        return {self.get_output_name(): self._transform(next(iter(inputs.values())))}


class OnlyNonNullTransformer(OneToOneTransformer):

    def _transform(self, name: str, token: Token):
        if isinstance(token, ListToken):
            ret = None
            for t in token.value:
                if utils.get_token_value(t) is not None:
                    if ret is not None:
                        raise WorkflowExecutionException(
                            "Expected only one source to be non-null in token {}".format(name))
                    ret = t
            if ret is None:
                raise WorkflowExecutionException("All sources are null in token {}".format(name))
            return ret
        elif isinstance(token.value, Token):
            return token.update(self._transform(name, token.value))
        else:
            raise WorkflowExecutionException("Invalid value for token {}".format(name))

    async def transform(self, inputs: MutableMapping[str, Token]) -> MutableMapping[str, Token]:
        return {self.get_output_name(): self._transform(*next(iter(inputs.items())))}


class ValueFromTransformer(ManyToOneTransformer):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 port_name: str,
                 processor: TokenProcessor,
                 value_from: str,
                 expression_lib: Optional[MutableSequence[str]] = None,
                 full_js: bool = False):
        super().__init__(name, workflow)
        self.port_name: str = port_name
        self.processor: TokenProcessor = processor
        self.value_from: str = value_from
        self.expression_lib: Optional[MutableSequence[str]] = expression_lib
        self.full_js: bool = full_js

    async def transform(self, inputs: MutableMapping[str, Token]) -> MutableMapping[str, Token]:
        if self.get_output_name() in inputs:
            inputs = {
                **inputs,
                **{self.get_output_name(): await self.processor.process(inputs, inputs[self.get_output_name()])}}
        context = utils.build_context(inputs)
        context = {**context, **{'self': context['inputs'].get(self.get_output_name())}}
        return {self.get_output_name(): Token(
            tag=get_tag(inputs.values()),
            value=utils.eval_expression(
                expression=self.value_from,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib))}
