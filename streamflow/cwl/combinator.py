from __future__ import annotations

from typing import Any, AsyncIterable, MutableMapping, MutableSequence

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.utils import get_tag
from streamflow.core.workflow import Token, Workflow
from streamflow.workflow.combinator import DotProductCombinator
from streamflow.workflow.token import IterationTerminationToken, ListToken


def _flatten_token_list(outputs: MutableSequence[Token]):
    flattened_list = []
    for token in sorted(outputs, key=lambda t: int(t.tag.split('.')[-1])):
        if isinstance(token, ListToken):
            flattened_list.extend(_flatten_token_list(token.value))
        else:
            flattened_list.append(token)
    return flattened_list


class ListMergeCombinator(DotProductCombinator):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 input_names: MutableSequence[str],
                 output_name: str,
                 flatten: bool = False):
        super().__init__(name, workflow)
        self.flatten: bool = flatten
        self.input_names: MutableSequence[str] = input_names
        self.output_name: str = output_name
        self.token_values: MutableMapping[str, MutableMapping[str, Any]] = {}

    @classmethod
    async def _load(cls,
                    context: StreamFlowContext,
                    row: MutableMapping[str, Any],
                    loading_context: DatabaseLoadingContext) -> ListMergeCombinator:
        return cls(
            name=row['name'],
            workflow=await loading_context.load_workflow(context, row['workflow']),
            input_names=row['input_names'],
            output_name=row['output_name'],
            flatten=row['flatten'])

    async def _save_additional_params(self, context: StreamFlowContext):
        return {**await super()._save_additional_params(context),
                **{'input_names': self.input_names,
                   'output_name': self.output_name,
                   'flatten': self.flatten}}

    async def combine(self,
                      port_name: str,
                      token: Token) -> AsyncIterable[MutableMapping[str, Token]]:
        if not isinstance(token, IterationTerminationToken):
            async for schema in super().combine(port_name, token):
                # If there is only one input, merge its value
                if len(self.input_names) == 1:
                    if isinstance(outputs := schema[self.input_names[0]], ListToken):
                        outputs = outputs.value
                    else:
                        outputs = [outputs]
                    tag = schema[self.input_names[0]].tag
                # Otherwise, merge multiple inputs in a single list
                else:
                    outputs = [schema[name] for name in self.input_names]
                    tag = get_tag(outputs)
                # Flatten if needed
                if self.flatten:
                    outputs = _flatten_token_list(outputs)
                yield {self.output_name: ListToken(
                    value=outputs,
                    tag=tag)}
