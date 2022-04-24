from typing import MutableMapping, MutableSequence, Any, AsyncIterable

from streamflow.core.utils import get_tag
from streamflow.core.workflow import Token, Workflow
from streamflow.workflow.combinator import DotProductCombinator
from streamflow.workflow.token import ListToken


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

    async def combine(self,
                      port_name: str,
                      token: Token) -> AsyncIterable[MutableMapping[str, Token]]:
        async for schema in super().combine(port_name, token):
            outputs = [schema[name] for name in self.input_names]
            # Flatten if needed
            if self.flatten:
                outputs = _flatten_token_list(outputs)
            yield {self.output_name: ListToken(
                value=outputs,
                tag=get_tag(outputs))}
