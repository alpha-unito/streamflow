from collections import deque
from typing import MutableMapping, Any, MutableSequence, Union, AsyncIterable, cast

from streamflow.core import utils
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.workflow import Token, Workflow
from streamflow.workflow.step import Combinator


def _add_to_list(token: Union[Token, MutableMapping[str, Token]],
                 token_values: MutableMapping[str, MutableMapping[str, MutableSequence[Any]]],
                 port_name: str,
                 depth: int = 0):
    tag = utils.get_tag(token.values()) if isinstance(token, MutableMapping) else token.tag
    if depth:
        tag = '.'.join(tag.split('.')[:-depth])
    for key in list(token_values.keys()):
        if tag == key:
            continue
        elif key.startswith(tag):
            _add_to_port(token, token_values[key], port_name)
        elif tag.startswith(key):
            if tag not in token_values:
                token_values[tag] = {}
            for p in token_values[key]:
                for t in token_values[key][p]:
                    _add_to_port(t, token_values[tag], p)
    if tag not in token_values:
        token_values[tag] = {}
    _add_to_port(token, token_values[tag], port_name)


def _add_to_port(token: Union[Token, MutableMapping[str, Token]],
                 tag_values: MutableMapping[str, MutableSequence[Any]],
                 port_name: str):
    if port_name not in tag_values:
        tag_values[port_name] = deque()
    tag_values[port_name].append(token)


class CartesianProductCombinator(Combinator):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 depth: int = 1):
        super().__init__(name, workflow)
        self.depth: int = depth
        self.token_values: MutableMapping[str, MutableMapping[str, MutableSequence[Any]]] = {}

    async def _product(self,
                       port_name: str,
                       token: Union[Token, MutableSequence[Token]]) -> AsyncIterable[MutableMapping[str, Token]]:
        # Get all combinations of the new element with the others
        tag = '.'.join(token.tag.split('.')[:-self.depth])
        if len(self.token_values[tag]) == len(self.items):
            cartesian_product = utils.dict_product(
                **{k: [token] if k == port_name else v for k, v in self.token_values[tag].items()})
            # Return all combination schemas
            for config in cartesian_product:
                schema = {}
                for key in self.items:
                    if key in self.combinators:
                        schema = {**schema, **config[key]}
                    else:
                        schema[key] = config[key]
                        schema[key] = config[key]
                suffix = [t.tag.split('.')[-1] for t in schema.values()]
                schema = {k: t.retag('.'.join(t.tag.split('.')[:-1] + suffix)) for k, t in schema.items()}
                yield schema

    async def combine(self,
                      port_name: str,
                      token: Token) -> AsyncIterable[MutableMapping[str, Token]]:
        # If port is associated to an inner combinator, call it and put shcemas in their related list
        if c := self.get_combinator(port_name):
            async for schema in cast(AsyncIterable, c.combine(port_name, token)):
                _add_to_list(schema, self.token_values, c.name, self.depth)
                async for product in self._product(port_name, token):
                    yield product
        # If port is associated directly with the current combinator, put the token in the list
        elif port_name in self.items:
            _add_to_list(token, self.token_values, port_name, self.depth)
            async for product in self._product(port_name, token):
                yield product
        # Otherwise throw Exception
        else:
            raise WorkflowExecutionException("No item to combine for token '{}'.".format(port_name))


class DotProductCombinator(Combinator):

    def __init__(self,
                 name: str,
                 workflow: Workflow):
        super().__init__(name, workflow)
        self.token_values: MutableMapping[str, MutableMapping[str, MutableSequence[Any]]] = {}

    async def _product(self) -> AsyncIterable[MutableMapping[str, Token]]:
        # Check if some complete input sets are available
        for tag in list(self.token_values):
            if len(self.token_values[tag]) == len(self.items):
                num_items = min(len(i) for i in self.token_values[tag].values())
                for i in range(num_items):
                    # Return the relative combination schema
                    schema = {}
                    for key, elements in self.token_values[tag].items():
                        element = elements.pop()
                        if key in self.combinators:
                            schema = {**schema, **element}
                        else:
                            schema[key] = element
                    tag = utils.get_tag(schema.values())
                    schema = {k: t.retag(tag) for k, t in schema.items()}
                    yield schema

    async def combine(self,
                      port_name: str,
                      token: Token) -> AsyncIterable[MutableMapping[str, Token]]:
        # If port is associated to an inner combinator, call it and put shcemas in their related list
        if c := self.get_combinator(port_name):
            async for schema in cast(AsyncIterable, c.combine(port_name, token)):
                _add_to_list(schema, self.token_values, c.name)
                async for product in self._product():
                    yield product
        # If port is associated directly with the current combinator, put the token in the list
        elif port_name in self.items:
            _add_to_list(token, self.token_values, port_name)
            async for product in self._product():
                yield product
        # Otherwise throw Exception
        else:
            raise WorkflowExecutionException("No item to combine for token '{}'.".format(port_name))
