from __future__ import annotations

from collections import deque
from typing import Any, AsyncIterable, MutableMapping, MutableSequence, cast

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Token, Workflow
from streamflow.workflow.step import Combinator
from streamflow.workflow.token import IterationTerminationToken


class CartesianProductCombinator(Combinator):
    def __init__(self, name: str, workflow: Workflow, depth: int = 1):
        super().__init__(name, workflow)
        self.depth: int = depth
        self.token_values: MutableMapping[
            str, MutableMapping[str, MutableSequence[Any]]
        ] = {}

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> CartesianProductCombinator:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            depth=row["depth"],
        )

    async def _product(
        self,
        port_name: str,
        token: Token | MutableSequence[Token],
    ) -> AsyncIterable[MutableMapping[str, Token]]:
        # Get all combinations of the new element with the others
        tag = ".".join(token.tag.split(".")[: -self.depth])
        if len(self.token_values[tag]) == len(self.items):
            cartesian_product = utils.dict_product(
                **{
                    k: [token] if k == port_name else v
                    for k, v in self.token_values[tag].items()
                }
            )
            # Return all combination schemas
            for config in cartesian_product:
                schema = {}
                for key in self.items:
                    if key in self.combinators:
                        schema = {**schema, **config[key]}
                    else:
                        schema[key] = config[key]
                suffix = [t.tag.split(".")[-1] for t in schema.values()]
                yield {
                    k: {
                        "token": t.retag(".".join(t.tag.split(".")[:-1] + suffix)),
                        "input_ids": [t.persistent_id],
                    }
                    for k, t in schema.items()
                }

    async def _save_additional_params(self, context: StreamFlowContext):
        return {
            **await super()._save_additional_params(context),
            **{"depth": self.depth},
        }

    async def combine(
        self,
        port_name: str,
        token: Token,
    ) -> AsyncIterable[MutableMapping[str, Token]]:
        # If port is associated to an inner combinator, call it and put schemas in their related list
        if c := self.get_combinator(port_name):
            async for schema in cast(
                AsyncIterable,
                c.combine(port_name, token),
            ):
                self._add_to_list(schema, c.name, self.depth)
                async for product in self._product(port_name, token):
                    yield product
        # If port is associated directly with the current combinator, put the token in the list
        elif port_name in self.items:
            self._add_to_list(token, port_name, self.depth)
            async for product in self._product(port_name, token):
                yield product
        # Otherwise throw Exception
        else:
            raise WorkflowExecutionException(
                f"No item to combine for token '{port_name}'."
            )

    def _add_to_port(
        self,
        token: Token | MutableMapping[str, Token],
        tag_values: MutableMapping[str, MutableSequence[Any]],
        port_name: str,
    ):
        if port_name not in tag_values:
            tag_values[port_name] = deque()
        for t in tag_values[port_name]:
            if t.tag == token.tag:
                return
        tag_values[port_name].append(token)


class DotProductCombinator(Combinator):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__(name, workflow)
        self.token_values: MutableMapping[
            str, MutableMapping[str, MutableSequence[Any]]
        ] = {}

    async def _product(self) -> AsyncIterable[MutableMapping[str, Token]]:
        # Check if some complete input sets are available
        for tag in list(self.token_values):
            if len(self.token_values[tag]) == len(self.items):
                num_items = min(len(i) for i in self.token_values[tag].values())
                for _ in range(num_items):
                    # Return the relative combination schema
                    schema = {}
                    for key, elements in self.token_values[tag].items():
                        element = elements.pop()
                        if key in self.combinators:
                            schema = {**schema, **element}
                        else:
                            schema[key] = {
                                "token": element,
                                "input_ids": [element.persistent_id],
                            }
                    tag = utils.get_tag([t["token"] for t in schema.values()])
                    yield {
                        k: {
                            "token": t["token"].retag(tag),
                            "input_ids": t["input_ids"],
                        }
                        for k, t in schema.items()
                    }

    async def combine(
        self,
        port_name: str,
        token: Token,
    ) -> AsyncIterable[MutableMapping[str, Token]]:
        # If port is associated to an inner combinator, call it and put schemas in their related list
        if c := self.get_combinator(port_name):
            async for schema in cast(
                AsyncIterable,
                c.combine(port_name, token),
            ):
                self._add_to_list(schema, c.name)
                async for product in self._product():
                    yield product
        # If port is associated directly with the current combinator, put the token in the list
        elif port_name in self.items:
            self._add_to_list(token, port_name)
            async for product in self._product():
                yield product
        # Otherwise throw Exception
        else:
            raise WorkflowExecutionException(
                f"No item to combine for token '{port_name}'."
            )


class LoopCombinator(DotProductCombinator):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__(name, workflow)
        self.iteration_map: MutableMapping[str, int] = {}

    async def _product(self) -> AsyncIterable[MutableMapping[str, Token]]:
        async for schema in super()._product():
            tag = utils.get_tag([t["token"] for t in schema.values()])
            prefix = ".".join(tag.split(".")[:-1])
            if prefix not in self.iteration_map:
                self.iteration_map[tag] = 0
                tag = ".".join(tag.split(".") + ["0"])
            else:
                self.iteration_map[prefix] += 1
                tag = ".".join(tag.split(".")[:-1] + [str(self.iteration_map[prefix])])
            yield {
                k: {"token": t["token"].retag(tag), "input_ids": t["input_ids"]}
                for k, t in schema.items()
            }


class LoopTerminationCombinator(DotProductCombinator):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__(name, workflow)
        self.output_items: MutableSequence[str] = []
        self.token_values: MutableMapping[
            str, MutableMapping[str, MutableSequence[Any]]
        ] = {}

    def add_output_item(self, item: str) -> None:
        self.output_items.append(item)

    async def _product(self) -> AsyncIterable[MutableMapping[str, Token]]:
        async for schema in super()._product():
            tag = utils.get_tag([t["token"] for t in schema.values()])
            yield {
                k: {
                    "token": IterationTerminationToken(tag=tag),
                    "input_ids": [id for t in schema.values() for id in t["input_ids"]],
                }
                for k in self.output_items
            }
