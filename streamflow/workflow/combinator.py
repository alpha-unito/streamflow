from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import AsyncIterable, MutableMapping, MutableSequence
from typing import Any, Self, cast

from streamflow.core import utils
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.persistence import Database, DatabaseLoadingContext
from streamflow.core.workflow import Token, Workflow
from streamflow.workflow.token import IterationTerminationToken


class CombinatorSchema:
    __slots__ = ("token", "input_ids")

    def __init__(self, token: Token, input_ids: MutableSequence[int]):
        self.token: Token = token
        self.input_ids: MutableSequence[int] = input_ids


class Combinator(ABC):
    def __init__(self, name: str, workflow: Workflow):
        self.name: str = name
        self.workflow: Workflow = workflow
        self.items: MutableSequence[str] = []
        self.combinators: MutableMapping[str, Combinator] = {}
        self.combinators_map: MutableMapping[str, str] = {}
        self._schemas: MutableMapping[
            tuple[str, ...],
            MutableMapping[
                str,
                MutableSequence[
                    CombinatorSchema | MutableMapping[str, CombinatorSchema]
                ],
            ],
        ] = {}
        self._children: MutableMapping[tuple[str, ...], set[str]] = {}
        self._propagate: bool = True

    @classmethod
    async def _load(
        cls,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(row["workflow"]),
        )

    async def _save_additional_params(
        self, database: Database
    ) -> MutableMapping[str, Any]:
        return {
            "name": self.name,
            "combinators": {
                k: v
                for k, v in zip(
                    self.combinators.keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(comb.save(database))
                            for comb in self.combinators.values()
                        )
                    ),
                    strict=True,
                )
            },
            "combinators_map": self.combinators_map,
            "items": self.items,
            "workflow": self.workflow.persistent_id,
        }

    def add_combinator(self, combinator: Combinator, items: set[str]) -> None:
        self.combinators[combinator.name] = combinator
        self.items.append(combinator.name)
        self.combinators_map |= {p: combinator.name for p in items}

    def add_item(self, item: str) -> None:
        self.items.append(item)

    def get_combinator(self, item: str) -> Combinator | None:
        return self.combinators.get(self.combinators_map.get(item, ""))

    def get_items(self, recursive: bool = False) -> set[str]:
        items = set(self.items)
        if recursive:
            for combinator in self.combinators.values():
                items.update(combinator.get_items(recursive))
        return items

    @abstractmethod
    async def combine(
        self, port_name: str, schema: CombinatorSchema
    ) -> AsyncIterable[MutableMapping[str, CombinatorSchema]]: ...

    @classmethod
    async def load(
        cls,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        type_ = cast(Self, utils.get_class_from_name(row["type"]))
        combinator = await type_._load(row["params"], loading_context)
        combinator.items = row["params"]["items"]
        combinator.combinators_map = row["params"]["combinators_map"]
        combinator.combinators = dict(
            zip(
                row["params"]["combinators"].keys(),
                await asyncio.gather(
                    *(
                        asyncio.create_task(Combinator.load(c, loading_context))
                        for c in row["params"]["combinators"].values()
                    )
                ),
                strict=True,
            )
        )
        return combinator

    async def restore(
        self, from_tags: MutableMapping[str, MutableSequence[str]]
    ) -> None:
        """
        Restore the combinator's internal state based on the provided tags.
        """
        return

    async def save(self, database: Database) -> MutableMapping[str, Any]:
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(database),
        }

    def _register(
        self,
        schema: CombinatorSchema | MutableMapping[str, CombinatorSchema],
        port_name: str,
        depth: int = 0,
    ) -> None:
        tag = tuple(
            (
                utils.get_tag([s.token for s in schema.values()])
                if isinstance(schema, MutableMapping)
                else schema.token.tag
            ).split(".")
        )
        if depth:
            tag = tuple(tag[:-depth])
        if self._propagate:
            # Propagate to successors
            stack: deque[tuple[str, ...]] = deque([tag])
            while stack:
                current_tag = stack.pop()
                if children := self._children.get(current_tag):
                    for c in children:
                        key = current_tag + (c,)
                        if key in self._schemas:
                            self._add_to_port(schema, key, port_name)
                        stack.append(key)
            # Propagate to predecessors
            for i in range(len(tag) - 1, 0, -1):
                for name, port in self._schemas.get(tag[:i], {}).items():
                    for t in port:
                        self._add_to_port(t, tag, name)
            # Register children
            for i in range(len(tag) - 1, -1, -1):
                prefix = tag[:i]
                if children := self._children.get(prefix):
                    children.add(tag[i])
                    break
                self._children[prefix] = {tag[i]}
        # Register token
        self._add_to_port(schema, tag, port_name)

    def _add_to_port(
        self,
        schema: CombinatorSchema | MutableMapping[str, CombinatorSchema],
        tag: tuple[str, ...],
        port_name: str,
    ) -> None:
        self._schemas.setdefault(tag, {}).setdefault(port_name, deque()).append(schema)


class CartesianProductCombinator(Combinator):
    def __init__(self, name: str, workflow: Workflow, depth: int = 1):
        super().__init__(name, workflow)
        self.depth: int = depth

    @classmethod
    async def _load(
        cls,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(row["workflow"]),
            depth=row["depth"],
        )

    async def _product(
        self,
        port_name: str,
        schema: CombinatorSchema | MutableSequence[CombinatorSchema],
    ) -> AsyncIterable[MutableMapping[str, CombinatorSchema]]:
        # Get all combinations of the new element with the others
        tag = tuple(schema.token.tag.split(".")[: -self.depth])
        if len(self._schemas[tag]) == len(self.items):
            cartesian_product = utils.dict_product(
                **{
                    k: [schema] if k == port_name else v
                    for k, v in self._schemas[tag].items()
                }
            )
            # Return all combination schemas
            for config in cartesian_product:
                schema: dict[str, CombinatorSchema] = {}
                for key in self.items:
                    if key in self.combinators:
                        schema |= config[key]
                    else:
                        schema[key] = config[key]
                suffix = [s.token.tag.rpartition(".")[-1] for s in schema.values()]
                yield {
                    k: CombinatorSchema(
                        token=s.token.retag(
                            ".".join(s.token.tag.split(".")[:-1] + suffix)
                        ),
                        input_ids=[s.token.persistent_id],
                    )
                    for k, s in schema.items()
                }

    async def _save_additional_params(
        self, database: Database
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(database)) | {
            "depth": self.depth
        }

    async def combine(
        self,
        port_name: str,
        schema: CombinatorSchema,
    ) -> AsyncIterable[MutableMapping[str, CombinatorSchema]]:
        # If port is associated to an inner combinator, call it and put schemas in their related list
        if c := self.get_combinator(port_name):
            async for s in cast(
                AsyncIterable,
                c.combine(port_name, schema),
            ):
                self._register(s, c.name, self.depth)
                async for product in self._product(port_name, schema):
                    yield product
        # If port is associated directly with the current combinator, put the token in the list
        elif port_name in self.items:
            self._register(schema, port_name, self.depth)
            async for product in self._product(port_name, schema):
                yield product
        # Otherwise throw Exception
        else:
            raise WorkflowExecutionException(
                f"No item to combine for token '{port_name}'."
            )

    def _add_to_port(
        self,
        schema: CombinatorSchema | MutableMapping[str, CombinatorSchema],
        tag: tuple[str, ...],
        port_name: str,
    ) -> None:
        queue = self._schemas.setdefault(tag, {}).setdefault(port_name, deque())
        for s in queue:
            if s.token.tag == schema.token.tag:
                return
        queue.append(schema)


class DotProductCombinator(Combinator):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__(name, workflow)
        self._complete: set[tuple[str, ...]] = set()

    async def _product(self) -> AsyncIterable[MutableMapping[str, CombinatorSchema]]:
        # Check if some complete input sets are available
        for tag in list(self._complete):
            while num_items := len(min(self._schemas[tag].values(), key=len)):
                for _ in range(num_items):
                    # Return the relative combination schema
                    schema: dict[str, CombinatorSchema] = {}
                    for key, elements in self._schemas[tag].items():
                        element = elements.pop()
                        if key in self.combinators:
                            schema |= element
                        else:
                            schema[key] = element
                    new_tag = utils.get_tag([s.token for s in schema.values()])
                    yield {
                        k: CombinatorSchema(
                            token=s.token.retag(new_tag), input_ids=s.input_ids
                        )
                        for k, s in schema.items()
                    }
            self._complete.remove(tag)

    def _add_to_port(
        self,
        schema: CombinatorSchema | MutableMapping[str, CombinatorSchema],
        tag: tuple[str, ...],
        port_name: str,
    ) -> None:
        super()._add_to_port(schema, tag, port_name)
        if len(self._schemas[tag]) == len(self.items):
            self._complete.add(tag)

    async def combine(
        self,
        port_name: str,
        schema: CombinatorSchema,
    ) -> AsyncIterable[MutableMapping[str, CombinatorSchema]]:
        # If port is associated to an inner combinator, call it and put schemas in their related list
        if c := self.get_combinator(port_name):
            async for s in cast(
                AsyncIterable,
                c.combine(port_name, schema),
            ):
                self._register(s, c.name)
                async for product in self._product():
                    yield product
        # If port is associated directly with the current combinator, put the token in the list
        elif port_name in self.items:
            self._register(schema, port_name)
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
        self._propagate: bool = False

    async def _product(self) -> AsyncIterable[MutableMapping[str, CombinatorSchema]]:
        async for schemas in super()._product():
            tag = utils.get_tag([s.token for s in schemas.values()])
            prefix = tag.rpartition(".")[0]
            if prefix not in self.iteration_map:
                self.iteration_map[tag] = 0
                tag = f"{tag}.0"
            else:
                self.iteration_map[prefix] += 1
                tag = f"{prefix}.{self.iteration_map[prefix]}"
            yield {
                k: CombinatorSchema(token=s.token.retag(tag), input_ids=s.input_ids)
                for k, s in schemas.items()
            }

    async def restore(self, from_tags: MutableMapping[str, tuple[str, str]]) -> None:
        """
        Restore the iteration counters.
        For each port, the input is a pair of tags where the first tag is the prefix,
        and the second tag contains the iteration from which the execution must be resumed.
        """
        for prefix, iteration in from_tags.values():
            iteration_num = int(iteration.rpartition(".")[-1])
            self.iteration_map[prefix] = max(
                self.iteration_map.get(prefix, iteration_num), iteration_num
            )


class LoopTerminationCombinator(DotProductCombinator):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__(name, workflow)
        self.output_items: MutableSequence[str] = []

    def add_output_item(self, item: str) -> None:
        self.output_items.append(item)

    async def _product(self) -> AsyncIterable[MutableMapping[str, CombinatorSchema]]:
        async for schemas in super()._product():
            tag = utils.get_tag([s.token for s in schemas.values()])
            yield {
                k: CombinatorSchema(
                    token=IterationTerminationToken(tag=tag),
                    input_ids=[id for s in schemas.values() for id in s.input_ids],
                )
                for k in self.output_items
            }

    @classmethod
    async def _load(
        cls,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        combinator = cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(row["workflow"]),
        )
        for item in row["output_items"]:
            combinator.add_output_item(item)
        return combinator

    async def _save_additional_params(
        self, database: Database
    ) -> MutableMapping[str, Any]:
        # self._token_values is not saved because it is always empty at the beginning of execution
        return cast(dict[str, Any], await super()._save_additional_params(database)) | {
            "output_items": self.output_items
        }
