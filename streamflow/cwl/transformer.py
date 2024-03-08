from __future__ import annotations

import functools
import json
from typing import Any, MutableMapping, MutableSequence, cast

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.utils import get_tag
from streamflow.core.workflow import Port, Token, TokenProcessor, Workflow
from streamflow.cwl import utils
from streamflow.cwl.step import build_token
from streamflow.workflow.port import JobPort
from streamflow.workflow.token import ListToken
from streamflow.workflow.transformer import ManyToOneTransformer, OneToOneTransformer
from streamflow.workflow.utils import get_token_value


class AllNonNullTransformer(OneToOneTransformer):
    def _transform(self, name: str, token: Token) -> Token:
        if isinstance(token, ListToken):
            return token.update(
                [t for t in token.value if get_token_value(t) is not None]
            )
        elif isinstance(token.value, Token):
            return token.update(self._transform(name, token.value))
        else:
            raise WorkflowExecutionException(f"Invalid value for token {name}")

    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        return {self.get_output_name(): self._transform(*next(iter(inputs.items())))}


class CartesianProductSizeTransformer(ManyToOneTransformer):
    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        for port_name, token in inputs.items():
            if not isinstance(token.value, int) or token.value < 0:
                raise WorkflowExecutionException(
                    f"Step {self.name} received {token.value} on port {port_name}, but it must be a positive integer"
                )
        tag = get_tag(inputs.values())
        value = functools.reduce(
            lambda x, y: x * y, (token.value for token in inputs.values())
        )
        return {self.get_output_name(): Token(value, tag=tag)}


class CloneTransformer(ManyToOneTransformer):
    def __init__(self, name: str, workflow: Workflow, replicas_port: Port):
        super().__init__(name, workflow)
        self.add_input_port("__replicas__", replicas_port)

    def get_input_port_name(self) -> str:
        return next(n for n in self.input_ports if n != "__replicas__")

    def add_input_port(self, name: str, port: Port) -> None:
        if len(self.input_ports) < 2 or name in self.input_ports:
            super().add_input_port(name, port)
        else:
            raise WorkflowDefinitionException(
                f"Step {self.name} must contain a single input port"
            )

    def get_input_port(self, name: str | None = None) -> Port:
        return super().get_input_port(
            self.get_input_port_name() if name is None else name
        )

    def get_replicas_port(self) -> Port:
        return self.get_input_port("__replicas__")

    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        # inputs has only two keys: __replicas__ and a port_name
        input_token = inputs[self.get_input_port_name()]
        size_token = inputs["__replicas__"]
        if not isinstance(size_token.value, int) or size_token.value < 0:
            raise WorkflowExecutionException(
                f"Step {self.name} received {size_token.value} on replicas port, but it must be a positive integer"
            )
        if size_token.tag != input_token.tag:
            raise WorkflowExecutionException(
                f"Step {self.name} received {size_token.tag} on replicas port and {input_token.tag} on {self.get_input_port_name()} port"
            )
        return {
            self.get_output_name(): [
                input_token.retag(f"{input_token.tag}.{i}")
                for i in range(size_token.value)
            ]
        }


class CWLTokenTransformer(ManyToOneTransformer):
    def __init__(
        self, name: str, workflow: Workflow, port_name: str, processor: TokenProcessor
    ):
        super().__init__(name, workflow)
        self.port_name: str = port_name
        self.processor: TokenProcessor = processor

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        params = json.loads(row["params"])
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            port_name=params["port_name"],
            processor=await TokenProcessor.load(
                context, params["processor"], loading_context
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {
            **await super()._save_additional_params(context),
            **{
                "port_name": self.port_name,
                "processor": await self.processor.save(context),
            },
        }

    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        return {
            self.get_output_name(): await self.processor.process(
                inputs, inputs[self.port_name]
            )
        }


class DefaultTransformer(ManyToOneTransformer):
    def __init__(self, name: str, workflow: Workflow, default_port: Port):
        super().__init__(name, workflow)
        self.default_port: Port = default_port
        self.default_token: Token | None = None

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        params = json.loads(row["params"])
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            default_port=await loading_context.load_port(
                context, params["default_port"]
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {
            **await super()._save_additional_params(context),
            **{"default_port": self.default_port.persistent_id},
        }

    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        if len(inputs) != 1:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single input port."
            )
        if not self.default_port:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a default port."
            )
        primary_token = next(iter(inputs[k] for k in inputs))
        if get_token_value(primary_token) is not None:
            return {self.get_output_name(): primary_token.update(primary_token.value)}
        else:
            if not self.default_token:
                self.default_token = (
                    await self._get_inputs({"__default__": self.default_port})
                )["__default__"]
            return {self.get_output_name(): self.default_token.retag(primary_token.tag)}


class DefaultRetagTransformer(DefaultTransformer):
    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        if not self.default_port:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a default port."
            )
        tag = get_tag(inputs.values())
        if not self.default_token:
            self.default_token = (
                await self._get_inputs({"__default__": self.default_port})
            )["__default__"]
        return {self.get_output_name(): self.default_token.retag(tag)}


class DotProductSizeTransformer(ManyToOneTransformer):
    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        values = {t.value for t in inputs.values()}
        if len(values) > 1:
            raise WorkflowExecutionException(
                f"Step {self.name} received {values}, but all sizes must be equal"
            )
        input_token = next(iter(inputs.values()))
        if not isinstance(input_token.value, int) or input_token.value < 0:
            raise WorkflowExecutionException(
                f"Step {self.name} received {input_token.value}, but it must be a positive integer"
            )
        return {self.get_output_name(): input_token.update(input_token.value)}


class FirstNonNullTransformer(OneToOneTransformer):
    def _transform(self, name: str, token: Token) -> Token:
        if isinstance(token, ListToken):
            for t in token.value:
                if get_token_value(t) is not None:
                    return t.update(t.value)
            raise WorkflowExecutionException(f"All sources are null in token {name}")
        elif isinstance(token.value, Token):
            return token.update(self._transform(name, token.value))
        else:
            raise WorkflowExecutionException(f"Invalid value for token {name}")

    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        return {self.get_output_name(): self._transform(*next(iter(inputs.items())))}


class ForwardTransformer(OneToOneTransformer):
    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        token = next(iter(inputs.values()))
        return {self.get_output_name(): token.update(token.value)}


class ListToElementTransformer(OneToOneTransformer):
    def _transform(self, token: Token) -> Token:
        if isinstance(token, ListToken):
            if len(token.value) == 1:
                return token.value[0].update(token.value[0].value)
            else:
                return token.update(token.value)
        elif isinstance(token.value, Token):
            return token.update(self._transform(token.value))
        else:
            raise WorkflowDefinitionException(
                f"Invalid token value: Token required, but received {type(token.value)}"
            )

    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        return {self.get_output_name(): self._transform(next(iter(inputs.values())))}


class OnlyNonNullTransformer(OneToOneTransformer):
    def _transform(self, name: str, token: Token):
        if isinstance(token, ListToken):
            ret = None
            for t in token.value:
                if get_token_value(t) is not None:
                    if ret is not None:
                        raise WorkflowExecutionException(
                            f"Expected only one source to be non-null in token {name}"
                        )
                    ret = t
            if ret is None:
                raise WorkflowExecutionException(
                    f"All sources are null in token {name}"
                )
            return ret.update(ret.value) if isinstance(ret, Token) else ret
        elif isinstance(token.value, Token):
            return token.update(self._transform(name, token.value))
        else:
            raise WorkflowExecutionException(f"Invalid value for token {name}")

    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        return {self.get_output_name(): self._transform(*next(iter(inputs.items())))}


class ValueFromTransformer(ManyToOneTransformer):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        port_name: str,
        processor: TokenProcessor,
        value_from: str,
        job_port: JobPort,
        expression_lib: MutableSequence[str] | None = None,
        full_js: bool = False,
    ):
        super().__init__(name, workflow)
        self.port_name: str = port_name
        self.processor: TokenProcessor = processor
        self.value_from: str = value_from
        self.expression_lib: MutableSequence[str] | None = expression_lib
        self.full_js: bool = full_js
        self.add_input_port("__job__", job_port)

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        params = json.loads(row["params"])
        job_port = cast(
            JobPort, await loading_context.load_port(context, params["job_port"])
        )
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            port_name=params["port_name"],
            processor=await TokenProcessor.load(
                context, params["processor"], loading_context
            ),
            value_from=params["value_from"],
            expression_lib=params["expression_lib"],
            full_js=params["full_js"],
            job_port=job_port,
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        job_port = self.get_input_port("__job__")
        await job_port.save(context)
        return {
            **await super()._save_additional_params(context),
            **{
                "port_name": self.port_name,
                "processor": await self.processor.save(context),
                "value_from": self.value_from,
                "expression_lib": self.expression_lib,
                "full_js": self.full_js,
                "job_port": job_port.persistent_id,
            },
        }

    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        output_name = self.get_output_name()
        if output_name in inputs:
            inputs = {
                **inputs,
                **{
                    output_name: await self.processor.process(
                        inputs, inputs[output_name]
                    )
                },
            }
        context = utils.build_context(inputs)
        context = {**context, **{"self": context["inputs"].get(output_name)}}
        token_value = utils.eval_expression(
            expression=self.value_from,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib,
        )
        token = await build_token(
            job=await cast(JobPort, self.get_input_port("__job__")).get_job(self.name),
            token_value=token_value,
            streamflow_context=self.workflow.context,
        )
        return {output_name: token}


class LoopValueFromTransformer(ValueFromTransformer):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        port_name: str,
        processor: TokenProcessor,
        value_from: str,
        job_port: JobPort,
        expression_lib: MutableSequence[str] | None = None,
        full_js: bool = False,
    ):
        super().__init__(
            name,
            workflow,
            port_name,
            processor,
            value_from,
            job_port,
            expression_lib,
            full_js,
        )
        self.loop_input_ports: MutableSequence[str] = []
        self.loop_source_port: str | None = None

    def add_loop_input_port(self, name: str, port: Port):
        self.add_input_port(name + "-in", port)
        self.loop_input_ports.append(name)

    def add_loop_source_port(self, name: str, port: Port):
        self.add_input_port(name + "-out", port)
        self.loop_source_port = name

    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]:
        loop_inputs = {k: inputs[k + "-in"] for k in self.loop_input_ports}
        self_token = (
            await self.processor.process(
                loop_inputs, inputs[self.loop_source_port + "-out"]
            )
            if self.loop_source_port
            else None
        )
        context = utils.build_context(loop_inputs)
        context = {**context, **{"self": get_token_value(self_token)}}
        return {
            self.get_output_name(): Token(
                tag=get_tag(inputs.values()),
                value=utils.eval_expression(
                    expression=self.value_from,
                    context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                ),
            )
        }
