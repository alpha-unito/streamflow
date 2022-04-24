from abc import ABC

from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.workflow import Port
from streamflow.workflow.step import Transformer


class ManyToOneTransformer(Transformer, ABC):

    def add_output_port(self, name: str, port: Port) -> None:
        if not self.output_ports or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException("{} step must contain a single output port.".format(self.name))

    def get_output_name(self):
        return next(iter(self.output_ports))

    async def run(self):
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException("{} step must contain a single output port.".format(self.name))
        await super().run()


class OneToOneTransformer(ManyToOneTransformer, ABC):

    def add_input_port(self, name: str, port: Port) -> None:
        if not self.input_ports:
            super().add_input_port(name, port)
        else:
            raise WorkflowDefinitionException("{} step must contain a single input port.".format(self.name))

    async def run(self):
        if len(self.input_ports) != 1:
            raise WorkflowDefinitionException("{} step must contain a single input port.".format(self.name))
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException("{} step must contain a single output port.".format(self.name))
        await super().run()
