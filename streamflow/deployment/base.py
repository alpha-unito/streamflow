from __future__ import annotations

from abc import abstractmethod, ABC
from typing import List, TYPE_CHECKING

from streamflow.core.deployment import Connector, ConnectorCopyKind
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import Any
    from typing_extensions import Text


class BaseConnector(Connector, ABC):

    @staticmethod
    def get_option(name: Text,
                   value: Any,
                   ) -> Text:
        if len(name) > 1:
            name = "-{name} ".format(name=name)
        if isinstance(value, bool):
            return "-{name} ".format(name=name) if value else ""
        elif isinstance(value, str):
            return "-{name} \"{value}\" ".format(name=name, value=value)
        elif isinstance(value, List):
            return "".join(["-{name} \"{value}\" ".format(name=name, value=item) for item in value])
        elif value is None:
            return ""
        else:
            raise TypeError("Unsupported value type")

    @abstractmethod
    async def _copy_remote_to_remote(self,
                                     src: Text,
                                     dst: Text,
                                     resource: Text,
                                     source_remote: Text) -> None:
        ...

    @abstractmethod
    async def _copy_remote_to_local(self,
                                    src: Text,
                                    dst: Text,
                                    resource: Text) -> None:
        ...

    @abstractmethod
    async def _copy_local_to_remote(self,
                                    src: Text,
                                    dst: Text,
                                    resource: Text) -> None:
        ...

    async def copy(self,
                   src: Text,
                   dst: Text,
                   resource: Text,
                   kind: ConnectorCopyKind,
                   source_remote: Text = None) -> None:
        source_remote = source_remote or resource
        if kind == ConnectorCopyKind.REMOTE_TO_REMOTE:
            if source_remote == resource:
                logger.info("Copying {src} to {dst} on {resource}".format(src=src, dst=dst, resource=resource))
            else:
                logger.info("Copying {source_remote}:{src} to {resource}:{dst}".format(
                    source_remote=source_remote,
                    src=src,
                    resource=resource,
                    dst=dst
                ))
            await self._copy_remote_to_remote(src, dst, resource, source_remote or resource)
        elif kind == ConnectorCopyKind.LOCAL_TO_REMOTE:
            logger.info(
                "Copying {src} to {dst}".format(src=src, dst="{resource}:{file}".format(resource=resource, file=dst)))
            await self._copy_local_to_remote(src, dst, resource)
        elif kind == ConnectorCopyKind.REMOTE_TO_LOCAL:
            logger.info(
                "Copying {src} to {dst}".format(src="{resource}:{file}".format(resource=resource, file=src), dst=dst))
            await self._copy_remote_to_local(src, dst, resource)
        else:
            raise NotImplementedError
