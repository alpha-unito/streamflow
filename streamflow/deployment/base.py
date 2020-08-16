from __future__ import annotations

from abc import abstractmethod, ABC
from typing import List, TYPE_CHECKING

from streamflow.core.deployment import Connector, ConnectorCopyKind
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import Any, Optional
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
                                     resources: List[Text],
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
                                    resources: List[Text]) -> None:
        ...

    async def copy(self,
                   src: Text,
                   dst: Text,
                   resources: List[Text],
                   kind: ConnectorCopyKind,
                   source_remote: Optional[Text] = None) -> None:
        if kind == ConnectorCopyKind.REMOTE_TO_REMOTE:
            if source_remote is None:
                raise Exception("Source resource is mandatory for remote to remote copy")
            if len(resources) > 1:
                logger.info("Copying {src} on resource {source_remote} to {dst} on resources:\n\t{resources}".format(
                    source_remote=source_remote,
                    src=src,
                    dst=dst,
                    resources='\n\t'.join(resources)
                ))
            else:
                logger.info("Copying {src} on resource {source_remote} to {dst} on resource {resource}".format(
                    source_remote=source_remote,
                    src=src,
                    dst=dst,
                    resource=resources[0]
                ))
            await self._copy_remote_to_remote(src, dst, resources, source_remote)
        elif kind == ConnectorCopyKind.LOCAL_TO_REMOTE:
            if len(resources) > 1:
                logger.info("Copying {src} on local file-system to {dst} on resources:\n\t{resources}".format(
                    source_remote=source_remote,
                    src=src,
                    dst=dst,
                    resources='\n\t'.join(resources)
                ))
            else:
                logger.info("Copying {src} on local file-system to {dst} on resource {resource}".format(
                    source_remote=source_remote,
                    src=src,
                    dst=dst,
                    resource=resources[0]
                ))
            await self._copy_local_to_remote(src, dst, resources)
        elif kind == ConnectorCopyKind.REMOTE_TO_LOCAL:
            if len(resources) > 1:
                raise Exception("Copy from multiple resources is not supported")
            logger.info("Copying {src} on resource {resource} to {dst} on local file-system".format(
                source_remote=source_remote,
                src=src,
                dst=dst,
                resource=resources[0]
            ))
            await self._copy_remote_to_local(src, dst, resources[0])
        else:
            raise NotImplementedError
