from __future__ import annotations

import asyncio
import base64
import os
import posixpath
import shutil
import stat
import tempfile
from abc import abstractmethod, ABC
from asyncio.subprocess import STDOUT
from typing import TYPE_CHECKING, MutableSequence

from streamflow.core import utils
from streamflow.core.deployment import Connector, ConnectorCopyKind
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import Any, Optional, MutableMapping, Union
    from typing_extensions import Text


class BaseConnector(Connector, ABC):

    @staticmethod
    def create_encoded_command(command: MutableSequence[Text],
                               resource: Text,
                               environment: MutableMapping[Text, Text] = None,
                               workdir: Optional[Text] = None,
                               stdin: Optional[Union[int, Text]] = None,
                               stdout: Union[int, Text] = STDOUT,
                               stderr: Union[int, Text] = STDOUT) -> Text:
        decoded_command = "".join(
            "{workdir}"
            "{environment}"
            "{command}"
            "{stdin}"
            "{stdout}"
            "{stderr}"
        ).format(
            workdir="cd {workdir} && ".format(workdir=workdir) if workdir is not None else "",
            environment="".join(["export %s=%s && " % (key, value) for (key, value) in
                                 environment.items()]) if environment is not None else "",
            command=" ".join(command),
            stdin=" < {stdin}".format(stdin=stdin) if stdin is not None else "",
            stdout=" > {stdout}".format(stdout=stdout) if stdout != STDOUT else "",
            stderr=" 2>&1" if stderr == stdout else (" 2>{stderr}".format(stderr=stderr) if stderr != STDOUT else "")
        )
        logger.debug("Executing command {command} on {resource}".format(command=decoded_command, resource=resource))
        return "echo {command} | base64 -d | sh".format(
            command=base64.b64encode(decoded_command.encode('utf-8')).decode('utf-8'),
        )

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
        elif isinstance(value, MutableSequence):
            return "".join(["-{name} \"{value}\" ".format(name=name, value=item) for item in value])
        elif value is None:
            return ""
        else:
            raise TypeError("Unsupported value type")

    async def _copy_remote_to_remote(self,
                                     src: Text,
                                     dst: Text,
                                     resources: MutableSequence[Text],
                                     source_remote: Text) -> None:
        # Check for the need of a temporary copy
        temp_dir = None
        for resource in resources:
            if source_remote != resource:
                temp_dir = tempfile.mkdtemp()
                await self._copy_remote_to_local(src, temp_dir, source_remote)
                break
        # Perform the actual copies
        copy_tasks = []
        for resource in resources:
            copy_tasks.append(asyncio.create_task(
                self._copy_remote_to_remote_single(src, dst, resource, source_remote, temp_dir)))
        await asyncio.gather(*copy_tasks)
        # If a temporary location was created, delete it
        if temp_dir is not None:
            shutil.rmtree(temp_dir)

    async def _copy_remote_to_remote_single(self,
                                            src: Text,
                                            dst: Text,
                                            resource: Text,
                                            source_remote: Text,
                                            temp_dir: Optional[Text]) -> None:
        if source_remote == resource:
            if src != dst:
                command = ['/bin/cp', "-rf", src, dst]
                await self.run(resource, command)
        else:
            copy_tasks = []
            for element in os.listdir(temp_dir):
                copy_tasks.append(asyncio.create_task(
                    self._copy_local_to_remote(os.path.join(temp_dir, element), dst, [resource])))
            await asyncio.gather(*copy_tasks)

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
                                    resources: MutableSequence[Text]) -> None:
        ...


    async def copy(self,
                   src: Text,
                   dst: Text,
                   resources: MutableSequence[Text],
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

