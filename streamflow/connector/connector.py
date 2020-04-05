import os
import pickle
import sys
import tempfile
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import MutableMapping, List, Any, Optional

from streamflow.log_handler import _logger


class ConnectorCopyKind(Enum):
    localToRemote = 1
    remoteToLocal = 2
    remoteToRemote = 3


def run_script(args):
    with open(args[0], 'rb') as connector_file:
        connector = pickle.load(connector_file)
    os.remove(args[0])
    with open(args[1], 'rb') as args_file:
        arguments = pickle.load(args_file)
    os.remove(args[1])
    output = connector.run(arguments['resource'], args[2:], arguments['environment'], arguments['workdir'], True)
    print(output.strip())


class Connector(object, metaclass=ABCMeta):

    @staticmethod
    def get_option(name: str,
                   value: Any,
                   ) -> str:
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

    @staticmethod
    def _run_current_file(impl, file, args) -> str:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            pickle.dump(impl, open(tmp_file.name, 'wb'), fix_imports=False)
            self_file = tmp_file.name
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            pickle.dump(args, open(tmp_file.name, 'wb'), fix_imports=False)
            args_file = tmp_file.name
        return (
            "{executable} "
            "{file} "
            "{pickle_dump} "
            "{arguments_dump} "
        ).format(
            executable=sys.executable,
            file=file,
            pickle_dump=self_file,
            arguments_dump=args_file
        )

    @abstractmethod
    def _copy_remote_to_remote(self,
                               src: str,
                               dst: str,
                               resource: str,
                               source_remote: str
                               ) -> None:
        ...

    @abstractmethod
    def _copy_remote_to_local(self,
                              src: str,
                              dst: str,
                              resource: str
                              ) -> None:
        ...

    @abstractmethod
    def _copy_local_to_remote(self,
                              src: str,
                              dst: str,
                              resource: str
                              ) -> None:
        ...

    def copy(self, src: str, dst: str, resource: str, kind: ConnectorCopyKind, source_remote: str = None) -> None:
        source_remote = source_remote or resource
        if kind == ConnectorCopyKind.remoteToRemote:
            if source_remote == resource:
                _logger.info("Copying {src} to {dst} on {resource}".format(src=src, dst=dst, resource=resource))
            else:
                _logger.info("Copying {source_remote}:{src} to {resource}:{dst}".format(
                    source_remote=source_remote,
                    src=src,
                    resource=resource,
                    dst=dst
                ))
            self._copy_remote_to_remote(src, dst, resource, source_remote or resource)
        elif kind == ConnectorCopyKind.localToRemote:
            _logger.info(
                "Copying {src} to {dst}".format(src=src, dst="{resource}:{file}".format(resource=resource, file=dst)))
            self._copy_local_to_remote(src, dst, resource)
        elif kind == ConnectorCopyKind.remoteToLocal:
            _logger.info(
                "Copying {src} to {dst}".format(src="{resource}:{file}".format(resource=resource, file=src), dst=dst))
            self._copy_remote_to_local(src, dst, resource)
        else:
            raise NotImplementedError

    @abstractmethod
    def deploy(self) -> None:
        ...

    @abstractmethod
    def get_available_resources(self, service: str) -> List[str]:
        ...

    @abstractmethod
    def get_runtime(self,
                    resource: str,
                    environment: MutableMapping[str, str] = None,
                    workdir: str = None
                    ) -> str:
        ...

    @abstractmethod
    def undeploy(self) -> None:
        ...

    @abstractmethod
    def run(self,
            resource: str,
            command: List[str],
            environment: MutableMapping[str, str] = None,
            workdir: str = None,
            capture_output: bool = False) -> Optional[Any]:
        ...
