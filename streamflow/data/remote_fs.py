from typing import List, Optional

from streamflow.connector.connector import Connector


def exists(connector: Connector, target: str, path: str) -> bool:
    return connector.run(
        resource=target,
        command=["if [ -e \"{path}\" ]; then echo \"{path}\"; fi".format(path=path)],
        capture_output=True
    ).strip()


def glob(connector: Connector, target: str, pattern: str) -> Optional[List[str]]:
    return connector.run(
        resource=target,
        command=["printf", "\"%s\\n\"", pattern, "|", "xargs", "-d", "'\\n'", "-n1", "-I{}",
                 "sh", "-c", "\"if [ -e \\\"{}\\\" ]; then echo \\\"{}\\\"; fi\""],
        capture_output=True
    ).split()


def isdir(connector: Connector, target: str, path: str):
    return connector.run(
        resource=target,
        command=["if [ -d \"{path}\" ]; then echo \"{path}\"; fi".format(path=path)],
        capture_output=True
    ).strip()


def isfile(connector: Connector, target: str, path: str):
    return connector.run(
        resource=target,
        command=["if [ -f \"{path}\" ]; then echo \"{path}\"; fi".format(path=path)],
        capture_output=True
    ).strip()


def mkdir(connector: Connector, target: str, path: str) -> None:
    connector.run(resource=target, command=["mkdir", "-p", path])


def symlink(connector: Connector, target: str, src: str, path: str) -> None:
    connector.run(resource=target, command=["ln", "-s", src, path])
