from __future__ import annotations

import asyncio
import json
import os
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files
from typing import Any, cast

import aiosqlite

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Target
from streamflow.core.persistence import DependencyType
from streamflow.core.utils import get_date_from_ns
from streamflow.core.workflow import Port, Status, Step, Token, Workflow
from streamflow.persistence.base import CachedDatabase
from streamflow.version import VERSION

DEFAULT_SQLITE_CONNECTION = os.path.join(
    os.path.expanduser("~"), ".streamflow", VERSION, "sqlite.db"
)


def _load_keys(
    row: MutableMapping[str, Any], keys: MutableSequence[str] | None = None
) -> MutableMapping[str, Any]:
    for key in keys or ["params"]:
        row[key] = json.loads(row[key])
    return row


class SqliteConnection:
    def __init__(self, connection: str, timeout: int, init_db: bool):
        self.connection: str = connection
        self.timeout: int = timeout
        self.init_db: bool = init_db
        self._connection: aiosqlite.Connection | None = None

    async def __aenter__(self) -> aiosqlite.Connection:
        if not self._connection:
            self._connection = await aiosqlite.connect(
                database=self.connection, timeout=self.timeout
            )
            if self.init_db:
                async with self._connection.cursor() as cursor:
                    await cursor.execute("PRAGMA journal_mode = WAL")
                    await cursor.execute("PRAGMA wal_autocheckpoint = 10")
                    await cursor.executescript(
                        files(__package__)
                        .joinpath("schemas")
                        .joinpath("sqlite.sql")
                        .read_text("utf-8")
                    )
            self._connection.row_factory = aiosqlite.Row
        return self._connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    async def close(self) -> None:
        if self._connection:
            async with self as db:
                await db.commit()
            await self._connection.close()
            self._connection = None


class SqliteDatabase(CachedDatabase):
    def __init__(
        self,
        context: StreamFlowContext,
        connection: str = DEFAULT_SQLITE_CONNECTION,
        timeout: int = 20,
    ):
        super().__init__(context)
        # Open connection to database
        if connection != ":memory:":
            os.makedirs(os.path.dirname(connection), exist_ok=True)
        self.connection: SqliteConnection = SqliteConnection(
            connection=connection,
            timeout=timeout,
            init_db=not os.path.exists(connection),
        )

    async def close(self) -> None:
        await self.connection.close()
        self.connection = None

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("sqlite.json")
            .read_text("utf-8")
        )

    async def add_dependency(
        self, step: int, port: int, type: DependencyType, name: str
    ) -> None:
        async with self.connection as db:
            await db.execute(
                "INSERT OR IGNORE INTO dependency(step, port, type, name) "
                "VALUES(:step, :port, :type, :name)",
                {"step": step, "port": port, "type": type.value, "name": name},
            )

    async def add_deployment(
        self,
        name: str,
        type: str,
        config: MutableMapping[str, Any],
        external: bool,
        lazy: bool,
        scheduling_policy: MutableMapping[str, Any],
        workdir: str | None,
        wraps: MutableMapping[str, Any] | None,
    ) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO deployment(name, type, config, external, lazy, scheduling_policy, workdir, wraps) "
                "VALUES (:name, :type, :config, :external, :lazy, :scheduling_policy, :workdir, :wraps)",
                {
                    "name": name,
                    "type": type,
                    "config": json.dumps(config),
                    "external": external,
                    "lazy": lazy,
                    "scheduling_policy": json.dumps(scheduling_policy),
                    "workdir": workdir,
                    "wraps": json.dumps(wraps) if wraps else None,
                },
            ) as cursor:
                return cast(int, cursor.lastrowid)

    async def add_execution(self, step_id: int, job_token_id: int, cmd: str) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO execution(step, job_token, cmd) "
                "VALUES(:step, :job_token, :cmd)",
                {"step": step_id, "job_token": job_token_id, "cmd": cmd},
            ) as cursor:
                return cast(int, cursor.lastrowid)

    async def add_filter(
        self,
        name: str,
        type: str,
        config: MutableMapping[str, Any],
    ) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO filter(name, type, config) "
                "VALUES (:name, :type, :config)",
                {
                    "name": name,
                    "type": type,
                    "config": json.dumps(config),
                },
            ) as cursor:
                return cast(int, cursor.lastrowid)

    async def add_port(
        self,
        name: str,
        workflow_id: int,
        type: type[Port],
        params: MutableMapping[str, Any],
    ) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO port(name, workflow, type, params) "
                "VALUES(:name, :workflow, :type, :params)",
                {
                    "name": name,
                    "workflow": workflow_id,
                    "type": utils.get_class_fullname(type),
                    "params": json.dumps(params),
                },
            ) as cursor:
                return cast(int, cursor.lastrowid)

    async def add_provenance(self, inputs: MutableSequence[int], token: int) -> None:
        provenance = [{"dependee": i, "depender": token} for i in inputs]
        async with self.connection as db:
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        db.execute(
                            "INSERT OR IGNORE INTO provenance(dependee, depender) "
                            "VALUES(:dependee, :depender)",
                            prov,
                        )
                    )
                    for prov in provenance
                )
            )

    async def add_step(
        self,
        name: str,
        workflow_id: int,
        status: int,
        type: type[Step],
        params: MutableMapping[str, Any],
    ) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO step(name, workflow, status, type, params) "
                "VALUES(:name, :workflow, :status, :type, :params)",
                {
                    "name": name,
                    "workflow": workflow_id,
                    "status": status,
                    "type": utils.get_class_fullname(type),
                    "params": json.dumps(params),
                },
            ) as cursor:
                return cast(int, cursor.lastrowid)

    async def add_target(
        self,
        deployment: int,
        type: type[Target],
        params: MutableMapping[str, Any],
        locations: int = 1,
        service: str | None = None,
        workdir: str | None = None,
    ) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO target(params, type, deployment, locations, service, workdir) "
                "VALUES (:params, :type, :deployment, :locations, :service, :workdir)",
                {
                    "params": json.dumps(params),
                    "type": utils.get_class_fullname(type),
                    "deployment": deployment,
                    "locations": locations,
                    "service": service,
                    "workdir": workdir,
                },
            ) as cursor:
                return cursor.lastrowid

    async def add_token(
        self,
        tag: str,
        type: type[Token],
        value: Any,
        port: int | None = None,
        recoverable: bool = False,
    ) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO token(port, type, tag, value) "
                "VALUES(:port, :type, :tag, :value)",
                {
                    "port": port,
                    "type": utils.get_class_fullname(type),
                    "tag": tag,
                    "value": json.dumps(value),
                },
            ) as cursor:
                token_id = cursor.lastrowid
            if recoverable:
                await db.execute(
                    "INSERT INTO recoverable(id) VALUES(:id)",
                    {
                        "id": token_id,
                    },
                )
            return token_id

    async def add_workflow(
        self,
        name: str,
        params: MutableMapping[str, Any],
        status: int,
        type: type[Workflow],
    ) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO workflow(name, params, status, type) "
                "VALUES(:name, :params, :status, :type)",
                {
                    "name": name,
                    "params": json.dumps(params),
                    "status": status,
                    "type": utils.get_class_fullname(type),
                },
            ) as cursor:
                return cursor.lastrowid

    async def get_dependees(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM provenance WHERE depender = :depender",
                {"depender": token_id},
            ) as cursor:
                return await cursor.fetchall()

    async def get_dependers(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM provenance WHERE dependee = :dependee",
                {"dependee": token_id},
            ) as cursor:
                return await cursor.fetchall()

    @cachedmethod(lambda self: self.deployment_cache)
    async def get_deployment(self, deployment_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM deployment WHERE id = :id", {"id": deployment_id}
            ) as cursor:
                row = _load_keys(
                    dict(await cursor.fetchone()), ["config", "scheduling_policy"]
                )
                row["wraps"] = json.loads(row["wraps"]) if row["wraps"] else None
                return row

    async def get_execution(self, execution_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM execution WHERE id = :id", {"id": execution_id}
            ) as cursor:
                return await cursor.fetchone()

    async def get_executions_by_step(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM execution WHERE step = :id", {"id": step_id}
            ) as cursor:
                return await cursor.fetchall()

    @cachedmethod(lambda self: self.filter_cache)
    async def get_filter(self, filter_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM filter WHERE id = :id", {"id": filter_id}
            ) as cursor:
                return _load_keys(dict(await cursor.fetchone()), ["config"])

    async def get_input_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM dependency WHERE step = :step AND type = :type",
                {"step": step_id, "type": DependencyType.INPUT.value},
            ) as cursor:
                return await cursor.fetchall()

    async def get_input_steps(
        self, port_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM dependency WHERE port = :port AND type = :type",
                {"port": port_id, "type": DependencyType.OUTPUT.value},
            ) as cursor:
                return await cursor.fetchall()

    async def get_output_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM dependency WHERE step = :step AND type = :type",
                {"step": step_id, "type": DependencyType.OUTPUT.value},
            ) as cursor:
                return await cursor.fetchall()

    async def get_output_steps(
        self, port_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM dependency WHERE port = :port AND type = :type",
                {"port": port_id, "type": DependencyType.INPUT.value},
            ) as cursor:
                return await cursor.fetchall()

    @cachedmethod(lambda self: self.port_cache)
    async def get_port(self, port_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM port WHERE id = :id", {"id": port_id}
            ) as cursor:
                return _load_keys(dict(await cursor.fetchone()))

    async def get_port_from_token(self, token_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT port.* "
                "FROM token JOIN port ON token.port = port.id "
                "WHERE token.id = :token_id",
                {"token_id": token_id},
            ) as cursor:
                return _load_keys(dict(await cursor.fetchone()))

    async def get_port_tokens(self, port_id: int) -> MutableSequence[int]:
        async with self.connection as db:
            async with db.execute(
                "SELECT id FROM token WHERE port = :port", {"port": port_id}
            ) as cursor:
                return [row["id"] for row in await cursor.fetchall()]

    async def get_reports(
        self, workflow: str, last_only: bool = False
    ) -> MutableSequence[MutableSequence[MutableMapping[str, Any]]]:
        async with self.connection as db:
            if last_only:
                async with db.execute(
                    "SELECT c.id, s.name, c.start_time, c.end_time "
                    "FROM step AS s, execution AS c "
                    "WHERE s.id = c.step "
                    "AND s.workflow = (SELECT id FROM workflow WHERE name = :workflow ORDER BY id DESC LIMIT 1)",
                    {"workflow": workflow},
                ) as cursor:
                    return [[dict(r) for r in await cursor.fetchall()]]
            else:
                async with db.execute(
                    "SELECT s.workflow, c.id, s.name, c.start_time, c.end_time "
                    "FROM step AS s, execution AS c "
                    "WHERE s.id = c.step "
                    "AND s.workflow IN (SELECT id FROM workflow WHERE name = :workflow) "
                    "ORDER BY s.workflow DESC",
                    {"workflow": workflow},
                ) as cursor:
                    result = {}
                    async for row in cursor:
                        result.setdefault(row["workflow"], []).append(
                            {k: row[k] for k in row.keys() if k != "workflow"}
                        )
                    return list(result.values())

    @cachedmethod(lambda self: self.step_cache)
    async def get_step(self, step_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM step WHERE id = :id", {"id": step_id}
            ) as cursor:
                return _load_keys(dict(await cursor.fetchone()))

    @cachedmethod(lambda self: self.target_cache)
    async def get_target(self, target_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM target WHERE id = :id", {"id": target_id}
            ) as cursor:
                return _load_keys(dict(await cursor.fetchone()))

    @cachedmethod(lambda self: self.token_cache)
    async def get_token(self, token_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT *, "
                "EXISTS(SELECT 1 FROM recoverable AS r WHERE r.id =:id) AS recoverable "
                "FROM token "
                "WHERE id =:id",
                {"id": token_id},
            ) as cursor:
                row = _load_keys(dict(await cursor.fetchone()), keys=["value"])
                row["recoverable"] = bool(row["recoverable"])
                return row

    async def get_workflow(self, workflow_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM workflow WHERE id = :id", {"id": workflow_id}
            ) as cursor:
                return _load_keys(dict(await cursor.fetchone()))

    async def get_workflow_ports(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM port WHERE workflow = :workflow",
                {"workflow": workflow_id},
            ) as cursor:
                return [_load_keys(dict(r)) for r in await cursor.fetchall()]

    async def get_workflow_steps(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM step WHERE workflow = :workflow",
                {"workflow": workflow_id},
            ) as cursor:
                return [_load_keys(dict(r)) for r in await cursor.fetchall()]

    async def get_workflows_by_name(
        self, workflow_name: str, last_only: bool = False
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM workflow WHERE name = :name ORDER BY id desc",
                {"name": workflow_name},
            ) as cursor:
                return (
                    [_load_keys(dict(await cursor.fetchone()))]
                    if last_only
                    else [_load_keys(dict(r)) for r in await cursor.fetchall()]
                )

    async def get_workflows_list(
        self, name: str | None
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            if name is not None:
                return [
                    {
                        "end_time": get_date_from_ns(row["end_time"]),
                        "start_time": get_date_from_ns(row["start_time"]),
                        "status": Status(row["status"]).name,
                        "type": row["type"],
                    }
                    for row in await self.get_workflows_by_name(name, last_only=False)
                ]
            else:
                async with db.execute(
                    "SELECT name, type, COUNT(*) AS num FROM workflow GROUP BY name, type ORDER BY name DESC"
                ) as cursor:
                    return await cursor.fetchall()

    async def update_deployment(
        self, deployment_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE deployment SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                cast(dict[str, Any], updates) | {"id": deployment_id},
            )
            self.deployment_cache.pop(deployment_id, None)
            return deployment_id

    async def update_execution(
        self, execution_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE execution SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                cast(dict[str, Any], updates) | {"id": execution_id},
            )
            return execution_id

    async def update_filter(
        self, filter_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE filter SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                cast(dict[str, Any], updates) | {"id": filter_id},
            )
            self.filter_cache.pop(filter_id, None)
            return filter_id

    async def update_port(self, port_id: int, updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE port SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                cast(dict[str, Any], updates) | {"id": port_id},
            )
            self.port_cache.pop(port_id, None)
            return port_id

    async def update_step(self, step_id: int, updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE step SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                cast(dict[str, Any], updates) | {"id": step_id},
            )
            self.step_cache.pop(step_id, None)
            return step_id

    async def update_target(
        self, target_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE target SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                cast(dict[str, Any], updates) | {"id": target_id},
            )
            self.target_cache.pop(target_id, None)
            return target_id

    async def update_workflow(
        self, workflow_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE workflow SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                cast(dict[str, Any], updates) | {"id": workflow_id},
            )
            self.workflow_cache.pop(workflow_id, None)
            return workflow_id
