from __future__ import annotations

import asyncio
import json
import os
from typing import Any, MutableMapping, MutableSequence

import aiosqlite
import pkg_resources

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Target
from streamflow.core.persistence import DependencyType
from streamflow.core.utils import get_date_from_ns
from streamflow.core.workflow import Port, Status, Step, Token
from streamflow.persistence.base import CachedDatabase
from streamflow.version import VERSION

DEFAULT_SQLITE_CONNECTION = os.path.join(
    os.path.expanduser("~"), ".streamflow", VERSION, "sqlite.db"
)


class SqliteConnection:
    def __init__(self, connection: str, timeout: int, init_db: bool):
        self.connection: str = connection
        self.timeout: int = timeout
        self.init_db: bool = init_db
        self._connection: aiosqlite.Connection | None = None

    async def __aenter__(self):
        if not self._connection:
            self._connection = await aiosqlite.connect(
                database=self.connection, timeout=self.timeout
            )
            if self.init_db:
                schema_path = pkg_resources.resource_filename(
                    __name__, os.path.join("schemas", "sqlite.sql")
                )
                with open(schema_path) as f:
                    async with self._connection.cursor() as cursor:
                        await cursor.execute("PRAGMA journal_mode = WAL")
                        await cursor.execute("PRAGMA wal_autocheckpoint = 10")
                        await cursor.executescript(f.read())
            self._connection.row_factory = aiosqlite.Row
        return self._connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def close(self):
        if self._connection:
            await self._connection.close()


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

    async def close(self):
        async with self.connection as db:
            await db.commit()
        await self.connection.close()

    @classmethod
    def get_schema(cls):
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "sqlite.json")
        )

    async def add_command(self, step_id: int, tag: str, cmd: str) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO command(step, tag, cmd) " "VALUES(:step, :tag, :cmd)",
                {"step": step_id, "tag": tag, "cmd": cmd},
            ) as cursor:
                return cursor.lastrowid

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
        config: str,
        external: bool,
        lazy: bool,
        workdir: str | None,
    ) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO deployment(name, type, config, external, lazy, workdir) "
                "VALUES (:name, :type, :config, :external, :lazy, :workdir)",
                {
                    "name": name,
                    "type": type,
                    "config": config,
                    "external": external,
                    "lazy": lazy,
                    "workdir": workdir,
                },
            ) as cursor:
                return cursor.lastrowid

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
                return cursor.lastrowid

    async def add_provenance(self, inputs: MutableSequence[int], token: int):
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
                return cursor.lastrowid

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
        self, tag: str, type: type[Token], value: Any, port: int | None = None
    ):
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO token(port, type, tag, value) "
                "VALUES(:port, :type, :tag, :value)",
                {
                    "port": port,
                    "type": utils.get_class_fullname(type),
                    "tag": tag,
                    "value": value,
                },
            ) as cursor:
                return cursor.lastrowid

    async def add_workflow(
        self, name: str, params: MutableMapping[str, Any], status: int, type: str
    ) -> int:
        async with self.connection as db:
            async with db.execute(
                "INSERT INTO workflow(name, params, status, type) "
                "VALUES(:name, :params, :status, :type)",
                {
                    "name": name,
                    "params": json.dumps(params),
                    "status": status,
                    "type": type,
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

    async def get_command(self, command_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM command WHERE id = :id", {"id": command_id}
            ) as cursor:
                return await cursor.fetchone()

    async def get_commands_by_step(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM command WHERE step = :id", {"id": step_id}
            ) as cursor:
                return await cursor.fetchall()

    @cachedmethod(lambda self: self.deployment_cache)
    async def get_deployment(self, deplyoment_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM deployment WHERE id = :id", {"id": deplyoment_id}
            ) as cursor:
                return await cursor.fetchone()

    async def get_input_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM dependency WHERE step = :step AND type = :type",
                {"step": step_id, "type": DependencyType.INPUT.value},
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

    @cachedmethod(lambda self: self.port_cache)
    async def get_port(self, port_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM port WHERE id = :id", {"id": port_id}
            ) as cursor:
                return await cursor.fetchone()

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
                    "FROM step AS s, command AS c "
                    "WHERE s.id = c.step "
                    "AND s.workflow = (SELECT id FROM workflow WHERE name = :workflow ORDER BY id DESC LIMIT 1)",
                    {"workflow": workflow},
                ) as cursor:
                    return [[dict(r) for r in await cursor.fetchall()]]
            else:
                async with db.execute(
                    "SELECT s.workflow, c.id, s.name, c.start_time, c.end_time "
                    "FROM step AS s, command AS c "
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
                return await cursor.fetchone()

    @cachedmethod(lambda self: self.target_cache)
    async def get_target(self, target_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM target WHERE id = :id", {"id": target_id}
            ) as cursor:
                return await cursor.fetchone()

    @cachedmethod(lambda self: self.token_cache)
    async def get_token(self, token_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM token WHERE id = :id", {"id": token_id}
            ) as cursor:
                return await cursor.fetchone()

    async def get_workflow(self, workflow_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM workflow WHERE id = :id", {"id": workflow_id}
            ) as cursor:
                return await cursor.fetchone()

    async def get_workflow_ports(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM port WHERE workflow = :workflow",
                {"workflow": workflow_id},
            ) as cursor:
                return await cursor.fetchall()

    async def get_workflow_steps(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM step WHERE workflow = :workflow",
                {"workflow": workflow_id},
            ) as cursor:
                return await cursor.fetchall()

    async def get_workflows_by_name(
        self, workflow_name: str, last_only: bool = False
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            async with db.execute(
                "SELECT * FROM workflow WHERE name = :name ORDER BY id desc",
                {"name": workflow_name},
            ) as cursor:
                return (
                    [await cursor.fetchone()] if last_only else await cursor.fetchall()
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

    async def update_command(
        self, command_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE command SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                {**updates, **{"id": command_id}},
            )
            return command_id

    async def update_deployment(
        self, deployment_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE deployment SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                {**updates, **{"id": deployment_id}},
            )
            self.deployment_cache.pop(deployment_id, None)
            return deployment_id

    async def update_port(self, port_id: int, updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE port SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                {**updates, **{"id": port_id}},
            )
            self.port_cache.pop(port_id, None)
            return port_id

    async def update_step(self, step_id: int, updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute(
                "UPDATE step SET {} WHERE id = :id".format(  # nosec
                    ", ".join([f"{k} = :{k}" for k in updates])
                ),
                {**updates, **{"id": step_id}},
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
                {**updates, **{"id": target_id}},
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
                {**updates, **{"id": workflow_id}},
            )
            self.workflow_cache.pop(workflow_id, None)
            return workflow_id
