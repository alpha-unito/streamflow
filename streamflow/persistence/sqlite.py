from __future__ import annotations

import asyncio
import os
import sys
from abc import ABC
from typing import Any, MutableMapping, MutableSequence, Optional, Type

import aiosqlite
import pkg_resources
from cachetools import Cache, LRUCache

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Target
from streamflow.core.persistence import Database, DependencyType
from streamflow.core.workflow import Port, Status, Step, Token


class CachedDatabase(Database, ABC):

    def __init__(self, context: StreamFlowContext):
        super().__init__(context)
        self.deployment_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.port_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.step_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.target_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.token_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.workflow_cache: Cache = LRUCache(maxsize=sys.maxsize)


class SqliteConnection(object):

    def __init__(self,
                 connection: str,
                 timeout: int,
                 init_db: bool):
        self.connection: str = connection
        self.timeout: int = timeout
        self.init_db: bool = init_db
        self._connection: Optional[aiosqlite.Connection] = None
        self.__row_factory = None

    async def __aenter__(self):
        if not self._connection:
            self._connection = await aiosqlite.connect(
                database=self.connection,
                timeout=self.timeout)
            if self.init_db:
                schema_path = pkg_resources.resource_filename(
                    __name__, os.path.join('schemas', 'sqlite.sql'))
                with open(schema_path, "r") as f:
                    async with self._connection.cursor() as cursor:
                        await cursor.execute("PRAGMA journal_mode = WAL")
                        await cursor.execute("PRAGMA wal_autocheckpoint = 10")
                        await cursor.executescript(f.read())
        self.__row_factory = self._connection.row_factory
        return self._connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._connection.row_factory = self.__row_factory

    async def close(self):
        if self._connection:
            await self._connection.close()


class SqliteDatabase(CachedDatabase):

    def __init__(self,
                 context: StreamFlowContext,
                 connection: str,
                 timeout: int = 20,
                 reset_db: bool = False):
        super().__init__(context)
        # If needed, reset the database
        if reset_db and os.path.isfile(connection):
            os.remove(connection)
        # Open connection to database
        os.makedirs(os.path.dirname(connection), exist_ok=True)
        self.connection: SqliteConnection = SqliteConnection(
            connection=connection,
            timeout=timeout,
            init_db=reset_db or not os.path.exists(connection))

    async def close(self):
        async with self.connection as db:
            await db.commit()
        await self.connection.close()

    @classmethod
    def get_schema(cls):
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'sqlite.json'))

    async def add_command(self,
                          step_id: int,
                          cmd: str) -> int:
        async with self.connection as db:
            async with db.execute(
                    "INSERT INTO command(step, cmd) "
                    "VALUES(:step, :cmd)", {
                        "step": step_id,
                        "cmd": cmd}) as cursor:
                return cursor.lastrowid

    async def add_dependency(self,
                             step: int,
                             port: int,
                             type: DependencyType,
                             name: str) -> None:
        async with self.connection as db:
            await db.execute(
                "INSERT OR IGNORE INTO dependency(step, port, type, name) "
                "VALUES(:step, :port, :type, :name)", {
                    "step": step,
                    "port": port,
                    "type": type.value,
                    "name": name})

    async def add_deployment(self,
                             name: str,
                             type: str,
                             config: str,
                             external: bool,
                             lazy: bool,
                             workdir: Optional[str]) -> int:
        async with self.connection as db:
            async with db.execute(
                    "INSERT INTO deployment(name, type, config, external, lazy, workdir) "
                    "VALUES (:name, :type, :config, :external, :lazy, :workdir)", {
                        "name": name,
                        "type": type,
                        "config": config,
                        "external": external,
                        "lazy": lazy,
                        "workdir": workdir}) as cursor:
                return cursor.lastrowid

    async def add_port(self,
                       name: str,
                       workflow_id: int,
                       type: Type[Port],
                       params: str) -> int:
        async with self.connection as db:
            async with db.execute(
                    "INSERT INTO port(name, workflow, type, params) "
                    "VALUES(:name, :workflow, :type, :params)", {
                        "name": name,
                        "workflow": workflow_id,
                        "type": utils.get_class_fullname(type),
                        "params": params}) as cursor:
                return cursor.lastrowid

    async def add_provenance(self,
                             inputs: MutableSequence[int],
                             token: int):
        provenance = [{'dependee': i, 'depender': token} for i in inputs]
        async with self.connection as db:
            await asyncio.gather(*(asyncio.create_task(db.execute(
                "INSERT OR IGNORE INTO provenance(dependee, depender) "
                "VALUES(:dependee, :depender)", prov)) for prov in provenance))

    async def add_step(self,
                       name: str,
                       workflow_id: int,
                       status: int,
                       type: Type[Step],
                       params: str) -> int:
        async with self.connection as db:
            async with db.execute(
                    "INSERT INTO step(name, workflow, status, type, params) "
                    "VALUES(:name, :workflow, :status, :type, :params)", {
                        "name": name,
                        "workflow": workflow_id,
                        "status": status,
                        "type": utils.get_class_fullname(type),
                        "params": params}) as cursor:
                return cursor.lastrowid

    async def add_target(self,
                         deployment: int,
                         type: Type[Target],
                         params: str,
                         locations: int = 1,
                         service: Optional[str] = None,
                         workdir: Optional[str] = None) -> int:
        async with self.connection as db:
            async with db.execute(
                    "INSERT INTO target(params, type, deployment, locations, service, workdir) "
                    "VALUES (:params, :type, :deployment, :locations, :service, :workdir)", {
                        "params": params,
                        "type": utils.get_class_fullname(type),
                        "deployment": deployment,
                        "locations": locations,
                        "service": service,
                        "workdir": workdir}) as cursor:
                return cursor.lastrowid

    async def add_token(self,
                        tag: str,
                        type: Type[Token],
                        value: Any,
                        port: Optional[int] = None):
        async with self.connection as db:
            async with db.execute(
                    "INSERT INTO token(port, type, tag, value) "
                    "VALUES(:port, :type, :tag, :value)", {
                        "port": port,
                        "type": utils.get_class_fullname(type),
                        "tag": tag,
                        "value": value}) as cursor:
                return cursor.lastrowid

    async def add_workflow(self,
                           name: str,
                           params: str,
                           status: int,
                           type: str) -> int:
        async with self.connection as db:
            async with db.execute(
                    "INSERT INTO workflow(name, params, status, type) "
                    "VALUES(:name, :params, :status, :type)", {
                        "name": name,
                        "params": params,
                        "status": status,
                        "type": type}) as cursor:
                return cursor.lastrowid

    @cachedmethod(lambda self: self.deployment_cache)
    async def get_deployment(self, deplyoment_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM deployment WHERE id = :id", {"id": deplyoment_id}) as cursor:
                return await cursor.fetchone()

    async def get_input_ports(self, step_id: int) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM dependency WHERE step = :step AND type = :type", {
                "step": step_id,
                "type": DependencyType.INPUT.value}) as cursor:
                return await cursor.fetchall()

    async def get_output_ports(self, step_id: int) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM dependency WHERE step = :step AND type = :type", {
                "step": step_id,
                "type": DependencyType.OUTPUT.value}) as cursor:
                return await cursor.fetchall()

    @cachedmethod(lambda self: self.port_cache)
    async def get_port(self, port_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM port WHERE id = :id", {"id": port_id}) as cursor:
                return await cursor.fetchone()

    async def get_port_tokens(self, port_id: int) -> MutableSequence[int]:
        async with self.connection as db:
            async with db.execute("SELECT id FROM token WHERE port = :port", {"port": port_id}) as cursor:
                return [row[0] for row in await cursor.fetchall()]

    async def get_report(self, workflow: str) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT c.id, s.name, c.start_time, c.end_time "
                                  "FROM step AS s, command AS c "
                                  "WHERE s.id = c.step "
                                  "AND s.workflow = (SELECT id FROM workflow WHERE name = :workflow)",
                                  {"workflow": workflow}) as cursor:
                return [dict(r) for r in await cursor.fetchall()]

    @cachedmethod(lambda self: self.step_cache)
    async def get_step(self, step_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM step WHERE id = :id", {"id": step_id}) as cursor:
                return await cursor.fetchone()

    @cachedmethod(lambda self: self.target_cache)
    async def get_target(self, target_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM target WHERE id = :id", {"id": target_id}) as cursor:
                return await cursor.fetchone()

    @cachedmethod(lambda self: self.token_cache)
    async def get_token(self, token_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM token WHERE id = :id", {"id": token_id}) as cursor:
                return await cursor.fetchone()

    async def get_workflow(self, workflow_id: int) -> MutableMapping[str, Any]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM workflow WHERE id = :id", {"id": workflow_id}) as cursor:
                return await cursor.fetchone()

    async def get_workflow_ports(self, workflow_id: int) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM port WHERE workflow = :workflow", {"workflow": workflow_id}) as cursor:
                return await cursor.fetchall()

    async def get_workflow_steps(self, workflow_id: int) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM step WHERE workflow = :workflow", {"workflow": workflow_id}) as cursor:
                return await cursor.fetchall()

    async def list_workflows(self) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.connection as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT name, status, type FROM workflow ORDER BY id DESC") as cursor:
                return [{'name': row[0], 'status': Status(row[1]).name, 'type': row[2]}
                        async for row in cursor]

    async def update_command(self,
                             command_id: int,
                             updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute("UPDATE command SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": command_id}})
            return command_id

    async def update_deployment(self,
                                deployment_id: int,
                                updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute("UPDATE deployment SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": deployment_id}})
            self.deployment_cache.pop(deployment_id, None)
            return deployment_id

    async def update_port(self,
                          port_id: int,
                          updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute("UPDATE port SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": port_id}})
            self.port_cache.pop(port_id, None)
            return port_id

    async def update_step(self,
                          step_id: int,
                          updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute("UPDATE step SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": step_id}})
            self.step_cache.pop(step_id, None)
            return step_id

    async def update_target(self,
                            target_id: int,
                            updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute("UPDATE target SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": target_id}})
            self.target_cache.pop(target_id, None)
            return target_id

    async def update_workflow(self,
                              workflow_id: int,
                              updates: MutableMapping[str, Any]) -> int:
        async with self.connection as db:
            await db.execute("UPDATE workflow SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])),
                {**updates, **{"id": workflow_id}})
            self.workflow_cache.pop(workflow_id, None)
            return workflow_id
