import os
from typing import Any, MutableMapping, Tuple, MutableSequence

import apsw
import pandas as pd

from streamflow.core.persistence import Database


class SqliteDatabase(Database):

    def __init__(self,
                 connection: str,
                 reset_db: bool = True):
        # If needed, reset the database
        if reset_db and os.path.isfile(connection):
            os.remove(connection)
        # Open connection to database
        os.makedirs(os.path.dirname(connection), exist_ok=True)
        self.connection = apsw.Connection(connection)
        self.connection.cursor().execute("PRAGMA journal_mode = WAL")
        self.connection.wal_autocheckpoint(10)
        # If is a new database, initialise it
        if reset_db:
            self._init_db()

    def __del__(self):
        # Force connection close
        if hasattr(self, 'connection') and self.connection:
            self.connection.close(True)

    def _init_db(self):
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schemas', 'sqlite.sql')
        with open(schema_path, "r") as f:
            with self.connection as db:
                db.cursor().execute(f.read())

    def add_step(self, name: str, status: int) -> int:
        with self.connection as db:
            db.cursor().execute("INSERT INTO step(name, status) VALUES(:name, :status)", {
                "name": name,
                "status": status})
            return db.last_insert_rowid()

    def update_step(self, step_id: int, updates: MutableMapping[str, Any]) -> int:
        with self.connection as db:
            db.cursor().execute("UPDATE step SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": step_id}})
            return step_id

    def get_steps(self) -> pd.DataFrame:
        return pd.read_sql_query("SELECT * FROM step", self.connection)

    def add_command(self, step_id: int, cmd: str) -> int:
        with self.connection as db:
            db.cursor().execute("INSERT INTO command(step, cmd) VALUES(:step, :cmd)", {
                "step": step_id,
                "cmd": cmd})
            return db.last_insert_rowid()

    def update_command(self, command_id: int, updates: MutableMapping[str, Any]) -> int:
        with self.connection as db:
            db.cursor().execute("UPDATE command SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": command_id}})
            return command_id

    def get_report(self) -> pd.DataFrame:
        return pd.read_sql_query("SELECT c.id, s.name, c.start_time, c.end_time "
                                 "FROM step AS s, command AS c "
                                 "WHERE s.id = c.step", self.connection)
