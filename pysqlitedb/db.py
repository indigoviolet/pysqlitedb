from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timedelta
from functools import cached_property, singledispatchmethod
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Literal

import attr
import pytz

InsertFallback = Literal["REPLACE", "IGNORE", "ROLLBACK"]


@attr.s(auto_attribs=True)
class Column:
    name: str
    type: str


@attr.s(auto_attribs=True)
class Table:
    name: str
    columns: List[Column]

    def create(self, db: DB):
        db.execute(create_table_sql(self))


@attr.s(auto_attribs=True)
class DB:
    db_file: Path
    pragmas: List[str] = []
    tables: List[Table] = []

    @classmethod
    @contextmanager
    def get(cls, db_file: Path, **kwargs) -> Generator[DB, None, None]:
        d = DB(db_file=db_file, **kwargs).setup()
        with closing(d):
            yield d

    @cached_property
    def conn(self):
        con = sqlite3.connect(str(self.db_file), isolation_level=None)
        con.row_factory = sqlite3.Row
        for p in ["journal_mode=wal"] + self.pragmas:
            self.execute(f"pragma {p}")

    def execute(self, statement: str, values: Iterable[Any] = []):
        values = [self.value_for_db(v) for v in values]
        return self.conn.execute(statement, values)

    @singledispatchmethod
    def value_for_db(self, v: Any):
        return v

    @value_for_db.register
    def _(self, v: datetime):
        assert v.utcoffset() == timedelta(0), "Must be UTC"
        return v.astimezone(pytz.UTC).isoformat()

    def close(self):
        self.conn.close()

    def setup(self):
        for t in self.tables:
            t.create(self)

    def update_row(
        self, tablename: str, values: Dict[str, Any], where: Dict[str, Any]
    ) -> None:
        values = {
            "updated_at": now_for_db(),
            **values,
        }
        self.execute(
            update_sql(tablename, values=values, where=where),
            tuple(values.values()) + tuple(where.values()),
        )

    def insert_row(
        self,
        tablename: str,
        values: Dict[str, Any],
        fallback: InsertFallback = "ROLLBACK",
    ) -> None:
        # set default value for created_at, so that we can use a
        # uniform format for all timestamps, CURRENT_TIMESTAMP is not
        # timezone-aware -- assuming every table will want created_at
        values = {
            "created_at": now_for_db(),
            **values,
        }
        self.execute(
            insert_sql(tablename, values, fallback=fallback), tuple(values.values()),
        )


def insert_sql(tablename: str, values: Dict[str, Any], fallback: InsertFallback) -> str:
    column_string = ", ".join(values.keys())
    placeholders = ", ".join("?" for _ in values)
    return (
        f"INSERT OR {fallback} INTO {tablename}({column_string}) VALUES({placeholders})"
    )


def update_sql(tablename: str, values: Dict[str, str], where: Dict[str, Any]) -> str:
    set_clause = ", ".join(f"{k} = ?" for k in values.keys())
    where_clause = ", ".join(f"{k} = ?" for k in where.keys())
    return f"UPDATE {tablename} SET {set_clause} WHERE {where_clause}"


def create_table_sql(table: Table) -> str:
    column_defs = ",\n".join(f"{col.name} {col.type}" for col in table.columns)
    return f"""CREATE TABLE IF NOT EXISTS {table.name} (
        {column_defs}
    )"""


def now_for_db() -> datetime:
    return datetime.now().astimezone(pytz.UTC)
