from __future__ import annotations

from abc import ABC
from typing import Any, List, Protocol, Tuple

from recap.types import StructType


class DbapiClient(ABC):
    def __init__(self, connection: Connection, converter: DbapiConverter) -> None:
        self.connection = connection
        self.converter = converter

    def ls(self, catalog: str | None = None, schema: str | None = None) -> list[str]:
        match (catalog, schema):
            case (None, None):
                return self.ls_catalogs()
            case (str(catalog), None):
                return self.ls_schemas(catalog)
            case (str(catalog), str(schema)):
                return self.ls_tables(catalog, schema)
            case _:
                raise ValueError("Invalid arguments")

    def ls_catalogs(self) -> list[str]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
                SELECT catalog_name
                FROM information_schema.catalogs
                ORDER BY catalog_name ASC
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def ls_schemas(self, catalog: str) -> list[str]:
        cursor = self.connection.cursor()
        cursor.execute(
            f"""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE catalog_name = {self.param_style}
            """,
            (catalog,),
        )
        return [row[0] for row in cursor.fetchall()]

    def ls_tables(self, catalog: str, schema: str) -> list[str]:
        cursor = self.connection.cursor()
        cursor.execute(
            f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_catalog = {self.param_style}
                    AND table_schema = {self.param_style}
            """,
            (catalog, schema),
        )
        return [row[0] for row in cursor.fetchall()]

    def get_schema(self, table: str, schema: str, catalog: str) -> StructType:
        cursor = self.connection.cursor()
        cursor.execute(
            f"""
                SELECT *
                FROM information_schema.columns
                WHERE table_name = {self.param_style}
                    AND table_schema = {self.param_style}
                    AND table_catalog = {self.param_style}
                ORDER BY ordinal_position ASC
            """,
            (table, schema, catalog),
        )

        names = [name[0].upper() for name in cursor.description]
        return self.converter.to_recap(
            # Make each row be a dict with the column names as keys
            [dict(zip(names, row)) for row in cursor.fetchall()]
        )

    @property
    def param_style(cls) -> str:
        return "%s"


class Connection(Protocol):
    def close(self) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...

    def cursor(self) -> Cursor:
        ...


class Cursor(Protocol):
    def execute(self, query: str, parameters: Tuple = ()) -> None:
        ...

    def executemany(self, query: str, parameter_list: List[Tuple]) -> None:
        ...

    def fetchone(self) -> Tuple:
        ...

    def fetchall(self) -> List[Tuple]:
        ...

    def fetchmany(self, size: int) -> List[Tuple]:
        ...

    @property
    def description(self) -> List[Tuple]:
        ...


class DbapiConverter(Protocol):
    def to_recap(self, columns: list[dict[str, Any]]) -> StructType:
        ...
