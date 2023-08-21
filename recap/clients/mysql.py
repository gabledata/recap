from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from recap.clients.dbapi import Connection, DbapiClient
from recap.converters.mysql import MysqlConverter

MYSQL_CATALOG_NAME = "def"


class MysqlClient(DbapiClient):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection, MysqlConverter())

    @staticmethod
    @contextmanager
    def create(
        paths: list[str] | None = None, **kwargs
    ) -> Generator[MysqlClient, None, None]:
        import mysql.connector
        from mysql.connector.constants import DEFAULT_CONFIGURATION

        if paths:
            kwargs["database"] = paths[0]

        # Only include kwargs that are valid for the MySQL connector
        kwargs = {k: v for k, v in kwargs.items() if k in DEFAULT_CONFIGURATION}

        with mysql.connector.connect(**kwargs) as client:
            yield MysqlClient(client)  # type: ignore

    def ls(self, schema: str | None = None) -> list[str]:
        match schema:
            case None:
                return self.ls_schemas(MYSQL_CATALOG_NAME)
            case str(schema):
                return self.ls_tables(MYSQL_CATALOG_NAME, schema)
            case _:
                raise ValueError("Invalid arguments")
