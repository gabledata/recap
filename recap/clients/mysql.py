from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

from recap.clients.dbapi import Connection, DbapiClient
from recap.converters.mysql import MysqlConverter
from recap.types import StructType

MYSQL_CATALOG_NAME = "def"


class MysqlClient(DbapiClient):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection, MysqlConverter())

    @staticmethod
    @contextmanager
    def create(
        paths: list[str] | None = None,
        **url_args,
    ) -> Generator[MysqlClient, None, None]:
        import mysql.connector
        from mysql.connector.constants import DEFAULT_CONFIGURATION

        if paths:
            url_args["database"] = paths[0]

        # Only include kwargs that are valid for the MySQL connector
        kwargs = {k: v for k, v in url_args.items() if k in DEFAULT_CONFIGURATION}

        with mysql.connector.connect(**kwargs) as client:
            yield MysqlClient(client)  # type: ignore

    @staticmethod
    def parse(method: str, **url_args) -> tuple[str, list[Any]]:
        from urllib.parse import urlunparse

        paths = url_args.get("paths", [])[::-1]
        schema = paths.pop() if paths else None
        table = paths.pop() if paths else None
        connection_url = urlunparse(
            [
                url_args.get("scheme"),
                url_args.get("netloc"),
                schema or "",
                url_args.get("params"),
                url_args.get("query"),
                url_args.get("fragment"),
            ]
        )

        match method:
            case "ls":
                return (connection_url, [schema])
            case "schema" if schema and table:
                return (connection_url, [schema, table])
            case _:
                raise ValueError("Invalid method")

    def ls(self, schema: str | None = None) -> list[str]:
        match schema:
            case None:
                return self.ls_schemas(MYSQL_CATALOG_NAME)
            case str(schema):
                return self.ls_tables(MYSQL_CATALOG_NAME, schema)
            case _:
                raise ValueError("Invalid arguments")

    def schema(self, schema: str, table: str) -> StructType:
        return super().schema(MYSQL_CATALOG_NAME, schema, table)
