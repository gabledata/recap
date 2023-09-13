from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from recap.clients.dbapi import Connection, DbapiClient
from recap.converters.snowflake import SnowflakeConverter


class SnowflakeClient(DbapiClient):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection, SnowflakeConverter())

    @staticmethod
    @contextmanager
    def create(**url_args) -> Generator[SnowflakeClient, None, None]:
        import snowflake.connector

        snowflake_args = {}
        snowflake_args["account"] = url_args.pop("host")

        match url_args.get("paths"):
            case str(database), None:
                snowflake_args["database"] = database
            case str(database), str(schema):
                snowflake_args["database"] = database
                snowflake_args["schema"] = schema

        with snowflake.connector.connect(**(snowflake_args | url_args)) as client:
            yield SnowflakeClient(client)  # type: ignore

    def ls_catalogs(self) -> list[str]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
                SELECT database_name
                FROM information_schema.databases
                ORDER BY database_name ASC
            """
        )
        return [row[0] for row in cursor.fetchall()]
