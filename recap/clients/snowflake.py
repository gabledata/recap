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
    def create(**kwargs) -> Generator[SnowflakeClient, None, None]:
        import snowflake.connector

        with snowflake.connector.connect(**kwargs) as client:
            yield SnowflakeClient(client)  # type: ignore
