from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from recap.clients.dbapi import Connection, DbapiClient
from recap.converters.postgresql import PostgresqlConverter
from recap.types import StructType

PSYCOPG2_CONNECT_ARGS = {
    "host",
    "hostaddr",
    "port",
    "dbname",
    "user",
    "password",
    "passfile",
    "channel_binding",
    "connect_timeout",
    "client_encoding",
    "options",
    "application_name",
    "fallback_application_name",
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_count",
    "tcp_user_timeout",
    "replication",
    "gssencmode",
    "sslmode",
    "requiressl",
    "sslcompression",
    "sslcert",
    "sslkey",
    "sslpassword",
    "sslrootcert",
    "sslcrl",
    "sslcrldir",
    "sslsni",
    "requirepeer",
    "ssl_min_protocol_version",
    "ssl_max_protocol_version",
    "krbsrvname",
    "gsslib",
    "service",
    "target_session_attrs",
}


class PostgresqlClient(DbapiClient):
    def __init__(
        self,
        connection: Connection,
        converter: PostgresqlConverter = PostgresqlConverter(),
    ) -> None:
        super().__init__(connection, converter)

    @staticmethod
    @contextmanager
    def create(
        paths: list[str] | None = None,
        **url_args,
    ) -> Generator[PostgresqlClient, None, None]:
        import psycopg2

        if paths:
            url_args["dbname"] = paths[0]

        # Only include kwargs that are valid for PsycoPG2 parse_dsn()
        url_args = {k: v for k, v in url_args.items() if k in PSYCOPG2_CONNECT_ARGS}

        with psycopg2.connect(**url_args) as client:
            yield PostgresqlClient(client)

    def ls_catalogs(self) -> list[str]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT datname
            FROM pg_database
            ORDER BY datname ASC
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def schema(self, catalog: str, schema: str, table: str) -> StructType:
        cursor = self.connection.cursor()
        cursor.execute(
            f"""
                SELECT
                    information_schema.columns.*,
                    pg_attribute.attndims,
                    enums.enum_values
                FROM information_schema.columns

                -- Join to get the array dimensions
                JOIN pg_catalog.pg_namespace
                    ON pg_catalog.pg_namespace.nspname = information_schema.columns.table_schema
                JOIN pg_catalog.pg_class
                    ON pg_catalog.pg_class.relname = information_schema.columns.table_name
                    AND pg_catalog.pg_class.relnamespace = pg_catalog.pg_namespace.oid
                JOIN pg_catalog.pg_attribute
                    ON pg_catalog.pg_attribute.attrelid = pg_catalog.pg_class.oid
                    AND pg_catalog.pg_attribute.attname = information_schema.columns.column_name

                -- Join to get the enum values
                LEFT JOIN pg_catalog.pg_type
                    ON pg_catalog.pg_type.oid = pg_catalog.pg_attribute.atttypid
                    AND pg_catalog.pg_type.typtype = 'e' -- Ensuring it's an enum type
                LEFT JOIN (
                    SELECT
                        enumtypid,
                        array_agg(enumlabel) AS enum_values
                    FROM pg_catalog.pg_enum
                    GROUP BY enumtypid
                ) enums ON enums.enumtypid = pg_catalog.pg_type.oid
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
