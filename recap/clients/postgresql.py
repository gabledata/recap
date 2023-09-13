from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from recap.clients.dbapi import Connection, DbapiClient
from recap.converters.postgresql import PostgresqlConverter

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
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection, PostgresqlConverter())

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
