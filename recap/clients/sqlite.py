from __future__ import annotations

from contextlib import contextmanager
from re import compile as re_compile
from typing import Any, Generator

from recap.clients.dbapi import Connection
from recap.converters.sqlite import SQLiteAffinity, SQLiteConverter
from recap.types import StructType

SQLITE3_CONNECT_ARGS = {
    "database",
    "timeout",
    "detect_types",
    "isolation_level",
    "check_same_thread",
    "factory",
    "cached_statements",
    "uri",
}


class SQLiteClient:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.converter = SQLiteConverter()

    @staticmethod
    @contextmanager
    def create(url: str, **url_args) -> Generator[SQLiteClient, None, None]:
        import sqlite3

        # Strip sqlite:/// URL prefix
        url_args["database"] = url[len("sqlite:///") :]

        # Only include kwargs that are valid for PsycoPG2 parse_dsn()
        url_args = {k: v for k, v in url_args.items() if k in SQLITE3_CONNECT_ARGS}

        with sqlite3.connect(**url_args) as client:
            yield SQLiteClient(client)  # type: ignore

    @staticmethod
    def parse(method: str, **url_args) -> tuple[str, list[Any]]:
        from urllib.parse import urlunparse

        match method:
            case "ls":
                return (url_args["url"], [])
            case "schema":
                table = url_args["paths"].pop(-1)
                connection_url = urlunparse(
                    [
                        url_args.get("dialect") or url_args.get("scheme"),
                        url_args.get("netloc"),
                        # Include / prefix for paths
                        "/".join(url_args.get("paths", [])),
                        url_args.get("params"),
                        url_args.get("query"),
                        url_args.get("fragment"),
                    ]
                )

                # urlunsplit does not double slashes if netloc is empty. But most
                # URLs with empty netloc should have a double slash (e.g.
                # bigquery:// or sqlite:///some/file.db). Include an extra "/"
                # because the root path is not included with an empty netloc
                # and join().
                if not url_args.get("netloc"):
                    connection_url = connection_url.replace(":", ":///", 1)

                return (connection_url, [table])
            case _:
                raise ValueError("Invalid method")

    def ls(self) -> list[str]:
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]

    def schema(self, table: str) -> StructType:
        cursor = self.connection.cursor()

        # Validate that table exists since we want to prevent SQL injections in
        # the PRAGMA call
        if not self._table_exists(table):
            raise ValueError(f"Table '{table}' does not exist in the database.")

        cursor.execute(f"PRAGMA table_info({table});")
        names = [name[0].upper() for name in cursor.description]
        rows = []

        for row_cells in cursor.fetchall():
            row = dict(zip(names, row_cells))
            row = self.add_information_schema(row)
            rows.append(row)

        return self.converter.to_recap(rows)

    def add_information_schema(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        SQLite does not have an INFORMATION_SCHEMA, so we need to add these
        columns.

        :param row: A row from the PRAGMA table_info() query.
        :return: The row with the INFORMATION_SCHEMA columns added.
        """

        is_not_null = row["NOTNULL"] or row["PK"]

        # Set defaults.
        information_schema_cols = {
            "COLUMN_NAME": row["NAME"],
            "IS_NULLABLE": "NO" if is_not_null else "YES",
            "COLUMN_DEFAULT": row["DFLT_VALUE"],
            "NUMERIC_PRECISION": None,
            "NUMERIC_SCALE": None,
            "CHARACTER_OCTET_LENGTH": None,
        }

        # Extract precision, scale, and octet length.
        # This regex matches the following patterns:
        # - <type>(<param1>(, <param2>)?)
        # param1 can be a precision or octet length, and param2 can be a scale.
        numeric_pattern = re_compile(r"(\w+)\((\d+)(?:,\s*(\d+))?\)")
        param_match = numeric_pattern.search(row["TYPE"])

        if param_match:
            # Extract matched values
            base_type, precision, scale = param_match.groups()
            base_type = base_type.upper()
            precision = int(precision)
            scale = int(scale) if scale else 0

            match SQLiteConverter.get_affinity(base_type):
                case SQLiteAffinity.INTEGER | SQLiteAffinity.REAL | SQLiteAffinity.NUMERIC:
                    information_schema_cols["NUMERIC_PRECISION"] = precision
                    information_schema_cols["NUMERIC_SCALE"] = scale
                case SQLiteAffinity.TEXT | SQLiteAffinity.BLOB:
                    information_schema_cols["CHARACTER_OCTET_LENGTH"] = precision

        return row | information_schema_cols

    def _table_exists(self, table: str) -> bool:
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        return bool(cursor.fetchone())
