import sqlite3
from typing import Generator

import pytest

from recap.clients.sqlite import SQLiteClient
from recap.types import (
    BytesType,
    FloatType,
    IntType,
    NullType,
    StringType,
    StructType,
    UnionType,
)


@pytest.fixture
def sqlite_client() -> Generator[SQLiteClient, None, None]:
    with sqlite3.connect(":memory:") as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS test_affinities (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                short_name CHAR(20) NOT NULL,
                variable_name VARCHAR(255) NOT NULL,
                descriptive_text CLOB NOT NULL,
                balance REAL,
                small_balance REAL(5, 2),
                small_dollars FLOAT(5),
                significant_figures DOUBLE,
                data BLOB,
                small_data BLOB(255),
                custom_type FOO,
                date_col DATE NOT NULL,
                datetime_col DATETIME NOT NULL,
                time_col TIME NOT NULL,
                timestamp_col TIMESTAMP NOT NULL
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS test_strict (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INT NOT NULL,
                balance REAL NOT NULL,
                data BLOB NOT NULL,
                anything ANY,
                nullable_int INT
            ) STRICT;
            """
        )
        connection.commit()
        yield SQLiteClient(connection)  # type: ignore


def test_schema_affinities(sqlite_client: SQLiteClient):
    schema = sqlite_client.schema("test_affinities")

    expected_schema = StructType(
        fields=[
            IntType(bits=64, signed=True, name="id"),
            StringType(bytes_=2147483647, name="name", variable=True),
            StringType(bytes_=20, name="short_name", variable=True),
            StringType(bytes_=255, name="variable_name", variable=True),
            StringType(bytes_=2147483647, name="descriptive_text", variable=True),
            UnionType(
                types=[NullType(), FloatType(bits=64)],
                name="balance",
                default=None,
            ),
            UnionType(
                types=[NullType(), FloatType(bits=32)],
                name="small_balance",
                default=None,
            ),
            UnionType(
                types=[NullType(), FloatType(bits=32)],
                name="small_dollars",
                default=None,
            ),
            UnionType(
                types=[NullType(), FloatType(bits=64)],
                name="significant_figures",
                default=None,
            ),
            UnionType(
                types=[NullType(), BytesType(bytes_=2147483647)],
                name="data",
                default=None,
            ),
            UnionType(
                types=[NullType(), BytesType(bytes_=255)],
                name="small_data",
                default=None,
            ),
            UnionType(
                default=None,
                name="custom_type",
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ],
            ),
            UnionType(
                name="date_col",
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ],
            ),
            UnionType(
                name="datetime_col",
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ],
            ),
            UnionType(
                name="time_col",
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ],
            ),
            UnionType(
                name="timestamp_col",
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ],
            ),
        ]
    )

    assert len(schema.fields) == len(expected_schema.fields)
    for field, expected_field in zip(schema.fields, expected_schema.fields):
        assert field == expected_field


def test_schemas_strict(sqlite_client: SQLiteClient):
    schema = sqlite_client.schema("test_strict")

    expected_schema = StructType(
        fields=[
            IntType(bits=64, signed=True, name="id"),
            StringType(bytes_=2147483647, name="name", variable=True),
            IntType(bits=64, signed=True, name="age"),
            FloatType(bits=64, name="balance"),
            BytesType(bytes_=2147483647, name="data"),
            UnionType(
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ],
                name="anything",
                default=None,
            ),
            UnionType(
                types=[NullType(), IntType(bits=64)],
                name="nullable_int",
                default=None,
            ),
        ]
    )

    assert len(schema.fields) == len(expected_schema.fields)
    for field, expected_field in zip(schema.fields, expected_schema.fields):
        assert field == expected_field


def test_ls(sqlite_client: SQLiteClient):
    assert set(sqlite_client.ls()) == {"test_affinities", "test_strict"}
