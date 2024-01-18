import pytest

from recap.converters.postgresql import MAX_FIELD_SIZE, PostgresqlConverter
from recap.types import (
    BoolType,
    BytesType,
    EnumType,
    FloatType,
    IntType,
    ListType,
    NullType,
    ProxyType,
    StringType,
    UnionType,
)


@pytest.mark.parametrize(
    "column_props,expected",
    [
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "bigint",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            IntType(bits=64, signed=True),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            IntType(bits=32, signed=True),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "smallint",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            IntType(bits=16, signed=True),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "double precision",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            FloatType(bits=64),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "real",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            FloatType(bits=32),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "boolean",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            BoolType(),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "text",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": 65536,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            StringType(bytes_=65536, variable=True),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "character varying",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": 255,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            StringType(bytes_=255, variable=True),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "char",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": 255,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            StringType(bytes_=255, variable=False),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "bytea",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            BytesType(bytes_=MAX_FIELD_SIZE),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "bit",
                "CHARACTER_MAXIMUM_LENGTH": 1,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            BytesType(bytes_=1, variable=False),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "bit",
                "CHARACTER_MAXIMUM_LENGTH": 17,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            BytesType(bytes_=3, variable=False),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "timestamp",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "DATETIME_PRECISION": 3,
                "ATTNDIMS": 0,
            },
            IntType(bits=64, logical="build.recap.Timestamp", unit="millisecond"),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "timestamp",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "UDT_NAME": None,
                "DATETIME_PRECISION": 3,
                "ATTNDIMS": 0,
            },
            IntType(bits=64, logical="build.recap.Timestamp", unit="millisecond"),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "decimal",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            BytesType(
                logical="build.recap.Decimal",
                bytes_=32,
                variable=False,
                precision=10,
                scale=2,
            ),
        ),
        (
            {
                "COLUMN_NAME": "test_column",
                "DATA_TYPE": "numeric",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 5,
                "NUMERIC_SCALE": 0,
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            BytesType(
                logical="build.recap.Decimal",
                bytes_=32,
                variable=False,
                precision=5,
                scale=0,
            ),
        ),
        (
            {
                "COLUMN_NAME": "test_enum",
                "DATA_TYPE": "USER-DEFINED",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "ENUM_VALUES": ["sad", "ok", "happy"],
                "UDT_NAME": None,
                "ATTNDIMS": 0,
            },
            EnumType(
                symbols=["sad", "ok", "happy"],
            ),
        ),
    ],
)
def test_postgresql_converter(column_props, expected):
    result = PostgresqlConverter()._parse_type(column_props)
    assert result == expected


def test_postgresql_converter_array_enforce_dimensions():
    converter = PostgresqlConverter(True)
    column_props = {
        "COLUMN_NAME": "test_column",
        "DATA_TYPE": "array",
        "CHARACTER_MAXIMUM_LENGTH": None,
        "CHARACTER_OCTET_LENGTH": None,
        "NUMERIC_PRECISION": 5,
        "NUMERIC_SCALE": 0,
        "UDT_NAME": "_int4",
        "ATTNDIMS": 1,
    }
    expected = ListType(
        values=UnionType(
            types=[
                NullType(),
                IntType(bits=32, signed=True),
            ],
        ),
    )
    result = converter._parse_type(column_props)
    assert result == expected


def test_postgresql_converter_array_no_enforce_dimensions():
    converter = PostgresqlConverter(False)
    column_props = {
        "COLUMN_NAME": "test_column",
        "DATA_TYPE": "array",
        "CHARACTER_MAXIMUM_LENGTH": None,
        "CHARACTER_OCTET_LENGTH": None,
        "NUMERIC_PRECISION": 5,
        "NUMERIC_SCALE": 0,
        "UDT_NAME": "_int4",
        "ATTNDIMS": 1,
    }
    expected = ListType(
        alias="_root.test_column",
        values=UnionType(
            types=[
                IntType(bits=32, signed=True),
                ProxyType(
                    alias="_root.test_column",
                    registry=converter.registry,
                ),
            ],
        ),
    )
    result = converter._parse_type(column_props)
    assert result == expected
