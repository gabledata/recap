import pytest

from recap.converters.sqlite import SQLiteConverter
from recap.types import BytesType, FloatType, IntType, NullType, StringType, UnionType


# Test cases for SQLiteConverter
@pytest.mark.parametrize(
    "column_props,expected",
    [
        # INTEGER affinity tests
        (
            {
                "COLUMN_NAME": "id",
                "TYPE": "integer",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=64),
        ),
        (
            {
                "COLUMN_NAME": "big_id",
                "TYPE": "bigint",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=64),
        ),
        (
            {
                "COLUMN_NAME": "unsigned_big_id",
                "TYPE": "unsigned bigint",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=64),
        ),
        # REAL affinity tests
        (
            {
                "COLUMN_NAME": "real_col",
                "TYPE": "real",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 22,
                "NUMERIC_SCALE": None,
            },
            FloatType(bits=32),
        ),
        (
            {
                "COLUMN_NAME": "float_col",
                "TYPE": "float",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 53,
                "NUMERIC_SCALE": None,
            },
            FloatType(bits=64),
        ),
        # TEXT affinity tests
        (
            {
                "COLUMN_NAME": "text_col",
                "TYPE": "text",
                "CHARACTER_OCTET_LENGTH": 255,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=255),
        ),
        (
            {
                "COLUMN_NAME": "varchar_col",
                "TYPE": "varchar(100)",
                "CHARACTER_OCTET_LENGTH": 100,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=100),
        ),
        # BLOB affinity tests
        (
            {
                "COLUMN_NAME": "blob_col",
                "TYPE": "blob",
                "CHARACTER_OCTET_LENGTH": 500,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            BytesType(bytes_=500),
        ),
        # NUMERIC affinity tests
        (
            {
                "COLUMN_NAME": "numeric_col",
                "TYPE": "numeric",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 5,
            },
            UnionType(
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ]
            ),
        ),
        (
            {
                "COLUMN_NAME": "decimal_col",
                "TYPE": "decimal(10,5)",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 5,
            },
            UnionType(
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ]
            ),
        ),
        # Edge case tests
        (
            {
                "COLUMN_NAME": "custom_col",
                "TYPE": "customtype",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            UnionType(
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ]
            ),
        ),
        (
            {
                "COLUMN_NAME": "no_type_col",
                "TYPE": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            BytesType(bytes_=2147483647),
        ),
        # Date and time
        (
            {
                "COLUMN_NAME": "date_col",
                "TYPE": "DATE",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            UnionType(
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ]
            ),
        ),
        (
            {
                "COLUMN_NAME": "datetime_col",
                "TYPE": "DATETIME",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            UnionType(
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ]
            ),
        ),
        (
            {
                "COLUMN_NAME": "timestamp_col",
                "TYPE": "TIMESTAMP",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            UnionType(
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ]
            ),
        ),
        (
            {
                "COLUMN_NAME": "time_col",
                "TYPE": "TIME",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            UnionType(
                types=[
                    NullType(),
                    IntType(bits=64),
                    FloatType(bits=64),
                    StringType(bytes_=2147483647),
                    BytesType(bytes_=2147483647),
                ]
            ),
        ),
    ],
)
def test_sqlite_converter(column_props, expected):
    result = SQLiteConverter()._parse_type(column_props)
    assert result == expected
