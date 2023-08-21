import pytest

from recap.converters.mysql import MysqlConverter
from recap.types import BytesType, FloatType, IntType, StringType


@pytest.mark.parametrize(
    "column_props,expected",
    [
        (
            {
                "DATA_TYPE": "bigint",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=64, signed=True),
        ),
        (
            {
                "DATA_TYPE": "int",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=32, signed=True),
        ),
        (
            {
                "DATA_TYPE": "integer",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=32, signed=True),
        ),
        (
            {
                "DATA_TYPE": "mediumint",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=24, signed=True),
        ),
        (
            {
                "DATA_TYPE": "smallint",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=16, signed=True),
        ),
        (
            {
                "DATA_TYPE": "tinyint",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=8, signed=True),
        ),
        (
            {
                "DATA_TYPE": "double",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            FloatType(bits=64),
        ),
        (
            {
                "DATA_TYPE": "float",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 24,
                "NUMERIC_SCALE": None,
            },
            FloatType(bits=64),
        ),
        (
            {
                "DATA_TYPE": "float",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 23,
                "NUMERIC_SCALE": None,
            },
            FloatType(bits=32),
        ),
        (
            {
                "DATA_TYPE": "text",
                "CHARACTER_OCTET_LENGTH": 1000,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=1000, variable=True),
        ),
        (
            {
                "DATA_TYPE": "json",
                "CHARACTER_OCTET_LENGTH": 1000,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=1000, variable=True),
        ),
        (
            {
                "DATA_TYPE": "mediumtext",
                "CHARACTER_OCTET_LENGTH": 1000,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=1000, variable=True),
        ),
        (
            {
                "DATA_TYPE": "longtext",
                "CHARACTER_OCTET_LENGTH": 1000,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=1000, variable=True),
        ),
        (
            {
                "DATA_TYPE": "tinytext",
                "CHARACTER_OCTET_LENGTH": 1000,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=1000, variable=True),
        ),
        (
            {
                "DATA_TYPE": "varchar(100)",
                "CHARACTER_OCTET_LENGTH": 100,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=100, variable=True),
        ),
        (
            {
                "DATA_TYPE": "char(100)",
                "CHARACTER_OCTET_LENGTH": 100,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=100, variable=False),
        ),
        (
            {
                "DATA_TYPE": "enum",
                "CHARACTER_OCTET_LENGTH": 100,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=100, variable=False),
        ),
        (
            {
                "DATA_TYPE": "set",
                "CHARACTER_OCTET_LENGTH": 100,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            StringType(bytes_=100, variable=False),
        ),
        (
            {
                "DATA_TYPE": "blob",
                "CHARACTER_OCTET_LENGTH": 1000,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            BytesType(bytes_=1000, variable=True),
        ),
        (
            {
                "DATA_TYPE": "mediumblob",
                "CHARACTER_OCTET_LENGTH": 1000,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            BytesType(bytes_=1000, variable=True),
        ),
        (
            {
                "DATA_TYPE": "longblob",
                "CHARACTER_OCTET_LENGTH": 1000,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            BytesType(bytes_=1000, variable=True),
        ),
        (
            {
                "DATA_TYPE": "tinyblob",
                "CHARACTER_OCTET_LENGTH": 1000,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            BytesType(bytes_=1000, variable=True),
        ),
        (
            {
                "DATA_TYPE": "varbinary(100)",
                "CHARACTER_OCTET_LENGTH": 100,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            BytesType(bytes_=100, variable=True),
        ),
        (
            {
                "DATA_TYPE": "binary(100)",
                "CHARACTER_OCTET_LENGTH": 100,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            BytesType(bytes_=100, variable=False),
        ),
        (
            {
                "DATA_TYPE": "bit(8",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
            },
            BytesType(bytes_=8, variable=False),
        ),
        (
            {
                "DATA_TYPE": "timestamp",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
                "DATETIME_PRECISION": 0,
            },
            IntType(bits=64, logical="build.recap.Timestamp", unit="second"),
        ),
        (
            {
                "DATA_TYPE": "datetime",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
                "DATETIME_PRECISION": 0,
            },
            IntType(bits=64, logical="build.recap.Timestamp", unit="second"),
        ),
        (
            {
                "DATA_TYPE": "dec",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
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
                "DATA_TYPE": "decimal",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
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
                "DATA_TYPE": "numeric",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
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
                "DATA_TYPE": "year",
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            IntType(bits=16, signed=False),
        ),
    ],
)
def test_mysql_converter(column_props, expected):
    result = MysqlConverter()._parse_type(column_props)
    assert result == expected
