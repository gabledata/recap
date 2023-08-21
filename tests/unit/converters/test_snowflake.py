import pytest

from recap.converters.snowflake import SnowflakeConverter
from recap.types import BoolType, BytesType, FloatType, IntType, StringType


@pytest.mark.parametrize(
    "column_props,expected_type",
    [
        # float types
        ({"DATA_TYPE": "float", "CHARACTER_OCTET_LENGTH": None}, FloatType(bits=64)),
        ({"DATA_TYPE": "float4", "CHARACTER_OCTET_LENGTH": None}, FloatType(bits=64)),
        ({"DATA_TYPE": "float8", "CHARACTER_OCTET_LENGTH": None}, FloatType(bits=64)),
        ({"DATA_TYPE": "double", "CHARACTER_OCTET_LENGTH": None}, FloatType(bits=64)),
        (
            {"DATA_TYPE": "double precision", "CHARACTER_OCTET_LENGTH": None},
            FloatType(bits=64),
        ),
        ({"DATA_TYPE": "real", "CHARACTER_OCTET_LENGTH": None}, FloatType(bits=64)),
        ({"DATA_TYPE": "boolean", "CHARACTER_OCTET_LENGTH": None}, BoolType()),
        (
            {
                "DATA_TYPE": "number",
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
                "CHARACTER_OCTET_LENGTH": None,
            },
            BytesType(
                logical="build.recap.Decimal",
                bytes_=16,
                variable=False,
                precision=10,
                scale=2,
            ),
        ),
        (
            {
                "DATA_TYPE": "decimal",
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
                "CHARACTER_OCTET_LENGTH": None,
            },
            BytesType(
                logical="build.recap.Decimal",
                bytes_=16,
                variable=False,
                precision=10,
                scale=2,
            ),
        ),
        (
            {
                "DATA_TYPE": "numeric",
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 2,
                "CHARACTER_OCTET_LENGTH": None,
            },
            BytesType(
                logical="build.recap.Decimal",
                bytes_=16,
                variable=False,
                precision=10,
                scale=2,
            ),
        ),
        (
            {"DATA_TYPE": "varchar", "CHARACTER_OCTET_LENGTH": 5},
            StringType(bytes_=5, variable=True),
        ),
        (
            {"DATA_TYPE": "nvarchar", "CHARACTER_OCTET_LENGTH": 5},
            StringType(bytes_=5, variable=True),
        ),
        (
            {"DATA_TYPE": "nvarchar2", "CHARACTER_OCTET_LENGTH": 5},
            StringType(bytes_=5, variable=True),
        ),
        (
            {"DATA_TYPE": "char varying", "CHARACTER_OCTET_LENGTH": 5},
            StringType(bytes_=5, variable=True),
        ),
        (
            {"DATA_TYPE": "nchar varying", "CHARACTER_OCTET_LENGTH": 5},
            StringType(bytes_=5, variable=True),
        ),
        (
            {"DATA_TYPE": "char", "CHARACTER_OCTET_LENGTH": 5},
            StringType(bytes_=5, variable=True),
        ),
        (
            {"DATA_TYPE": "nchar", "CHARACTER_OCTET_LENGTH": 5},
            StringType(bytes_=5, variable=True),
        ),
        (
            {"DATA_TYPE": "character", "CHARACTER_OCTET_LENGTH": 5},
            StringType(bytes_=5, variable=True),
        ),
        ({"DATA_TYPE": "binary", "CHARACTER_OCTET_LENGTH": 5}, BytesType(bytes_=5)),
        ({"DATA_TYPE": "varbinary", "CHARACTER_OCTET_LENGTH": 5}, BytesType(bytes_=5)),
        ({"DATA_TYPE": "blob", "CHARACTER_OCTET_LENGTH": 5}, BytesType(bytes_=5)),
        (
            {"DATA_TYPE": "date", "CHARACTER_OCTET_LENGTH": None},
            IntType(bits=32, logical="build.recap.Date", unit="day"),
        ),
        (
            {"DATA_TYPE": "timestamp(3)", "CHARACTER_OCTET_LENGTH": None},
            IntType(bits=64, logical="build.recap.Timestamp", unit="millisecond"),
        ),
        (
            {"DATA_TYPE": "timestamp", "CHARACTER_OCTET_LENGTH": None},
            IntType(bits=64, logical="build.recap.Timestamp", unit="nanosecond"),
        ),
        (
            {"DATA_TYPE": "time", "CHARACTER_OCTET_LENGTH": None},
            IntType(bits=32, logical="build.recap.Time", unit="nanosecond"),
        ),
    ],
)
def test_snowflake_converter(column_props, expected_type):
    assert SnowflakeConverter()._parse_type(column_props) == expected_type
